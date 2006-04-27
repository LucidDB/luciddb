/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2006-2006 The Eigenbase Project
// Copyright (C) 2006-2006 Disruptive Tech
// Copyright (C) 2006-2006 LucidEra, Inc.
//
// This program is free software; you can redistribute it and/or modify it
// under the terms of the GNU General Public License as published by the Free
// Software Foundation; either version 2 of the License, or (at your option)
// any later version approved by The Eigenbase Project.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program; if not, write to the Free Software
// Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
*/
package org.eigenbase.rel.metadata;

import org.eigenbase.rel.*;
import org.eigenbase.rel.rules.*;
import org.eigenbase.relopt.*;
import org.eigenbase.rex.*;
import org.eigenbase.sql.*;
import org.eigenbase.sql.fun.*;
import org.eigenbase.sql.type.*;

import java.math.*;
import java.util.*;

/**
 * RelMdUtil provides utility methods used by the metadata provider methods.
 *
 * @author Zelaine Fong
 * @version $Id$
 */
public class RelMdUtil
{
    public static final SqlFunction artificialSelectivityFunc =
        new SqlFunction(
        "ARTIFICIAL_SELECTIVITY",
        SqlKind.Function,
        SqlTypeStrategies.rtiBoolean,  // returns boolean since we'll AND it
        null,
        SqlTypeStrategies.otcNumeric,  // takes a numeric param
        SqlFunctionCategory.System);

    /**
     * Creates a RexNode that stores a selectivity value corresponding to the
     * selectivity of a semijoin.  This can be added to a filter to simulate
     * the effect of the semijoin during costing, but should never appear in a
     * real plan since it has no physical implementation.
     *
     * @param rel the semijoin of interest
     *
     * @return constructed rexnode
     */
    public static RexNode makeSemiJoinSelectivityRexNode(SemiJoinRel rel)
    {
        BitSet rightKey = new BitSet();
        for (int dimCol : rel.getRightKeys()) {
            rightKey.set(dimCol);
        }
        RexBuilder rexBuilder = rel.getCluster().getRexBuilder();
        double selectivity = computeSemiJoinSelectivity(
            rel.getRight(), rightKey);
        RexNode selec = rexBuilder.makeApproxLiteral(
            new BigDecimal(selectivity));
        return rexBuilder.makeCall(artificialSelectivityFunc, selec);
    }
    
    /**
     * Returns the selectivity value stored in the rexnode
     * 
     * @param artificialSelecFuncNode rexnode containing the selectivity value
     * @return selectivity value
     */
    public static double getSelectivityValue(RexNode artificialSelecFuncNode)
    {
        assert(artificialSelecFuncNode instanceof RexCall);
        RexCall call = (RexCall) artificialSelecFuncNode;
        assert(call.getOperator() == artificialSelectivityFunc);
        RexNode operand = call.getOperands()[0];
        BigDecimal bd = (BigDecimal) ((RexLiteral) operand).getValue();
        return bd.doubleValue();
    }
    
    /**
     * Computes the selectivity of a semijoin filter if it is applied on a
     * fact table.  The computation is based on the selectivity of the 
     * dimension table/columns.
     * 
     * @param dimRel relational expression representing the dimension table
     * @param dimCols bitmap representing the dimension columns
     * @return calculated selectivity
     */
    public static double computeSemiJoinSelectivity(
        RelNode dimRel, BitSet dimCols)
    {
        Double dimCard = RelMetadataQuery.getDistinctRowCount(
            dimRel, dimCols, null);
        Double dimPop = RelMetadataQuery.getPopulationSize(dimRel, dimCols);
        
        // if cardinality and population are available, use them; otherwise
        // use percentage original rows
        Double selectivity;
        if (dimCard != null && dimPop != null) {
            // to avoid division by zero
            if (dimPop < 1.0) {
                dimPop = 1.0;
            }
            selectivity = dimCard / dimPop;
        } else {
            selectivity = RelMetadataQuery.getPercentageOriginalRows(dimRel);
        }
        
        if (selectivity == null) {
            // set a default selectivity based on the number of semijoin keys
            selectivity = Math.pow(0.1, dimCols.cardinality());
        } else if (selectivity > 1.0) {
            selectivity = 1.0;
        }
        return selectivity;
    }
    
    /**
     * Returns true if the columns represented in a bit mask form a unique
     * column set
     * 
     * @param rel the relnode that the column mask correponds to
     * @param colMask bit mask containing columns that will be determined
     * if they are unique
     * @return true if bit mask represents a unique column set, or null if
     * no information on unique keys
     */
    public static Boolean areColumnsUnique(RelNode rel, BitSet colMask)
    {
        Set<BitSet> uniqueColSets = RelMetadataQuery.getUniqueKeys(rel);
        if (uniqueColSets == null) {
            return null;
        }
        Iterator it = uniqueColSets.iterator();
        while (it.hasNext()) {
            BitSet colSet = (BitSet) it.next();
            if (RelOptUtil.contains(colMask, colSet)) {
                return true;
            }
        }
        return false;
    }
    
    /**
     * Separates a bitmask representing a join into masks representing the
     * left and right inputs into the join
     * 
     * @param groupKey original bitmask
     * @param leftMask left bitmask to be set
     * @param rightMask right bitmask to be set
     * @param nFieldsOnLeft number of fields in the left input
     */
    public static void setLeftRightBitmaps(
        BitSet groupKey, BitSet leftMask, BitSet rightMask, int nFieldsOnLeft)
    {
        for (int bit = groupKey.nextSetBit(0); bit >= 0;
            bit = groupKey.nextSetBit(bit + 1))
        {
            if (bit < nFieldsOnLeft) {
                leftMask.set(bit);
            } else {
                rightMask.set(bit - nFieldsOnLeft);
            }
        }
    }
    
    /**
     * Returns the number of distinct values provided numSelected are
     * selected where there are domainSize distinct values
     * 
     * @param domainSize number of distinct values in the domain
     * @param numSelected number selected from the domain
     * @return number of distinct values for subset selected
     */
    public static Double numDistinctVals(
        Double domainSize, Double numSelected)
    {
        if (domainSize == null || numSelected == null) {
            return null;
        }
        
        // The formula for this is:
        // 1. Assume we pick 80 random values between 1 and 100.
        // 2. The chance we skip any given value is .99 ^ 80
        // 3. Thus on average we will skip .99 ^ 80 percent of the values
        //    in the domain
        // 4. generalized, we skip ( (n-k)/n ) ^ n values where n is the
        //    number of possible values and k is the number we are selecting
        // 5. Solving this we convert it to e ^ log( ( n-k)/n ) and after
        //    a lot of math we get the formula below.
        double res =  domainSize > 0 ?
            (1.0 - Math.exp(-1 * numSelected / domainSize)) * domainSize :
             0 ;

        // fix the boundary cases
        if (res > domainSize)
            res = domainSize;

        if (res > numSelected)
            res = numSelected;

        if (res < 0)
            res = 0;

        return res;
    }
    
    /**
     * Return default estimates for selectivities, in the absence of stats
     * 
     * @param predicate predicate for which selectivity will be computed
     * @return estimated selectivity
     */
    public static double guessSelectivity(RexNode predicate)
    {
        return guessSelectivity(predicate, false);
    }
    
    /**
     * Return default estimates for selectivities, in the absence of stats
     * 
     * @param predicate predicate for which selectivity will be computed
     * @param artificialOnly return only the selectivity contribution
     * from artificial nodes
     * @return estimated selectivity
     */
    public static double guessSelectivity(
        RexNode predicate, boolean artificialOnly)
    {
        double sel = 1.0;
        if (predicate == null || predicate.isAlwaysTrue()) {
            return sel;
        }
        
        double artificialSel = 1.0;
        
        List<RexNode> predList = new ArrayList<RexNode>();
        RelOptUtil.decompCF(predicate, predList);
        
        for (RexNode pred : predList) {
            if (pred instanceof RexCall &&
                ((RexCall) pred).getOperator() ==
                    SqlStdOperatorTable.isNotNullOperator)
            {
                sel *= .9;
            } else if (pred instanceof RexCall &&
                ((RexCall) pred).getOperator() ==
                    RelMdUtil.artificialSelectivityFunc)
            {
                artificialSel *= RelMdUtil.getSelectivityValue(pred);
            } else if (pred.isA(RexKind.Equals)) {
                sel *= .15;
            } else if (pred.isA(RexKind.Comparison)) {
                sel *= .5;
            } else {
                sel *= .25;
            }
        }

        if (artificialOnly) {
            return artificialSel;
        } else {
            return sel * artificialSel;
        }
    }
        
    /**
     * Locates the columns corresponding to equijoins within a joinrel.
     * 
     * @param rel the join rel
     * @param predicate join predicate
     * @param leftJoinCols bitmap that will be set with the columns on the
     * LHS of the join that participate in equijoins
     * @param rightJoinCols bitmap that will be set with the columns on the
     * RHS of the join that participate in equijoins
     * @return remaining join filters that are not equijoins
     */
    public static RexNode findEquiJoinCols(
        JoinRelBase rel, RexNode predicate,
        BitSet leftJoinCols, BitSet rightJoinCols)
    {
        // locate the equijoin conditions
        List<Integer> leftKeys = new ArrayList<Integer>();
        List<Integer> rightKeys = new ArrayList<Integer>();
        RexNode nonEquiJoin = RelOptUtil.splitJoinCondition(
            rel.getLeft(), rel.getRight(), predicate, leftKeys,
            rightKeys);
        
        // mark the columns referenced on each side of the equijoin filters
        for (int i = 0; i < leftKeys.size(); i++) {
            leftJoinCols.set(leftKeys.get(i));
            rightJoinCols.set(rightKeys.get(i));
        }
        
        return nonEquiJoin;
    }
    
    /**
     * AND's two predicates together, either of which may be null, removing
     * redundant filters.
     * 
     * @param rexBuilder rexBuilder used to construct AND'd RexNode
     * @param pred1 first predicate
     * @param pred2 second predicate
     * @return AND'd predicate or individual predicates if one is null
     */
    public static RexNode unionPreds(
        RexBuilder rexBuilder, RexNode pred1, RexNode pred2)
    {
        List<RexNode> list1 = new ArrayList<RexNode>();
        List<RexNode> list2 = new ArrayList<RexNode>();
        List<RexNode> unionList = new ArrayList<RexNode>();
        RelOptUtil.decompCF(pred1, list1);
        RelOptUtil.decompCF(pred2, list2);
        
        for (RexNode rex : list1) {
            unionList.add(rex);
        }
        for (RexNode rex2 : list2) {
            boolean add = true;
            for (RexNode rex1 : list1) {
                if (rex2.toString().compareTo(rex1.toString()) == 0) {
                    add = false;
                    break;
                }
                if (!add) {
                    break;
                }
            }
            if (add) {
                unionList.add(rex2);
            }
        }
        
        return RexUtil.andRexNodeList(rexBuilder, unionList);
    } 
}

// End RelMdUtil.java
