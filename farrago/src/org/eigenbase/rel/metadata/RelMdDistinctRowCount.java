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

import org.eigenbase.util14.*;
import org.eigenbase.rel.*;
import org.eigenbase.rel.rules.*;
import org.eigenbase.relopt.*;
import org.eigenbase.rex.*;
import org.eigenbase.sql.fun.*;

import java.util.*;

/**
 * RelMdDistinctRowCount supplies a default implementation of
 * {@link RelMetadataQuery#getDistinctRowCount} for the standard logical
 * algebra.
 *
 * @author Zelaine Fong
 * @version $Id$
 */
public class RelMdDistinctRowCount extends ReflectiveRelMetadataProvider
{
    public RelMdDistinctRowCount()
    {
        // Tell superclass reflection about parameter types expected
        // for various metadata queries.

        // This corresponds to getDistinctRowCount(rel, RexNode predicate);
        // note that we don't specify the rel type because we always overload
        // on that.
        List<Class> args = new ArrayList<Class>();
        args.add((Class) BitSet.class);
        args.add((Class) RexNode.class);
        mapParameterTypes("getDistinctRowCount", args);
    }
    
    public Double getDistinctRowCount(
        UnionRelBase rel, BitSet groupKey, RexNode predicate)
    {
        Double rowCount = 0.0;
        for (RelNode input : rel.getInputs()) {
            Double partialRowCount = RelMetadataQuery.getDistinctRowCount(
                input, groupKey, predicate);
            if (partialRowCount == null) {
                return null;
            }
            rowCount += partialRowCount;                
        }
        return rowCount;
    }
    
    public Double getDistinctRowCount(
        SortRel rel, BitSet groupKey, RexNode predicate)
    {
        return RelMetadataQuery.getDistinctRowCount(
            rel.getChild(), groupKey, predicate);
    }
    
    public Double getDistinctRowCount(
        FilterRelBase rel, BitSet groupKey, RexNode predicate)
    {
        // REVIEW zfong 4/18/06 - In the Broadbase code, duplicates are not
        // removed from the two filter lists.  However, the code below is
        // doing so.
        RexNode unionPreds = RelMdUtil.unionPreds(
            rel.getCluster().getRexBuilder(), predicate, rel.getCondition());
        
        return RelMetadataQuery.getDistinctRowCount(
            rel.getChild(), groupKey, unionPreds);
    }
    
    public Double getDistinctRowCount(
        JoinRelBase rel, BitSet groupKey, RexNode predicate)
    {
        Double distRowCount; 
        BitSet leftMask = new BitSet();
        BitSet rightMask = new BitSet();
        
        RelMdUtil.setLeftRightBitmaps(
            groupKey, leftMask, rightMask,
            rel.getLeft().getRowType().getFieldCount());
           
        // determine which filters apply to the left vs right
        RexNode leftPred = null;
        RexNode rightPred = null;
        if (predicate != null) {
            List<RexNode> leftFilters = new ArrayList<RexNode>();
            List<RexNode> rightFilters = new ArrayList<RexNode>();
            List<RexNode> joinFilters = new ArrayList<RexNode>();
            List<RexNode> predList = new ArrayList<RexNode>();
            RelOptUtil.decompCF(predicate, predList);
        
            RelOptUtil.classifyFilters(
                rel, predList, (rel.getJoinType() == JoinRelType.INNER), 
                !rel.getJoinType().generatesNullsOnLeft(),
                !rel.getJoinType().generatesNullsOnRight(),
                joinFilters, leftFilters, rightFilters);
            
            RexBuilder rexBuilder = rel.getCluster().getRexBuilder();
            leftPred = RexUtil.andRexNodeList(rexBuilder, leftFilters);
            rightPred = RexUtil.andRexNodeList(rexBuilder, rightFilters);
        }
              
        distRowCount = NumberUtil.multiply(
            RelMetadataQuery.getDistinctRowCount(
                rel.getLeft(), leftMask, leftPred),
            RelMetadataQuery.getDistinctRowCount(
                rel.getRight(), rightMask, rightPred));
        
        return RelMdUtil.numDistinctVals(
            distRowCount, RelMetadataQuery.getRowCount(rel));           
    }
    
    public Double getDistinctRowCount(
        SemiJoinRel rel, BitSet groupKey, RexNode predicate)
    {
        // create a RexNode representing the selectivity of the
        // semijoin filter and pass it to getDistinctRowCount          
        RexNode newPred = RelMdUtil.makeSemiJoinSelectivityRexNode(rel);
        if (predicate != null) {
            RexBuilder rexBuilder = rel.getCluster().getRexBuilder();
            newPred = rexBuilder.makeCall(
                SqlStdOperatorTable.andOperator, newPred, predicate);
        }
        
        return RelMetadataQuery.getDistinctRowCount(
            rel.getLeft(), groupKey, newPred);
    }
    
    // Catch-all rule when none of the others apply.  Have not implemented
    // rules for aggregation and projection.
    public Double getDistinctRowCount(
        RelNode rel, BitSet groupKey, RexNode predicate)
    {
        // REVIEW zfong 4/19/06 - Broadbase code does not take into
        // consideration selectivity of predicates passed in.  Also, they
        // assume the rows are unique even if the table is not 
        Boolean uniq = RelMdUtil.areColumnsUnique(rel, groupKey);
        if (uniq != null && uniq) {
            return NumberUtil.multiply(
                RelMetadataQuery.getRowCount(rel),
                RelMetadataQuery.getSelectivity(rel, predicate));
        }
        return null;
    }  
}

// End RelMdDistinctRowCount.java
