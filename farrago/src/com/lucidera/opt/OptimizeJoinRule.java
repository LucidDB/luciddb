/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2005 LucidEra, Inc.
// Copyright (C) 2005-2005 The Eigenbase Project
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
package com.lucidera.opt;

import java.util.*;

import com.lucidera.lcs.*;

import net.sf.farrago.fem.med.*;

import org.eigenbase.rel.*;
import org.eigenbase.rel.metadata.*;
import org.eigenbase.rel.rules.*;
import org.eigenbase.relopt.*;
import org.eigenbase.reltype.*;
import org.eigenbase.rex.*;
import org.eigenbase.sql.*;
import org.eigenbase.sql.fun.*;

/**
 * OptimizeJoinRule implements the heuristic planner for determining optimal
 * join orderings.  It is triggered by the pattern ProjectRel(MultiJoinRel).
 * 
 * @author Zelaine Fong
 * @version $Id$
 */
public class OptimizeJoinRule extends RelOptRule
{
    // minimum score required for a join filter to be considered
    private static int thresholdScore = 10;
    
    //~ Instance fields --------------------------------------------------------
    
    /**
     * The MultiJoinRel being optimized
     */
    MultiJoinRel multiJoin;
    
    /**
     * Join filters associated with the MultiJoinRel, decomposed into
     * a list
     */
    private List<RexNode> joinFilters;
    
    /**
     * Number of factors into the MultiJoinRel
     */
    private int nJoinFactors;
    
    /**
     * Total number of fields in the MultiJoinRel
     */
    private int nTotalFields;
    
    /**
     * Original inputs into the MultiJoinRel
     */
    private RelNode[] joinFactors;
    
    /**
     * Semijoins corresponding to each join factor, if they are going to be
     * filtered by semijoins.  Otherwise, the entry is the original
     * join factor.
     */
    private RelNode[] chosenSemiJoins;
    
    /**
     * Maps a chosenSemiJoins entry to its corresponding factor index
     */
    private Map<RelNode, Integer> chosenSemiJoinsMap;

    /**
     * For each join filter, associates a bitmap indicating all factors
     * referenced by the filter
     */
    private Map<RexNode, BitSet> factorsRefByJoinFilter;
    
    /**
     * For each join filter, associates a bitmap indicating all fields
     * referenced by the filter
     */
    private Map<RexNode, BitSet> fieldsRefByJoinFilter;
    
    /**
     * Starting RexInputRef index corresponding to each join factor
     */
    int joinStart[];
    
    /**
     * Number of fields in each join factor
     */
    int nFieldsInJoinFactor[];

    /**
     * RexBuilder for constructing new RexNodes
     */
    private RexBuilder rexBuilder;
    
    /**
     * Associates potential semijoins with each fact table factor. The first
     * parameter in the map corresponds to the fact table.  The second
     * corresponds to the dimension table and a SemiJoinRel that captures
     * all the necessary semijoin data between that fact and dimension table
     */
    private Map<Integer, Map<Integer, SemiJoinRel>> possibleSemiJoins;
    
    /**
     * Weights of each factor combination
     */
    int[][] factorWeights;
    
    /**
     * Bitmap indicating which factors each factor references in join filters
     * that correspond to comparisons
     */
    BitSet[] factorsRefByFactor;

    private final Comparator factorCostComparator = new FactorCostComparator();
    
    //~ Constructors -----------------------------------------------------------

    public OptimizeJoinRule()
    {
        super(new RelOptRuleOperand(
                ProjectRel.class,
                new RelOptRuleOperand [] {
                    new RelOptRuleOperand(MultiJoinRel.class, null)
                }));
    }

    //~ Methods ----------------------------------------------------------------

    // implement RelOptRule
    public void onMatch(RelOptRuleCall call)
    {
        initMemberFields(call);
        
        // determine which join factors each join filter references
        setJoinFilterRefs(multiJoin.getRowType().getFields().length);
        
        // determine all possible semijoins
        makePossibleSemiJoins();
        
        // select the optimal join filters for semijoin filtering by
        // iteratively calling chooseBestSemiJoin; chooseBestSemiJoin will
        // apply semijoins in sort order, based on the cost of scanning each
        // factor; as it selects semijoins to apply and iterates through the
        // loop, the cost of scanning an factor will decrease in accordance
        // with the semijoins selected
        int iterations = 0;
        do {
            if (!chooseBestSemiJoin()) {
                break;
            }
            if (iterations++ > 10) {
                break;
            }
        } while (true);
        
        setFactorWeights();

        findBestOrderings(call);
    }
    
    private void initMemberFields(RelOptRuleCall call)
    {
        multiJoin = (MultiJoinRel) call.rels[1];
        joinFactors = multiJoin.getInputs();
        nJoinFactors = joinFactors.length;
        
        joinFilters = new ArrayList<RexNode>();
        RelOptUtil.decompCF(multiJoin.getJoinFilter(), joinFilters);
        
        rexBuilder = multiJoin.getCluster().getRexBuilder();
        
        int start = 0;
        nTotalFields = multiJoin.getRowType().getFields().length;
        joinStart = new int[nJoinFactors];
        nFieldsInJoinFactor = new int[nJoinFactors];
        for (int i = 0; i < nJoinFactors; i++) {
            joinStart[i] = start;
            nFieldsInJoinFactor[i] =
                joinFactors[i].getRowType().getFields().length;
            start += nFieldsInJoinFactor[i];
        }
        
        // there are no semijoins yet, so initialize to the original
        // factors
        chosenSemiJoins = new RelNode[nJoinFactors];
        chosenSemiJoinsMap = new HashMap<RelNode, Integer>();
        for (int i = 0; i < nJoinFactors; i++) {
            chosenSemiJoins[i] = joinFactors[i];
            chosenSemiJoinsMap.put(chosenSemiJoins[i], i);
        }
    }
    
    /**
     * Sets bitmaps indicating which factors and fields each join filter
     * references
     * 
     * @param nTotalFields total number of fields referenced by the
     * MultiJoinRel being optimized
     */
    private void setJoinFilterRefs(int nTotalFields)
    {
        fieldsRefByJoinFilter = new HashMap<RexNode, BitSet>();
        factorsRefByJoinFilter = new HashMap<RexNode, BitSet>();
        ListIterator filterIter = joinFilters.listIterator();
        while (filterIter.hasNext()) {
            RexNode joinFilter = (RexNode) filterIter.next();
            // ignore the literal filter; if necessary, we'll add it back
            // later
            if (joinFilter.isAlwaysTrue()) {
                filterIter.remove();
            }
            BitSet fieldRefBitmap = new BitSet(nTotalFields);
            RelOptUtil.findRexInputRefs(joinFilter, fieldRefBitmap);
            fieldsRefByJoinFilter.put(joinFilter, fieldRefBitmap);
            
            BitSet factorRefBitmap = new BitSet(nJoinFactors);
            setFactorBitmap(factorRefBitmap, fieldRefBitmap);
            factorsRefByJoinFilter.put(joinFilter, factorRefBitmap);
        }
    }
    
    /**
     * Sets the bitmap indicating which factors a filter references based
     * on which fields it references
     * 
     * @param factorRefBitmap bitmap representing factors referenced that will
     * be set by this method
     * @param fieldRefBitmap bitmap reprepsenting fields referenced
     */
    private void setFactorBitmap(BitSet factorRefBitmap, BitSet fieldRefBitmap)
    {
        for (int field = fieldRefBitmap.nextSetBit(0); field >= 0;
            field = fieldRefBitmap.nextSetBit(field + 1))
        {
            int factor = findRef(field);
            factorRefBitmap.set(factor);
        }
    }
    
    /**
     * Determines all possible semijoins that can be used by dimension
     * tables to filter fact tables.  Constructs SemiJoinRels corresponding
     * to potential dimension table filters and stores them in the member
     * field "possibleSemiJoins"
     */
    private void makePossibleSemiJoins()
    {
        possibleSemiJoins =
            new HashMap<Integer, Map<Integer, SemiJoinRel>>();
        
        for (int factIdx = 0; factIdx < nJoinFactors; factIdx++) {
            Map<Integer, List<RexNode>> dimFilters =
                new HashMap<Integer, List<RexNode>>();
            Map<Integer, SemiJoinRel> semiJoinMap =
                new HashMap<Integer, SemiJoinRel>();
            
            // skip over factors corresponding to non-simple factors
            LcsTable lcsTable = isSingleLcsTable(
                joinFactors[factIdx], nFieldsInJoinFactor[factIdx]);
            if (lcsTable == null) {
                continue;
            }

            // make sure the table has indexes
            LcsIndexGuide indexGuide = lcsTable.getIndexGuide();
            List<FemLocalIndex> indexes = indexGuide.getUnclusteredIndexes();
            if (indexes.isEmpty()) {
                continue;
            }
            
            // loop over all filters and find equality filters that reference
            // this factor and one other factor
            for (RexNode joinFilter : joinFilters) {
                int dimIdx = isSuitableFilter(joinFilter, factIdx);
                if (dimIdx == -1) {
                    continue;
                }
                // if we've already matched against this dimension factor,
                // then add the filter to the list associated with
                // that dimension factor; otherwise, create a new entry
                List<RexNode> currDimFilters = dimFilters.get(dimIdx);
                if (currDimFilters == null) {
                    currDimFilters = new ArrayList<RexNode>();
                }
                currDimFilters.add(joinFilter);
                dimFilters.put(dimIdx, currDimFilters);
            }
            
            // if there are potential dimension filters, determine if there
            // are appropriate indexes
            Set<Integer> dimKeys = dimFilters.keySet();
            Iterator it = dimKeys.iterator();
            while (it.hasNext()) {
                Integer dimIdx = (Integer) it.next();
                List<RexNode> joinFilters = dimFilters.get(dimIdx);
                if (joinFilters != null) {
                    SemiJoinRel semiJoin = findSemiJoinIndex(
                        indexGuide, joinFilters, factIdx, dimIdx);
            
                    // if an index is available, keep track of it as a
                    // possible semijoin
                    if (semiJoin != null) {
                        semiJoinMap.put(dimIdx, semiJoin);
                        possibleSemiJoins.put(factIdx, semiJoinMap);
                    }
                }
            }
        }
    }
    
    /**
     * Determines if a RelNode corresponds to a single LcsTable
     * 
     * @param rel RelNode being examined
     * @param nFields number of fields in the RelNode
     * @return
     */
    private LcsTable isSingleLcsTable(RelNode rel, int nFields)
    {
        // verify that all columns in the RelNode originate from a single
        // source that is the same LcsTable
        RelOptTable theTable = null;
        for (int i = 0; i < nFields; i++) {
            Set<RelColumnOrigin> colOrigin =
                RelMetadataQuery.getColumnOrigins(rel, i);
            if (colOrigin.size() != 1) {
                return null;
            }
            RelColumnOrigin[] coList =
                (RelColumnOrigin[]) colOrigin.toArray(new RelColumnOrigin[1]);
            RelOptTable table = coList[0].getOriginTable();
            if (theTable == null) {
                if (!(table instanceof LcsTable)) {
                    return null;
                } else {
                    theTable = table;
                }
            } else if (table != theTable) {
                return null;
            }
        }
        return (LcsTable) theTable;
    }
    
    /**
     * Determines if a join filter can be used with a semijoin against a
     * specified fact table.  A suitable filter is of the form
     * "factable.col1 = dimTable.col2".
     * 
     * @param joinFilter filter to be analyzed
     * @param factIdx index corresponding to the fact table
     * @return index of corresponding dimension table if the filter is
     * appropriate; otherwise -1 is returned
     */
    private int isSuitableFilter(RexNode joinFilter, int factIdx)
    {
        // ignore non-equality filters where the operands are not
        // RexInputRefs
        if (!(joinFilter instanceof RexCall) ||
            ((RexCall) joinFilter).getOperator() !=
                SqlStdOperatorTable.equalsOperator) 
        {
            return -1;
        } 
        RexNode[] operands = ((RexCall) joinFilter).getOperands();
        if (!(operands[0] instanceof RexInputRef) ||
            !(operands[1] instanceof RexInputRef))
        {
            return -1;    
        }
        
        // filter is suitable if each side of the filter only contains a
        // single factor reference and one side references the fact table and
        // the other references the dimension table; since we know this is
        // a join filter and we've already verified that the operands are
        // RexInputRefs, verify that the factors belong to the fact and
        // dimension table
        BitSet joinRefs = factorsRefByJoinFilter.get(joinFilter);
        assert(joinRefs.cardinality() == 2);
        int factor1 = joinRefs.nextSetBit(0);
        int factor2 = joinRefs.nextSetBit(factor1 + 1);
        if (factor1 == factIdx) {
            return factor2;
        }
        if (factor2 == factIdx) {
            return factor1;
        }
        return -1;
    }
    
    /**
     * Determines the join factor corresponding to a RexInputRef
     * 
     * @param rexInputRef rexInputRef index
     * @return index corresponding to join factor
     */
    private int findRef(int rexInputRef)
    {
        for (int i = 0; i < nJoinFactors; i++) {
            if (rexInputRef >= joinStart[i] &&
                rexInputRef < joinStart[i] + nFieldsInJoinFactor[i])
            {
                return i;
            }
        }
        assert(false);
        return 0;
    }
 
    /**
     * Given a list of possible filters on a fact table, determine if there
     * is an index that can be used.
     * 
     * @param indexGuide index guide associated with fact table
     * @param joinFilters filters to be used on the fact table
     * @param factIdx index in join factors corresponding to the fact table
     * @param dimIdx index in join factors corresponding to the dimension table
     * @return SemiJoinRel containing information regarding the semijoin that
     * can be used to filter the fact table
     */
    private SemiJoinRel findSemiJoinIndex(
        LcsIndexGuide indexGuide,
        List<RexNode> joinFilters,
        int factIdx, int dimIdx)
    {
        // create a SemiJoinRel with the semijoin condition and keys
        RexNode semiJoinCondition = RexUtil.andRexNodeList(
            rexBuilder, joinFilters);
        
        int leftAdjustment = 0;
        for (int i = 0; i < factIdx; i++) {
            leftAdjustment -= nFieldsInJoinFactor[i];
        }
        semiJoinCondition = adjustSemiJoinCondition(
            leftAdjustment, semiJoinCondition, factIdx, dimIdx);
        
        List<Integer> leftKeys = new ArrayList<Integer>();
        List<Integer> rightKeys = new ArrayList<Integer>();
        RelOptUtil.splitJoinCondition(
            joinFactors[factIdx], joinFactors[dimIdx], semiJoinCondition,
            leftKeys, rightKeys);
        assert(leftKeys.size() > 0);

        // find the best index
        List<Integer> keyOrder = new ArrayList<Integer>();
        indexGuide.findSemiJoinIndex(leftKeys, keyOrder);
        if (keyOrder.size() == 0) {
            return null;
        }
        // if necessary, truncate the keys to reflect the ones that match
        // the index and remove the corresponding, unnecessary filters from
        // the condition
        List<Integer> actualLeftKeys;
        List<Integer> actualRightKeys;
        if (leftKeys.size() == keyOrder.size()) {
            actualLeftKeys = leftKeys;
            actualRightKeys = rightKeys;
        } else {
            actualLeftKeys = new ArrayList<Integer>();
            actualRightKeys = new ArrayList<Integer>();
            for (int key : keyOrder) {
                actualLeftKeys.add(leftKeys.get(key));
                actualRightKeys.add(rightKeys.get(key));
            }
            semiJoinCondition = removeExtraFilters(
                actualLeftKeys, nFieldsInJoinFactor[factIdx],
                semiJoinCondition);
        }
        SemiJoinRel semiJoin = new SemiJoinRel(
            joinFactors[factIdx].getCluster(),
            joinFactors[factIdx],
            joinFactors[dimIdx],
            semiJoinCondition,
            actualLeftKeys,
            actualRightKeys);
        return semiJoin;
    }
    
    /**
     * Modifies the semijoin condition to reflect the fact that the RHS
     * is now the second factor into a join and the LHS is the first
     * 
     * @param leftAdjustment amount the left RexInputRefs need to be adjusted
     * by
     * @param semiJoinCondition condition to be adjusted
     * @param leftIdx index of the join factor corresponding to the LHS of the
     * semijoin,
     * @param rightIdx index of the join factor corresponding to the RHS of the
     * semijoin
     * @return modified semijoin condition
     */
    private RexNode adjustSemiJoinCondition(
        int leftAdjustment, RexNode semiJoinCondition,
        int leftIdx, int rightIdx)
    {
        // adjust the semijoin condition to reflect the fact that the
        // RHS is now the second factor into the semijoin and the LHS
        // is the first
        int rightAdjustment = 0;
        for (int i = 0; i < rightIdx; i++) {
            rightAdjustment -= nFieldsInJoinFactor[i];
        }
        int rightStart = -rightAdjustment;
        rightAdjustment += nFieldsInJoinFactor[leftIdx];
        
        // only adjust the filter if adjustments are required
        if (leftAdjustment != 0 || rightAdjustment != 0) {
            int adjustments[] = new int[nTotalFields];
            if (leftAdjustment != 0) {
                for (int i = -leftAdjustment;
                    i < -leftAdjustment + nFieldsInJoinFactor[leftIdx];
                    i++)
                {
                    adjustments[i] = leftAdjustment;
                }
            }
            if (rightAdjustment != 0) {
                for (int i = rightStart;
                    i < rightStart + nFieldsInJoinFactor[rightIdx];
                    i++)
                {
                    adjustments[i] = rightAdjustment;
                }   
            }
            return RelOptUtil.convertRexInputRefs(
                rexBuilder, semiJoinCondition,
                multiJoin.getRowType().getFields(),
                adjustments);
        }
        
        return semiJoinCondition;
    }
    
    /**
     * Removes from an expression any sub-expressions that reference key
     * values that aren't contained in a key list passed in.  The keys 
     * represent join keys on one side of a join.  The subexpressions are all
     * assumed to be of the form "tab1.col1 = tab2.col2".
     * 
     * @param keys join keys from one side of the join
     * @param nFields number of fields in the side of the join for which the
     * keys correspond
     * @param condition original expression
     * @return modified expression with filters that don't reference specified
     * keys removed
     */
    private RexNode removeExtraFilters(
        List<Integer> keys, int nFields, RexNode condition)
    {
        // recursively walk the expression; if all sub-expressions are
        // removed from one side of the expression, just return what remains
        // from the other side
        assert(condition instanceof RexCall);
        RexCall call = (RexCall) condition;
        if (condition.isA(RexKind.And)) {
            RexNode[] operands = call.getOperands();
            RexNode left = removeExtraFilters(
                keys, nFields, operands[0]);
            RexNode right = removeExtraFilters(
                keys, nFields, operands[1]);
            if (left == null) {
                return right;
            }
            if (right == null) {
                return left;
            }
            return rexBuilder.makeCall(
                SqlStdOperatorTable.andOperator, left, right);
        }
        
        // determine which side of the equality filter references the join
        // operand we're interested in; then, check if it is contained in
        // our key list
        assert(call.getOperator() == SqlStdOperatorTable.equalsOperator);
        RexNode[] operands = call.getOperands();
        assert(operands[0] instanceof RexInputRef);
        assert(operands[1] instanceof RexInputRef);
        int idx = ((RexInputRef) operands[0]).getIndex();
        if (idx < nFields) {
            if (!keys.contains(idx)) {
                return null;
            }
        } else {
            idx = ((RexInputRef) operands[1]).getIndex();
            if (!keys.contains(idx)) {
                return null;
            }
        }
        return condition;
    }
    
    /**
     * Finds the optimal semijoin for filtering the least costly fact
     * table from among the remaining possible semijoins to choose from.
     * The chosen semijoin is stored in the chosenSemiJoins array
     * 
     * @return true if a suitable semijoin is found; false otherwise
     */
    private boolean chooseBestSemiJoin()
    {
        // sort the join factors based on the cost of each factor filtered by
        // semijoins, if semijoins have been chosen
        Integer[] sortedFactors = new Integer[nJoinFactors];
        for (int i = 0; i < nJoinFactors; i++) {
            sortedFactors[i] = i;
        }
        Arrays.sort(sortedFactors, factorCostComparator);
        
        // loop through the factors in sort order, treating the factor as
        // a fact table; analyze the possible semijoins associated with
        // that fact table
        for (int i = 0; i < nJoinFactors; i++) {
            
            Integer factIdx = sortedFactors[i];
            RelNode factRel = chosenSemiJoins[factIdx];
            Map<Integer, SemiJoinRel> possibleDimensions = 
                possibleSemiJoins.get(factIdx);
            if (possibleDimensions == null) {
                continue;
            }
            double bestScore = 0.0;
            int bestDimIdx = -1;
            
            // loop through each dimension table associated with the current
            // fact table and analyze the ones that have semijoins with this
            // fact table
            Set<Integer> dimKeys = possibleDimensions.keySet();
            Iterator it = dimKeys.iterator();
            while (it.hasNext()) {
                Integer dimIdx = (Integer) it.next();
                SemiJoinRel semiJoin = possibleDimensions.get(dimIdx);
                if (semiJoin == null) {
                    continue;
                }

                // keep track of the dimension table that has the best score
                // for filtering this fact table
                double score = computeScore(
                    factRel, chosenSemiJoins[dimIdx], semiJoin);
                if (score > thresholdScore && score > bestScore) {
                    bestDimIdx = dimIdx;
                    bestScore = score;
                }
            }
        
            // if a suitable dimension table has been found, associate it
            // with the fact table in the chosenSemiJoins array; also remove
            // the entry from possibleSemiJoins so we won't chose it again;
            // note that we create the SemiJoinRel using the chosen semijoins
            // already created for each factor so any chaining of filters will
            // be accounted for
            if (bestDimIdx != -1) {
                SemiJoinRel semiJoin = possibleDimensions.get(bestDimIdx);
                SemiJoinRel chosenSemiJoin = new SemiJoinRel(
                    factRel.getCluster(),
                    factRel,
                    chosenSemiJoins[bestDimIdx],
                    semiJoin.getCondition(),
                    semiJoin.getLeftKeys(),
                    semiJoin.getRightKeys());
                chosenSemiJoins[factIdx] = chosenSemiJoin;
                chosenSemiJoinsMap.put(chosenSemiJoin, factIdx);
           
                removePossibleSemiJoin(
                    possibleDimensions, factIdx, bestDimIdx);   
                // need to also remove the semijoin from the possible
                // semijoins associated with this dimension table, as the
                // semijoin can only be used to filter one table, not both
                removePossibleSemiJoin(
                    possibleSemiJoins.get(bestDimIdx), bestDimIdx, factIdx);
                return true;
            }
        
            // continue searching on the next fact table if we couldn't find
            // a semijoin for the current fact table
        }
        
        return false;
    }
    
    /**
     * Computes a score relevant to applying a set of semijoins on
     * a fact table.  The higher the score, the better.
     * 
     * @param factRel fact table being filtered
     * @param dimRel dimension table that participates in semijoin
     * @param semiJoin semijoin between fact and dimension tables
     * @return computed score of applying the dimension table filters on the
     * fact table
     */
    private double computeScore(
        RelNode factRel, RelNode dimRel, SemiJoinRel semiJoin)
    {
        // TODO - replace with real costing; for now, score is based on the
        // the order in which the factors appear in the original list;
        // then we can also get rid of chosenSemiJoinsMap
        double savings = 10 * (nJoinFactors - chosenSemiJoinsMap.get(factRel));
        double cost = nJoinFactors - chosenSemiJoinsMap.get(dimRel);

        return savings / cost;
    }
    
    /**
     * Removes a dimension table from a fact table's list of possible
     * semijoins
     * 
     * @param possibleDimensions possible dimension tables associated with
     * the fact table
     * @param factIdx index corresponding to fact table
     * @param dimIdx index corresponding to dimension table
     */
    private void removePossibleSemiJoin(
        Map<Integer, SemiJoinRel> possibleDimensions,
        Integer factIdx, Integer dimIdx)
    {
        // dimension table may not have a corresponding semijoin if it
        // wasn't indexable
        if (possibleDimensions == null) {
            return;
        }
        possibleDimensions.remove(dimIdx);
        if (possibleDimensions.isEmpty()) {
            possibleSemiJoins.remove(factIdx);
        } else {
            possibleSemiJoins.put(factIdx, possibleDimensions);
        }
    }
    
    /**
     * Sets weighting for each combination of factors, depending on which
     * join filters reference which factors.  Greater weight is given to
     * equality conditions.  Also, sets bitmaps indicating which factors
     * are referenced by each factor within join filters that are comparisons.
     */
    private void setFactorWeights()
    {
        factorWeights = new int[nJoinFactors][nJoinFactors];
        factorsRefByFactor = new BitSet[nJoinFactors];
        for (int i = 0; i < nJoinFactors; i++) {
            factorsRefByFactor[i] = new BitSet(nJoinFactors);
        }

        for (RexNode joinFilter : joinFilters) {
            BitSet factorRefs = factorsRefByJoinFilter.get(joinFilter);
            // don't give weights to non-comparison expressions
            if (!(joinFilter instanceof RexCall)) {
                continue;
            }
            if (!joinFilter.isA(RexKind.Comparison)) {
                continue;
            }
            
            // OR the factors referenced in this join filter into the
            // bitmaps corresponding to each of the factors; however,
            // exclude the bit corresponding to the factor itself
            for (int factor = factorRefs.nextSetBit(0); factor >= 0;
                factor = factorRefs.nextSetBit(factor + 1))
            {
                factorsRefByFactor[factor].or(factorRefs);
                factorsRefByFactor[factor].clear(factor);
            }
            
            if (factorRefs.cardinality() == 2) {
                int leftFactor = factorRefs.nextSetBit(0);
                int rightFactor = factorRefs.nextSetBit(leftFactor + 1);

                BitSet leftFields = new BitSet(nTotalFields);
                RexNode[] operands = ((RexCall) joinFilter).getOperands();
                RelOptUtil.findRexInputRefs(operands[0], leftFields);
                BitSet leftBitmap = new BitSet(nJoinFactors);
                setFactorBitmap(leftBitmap, leftFields);
                
                // filter contains only two factor references, one on each
                // side of the operator
                if (leftBitmap.cardinality() == 1) {
                    
                    // give higher weight to equijoins
                    if (((RexCall) joinFilter).getOperator() ==
                            SqlStdOperatorTable.equalsOperator)
                    {
                        setFactorWeight(3, leftFactor, rightFactor);
                    } else {
                        setFactorWeight(2, leftFactor, rightFactor);
                    }
                } else {
                    // cross product of two tables
                    setFactorWeight(1, leftFactor, rightFactor);
                }
            } else {
                // multiple factor references -- set a weight for each 
                // combination of factors referenced within the filter
                for (int outer = factorRefs.nextSetBit(0); outer >= 0;
                    outer = factorRefs.nextSetBit(outer + 1))
                {
                    for (int inner = factorRefs.nextSetBit(0); inner >= 0;
                        inner = factorRefs.nextSetBit(inner + 1))
                    {
                        if (outer != inner) {
                            setFactorWeight(1, outer, inner);
                        }
                    }
                }
            }
        }
    }
    
    /**
     * Sets an individual weight if the new weight is better than the current
     * one
     * 
     * @param weight weight to be set
     * @param leftFactor index of left factor
     * @param rightFactor index of right factor
     */
    private void setFactorWeight(int weight, int leftFactor, int rightFactor)
    {
        if (factorWeights[leftFactor][rightFactor] < weight) {
            factorWeights[leftFactor][rightFactor] = weight;
            factorWeights[rightFactor][leftFactor] = weight;
        }
    }
        
    /**
     * Generates N optimal join orderings.  Each ordering contains each factor
     * as the first factor in the ordering.
     */
    private void findBestOrderings(RelOptRuleCall call)
    {
        ProjectRel project = (ProjectRel) call.rels[0];
        RexNode[] origProjExprs = project.getProjectExps();
        int projLength = project.getProjectExps().length;
        String fieldNames[] = new String[projLength];
        for (int i = 0; i < projLength; i++) {
            fieldNames[i] = project.getRowType().getFields()[i].getName();
        }
        
        List<RelNode> plans = new ArrayList<RelNode>();
        
        int[] cardinalities = new int[nJoinFactors];
        // TODO - change to set actual factor cardinalities
        for (int i = 0; i < nJoinFactors; i++) {
            cardinalities[i] = nJoinFactors - i;
        }
        
        for (int i = 0; i < nJoinFactors; i++) {
            
            LoptJoinTree joinTree = createOrdering(i, cardinalities);
            if (joinTree == null) {
                continue;
            }
            
            // determine the topmost adjustment factors based on the
            // selected join ordering
            int[] adjustments = new int[nTotalFields];
            RexNode[] newProjExprs;
            if (needsAdjustment(adjustments, joinTree, null)) {

                // adjust projection expressions
                newProjExprs = new RexNode[projLength];
                for (int j = 0; j < projLength; j++) {
                    newProjExprs[j] = RelOptUtil.convertRexInputRefs(
                        rexBuilder, origProjExprs[j],
                        multiJoin.getRowType().getFields(), adjustments);
                }
            } else {
                newProjExprs = origProjExprs;
            }
            
            ProjectRel newProject =
                (ProjectRel) CalcRel.createProject(
                    joinTree.getJoinTree(), newProjExprs, fieldNames);
            plans.add(newProject);
        }
        
        // transform the selected plans; note that we wait till then the end
        // to transform everything so any intermediate RelNodes we create
        // are not converted to RelSubsets
        for (RelNode plan : plans) {
            call.transformTo(plan);
        }
    }
    
    /**
     * Generates a join tree with a specific factor as the first factor
     * in the join tree
     * 
     * @param firstFactor first factor in the tree
     * @param cardinalities cardinalities of each of the factors
     * @return constructed join tree or null if it is not possible for
     * firstFactor to appear as the first factor in the join
     */
    private LoptJoinTree createOrdering(int firstFactor, int[] cardinalities)
    {
        LoptJoinTree joinTree = null;
        BitSet factorsToAdd = new BitSet(nJoinFactors);
        BitSet factorsAdded = new BitSet(nJoinFactors);
        factorsToAdd.flip(0, nJoinFactors);
        List<RexNode> filtersToAdd = new ArrayList<RexNode>(joinFilters);
        
        while (factorsToAdd.cardinality() > 0) {
            int nextFactor = -1;
            if (factorsAdded.cardinality() == 0) {
                nextFactor = firstFactor;
            } else {
                // iterate through the remaining factors and determine the
                // best one to add next
                int bestWeight = 0;
                int bestCardinality = 0; 
                for (int factor = factorsToAdd.nextSetBit(0); factor >= 0;
                    factor = factorsToAdd.nextSetBit(factor + 1))
                {
                    
                    // determine the best weight between the current factor
                    // under consideration and the factors that have already
                    // been added to the tree
                    int dimWeight = 0;
                    for (int prevFactor = factorsAdded.nextSetBit(0);
                        prevFactor >= 0;
                        prevFactor = factorsAdded.nextSetBit(prevFactor + 1))
                    {
                        if (factorWeights[prevFactor][factor] > dimWeight) {
                            dimWeight = factorWeights[prevFactor][factor];
                        }
                    }
                    // if two factors have the same weight, pick the one
                    // with the higher cardinality join key
                    if ((dimWeight > bestWeight) ||
                        (dimWeight == bestWeight &&
                            cardinalities[factor] > bestCardinality)) 
                    {
                        nextFactor = factor;
                        bestWeight = dimWeight;
                        bestCardinality = cardinalities[factor];
                    }
                }
            }
                
            // add the factor; pass in a bitmap representing the factors
            // this factor joins with that have already been added to
            // the tree
            BitSet factorsNeeded = new BitSet(nJoinFactors);
            factorsNeeded = factorsRefByFactor[nextFactor];
            factorsNeeded.and(factorsAdded);
            joinTree = addFactorToTree(
                joinTree, nextFactor, factorsNeeded, filtersToAdd);
            if (joinTree == null) {
                return null;
            }
            factorsToAdd.clear(nextFactor);
            factorsAdded.set(nextFactor);
        }
        
        return joinTree;
    }
    
    /**
     * Returns true if a relnode corresponds to a JoinRel that wasn't one
     * of the original MultiJoinRel input factors
     */
    private boolean isJoinTree(RelNode rel) 
    {
        // TODO - when we support outer joins, this check needs to distinguish
        // between joins that correspond to factors as opposed to joins that
        // that replace the MultiJoinRel
        return (rel instanceof JoinRel);
    }

    /**
     * Adds a new factor into the current join tree.  The factor is either
     * pushed down into one of the subtrees of the join recursively, or it
     * is added to the top of the current tree, whichever yields a better
     * ordering.
     * 
     * @param joinTree current join tree
     * @param factorToAdd new factor to be added
     * @param factorsNeeded factors that must precede the factor to be added
     * @param filtersToAdd filters remaining to be added; filters added to
     * the new join tree are removed from the list
     * @return optimal join tree with the new factor added if it is possible
     * to add the factor; otherwise, null is returned
     */
    private LoptJoinTree addFactorToTree(
        LoptJoinTree joinTree, int factorToAdd, BitSet factorsNeeded,
        List<RexNode> filtersToAdd)
    {
        if (joinTree == null) {
            return new LoptJoinTree(chosenSemiJoins[factorToAdd], factorToAdd);
        }
        
        // create a temporary copy of the filter list as we need the original
        // list to pass into addToTop()
        List<RexNode> tmpFilters = new ArrayList<RexNode>(filtersToAdd);
        LoptJoinTree pushDownTree = pushDownFactor(
            joinTree, factorToAdd, factorsNeeded, tmpFilters);
        
        LoptJoinTree topTree = addToTop(joinTree, factorToAdd, filtersToAdd);
        
        // pick the lower cost option, and replace the join ordering with
        // the ordering associated with the best option
        LoptJoinTree bestTree;
        if (pushDownTree == null || 
            getCost(pushDownTree.getJoinTree()) >
                getCost(topTree.getJoinTree())) {
            bestTree = topTree;
        } else {
            bestTree = pushDownTree;
        }
        
        return bestTree;
    }
    
    /**
     * Creates a join tree where the new factor is pushed down one of the
     * operands of the current join tree
     * 
     * @param joinTree current join tree
     * @param factorToAdd new factor to be added
     * @param factorsNeeded factors that must precede the factor to be added
     * @param filtersToAdd filters remaining to be added; filters that are
     * added to the join tree are removed from the list
     * @return optimal join tree with thew new factor pushed down the current
     * join tree if it is possible to do the pushdown; otherwise, null is
     * returned
     */
    private LoptJoinTree pushDownFactor(
        LoptJoinTree joinTree, int factorToAdd, BitSet factorsNeeded,
        List<RexNode> filtersToAdd)
    {
        // pushdown option only works if we already have a join tree
        if (!isJoinTree(joinTree.getJoinTree())) {
            return null;
        }
        int childNo = -1;
        LoptJoinTree left = joinTree.getLeft();
        LoptJoinTree right = joinTree.getRight();
        // if there are no constraints as to which side the factor must
        // be pushed, arbitrarily push to the left
        if (factorsNeeded.cardinality() == 0) {
            childNo = 0;
        } else {
            // push to the left if the LHS contains all factors that the
            // current factor needs; same check for RHS
            if (hasAllFactors(left, factorsNeeded)) {
                childNo = 0;
            } else if (hasAllFactors(right, factorsNeeded)) {
                childNo = 1;
            }
            // if it couldn't be pushed down to either side, then it can
            // only be put on top
        }
        if (childNo == -1) {
            return null;
        }

        // remember the original join order before the pushdown so we can
        // appropriately adjust any filters already attached to the join
        // node
        List<Integer> origJoinOrder = new ArrayList<Integer>();
        joinTree.getTreeOrder(origJoinOrder);
        
        // recursively pushdown the factor
        LoptJoinTree subTree = (childNo == 0) ? left : right;
        subTree = addFactorToTree(
            subTree, factorToAdd, factorsNeeded, filtersToAdd);
  
        if (childNo == 0) {
            left = subTree;
        } else {
            right = subTree; 
        }
        
        // adjust the join condition from the original join tree to reflect
        // pushdown of the new factor as well as any swapping that may have
        // been done during the pushdown
        RexNode origCondition =
            ((JoinRel) joinTree.getJoinTree()).getCondition();
        origCondition = adjustFilter(
            left, right, origCondition, factorToAdd, origJoinOrder,
            joinTree.getJoinTree().getRowType().getFields());
        
        // determine if additional filters apply as a result of adding the
        // new factor
        RexNode condition = addFilters(left, right, filtersToAdd, true);
        condition = RelOptUtil.andJoinFilters(
            rexBuilder, origCondition, condition);
        
        // create the new join tree with the factor pushed down
        return createJoinSubtree(left, right, condition, false);
    }
    
    /**
     * Creates a join tree with the new factor added to the top of the tree
     * 
     * @param joinTree current join tree
     * @param factorToAdd new factor to be added
     * @param filtersToAdd filters remaining to be added; modifies the list
     * to remove filters that can be added to the join tree
     * @return new join tree
     */
    private LoptJoinTree addToTop(
        LoptJoinTree joinTree, int factorToAdd, List<RexNode> filtersToAdd)
    {
        LoptJoinTree rightTree = new LoptJoinTree(
            chosenSemiJoins[factorToAdd], factorToAdd);
        RexNode condition = addFilters(
            joinTree, rightTree, filtersToAdd, false);
       
        return createJoinSubtree(joinTree, rightTree, condition, true);
    }
    
    /**
     * Returns true if a join tree contains all factors required
     * 
     * @param joinTree join tree to be examined
     * @param factorsNeeded bitmap of factors required
     * @return true if join tree contains all required factors
     */
    private boolean hasAllFactors(LoptJoinTree joinTree, BitSet factorsNeeded)
    {
        BitSet childFactors = new BitSet(nJoinFactors);
        getChildFactors(joinTree, childFactors);
        return RelOptUtil.contains(childFactors, factorsNeeded);
    }
    
    /**
     * Sets a bitmap indicating all child RelNodes in a join tree
     * 
     * @param joinTree join tree to be examined
     * @param childFactors bitmap to be set
     */
    private void getChildFactors(LoptJoinTree joinTree, BitSet childFactors)
    {
        List<Integer> children = new ArrayList<Integer>();
        joinTree.getTreeOrder(children);
        for (int child : children) {
            childFactors.set(child);
        }
    }
    
    /**
     * Determines which join filters can be added to the current join tree.
     * Note that the join filter still reflects the original join ordering.
     * It will only be adjusted to reflect the new join ordering if the
     * "adjust" parameter is set to true.
     * 
     * @param leftTree left subtree of the join tree
     * @param rightTree right subtree of the join tree
     * @param filtersToAdd remaining join filters that need to be added; those
     * that are added are removed from the list
     * @param adjust if true, adjust filter to reflect new join ordering
     * @return AND'd expression of the join filters that can be added to the
     * current join tree
     */
    private RexNode addFilters(
        LoptJoinTree leftTree, LoptJoinTree rightTree,
        List<RexNode> filtersToAdd, boolean adjust)
    {
        // loop through the remaining filters to be added and pick out the
        // ones that reference only the factors in the new join tree
        RexNode condition = null;
        ListIterator filterIter = filtersToAdd.listIterator();
        while (filterIter.hasNext()) {
            RexNode joinFilter = (RexNode) filterIter.next();
            BitSet filterBitmap = factorsRefByJoinFilter.get(joinFilter);
            
            BitSet childFactors = new BitSet(nJoinFactors);
            getChildFactors(leftTree, childFactors);
            getChildFactors(rightTree, childFactors);
            
            // if all factors in the join filter are in the join tree,
            // AND the filter to the current join condition
            if (RelOptUtil.contains(childFactors, filterBitmap)) {
                if (condition == null) {
                    condition = joinFilter;
                } else {
                    condition = rexBuilder.makeCall(
                        SqlStdOperatorTable.andOperator,
                        condition, joinFilter);
                }
                filterIter.remove();
            }
        }
         
        if (adjust && condition != null) {
            int[] adjustments = new int[nTotalFields];
            if (needsAdjustment(adjustments, leftTree, rightTree)) {
                condition = RelOptUtil.convertRexInputRefs(
                    rexBuilder, condition,
                    multiJoin.getRowType().getFields(), adjustments);
            }
        }
        
        if (condition == null) {
            condition = rexBuilder.makeLiteral(true);
        }
        
        return condition;
    }
    
    /**
     * Adjusts a filter to reflect a newly added factor in the middle of an
     * existing join tree
     * 
     * @param left left subtree of the join
     * @param right right subtree of the join
     * @param condition current join condition
     * @param factorAdded index corresponding to the newly added factor
     * @param origJoinOrder original join order, before factor was pushed
     * into the tree
     * @param origFields fields from the original join before the factor was
     * added
     * @return modified join condition reflecting addition of the new factor
     */
    private RexNode adjustFilter(
        LoptJoinTree left, LoptJoinTree right, RexNode condition,
        int factorAdded,
        List<Integer> origJoinOrder, RelDataTypeField[] origFields)
    {
        List<Integer> newJoinOrder = new ArrayList<Integer>();
        left.getTreeOrder(newJoinOrder);
        right.getTreeOrder(newJoinOrder);
        
        int totalFields =
            left.getJoinTree().getRowType().getFields().length +
                right.getJoinTree().getRowType().getFields().length -
                nFieldsInJoinFactor[factorAdded];
        int[] adjustments = new int[totalFields];
        
        // go through each factor and adjust relative to the original
        // join order
        boolean needAdjust = false;
        int nFieldsNew = 0;
        for (int newPos = 0; newPos < newJoinOrder.size(); newPos++) {
            int nFieldsOld = 0;
            // no need to make any adjustments on the newly added factor
            if (newJoinOrder.get(newPos) != factorAdded) {
                for (int oldPos = 0; oldPos < origJoinOrder.size(); oldPos++) {
                    if (newJoinOrder.get(newPos) ==
                        origJoinOrder.get(oldPos))
                    {
                        break;
                    }
                    nFieldsOld +=
                        nFieldsInJoinFactor[origJoinOrder.get(oldPos)];
                }
                if (-nFieldsOld + nFieldsNew != 0) {
                    needAdjust = true;
                    for (int i = 0;
                        i < nFieldsInJoinFactor[newJoinOrder.get(newPos)];
                        i++)
                    {
                        // subtract off the number of fields to the left
                        // in the original join order and then add on the
                        // number of fields on the left in the new join order
                        adjustments[i + nFieldsOld] = -nFieldsOld + nFieldsNew;
                    }
                }
            }
            nFieldsNew += nFieldsInJoinFactor[newJoinOrder.get(newPos)];
        }
        
        if (needAdjust) {
            condition = RelOptUtil.convertRexInputRefs(
                rexBuilder, condition, origFields, adjustments);
        }
        
        return condition;
    }
    
    /**
     * Creates a JoinRel given left and right operands and a join condition.
     * Swaps the operands if beneficial.
     * 
     * @param left left operand
     * @param right right operand
     * @param condition join condition
     * @param fullAdjust true if the join condition reflects the original
     * join ordering and therefore has not gone through any type of
     * adjustment yet; otherwise, the condition has already been partially
     * adjusted and only needs to be further adjusted if swapping is done
     * @return created JoinRel
     */
    private LoptJoinTree createJoinSubtree(
        LoptJoinTree left, LoptJoinTree right, RexNode condition,
        boolean fullAdjust)
    {
        // swap the inputs if beneficial
        if (swapInputs(left, right, condition)) {
            LoptJoinTree tmp = right;
            right = left;
            left = tmp;
            if (!fullAdjust) {
                condition = swapFilter(right, left, condition);
            }
        }
      
        if (fullAdjust) {
            int[] adjustments = new int[nTotalFields];
            if (needsAdjustment(adjustments, left, right)) {
                condition = RelOptUtil.convertRexInputRefs(
                    rexBuilder, condition,
                    multiJoin.getRowType().getFields(), adjustments);
            }
        }
        
        JoinRel joinTree = new JoinRel(
            multiJoin.getCluster(), left.getJoinTree(), right.getJoinTree(),
            condition, JoinRelType.INNER, Collections.EMPTY_SET, true, true);
        return new LoptJoinTree(
            joinTree, left.getFactorTree(), right.getFactorTree());
    }
     
    /**
     * Swaps the operands to a join, if in the join condition, the RHS 
     * references more columns than the right.  This is done so queries
     * like  (select * from A,B where A.A between B.X and B.Y) will result
     * in B being on the left.  If both sides have the same number of
     * references in the join condition, then the smaller input is put on
     * the right.
     * 
     * @param left left side of join tree
     * @param right right hand side of join tree
     * @param condition join condition between left and right
     * @return true if swapping should be done
     */
    private boolean swapInputs(
        LoptJoinTree left, LoptJoinTree right, RexNode condition)
    {
        boolean swap = false;
        
        // determine how many fields within the join condition each side
        // of the join references
        BitSet leftFields = new BitSet(nTotalFields);
        setFieldBitmap(left, leftFields);
        
        BitSet rightFields = new BitSet(nTotalFields);
        setFieldBitmap(right, rightFields);

        // all fields referenced in the join condition
        BitSet filterRefs = new BitSet(nTotalFields);
        RelOptUtil.findRexInputRefs(condition, filterRefs);
        
        // count how many fields each side of the join references by AND'ing
        // the bits referenced in the filter with the bits corresponding to
        // all fields on each side of the join
        leftFields.and(filterRefs);
        rightFields.and(filterRefs);
        
        if (rightFields.cardinality() > leftFields.cardinality()) {
            swap = true;
        } else if (rightFields.cardinality() == leftFields.cardinality()) {
            if (getRowCount(left.getJoinTree()) < getRowCount(right.getJoinTree())) {
                swap = true;
            }
        }
        
        return swap;
    }
    
    /**
     * Adjusts a filter to reflect swapping of join inputs
     * 
     * @param origLeft original LHS of the join tree (before swap)
     * @param origRight original RHS of the join tree (before swap)
     * @param condition original join condition
     * @return join condition reflect swap of join inputs
     */
    private RexNode swapFilter(
        LoptJoinTree origLeft, LoptJoinTree origRight, RexNode condition)
    {
        int nFieldsOnLeft =
            origLeft.getJoinTree().getRowType().getFields().length;
        int nFieldsOnRight =
            origRight.getJoinTree().getRowType().getFields().length;
        int[] adjustments = new int[nFieldsOnLeft + nFieldsOnRight];
        
        for (int i = 0; i < nFieldsOnLeft; i++) {
            adjustments[i] = nFieldsOnRight;
        }
        for (int i = nFieldsOnLeft; i < nFieldsOnLeft + nFieldsOnRight; i++) {
            adjustments[i] = -nFieldsOnLeft;
        }
        
        RelDataType joinRowType =
            multiJoin.getCluster().getTypeFactory().createJoinType(
                new RelDataType[]
                    { origLeft.getJoinTree().getRowType(),
                    origRight.getJoinTree().getRowType() });
        
        condition = RelOptUtil.convertRexInputRefs(
            rexBuilder, condition, joinRowType.getFields(), adjustments);
        
        return condition;
    }
    
    /**
     * Sets a bitmap representing all fields corresponding to a RelNode
     * 
     * @param rel relnode for which fields will be set
     * @param fields bitmap containing set bits for each field in a RelNode
     */
    private void setFieldBitmap(LoptJoinTree rel, BitSet fields)
    {
        // iterate through all factors within the RelNode
        BitSet factors = new BitSet(nJoinFactors);
        getChildFactors(rel, factors);
        for (int factor = factors.nextSetBit(0); factor >= 0;
            factor = factors.nextSetBit(factor + 1))
        {
            // set a bit for each field
            for (int i = 0; i < nFieldsInJoinFactor[factor]; i++) {
                fields.set(joinStart[factor] + i);
            }
        }
    }
    
    /**
     * Sets an array indicating how much each factor in a join tree
     * needs to be adjusted to reflect the tree's join ordering
     * 
     * @param adjustments array to be filled out
     * @param joinTree join tree
     * @param otherTree null unless joinTree only represents the left side
     * of the join tree
     * @return true if some adjustment is required; false otherwise
     */
    private boolean needsAdjustment(
        int[] adjustments, LoptJoinTree joinTree, LoptJoinTree otherTree)
    {
        boolean needAdjustment = false;
        
        List<Integer> joinOrder = new ArrayList<Integer>();
        joinTree.getTreeOrder(joinOrder);
        if (otherTree != null) {
            otherTree.getTreeOrder(joinOrder);
        }
        
        int nFields = 0;
        for (int newPos = 0; newPos < joinOrder.size(); newPos++) {
            // factor needs to be adjusted as follows:
            // - first subtract, based on where the factor was in the
            //   orginal join input
            // - then add on the number of fields in the factors that now
            //   precede this factor in the new join ordering
            int origPos = joinOrder.get(newPos);
            int adjustment = -joinStart[origPos] + nFields;
            nFields += nFieldsInJoinFactor[origPos];
            if (adjustment != 0) {
                needAdjustment = true;
                for (int i = 0; i < nFieldsInJoinFactor[origPos]; i++) {   
                    adjustments[joinStart[origPos] + i] = adjustment;
                }
            }
        }
            
        return needAdjustment;
    }
    
    /**
     * Dummy costing routine
     */
    private int getCost(RelNode rel) {
        // TODO - replace with real costing
        if (rel instanceof JoinRel) {
            return getCost(rel.getInput(0)) * getCost(rel.getInput(1));
        } else if (rel instanceof SemiJoinRel) {
            return (getCost(rel.getInput(0)) + getCost(rel.getInput(1))) / 2;
        } else {
            for (int i = 0; i < nJoinFactors; i++) {
                if (rel == joinFactors[i]) {
                    return nJoinFactors - i;
                }
            }
            return 0;
        }
    }
    
    /**
     * Dummy row count routine
     */
    private int getRowCount(RelNode rel)
    {
        // TODO - replace with real stats
        return getCost(rel);
    }
    
    //~ Inner Classes ----------------------------------------------------------
    
    private class FactorCostComparator implements Comparator
    {
        public int compare(Object o1, Object o2)
        {
            int rel1Idx = (Integer) o1;
            int rel2Idx = (Integer) o2;
            int c1 = getCost(chosenSemiJoins[rel1Idx]);
            int c2 = getCost(chosenSemiJoins[rel2Idx]);
            return (c1 < c2) ? -1 : ((c1 > c2) ? 1 : 0);
        }
    }
}

// End OptimizeJoinRule.java
