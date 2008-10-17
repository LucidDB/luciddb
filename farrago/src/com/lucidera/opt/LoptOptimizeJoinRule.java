/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2007 LucidEra, Inc.
// Copyright (C) 2005-2007 The Eigenbase Project
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

import org.eigenbase.rel.*;
import org.eigenbase.rel.metadata.*;
import org.eigenbase.rel.rules.*;
import org.eigenbase.relopt.*;
import org.eigenbase.reltype.*;
import org.eigenbase.rex.*;
import org.eigenbase.sql.fun.*;


/**
 * LoptOptimizeJoinRule implements the heuristic planner for determining optimal
 * join orderings. It is triggered by the pattern ProjectRel(MultiJoinRel).
 *
 * @author Zelaine Fong
 * @version $Id$
 */
public class LoptOptimizeJoinRule
    extends RelOptRule
{
    //~ Constructors -----------------------------------------------------------

    public LoptOptimizeJoinRule()
    {
        super(new RelOptRuleOperand(MultiJoinRel.class, ANY));
    }

    //~ Methods ----------------------------------------------------------------

    // implement RelOptRule
    public void onMatch(RelOptRuleCall call)
    {
        MultiJoinRel multiJoinRel = (MultiJoinRel) call.rels[0];
        LoptMultiJoin multiJoin = new LoptMultiJoin(multiJoinRel);

        RexBuilder rexBuilder = multiJoinRel.getCluster().getRexBuilder();
        LoptSemiJoinOptimizer semiJoinOpt =
            new LoptSemiJoinOptimizer(multiJoin, rexBuilder);

        // determine all possible semijoins
        semiJoinOpt.makePossibleSemiJoins(multiJoin);

        // select the optimal join filters for semijoin filtering by
        // iteratively calling chooseBestSemiJoin; chooseBestSemiJoin will
        // apply semijoins in sort order, based on the cost of scanning each
        // factor; as it selects semijoins to apply and iterates through the
        // loop, the cost of scanning a factor will decrease in accordance
        // with the semijoins selected
        int iterations = 0;
        do {
            if (!semiJoinOpt.chooseBestSemiJoin(multiJoin)) {
                break;
            }
            if (iterations++ > 10) {
                break;
            }
        } while (true);

        multiJoin.setFactorWeights();

        findRemovableOuterJoins(multiJoin);

        findBestOrderings(multiJoin, semiJoinOpt, call);
    }

    /**
     * Locates all null generating factors whose outer join can be removed. The
     * outer join can be removed if the join keys corresponding to the null
     * generating factor are unique and no columns are projected from it.
     *
     * @param multiJoin join factors being optimized
     */
    private void findRemovableOuterJoins(LoptMultiJoin multiJoin)
    {
        List<Integer> removalCandidates = new ArrayList<Integer>();
        for (int factIdx = 0;
            factIdx < multiJoin.getNumJoinFactors();
            factIdx++)
        {
            if (multiJoin.isNullGenerating(factIdx)) {
                removalCandidates.add(factIdx);
            }
        }
        
        while (!removalCandidates.isEmpty()) {
            Set<Integer> retryCandidates = new HashSet<Integer>();
            
outerForLoop:
            for (int factIdx : removalCandidates) {
                // reject the factor if it is referenced in the projection list
                BitSet projFields = multiJoin.getProjFields(factIdx);
                if ((projFields == null) || (projFields.cardinality() > 0)) {     
                    continue;
                }

                // setup a bitmap containing the equi-join keys corresponding to
                // the null generating factor; both operands in the filter must
                // be RexInputRefs and only one side corresponds to the null
                // generating factor
                RexNode outerJoinCond = multiJoin.getOuterJoinCond(factIdx);
                List<RexNode> ojFilters = new ArrayList<RexNode>();
                RelOptUtil.decomposeConjunction(outerJoinCond, ojFilters);
                int numFields = multiJoin.getNumFieldsInJoinFactor(factIdx);
                BitSet joinKeys = new BitSet(numFields);
                BitSet otherJoinKeys =
                    new BitSet(multiJoin.getNumTotalFields());
                int firstFieldNum = multiJoin.getJoinStart(factIdx);
                int lastFieldNum = firstFieldNum + numFields;
                for (RexNode filter : ojFilters) {
                    if (!(filter instanceof RexCall)) {
                        continue;
                    }
                    RexCall filterCall = (RexCall) filter;
                    if ((filterCall.getOperator()
                            != SqlStdOperatorTable.equalsOperator)
                        || !(filterCall.getOperands()[0]
                            instanceof RexInputRef)
                        || !(filterCall.getOperands()[1]
                            instanceof RexInputRef))
                    {
                        continue;
                    }
                    int leftRef =
                        ((RexInputRef) filterCall.getOperands()[0]).getIndex();
                    int rightRef =
                        ((RexInputRef) filterCall.getOperands()[1]).getIndex();
                    setJoinKey(
                        joinKeys,
                        otherJoinKeys,
                        leftRef,
                        rightRef,
                        firstFieldNum,
                        lastFieldNum,
                        true);
                }

                if (joinKeys.cardinality() == 0) {
                    continue;
                }

                // make sure the only join fields referenced are the ones in
                // the current outer join
                int [] joinFieldRefCounts =
                    multiJoin.getJoinFieldRefCounts(factIdx);
                for (int i = 0; i < joinFieldRefCounts.length; i++) {
                    if ((joinFieldRefCounts[i] > 1) ||
                        (!joinKeys.get(i) && joinFieldRefCounts[i] == 1))
                    {
                        continue outerForLoop;
                    }
                }

                // see if the join keys are unique
                if (RelMdUtil.areColumnsDefinitelyUnique(
                        multiJoin.getJoinFactor(factIdx),
                        joinKeys))
                {
                    multiJoin.addRemovableOuterJoinFactor(factIdx);
                    // Since we are no longer joining this factor,
                    // decrement the reference counters corresponding to
                    // the join keys from the other factors that join with
                    // this one.  Later, in the outermost loop, we'll have
                    // the opportunity to retry removing those factors.
                    for (int otherKey = otherJoinKeys.nextSetBit(0);
                        otherKey >= 0;
                        otherKey = otherJoinKeys.nextSetBit(otherKey + 1))
                    {
                        int otherFactor = multiJoin.findRef(otherKey);
                        if (multiJoin.isNullGenerating(otherFactor)) {
                            retryCandidates.add(otherFactor);
                        }
                        int [] otherJoinFieldRefCounts =
                            multiJoin.getJoinFieldRefCounts(otherFactor);
                        int offset = multiJoin.getJoinStart(otherFactor);
                        --otherJoinFieldRefCounts[otherKey - offset];
                    }
                }
            }
            removalCandidates.clear();
            removalCandidates.addAll(retryCandidates);
        }
    }

    /**
     * Sets a join key if only one of the specified input references corresponds
     * to a specified factor as determined by its field numbers.  Also keeps
     * track of the keys from the other factor.
     *
     * @param joinKeys join keys to be set if a key is found
     * @param otherJoinKeys join keys for the other join factor
     * @param ref1 first input reference
     * @param ref2 second input reference
     * @param firstFieldNum first field number of the factor
     * @param lastFieldNum last field number + 1 of the factor
     * @param swap if true, check for the desired input reference in the second
     * input reference parameter if the first input reference isn't the correct
     * one
     */
    private void setJoinKey(
        BitSet joinKeys,
        BitSet otherJoinKeys,
        int ref1,
        int ref2,
        int firstFieldNum,
        int lastFieldNum,
        boolean swap)
    {
        if ((ref1 >= firstFieldNum) && (ref1 < lastFieldNum)) {
            if (!((ref2 >= firstFieldNum) && (ref2 < lastFieldNum))) {
                joinKeys.set(ref1 - firstFieldNum);
                otherJoinKeys.set(ref2);
            }
            return;
        }
        if (swap) {
            setJoinKey(
                joinKeys,
                otherJoinKeys,
                ref2,
                ref1,
                firstFieldNum,
                lastFieldNum,
                false);
        }
    }

    /**
     * Generates N optimal join orderings. Each ordering contains each factor as
     * the first factor in the ordering.
     *
     * @param multiJoin join factors being optimized
     * @param semiJoinOpt optimal semijoins for each factor
     * @param call RelOptRuleCall associated with this rule
     */
    private void findBestOrderings(
        LoptMultiJoin multiJoin,
        LoptSemiJoinOptimizer semiJoinOpt,
        RelOptRuleCall call)
    {
        List<RelNode> plans = new ArrayList<RelNode>();

        String [] fieldNames =
            RelOptUtil.getFieldNames(
                multiJoin.getMultiJoinRel().getRowType());

        // generate the N join orderings
        for (int i = 0; i < multiJoin.getNumJoinFactors(); i++) {
            // first factor cannot be null generating
            if (multiJoin.isNullGenerating(i)) {
                continue;
            }
            LoptJoinTree joinTree =
                createOrdering(
                    multiJoin,
                    semiJoinOpt,
                    i);
            if (joinTree == null) {
                continue;
            }

            ProjectRel newProject =
                createTopProject(multiJoin, joinTree, fieldNames);
            plans.add(newProject);
        }

        // transform the selected plans; note that we wait till then the end to
        // transform everything so any intermediate RelNodes we create are not
        // converted to RelSubsets The HEP planner will choose the join subtree
        // with the best cumulative cost. Volcano planner keeps the alternative
        // join subtrees and cost the final plan to pick the best one.
        for (RelNode plan : plans) {
            call.transformTo(plan);
        }
    }

    /**
     * Creates the topmost projection that will sit on top of the selected join
     * ordering. The projection needs to match the original join ordering.
     *
     * @param multiJoin join factors being optimized
     * @param joinTree selected join ordering
     * @param fieldNames fieldnames corresponding to the proejction expressions
     *
     * @return created projection
     */
    private ProjectRel createTopProject(
        LoptMultiJoin multiJoin,
        LoptJoinTree joinTree,
        String [] fieldNames)
    {
        int nTotalFields = multiJoin.getNumTotalFields();
        RexNode [] newProjExprs;
        RexBuilder rexBuilder =
            multiJoin.getMultiJoinRel().getCluster().getRexBuilder();

        // create a projection on top of the joins, matching the original
        // join order
        newProjExprs = new RexNode[nTotalFields];
        List<Integer> newJoinOrder = new ArrayList<Integer>();
        joinTree.getTreeOrder(newJoinOrder);
        int currField = 0;
        int nJoinFactors = multiJoin.getNumJoinFactors();
        RelDataTypeField [] fields = multiJoin.getMultiJoinFields();
        for (int currFactor = 0; currFactor < nJoinFactors; currFactor++) {
            // locate the join factor in the new join ordering
            int fieldStart = 0;
            for (int pos = 0; pos < nJoinFactors; pos++) {
                if (newJoinOrder.get(pos).intValue() == currFactor) {
                    break;
                }
                fieldStart +=
                    multiJoin.getNumFieldsInJoinFactor(newJoinOrder.get(pos));
            }

            for (
                int fieldPos = 0;
                fieldPos < multiJoin.getNumFieldsInJoinFactor(currFactor);
                fieldPos++)
            {
                newProjExprs[currField] =
                    rexBuilder.makeInputRef(
                        fields[currField].getType(),
                        fieldStart + fieldPos);
                currField++;
            }
        }

        ProjectRel newProject =
            (ProjectRel) CalcRel.createProject(
                joinTree.getJoinTree(),
                newProjExprs,
                fieldNames);

        return newProject;
    }

    /**
     * Computes the cardinality of the join columns from a particular factor,
     * when that factor is joined with another join tree.
     *
     * @param multiJoin join factors being optimized
     * @param semiJoinOpt optimal semijoins chosen for each factor
     * @param joinTree the join tree that the factor is being joined with
     * @param filters possible join filters to select from
     * @param factor the factor being added
     *
     * @return computed cardinality
     */
    private Double computeJoinCardinality(
        LoptMultiJoin multiJoin,
        LoptSemiJoinOptimizer semiJoinOpt,
        LoptJoinTree joinTree,
        List<RexNode> filters,
        int factor)
    {
        int nJoinFactors = multiJoin.getNumJoinFactors();
        BitSet childFactors = new BitSet(nJoinFactors);
        multiJoin.getChildFactors(joinTree, childFactors);  
        childFactors.set(factor);

        int factorStart = multiJoin.getJoinStart(factor);
        int nFields = multiJoin.getNumFieldsInJoinFactor(factor);
        BitSet joinKeys = new BitSet(nFields);
        
        // first loop through the inner join filters, picking out the ones
        // that reference only the factors in either the join tree or the
        // factor that will be added
        setFactorJoinKeys(
            multiJoin,
            filters,
            childFactors,
            factorStart,
            nFields,
            joinKeys);
        
        // then loop through the outer join filters where the factor being
        // added is the null generating factor in the outer join
        RexNode outerJoinCond = multiJoin.getOuterJoinCond(factor);
        List<RexNode> outerJoinFilters = new ArrayList<RexNode>();
        RelOptUtil.decomposeConjunction(outerJoinCond, outerJoinFilters);
        setFactorJoinKeys(
            multiJoin,
            outerJoinFilters,
            childFactors,
            factorStart,
            nFields,
            joinKeys);
        
        // if the join tree doesn't contain all the necessary factors in 
        // any of the join filters, then joinKeys will be empty, so return
        // null in that case
        if (joinKeys.isEmpty()) {
            return null;
        } else {
            return RelMetadataQuery.getDistinctRowCount(
                semiJoinOpt.getChosenSemiJoin(factor),
                joinKeys,
                null);
        }
    }
    
    /**
     * Locates from a list of filters those that correspond to a particular
     * join tree.  Then, for each of those filters, extracts the fields
     * corresponding to a particular factor, setting them in a bitmap.
     * 
     * @param multiJoin join factors being optimized
     * @param filters list of join filters
     * @param joinFactors bitmap containing the factors in a particular join
     * tree
     * @param factorStart the initial offset of the factor whose join keys
     * will be extracted
     * @param nFields the number of fields in the factor whose join keys will
     * be extracted
     * @param joinKeys the bitmap that will be set with the join keys
     */
    private void setFactorJoinKeys(
        LoptMultiJoin multiJoin,
        List<RexNode> filters,
        BitSet joinFactors,
        int factorStart,
        int nFields,
        BitSet joinKeys)
    {
        ListIterator<RexNode> filterIter = filters.listIterator();
        while (filterIter.hasNext()) {
            RexNode joinFilter = filterIter.next();
            BitSet filterFactors =
                multiJoin.getFactorsRefByJoinFilter(joinFilter);

            // if all factors in the join filter are in the bitmap containing
            // the factors in a join tree, then from that filter, add the
            // fields corresponding to the specified factor to the join key
            // bitmap; in doing so, adjust the join keys so they start at
            // offset 0
            if (RelOptUtil.contains(joinFactors, filterFactors)) {
                BitSet joinFields =
                    multiJoin.getFieldsRefByJoinFilter(joinFilter);
                for (int field = joinFields.nextSetBit(factorStart);
                    field >= 0 && field < factorStart + nFields;
                    field = joinFields.nextSetBit(field + 1))
                {
                    joinKeys.set(field - factorStart);
                }
            }
        }
    }

    /**
     * Generates a join tree with a specific factor as the first factor in the
     * join tree
     *
     * @param multiJoin join factors being optimized
     * @param semiJoinOpt optimal semijoins for each factor
     * @param firstFactor first factor in the tree
     *
     * @return constructed join tree or null if it is not possible for
     * firstFactor to appear as the first factor in the join
     */
    private LoptJoinTree createOrdering(
        LoptMultiJoin multiJoin,
        LoptSemiJoinOptimizer semiJoinOpt,
        int firstFactor)
    {
        LoptJoinTree joinTree = null;
        int nJoinFactors = multiJoin.getNumJoinFactors();
        BitSet factorsToAdd = new BitSet(nJoinFactors);
        BitSet factorsAdded = new BitSet(nJoinFactors);
        factorsToAdd.flip(0, nJoinFactors);
        List<RexNode> filtersToAdd =
            new ArrayList<RexNode>(multiJoin.getJoinFilters());
        int [][] factorWeights = multiJoin.getFactorWeights();

        while (factorsToAdd.cardinality() > 0) {
            int nextFactor = -1;
            if (factorsAdded.cardinality() == 0) {
                nextFactor = firstFactor;
            } else {
                // iterate through the remaining factors and determine the
                // best one to add next
                int bestWeight = 0;
                Double bestCardinality = null;
                for (
                    int factor = factorsToAdd.nextSetBit(0);
                    factor >= 0;
                    factor = factorsToAdd.nextSetBit(factor + 1))
                {
                    // if the factor corresponds to a dimension table whose
                    // join we can remove, make sure the the corresponding fact
                    // table is in the current join tree
                    Integer factIdx = multiJoin.getJoinRemovalFactor(factor);
                    if (factIdx != null) {
                        if (!factorsAdded.get(factIdx)) {
                            continue;
                        }
                    }

                    // can't add a null-generating factor if its dependent,
                    // non-null generating factors haven't been added yet
                    if (multiJoin.isNullGenerating(factor)) {
                        BitSet tmp =
                            (BitSet) multiJoin.getOuterJoinFactors(factor)
                            .clone();
                        tmp.andNot(factorsAdded);
                        if (tmp.cardinality() != 0) {
                            continue;
                        }
                    }

                    // determine the best weight between the current factor
                    // under consideration and the factors that have already
                    // been added to the tree
                    int dimWeight = 0;
                    for (
                        int prevFactor = factorsAdded.nextSetBit(0);
                        prevFactor >= 0;
                        prevFactor = factorsAdded.nextSetBit(prevFactor + 1))
                    {
                        if (factorWeights[prevFactor][factor] > dimWeight) {
                            dimWeight = factorWeights[prevFactor][factor];
                        }
                    }

                    // only compute the join cardinality if we know that
                    // this factor joins with some part of the current join
                    // tree and is potentially better than other factors
                    // already considered
                    Double cardinality = null;
                    if (dimWeight > 0 &&
                        (dimWeight > bestWeight || dimWeight == bestWeight))
                    {
                        cardinality = 
                            computeJoinCardinality(
                                multiJoin,
                                semiJoinOpt,
                                joinTree,
                                filtersToAdd,
                                factor);
                    }
                    
                    // if two factors have the same weight, pick the one
                    // with the higher cardinality join key, relative to
                    // the join being considered
                    if ((dimWeight > bestWeight)
                        || ((dimWeight == bestWeight)
                            && ((bestCardinality == null)
                                || ((cardinality != null)
                                    && (cardinality > bestCardinality)))))
                    {
                        nextFactor = factor;
                        bestWeight = dimWeight;
                        bestCardinality = cardinality;
                    }
                }
            }

            // add the factor; pass in a bitmap representing the factors
            // this factor joins with that have already been added to
            // the tree
            BitSet factorsNeeded =
                (BitSet) multiJoin.getFactorsRefByFactor(nextFactor).clone();
            if (multiJoin.isNullGenerating(nextFactor)) {
                factorsNeeded.or(multiJoin.getOuterJoinFactors(nextFactor));
            }
            factorsNeeded.and(factorsAdded);
            joinTree =
                addFactorToTree(
                    multiJoin,
                    semiJoinOpt,
                    joinTree,
                    nextFactor,
                    factorsNeeded,
                    filtersToAdd);
            if (joinTree == null) {
                return null;
            }
            factorsToAdd.clear(nextFactor);
            factorsAdded.set(nextFactor);
        }

        assert(filtersToAdd.size() == 0);
        return joinTree;
    }

    /**
     * Returns true if a relnode corresponds to a JoinRel that wasn't one of the
     * original MultiJoinRel input factors
     */
    private boolean isJoinTree(RelNode rel)
    {
        // full outer joins were already optimized in a prior instantiation
        // of this rule; therefore we should never see a join input that's
        // a full outer join
        if (rel instanceof JoinRel) {
            assert (((JoinRel) rel).getJoinType() != JoinRelType.FULL);
            return true;
        } else {
            return false;
        }
    }

    /**
     * Adds a new factor into the current join tree. The factor is either pushed
     * down into one of the subtrees of the join recursively, or it is added to
     * the top of the current tree, whichever yields a better ordering.
     *
     * @param multiJoin join factors being optimized
     * @param semiJoinOpt optimal semijoins for each factor
     * @param joinTree current join tree
     * @param factorToAdd new factor to be added
     * @param factorsNeeded factors that must precede the factor to be added
     * @param filtersToAdd filters remaining to be added; filters added to the
     * new join tree are removed from the list
     *
     * @return optimal join tree with the new factor added if it is possible to
     * add the factor; otherwise, null is returned
     */
    private LoptJoinTree addFactorToTree(
        LoptMultiJoin multiJoin,
        LoptSemiJoinOptimizer semiJoinOpt,
        LoptJoinTree joinTree,
        int factorToAdd,
        BitSet factorsNeeded,
        List<RexNode> filtersToAdd)
    {
        // if the factor corresponds to the null generating factor in an outer
        // join that can be removed, then create a replacement join
        if (multiJoin.isRemovableOuterJoinFactor(factorToAdd)) {
            return createReplacementJoin(
                multiJoin,
                semiJoinOpt,
                joinTree,
                -1,
                factorToAdd,
                new ArrayList<Integer>(),
                null,
                filtersToAdd);
        }

        // if the factor corresponds to a dimension table whose join we
        // can remove, create a replacement join if the corresponding fact
        // table is in the current join tree
        if (multiJoin.getJoinRemovalFactor(factorToAdd) != null) {
            return createReplacementSemiJoin(
                multiJoin,
                semiJoinOpt,
                joinTree,
                factorToAdd,
                filtersToAdd);
        }

        // if this is the first factor in the tree, create a join tree with
        // the single factor
        if (joinTree == null) {
            return new LoptJoinTree(
                semiJoinOpt.getChosenSemiJoin(factorToAdd),
                factorToAdd);
        }

        // create a temporary copy of the filter list as we need the original
        // list to pass into addToTop()
        List<RexNode> tmpFilters = new ArrayList<RexNode>(filtersToAdd);
        LoptJoinTree pushDownTree =
            pushDownFactor(
                multiJoin,
                semiJoinOpt,
                joinTree,
                factorToAdd,
                factorsNeeded,
                tmpFilters);

        LoptJoinTree topTree =
            addToTop(
                multiJoin,
                semiJoinOpt,
                joinTree,
                factorToAdd,
                filtersToAdd);

        // pick the lower cost option, and replace the join ordering with
        // the ordering associated with the best option
        LoptJoinTree bestTree;
        RelOptCost costPushDown = null;
        if (pushDownTree != null) {
            costPushDown =
                RelMetadataQuery.getCumulativeCost(pushDownTree.getJoinTree());
        }
        RelOptCost costTop =
            RelMetadataQuery.getCumulativeCost(topTree.getJoinTree());
        
        bestTree = topTree;
        if ((costPushDown != null) && (costTop != null))  {
            if (costPushDown.isEqWithEpsilon(costTop)) {
                // if both plans cost the same (with an allowable round-off
                // margin of error), favor the one that passes
                // around the wider rows further up in the tree
                if (rowWidthCost(pushDownTree.getJoinTree()) <
                    rowWidthCost(topTree.getJoinTree()))
                {
                    bestTree = pushDownTree;
                }
            } else if (costPushDown.isLt(costTop)) {
                bestTree = pushDownTree;
            }
        }
        
        return bestTree;
    }
        
    /**
     * Computes a cost for a join tree based on the row widths of the
     * inputs into the join.  Joins where the inputs have the fewest number
     * of columns lower in the tree are better than equivalent joins where
     * the inputs with the larger number of columns are lower in the tree.
     * 
     * @param tree a tree of RelNodes
     * 
     * @return the cost associated with the width of the tree
     */
    private int rowWidthCost(RelNode tree)
    {
        // The width cost is the width of the tree itself plus the widths
        // of its children.  Hence, skinnier rows are better when they're
        // lower in the tree since the width of a RelNode contributes to
        // the cost of each JoinRel that appears above that RelNode.
        int width = tree.getRowType().getFieldCount();
        if (isJoinTree(tree)) {
            JoinRel joinRel = (JoinRel) tree;
            width +=
                rowWidthCost(joinRel.getLeft()) +
                rowWidthCost(joinRel.getRight());
        }
        return width;
    }

    /**
     * Creates a join tree where the new factor is pushed down one of the
     * operands of the current join tree
     *
     * @param multiJoin join factors being optimized
     * @param semiJoinOpt optimal semijoins for each factor
     * @param joinTree current join tree
     * @param factorToAdd new factor to be added
     * @param factorsNeeded factors that must precede the factor to be added
     * @param filtersToAdd filters remaining to be added; filters that are added
     * to the join tree are removed from the list
     *
     * @return optimal join tree with thew new factor pushed down the current
     * join tree if it is possible to do the pushdown; otherwise, null is
     * returned
     */
    private LoptJoinTree pushDownFactor(
        LoptMultiJoin multiJoin,
        LoptSemiJoinOptimizer semiJoinOpt,
        LoptJoinTree joinTree,
        int factorToAdd,
        BitSet factorsNeeded,
        List<RexNode> filtersToAdd)
    {
        // pushdown option only works if we already have a join tree
        if (!isJoinTree(joinTree.getJoinTree())) {
            return null;
        }
        int childNo = -1;
        LoptJoinTree left = joinTree.getLeft();
        LoptJoinTree right = joinTree.getRight();
        JoinRelType joinType = ((JoinRel) joinTree.getJoinTree()).getJoinType();

        // if there are no constraints as to which side the factor must
        // be pushed, arbitrarily push to the left
        if ((factorsNeeded.cardinality() == 0)
            && !joinType.generatesNullsOnLeft())
        {
            childNo = 0;
        } else {
            // push to the left if the LHS contains all factors that the
            // current factor needs and that side is not null-generating;
            // same check for RHS
            if (multiJoin.hasAllFactors(left, factorsNeeded)
                && !joinType.generatesNullsOnLeft())
            {
                childNo = 0;
            } else if (
                multiJoin.hasAllFactors(right, factorsNeeded)
                && !joinType.generatesNullsOnRight())
            {
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
        subTree =
            addFactorToTree(
                multiJoin,
                semiJoinOpt,
                subTree,
                factorToAdd,
                factorsNeeded,
                filtersToAdd);

        if (childNo == 0) {
            left = subTree;
        } else {
            right = subTree;
        }

        // adjust the join condition from the original join tree to reflect
        // pushdown of the new factor as well as any swapping that may have
        // been done during the pushdown
        RexNode newCondition =
            ((JoinRel) joinTree.getJoinTree()).getCondition();
        newCondition =
            adjustFilter(
                multiJoin,
                left,
                right,
                newCondition,
                factorToAdd,
                origJoinOrder,
                joinTree.getJoinTree().getRowType().getFields());

        // determine if additional filters apply as a result of adding the
        // new factor, provided this isn't a left or right outer join; for
        // those cases, the additional filters will be added on top of the
        // join in createJoinSubtree
        if ((joinType != JoinRelType.LEFT) && (joinType != JoinRelType.RIGHT)) {
            RexNode condition =
                addFilters(
                    multiJoin,
                    left,
                    -1,
                    right,
                    filtersToAdd,
                    true);
            RexBuilder rexBuilder =
                multiJoin.getMultiJoinRel().getCluster().getRexBuilder();
            newCondition =
                RelOptUtil.andJoinFilters(
                    rexBuilder,
                    newCondition,
                    condition);
        }

        // create the new join tree with the factor pushed down
        return createJoinSubtree(
            multiJoin,
            left,
            right,
            newCondition,
            joinType,
            filtersToAdd,
            false);
    }

    /**
     * Creates a join tree with the new factor added to the top of the tree
     *
     * @param multiJoin join factors being optimized
     * @param semiJoinOpt optimal semijoins for each factor
     * @param joinTree current join tree
     * @param factorToAdd new factor to be added
     * @param filtersToAdd filters remaining to be added; modifies the list to
     * remove filters that can be added to the join tree
     *
     * @return new join tree
     */
    private LoptJoinTree addToTop(
        LoptMultiJoin multiJoin,
        LoptSemiJoinOptimizer semiJoinOpt,
        LoptJoinTree joinTree,
        int factorToAdd,
        List<RexNode> filtersToAdd)
    {
        // if the factor being added is null-generating, create the join
        // as a left outer join since it's being added to the RHS side of
        // the join; createJoinSubTree may swap the inputs and therefore
        // convert the left outer join to a right outer join; if the original
        // MultiJoinRel was a full outer join, these should be the only
        // factors in the join, so create the join as a full outer join
        JoinRelType joinType;
        if (multiJoin.getMultiJoinRel().isFullOuterJoin()) {
            assert (multiJoin.getNumJoinFactors() == 2);
            joinType = JoinRelType.FULL;
        } else if (multiJoin.isNullGenerating(factorToAdd)) {
            joinType = JoinRelType.LEFT;
        } else {
            joinType = JoinRelType.INNER;
        }

        LoptJoinTree rightTree =
            new LoptJoinTree(
                semiJoinOpt.getChosenSemiJoin(factorToAdd),
                factorToAdd);

        // in the case of a left or right outer join, use the specific
        // outer join condition
        RexNode condition;
        if ((joinType == JoinRelType.LEFT) || (joinType == JoinRelType.RIGHT)) {
            condition = multiJoin.getOuterJoinCond(factorToAdd);
        } else {
            condition =
                addFilters(
                    multiJoin,
                    joinTree,
                    -1,
                    rightTree,
                    filtersToAdd,
                    false);
        }

        return createJoinSubtree(
            multiJoin,
            joinTree,
            rightTree,
            condition,
            joinType,
            filtersToAdd,
            true);
    }

    /**
     * Determines which join filters can be added to the current join tree. Note
     * that the join filter still reflects the original join ordering. It will
     * only be adjusted to reflect the new join ordering if the "adjust"
     * parameter is set to true.
     *
     * @param multiJoin join factors being optimized
     * @param leftTree left subtree of the join tree
     * @param leftIdx if >=0, only consider filters that reference leftIdx in
     * leftTree; otherwise, consider all filters that reference any factor in
     * leftTree
     * @param rightTree right subtree of the join tree
     * @param filtersToAdd remaining join filters that need to be added; those
     * that are added are removed from the list
     * @param adjust if true, adjust filter to reflect new join ordering
     *
     * @return AND'd expression of the join filters that can be added to the
     * current join tree
     */
    private RexNode addFilters(
        LoptMultiJoin multiJoin,
        LoptJoinTree leftTree,
        int leftIdx,
        LoptJoinTree rightTree,
        List<RexNode> filtersToAdd,
        boolean adjust)
    {
        // loop through the remaining filters to be added and pick out the
        // ones that reference only the factors in the new join tree
        RexNode condition = null;
        ListIterator<RexNode> filterIter = filtersToAdd.listIterator();
        int nJoinFactors = multiJoin.getNumJoinFactors();
        RexBuilder rexBuilder =
            multiJoin.getMultiJoinRel().getCluster().getRexBuilder();
        BitSet childFactors = new BitSet(nJoinFactors);
        if (leftIdx >= 0) {
            childFactors.set(leftIdx);
        } else {
            multiJoin.getChildFactors(leftTree, childFactors);
        }  
        multiJoin.getChildFactors(rightTree, childFactors);

        while (filterIter.hasNext()) {
            RexNode joinFilter = filterIter.next();
            BitSet filterBitmap =
                multiJoin.getFactorsRefByJoinFilter(joinFilter);

            // if all factors in the join filter are in the join tree,
            // AND the filter to the current join condition
            if (RelOptUtil.contains(childFactors, filterBitmap)) {
                if (condition == null) {
                    condition = joinFilter;
                } else {
                    condition =
                        rexBuilder.makeCall(
                            SqlStdOperatorTable.andOperator,
                            condition,
                            joinFilter);
                }
                filterIter.remove();
            }
        }

        if (adjust && (condition != null)) {
            int [] adjustments = new int[multiJoin.getNumTotalFields()];
            if (needsAdjustment(multiJoin, adjustments, leftTree, rightTree)) {
                condition =
                    condition.accept(
                        new RelOptUtil.RexInputConverter(
                            rexBuilder,
                            multiJoin.getMultiJoinFields(),
                            leftTree.getJoinTree().getRowType().getFields(),
                            rightTree.getJoinTree().getRowType().getFields(),
                            adjustments));
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
     * @param multiJoin join factors being optimized
     * @param left left subtree of the join
     * @param right right subtree of the join
     * @param condition current join condition
     * @param factorAdded index corresponding to the newly added factor
     * @param origJoinOrder original join order, before factor was pushed into
     * the tree
     * @param origFields fields from the original join before the factor was
     * added
     *
     * @return modified join condition reflecting addition of the new factor
     */
    private RexNode adjustFilter(
        LoptMultiJoin multiJoin,
        LoptJoinTree left,
        LoptJoinTree right,
        RexNode condition,
        int factorAdded,
        List<Integer> origJoinOrder,
        RelDataTypeField [] origFields)
    {
        List<Integer> newJoinOrder = new ArrayList<Integer>();
        left.getTreeOrder(newJoinOrder);
        right.getTreeOrder(newJoinOrder);

        int totalFields =
            left.getJoinTree().getRowType().getFields().length
            + right.getJoinTree().getRowType().getFields().length
            - multiJoin.getNumFieldsInJoinFactor(factorAdded);
        int [] adjustments = new int[totalFields];

        // go through each factor and adjust relative to the original
        // join order
        boolean needAdjust = false;
        int nFieldsNew = 0;
        for (int newPos = 0; newPos < newJoinOrder.size(); newPos++) {
            int nFieldsOld = 0;

            // no need to make any adjustments on the newly added factor
            if (newJoinOrder.get(newPos).intValue() != factorAdded) {
                for (int oldPos = 0; oldPos < origJoinOrder.size(); oldPos++) {
                    if (newJoinOrder.get(newPos).intValue()
                        == origJoinOrder.get(oldPos).intValue())
                    {
                        break;
                    }
                    nFieldsOld +=
                        multiJoin.getNumFieldsInJoinFactor(
                            origJoinOrder.get(oldPos));
                }
                if ((-nFieldsOld + nFieldsNew) != 0) {
                    needAdjust = true;
                    for (
                        int i = 0;
                        i
                        < multiJoin.getNumFieldsInJoinFactor(
                            newJoinOrder.get(newPos));
                        i++)
                    {
                        // subtract off the number of fields to the left
                        // in the original join order and then add on the
                        // number of fields on the left in the new join order
                        adjustments[i + nFieldsOld] = -nFieldsOld + nFieldsNew;
                    }
                }
            }
            nFieldsNew +=
                multiJoin.getNumFieldsInJoinFactor(
                    newJoinOrder.get(newPos));
        }

        if (needAdjust) {
            RexBuilder rexBuilder =
                multiJoin.getMultiJoinRel().getCluster().getRexBuilder();
            condition =
                condition.accept(
                    new RelOptUtil.RexInputConverter(
                        rexBuilder,
                        origFields,
                        left.getJoinTree().getRowType().getFields(),
                        right.getJoinTree().getRowType().getFields(),
                        adjustments));
        }

        return condition;
    }

    /**
     * In the event that a dimension table does not need to be joined because of
     * a semijoin, this method creates a join tree that consists of a projection
     * on top of an existing join tree. The existing join tree must contain the
     * fact table in the semijoin that allows the dimension table to be removed.
     *
     * <p>The projection created on top of the join tree mimics a join of the
     * fact and dimension tables. In order for the dimension table to have been
     * removed, the only fields referenced from the dimension table are its
     * dimension keys. Therefore, we can replace these dimension fields with the
     * fields corresponding to the semijoin keys from the fact table in the
     * projection.
     *
     * @param multiJoin join factors being optimized
     * @param semiJoinOpt optimal semijoins for each factor
     * @param factTree existing join tree containing the fact table
     * @param dimIdx dimension table factor id
     * @param filtersToAdd filters remaining to be added; filters added to the
     * new join tree are removed from the list
     *
     * @return created join tree or null if the corresponding fact table has not
     * been joined in yet
     */
    private LoptJoinTree createReplacementSemiJoin(
        LoptMultiJoin multiJoin,
        LoptSemiJoinOptimizer semiJoinOpt,
        LoptJoinTree factTree,
        int dimIdx,
        List<RexNode> filtersToAdd)
    {
        // if the current join tree doesn't contain the fact table, then
        // don't bother trying to create the replacement join just yet
        if (factTree == null) {
            return null;
        }

        int factIdx = multiJoin.getJoinRemovalFactor(dimIdx);
        List<Integer> joinOrder = new ArrayList<Integer>();
        factTree.getTreeOrder(joinOrder);
        assert(joinOrder.contains(factIdx));

        // figure out the position of the fact table in the current jointree
        int adjustment = 0;
        for (Integer factor : joinOrder) {
            if (factor == factIdx) {
                break;
            }
            adjustment += multiJoin.getNumFieldsInJoinFactor(factor);
        }

        // map the dimension keys to the corresponding keys from the fact
        // table, based on the fact table's position in the current jointree
        RelDataTypeField [] dimFields =
            multiJoin.getJoinFactor(dimIdx).getRowType().getFields();
        int nDimFields = dimFields.length;
        Integer [] replacementKeys = new Integer[nDimFields];
        SemiJoinRel semiJoin = multiJoin.getJoinRemovalSemiJoin(dimIdx);
        List<Integer> dimKeys = semiJoin.getRightKeys();
        List<Integer> factKeys = semiJoin.getLeftKeys();
        for (int i = 0; i < dimKeys.size(); i++) {
            replacementKeys[dimKeys.get(i)] = factKeys.get(i) + adjustment;
        }

        return createReplacementJoin(
            multiJoin,
            semiJoinOpt,
            factTree,
            factIdx,
            dimIdx,
            dimKeys,
            replacementKeys,
            filtersToAdd);
    }

    /**
     * Creates a replacement join, projecting either dummy columns or
     * replacement keys from the factor that doesn't actually need to be joined.
     *
     * @param multiJoin join factors being optimized
     * @param semiJoinOpt optimal semijoins for each factor
     * @param currJoinTree current join tree being added to
     * @param leftIdx if >=0, when creating the replacement join, only
     * consider filters that reference leftIdx in currJoinTree; otherwise,
     * consider all filters that reference any factor in currJoinTree
     * @param factorToAdd new factor whose join can be removed
     * @param newKeys join keys that need to be replaced
     * @param replacementKeys the keys that replace the join keys; null if we're
     * removing the null generating factor in an outer join
     * @param filtersToAdd filters remaining to be added; filters added to the
     * new join tree are removed from the list
     *
     * @return created join tree with an appropriate projection for the factor
     * that can be removed
     */
    private LoptJoinTree createReplacementJoin(
        LoptMultiJoin multiJoin,
        LoptSemiJoinOptimizer semiJoinOpt,
        LoptJoinTree currJoinTree,
        int leftIdx,
        int factorToAdd,
        List<Integer> newKeys,
        Integer [] replacementKeys,
        List<RexNode> filtersToAdd)
    {
        // create a projection, projecting the fields from the join tree
        // containing the current joinRel and the new factor; for fields
        // corresponding to join keys, replace them with the corresponding key
        // from the replacementKeys passed in; for other fields, just create a
        // null expression as a placeholder for the column; this is done so we
        // don't have to adjust the offsets of other expressions that reference
        // the new factor; the placeholder expression values should never be
        // referenced, so that's why it's ok to create these possibly invalid
        // expressions
        RelNode currJoinRel = currJoinTree.getJoinTree();
        RelDataTypeField [] currFields = currJoinRel.getRowType().getFields();
        int nCurrFields = currFields.length;
        RelDataTypeField [] newFields =
            multiJoin.getJoinFactor(factorToAdd).getRowType().getFields();
        int nNewFields = newFields.length;
        RexNode [] projExprs = new RexNode[nCurrFields + nNewFields];
        String [] fieldNames = new String[nCurrFields + nNewFields];
        RexBuilder rexBuilder = currJoinRel.getCluster().getRexBuilder();
        RelDataTypeFactory typeFactory = rexBuilder.getTypeFactory();

        for (int i = 0; i < nCurrFields; i++) {
            projExprs[i] = rexBuilder.makeInputRef(currFields[i].getType(), i);
            fieldNames[i] = currFields[i].getName();
        }
        for (int i = 0; i < nNewFields; i++) {
            RexNode projExpr;
            RelDataType newType = newFields[i].getType();
            if (!newKeys.contains(i)) {
                if (replacementKeys == null) {
                    // null generating factor in an outer join; so make the
                    // type nullable
                    newType =
                        typeFactory.createTypeWithNullability(newType, true);
                }
                projExpr =
                    rexBuilder.makeCast(newType, rexBuilder.constantNull());
            } else {
                RelDataTypeField mappedField = currFields[replacementKeys[i]];
                RexNode mappedInput =
                    rexBuilder.makeInputRef(
                        mappedField.getType(),
                        replacementKeys[i]);

                // if the types aren't the same, create a cast
                if (mappedField.getType() == newType) {
                    projExpr = mappedInput;
                } else {
                    projExpr =
                        rexBuilder.makeCast(
                            newFields[i].getType(),
                            mappedInput);
                }
            }
            projExprs[i + nCurrFields] = projExpr;
            fieldNames[i + nCurrFields] = newFields[i].getName();
        }
        ProjectRel projRel =
            (ProjectRel) CalcRel.createProject(
                currJoinRel,
                projExprs,
                fieldNames);

        // remove the join conditions corresponding to the join we're removing;
        // we don't actually need to use them, but we need to remove them
        // from the list since they're no longer needed
        LoptJoinTree newTree =
            new LoptJoinTree(
                semiJoinOpt.getChosenSemiJoin(factorToAdd),
                factorToAdd);
        addFilters(
            multiJoin,
            currJoinTree,
            leftIdx,
            newTree,
            filtersToAdd,
            false);
        
        // Filters referencing factors other than leftIdx and factorToAdd
        // still do need to be applied.  So, add them into a separate
        // FilterRel placed on top off the projection created above.
        RelNode topRelNode = projRel;
        if (leftIdx >= 0) {
            topRelNode = 
                addAdditionalFilters(
                    topRelNode,
                    multiJoin,
                    currJoinTree,
                    newTree,
                    filtersToAdd);
        }
        
        // finally, create a join tree consisting of the current join's join
        // tree with the newly created projection; note that in the factor
        // tree, we act as if we're joining in the new factor, even
        // though we really aren't; this is needed so we can map the columns
        // from the new factor as we go up in the join tree
        return new LoptJoinTree(
            topRelNode,
            currJoinTree.getFactorTree(),
            newTree.getFactorTree());
    }

    /**
     * Creates a JoinRel given left and right operands and a join condition.
     * Swaps the operands if beneficial.
     *
     * @param multiJoin join factors being optimized
     * @param left left operand
     * @param right right operand
     * @param condition join condition
     * @param joinType the join type
     * @param fullAdjust true if the join condition reflects the original join
     * ordering and therefore has not gone through any type of adjustment yet;
     * otherwise, the condition has already been partially adjusted and only
     * needs to be further adjusted if swapping is done
     * @param filtersToAdd additional filters that may be added on top of the
     * resulting JoinRel, if the join is a left or right outer join
     *
     * @return created JoinRel
     */
    private LoptJoinTree createJoinSubtree(
        LoptMultiJoin multiJoin,
        LoptJoinTree left,
        LoptJoinTree right,
        RexNode condition,
        JoinRelType joinType,
        List<RexNode> filtersToAdd,
        boolean fullAdjust)
    {
        RexBuilder rexBuilder =
            multiJoin.getMultiJoinRel().getCluster().getRexBuilder();

        // swap the inputs if beneficial
        if (swapInputs(left, right)) {
            LoptJoinTree tmp = right;
            right = left;
            left = tmp;
            if (!fullAdjust) {
                condition =
                    swapFilter(
                        rexBuilder,
                        multiJoin,
                        right,
                        left,
                        condition);
            }
            if ((joinType != JoinRelType.INNER)
                && (joinType != JoinRelType.FULL))
            {
                joinType =
                    (joinType == JoinRelType.LEFT) ? JoinRelType.RIGHT
                    : JoinRelType.LEFT;
            }
        }

        if (fullAdjust) {
            int [] adjustments = new int[multiJoin.getNumTotalFields()];
            if (needsAdjustment(multiJoin, adjustments, left, right)) {
                condition =
                    condition.accept(
                        new RelOptUtil.RexInputConverter(
                            rexBuilder,
                            multiJoin.getMultiJoinFields(),
                            left.getJoinTree().getRowType().getFields(),
                            right.getJoinTree().getRowType().getFields(),
                            adjustments));
            }
        }

        RelNode joinTree =
            new JoinRel(
                multiJoin.getMultiJoinRel().getCluster(),
                left.getJoinTree(),
                right.getJoinTree(),
                condition,
                joinType,
                Collections.<String>emptySet(),
                true);

        // if this is a left or right outer join, and additional filters can
        // be applied to the resulting join, then they need to be applied
        // as a filter on top of the outer join result
        if ((joinType == JoinRelType.LEFT) || (joinType == JoinRelType.RIGHT)) {
            joinTree =
                addAdditionalFilters(
                    joinTree,
                    multiJoin,
                    left,
                    right,
                    filtersToAdd);
        }

        return new LoptJoinTree(
            joinTree,
            left.getFactorTree(),
            right.getFactorTree());
    }
    
    /**
     * Determines if any additional filters are applicable to a jointree.  If
     * there are any, then create a filter node on top of the join tree with
     * the additional filters
     * 
     * @param joinTree current join tree
     * @param multiJoin join factors being optimized
     * @param left left side of join tree
     * @param right right side of join tree
     * @param filtersToAdd remaining filters
     * 
     * @return a filter node if additional filters are found; otherwise,
     * returns original joinTree
     */
    private RelNode addAdditionalFilters(
        RelNode joinTree,
        LoptMultiJoin multiJoin,
        LoptJoinTree left,
        LoptJoinTree right,
        List<RexNode> filtersToAdd)      
    {
        RexNode filterCond =
            addFilters(multiJoin, left, -1, right, filtersToAdd, false);
        if (filterCond.isAlwaysTrue()) {
            return joinTree;
        } else {
            // adjust the filter to reflect the outer join output
            int [] adjustments = new int[multiJoin.getNumTotalFields()];
            if (needsAdjustment(multiJoin, adjustments, left, right)) {
                RexBuilder rexBuilder =
                    multiJoin.getMultiJoinRel().getCluster().getRexBuilder();
                filterCond =
                    filterCond.accept(
                        new RelOptUtil.RexInputConverter(
                            rexBuilder,
                            multiJoin.getMultiJoinFields(),
                            joinTree.getRowType().getFields(),
                            adjustments));
            }
            joinTree = CalcRel.createFilter(joinTree, filterCond);
            return joinTree;
        }
    }

    /**
     * Swaps the operands to a join, so the smaller input is on the right.
     * 
     * <p>Note that unlike Broadbase, we do not swap if in the join condition,
     * the RHS references more columns than the LHS. This can help for
     * queries like (select * from A,B where A.A between B.X and B.Y).  By
     * putting B on the left, that would result in a sargable predicate
     * with two endpoints.  However, since {@link
     * org.eigenbase.sarg.SargRexAnalyzer} currently
     * doesn't handle these type of sargable predicates, there's no point in
     * doing the swap for this reason.
     *
     * @param left left side of join tree
     * @param right right hand side of join tree
     *
     * @return true if swapping should be done
     */
    private boolean swapInputs(LoptJoinTree left, LoptJoinTree right)
    {
        boolean swap = false;
        Double leftRowCount = RelMetadataQuery.getRowCount(left.getJoinTree());
        Double rightRowCount =
            RelMetadataQuery.getRowCount(right.getJoinTree());
        if ((leftRowCount != null) && (rightRowCount != null)
           && (leftRowCount < rightRowCount))
        {
            swap = true;
        }
        return swap;
    }

    /**
     * Adjusts a filter to reflect swapping of join inputs
     *
     * @param rexBuilder rexBuilder
     * @param multiJoin join factors being optimized
     * @param origLeft original LHS of the join tree (before swap)
     * @param origRight original RHS of the join tree (before swap)
     * @param condition original join condition
     *
     * @return join condition reflect swap of join inputs
     */
    private RexNode swapFilter(
        RexBuilder rexBuilder,
        LoptMultiJoin multiJoin,
        LoptJoinTree origLeft,
        LoptJoinTree origRight,
        RexNode condition)
    {
        int nFieldsOnLeft =
            origLeft.getJoinTree().getRowType().getFields().length;
        int nFieldsOnRight =
            origRight.getJoinTree().getRowType().getFields().length;
        int [] adjustments = new int[nFieldsOnLeft + nFieldsOnRight];

        for (int i = 0; i < nFieldsOnLeft; i++) {
            adjustments[i] = nFieldsOnRight;
        }
        for (int i = nFieldsOnLeft; i < (nFieldsOnLeft + nFieldsOnRight); i++) {
            adjustments[i] = -nFieldsOnLeft;
        }

        condition =
            condition.accept(
                new RelOptUtil.RexInputConverter(
                    rexBuilder,
                    multiJoin.getJoinFields(origLeft, origRight),
                    multiJoin.getJoinFields(origRight, origLeft),
                    adjustments));

        return condition;
    }

    /**
     * Sets an array indicating how much each factor in a join tree needs to be
     * adjusted to reflect the tree's join ordering
     *
     * @param multiJoin join factors being optimized
     * @param adjustments array to be filled out
     * @param joinTree join tree
     * @param otherTree null unless joinTree only represents the left side of
     * the join tree
     *
     * @return true if some adjustment is required; false otherwise
     */
    private boolean needsAdjustment(
        LoptMultiJoin multiJoin,
        int [] adjustments,
        LoptJoinTree joinTree,
        LoptJoinTree otherTree)
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
            int joinStart = multiJoin.getJoinStart(origPos);
            int adjustment = -joinStart + nFields;
            int nJoinFields = multiJoin.getNumFieldsInJoinFactor(origPos);
            nFields += nJoinFields;
            if (adjustment != 0) {
                needAdjustment = true;
                for (int i = 0; i < nJoinFields; i++) {
                    adjustments[joinStart + i] = adjustment;
                }
            }
        }

        return needAdjustment;
    }
}

// End LoptOptimizeJoinRule.java
