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

    public LoptOptimizeJoinRule(RelOptRuleOperand rule, String id)
    {
        // This rule is fired for either of the following two patterns:
        //
        // RelOptRuleOperand(
        //     ProjectRel.class,
        //     new RelOptRuleOperand [] {
        //         new RelOptRuleOperand(MultiJoinRel.class, null)})
        //
        // RelOptRuleOperand(MultiJoinRel.class, null)
        //
        super(rule);
        description = "LoptOptimizeJoinRule: " + id;
    }

    //~ Methods ----------------------------------------------------------------

    // implement RelOptRule
    public void onMatch(RelOptRuleCall call)
    {
        MultiJoinRel multiJoinRel;
        if (call.rels.length == 1) {
            multiJoinRel = (MultiJoinRel) call.rels[0];
        } else {
            multiJoinRel = (MultiJoinRel) call.rels[1];
        }
        
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

        findBestOrderings(multiJoin, semiJoinOpt, call);
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
        // Setup the fieldnames for the projection.  The type of projection
        // we create depends on whether this rule was matched with or
        // without a projection.  In the case of the former, the projection
        // will consist of all the join fields, reordered to match the
        // original join ordering
        int nJoinFactors = multiJoin.getNumJoinFactors();
        String [] fieldNames;
        if (call.rels.length == 1) {
            int nTotalFields = multiJoin.getNumTotalFields();
            fieldNames = new String[nTotalFields];
            int currField = 0;

            for (int i = 0; i < nJoinFactors; i++) {
                RelDataTypeField [] fields =
                    multiJoin.getJoinFactor(i).getRowType().getFields();
                for (int j = 0; j < multiJoin.getNumFieldsInJoinFactor(i);
                     j++) {
                    fieldNames[currField] = fields[j].getName();
                    currField++;
                }
            }
        } else {
            ProjectRel project = (ProjectRel) call.rels[0];
            int projLength = project.getProjectExps().length;
            fieldNames = new String[projLength];
            for (int i = 0; i < projLength; i++) {
                fieldNames[i] = project.getRowType().getFields()[i].getName();
            }
        }

        List<RelNode> plans = new ArrayList<RelNode>();

        Double [] cardJoinCols =
            computeJoinCardinalities(
                multiJoin,
                semiJoinOpt);

        // generate the N join orderings
        for (int i = 0; i < nJoinFactors; i++) {
            // first factor cannot be null generating
            if (multiJoin.isNullGenerating(i)) {
                continue;
            }
            LoptJoinTree joinTree =
                createOrdering(
                    multiJoin,
                    semiJoinOpt,
                    i,
                    cardJoinCols);
            if (joinTree == null) {
                continue;
            }

            ProjectRel newProject =
                createTopProject(
                    multiJoin,
                    joinTree,
                    fieldNames,
                    call);

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
     * Creates the topmost projection that will sit on top of the selected join
     * ordering. Depending on whether this rule was matched with or without a
     * projection, the projection expressions are created accordingly.
     *
     * @param multiJoin join factors being optimized
     * @param joinTree selected join ordering
     * @param fieldNames fieldnames corresponding to the proejction expressions
     * @param call relopt call
     *
     * @return created projection
     */
    private ProjectRel createTopProject(
        LoptMultiJoin multiJoin,
        LoptJoinTree joinTree,
        String [] fieldNames,
        RelOptRuleCall call)
    {
        int nTotalFields = multiJoin.getNumTotalFields();
        RexNode [] newProjExprs;
        RexBuilder rexBuilder =
            multiJoin.getMultiJoinRel().getCluster().getRexBuilder();

        if (call.rels.length == 1) {
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
                    if (newJoinOrder.get(pos) == currFactor) {
                        break;
                    }
                    fieldStart +=
                        multiJoin.getNumFieldsInJoinFactor(
                            newJoinOrder.get(pos));
                }
                
                for (int fieldPos = 0;
                     fieldPos < multiJoin.getNumFieldsInJoinFactor(currFactor);
                     fieldPos++) {
                    newProjExprs[currField] =
                        rexBuilder.makeInputRef(
                            fields[currField].getType(),
                            fieldStart + fieldPos);
                    currField++;
                }
            }
        } else {
            // maintain the original projection, but we need to adjust
            // the references based on the selected join ordering
            ProjectRel project = (ProjectRel) call.rels[0];
            int [] adjustments = new int[nTotalFields];
            RexNode [] origProjExprs = project.getProjectExps();
            if (needsAdjustment(multiJoin, adjustments, joinTree, null)) {
                // adjust projection expressions
                int projLength = project.getProjectExps().length;
                newProjExprs = new RexNode[projLength];
                for (int j = 0; j < projLength; j++) {
                    newProjExprs[j] =
                        origProjExprs[j].accept(
                            new RelOptUtil.RexInputConverter(
                                rexBuilder,
                                multiJoin.getMultiJoinFields(),
                                joinTree.getJoinTree().getRowType().getFields(),
                                adjustments));
                }
            } else {
                newProjExprs = origProjExprs;
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
     * Computes the cardinality of the columns that participate in join filters
     *
     * @param multiJoin join factors being optimized
     * @param semiJoinOpt optimal semijoins chosen for each factor
     *
     * @return computed cardinalities
     */
    private Double [] computeJoinCardinalities(
        LoptMultiJoin multiJoin,
        LoptSemiJoinOptimizer semiJoinOpt)
    {
        // OR together all fields referenced by all join filters
        int nTotalFields = multiJoin.getNumTotalFields();
        BitSet allJoinFields = new BitSet(nTotalFields);
        for (RexNode joinFilter : multiJoin.getJoinFilters()) {
            allJoinFields.or(multiJoin.getFieldsRefByJoinFilter(joinFilter));
        }

        // AND the fields referenced by all join filters with the fields
        // corresponding to each factor in order to get the join fields
        // referenced by that factor; then use that to determine the
        // cardinality of the factor, relative to the join columns it
        // references
        int fieldStart = 0;
        int nJoinFactors = multiJoin.getNumJoinFactors();
        Double [] cardJoinCols = new Double[nJoinFactors];
        for (int i = 0; i < nJoinFactors; i++) {
            BitSet factorFields = new BitSet(nTotalFields);
            int numFields = multiJoin.getNumFieldsInJoinFactor(i);
            RelOptUtil.setRexInputBitmap(
                factorFields,
                fieldStart,
                numFields);
            factorFields.and(allJoinFields);

            // except for the first factor, need to adjust the bits to the left
            if (i > 0) {
                for (int bit = factorFields.nextSetBit(fieldStart);
                     bit >= fieldStart; bit = factorFields.nextSetBit(bit + 1)) {
                    factorFields.clear(bit);
                    factorFields.set(bit - fieldStart);
                }
            }
            cardJoinCols[i] =
                RelMetadataQuery.getDistinctRowCount(
                    semiJoinOpt.getChosenSemiJoin(i),
                    factorFields,
                    null);
            fieldStart += numFields;
        }

        return cardJoinCols;
    }

    /**
     * Generates a join tree with a specific factor as the first factor in the
     * join tree
     *
     * @param multiJoin join factors being optimized
     * @param semiJoinOpt optimal semijoins for each factor
     * @param firstFactor first factor in the tree
     * @param cardinalities cardinalities of each of the factors, relative to
     * the join fields each references
     *
     * @return constructed join tree or null if it is not possible for
     * firstFactor to appear as the first factor in the join
     */
    private LoptJoinTree createOrdering(
        LoptMultiJoin multiJoin,
        LoptSemiJoinOptimizer semiJoinOpt,
        int firstFactor,
        Double [] cardinalities)
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
                for (int factor = factorsToAdd.nextSetBit(0); factor >= 0;
                    factor = factorsToAdd.nextSetBit(factor + 1))
                {
                    // can't add a null-generating factor if its dependent,
                    // non-null generating factors haven't been added yet
                    if (multiJoin.isNullGenerating(factor)) {
                        BitSet tmp =
                            (BitSet) multiJoin.getOuterJoinFactors(factor).
                                clone();
                        tmp.andNot(factorsAdded);
                        if (tmp.cardinality() != 0) {
                            continue;
                        }                       
                    }
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
                    if ((dimWeight > bestWeight)
                        || (
                            (dimWeight == bestWeight)
                            && (
                                (bestCardinality == null)
                                || (
                                    (cardinalities[factor] != null)
                                    && (cardinalities[factor] > bestCardinality)
                                   )
                               )
                           ))
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
            BitSet factorsNeeded = multiJoin.getFactorsRefByFactor(nextFactor);
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
            assert(((JoinRel) rel).getJoinType() != JoinRelType.FULL);
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
        if (joinTree == null) {
            return
                new LoptJoinTree(
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
        if ((costPushDown != null) && (costTop != null)
            && costPushDown.isLt(costTop)) {
            bestTree = pushDownTree;
        } else {
            bestTree = topTree;
        }

        return bestTree;
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
        JoinRelType joinType =
            ((JoinRel) joinTree.getJoinTree()).getJoinType();

        // if there are no constraints as to which side the factor must
        // be pushed, arbitrarily push to the left
        if (factorsNeeded.cardinality() == 0 &&
            !joinType.generatesNullsOnLeft())
        {
            childNo = 0;
        } else {
            // push to the left if the LHS contains all factors that the
            // current factor needs and that side is not null-generating;
            // same check for RHS
            if (multiJoin.hasAllFactors(left, factorsNeeded) &&
                !joinType.generatesNullsOnLeft())
            {
                childNo = 0;
            } else if (multiJoin.hasAllFactors(right, factorsNeeded) &&
                !joinType.generatesNullsOnRight())
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
        RexNode origCondition =
            ((JoinRel) joinTree.getJoinTree()).getCondition();
        origCondition =
            adjustFilter(
                multiJoin,
                left,
                right,
                origCondition,
                factorToAdd,
                origJoinOrder,
                joinTree.getJoinTree().getRowType().getFields());

        // determine if additional filters apply as a result of adding the
        // new factor
        RexNode condition =
            addFilters(
                multiJoin,
                left,
                right,
                filtersToAdd,
                true);
        RexBuilder rexBuilder =
            multiJoin.getMultiJoinRel().getCluster().getRexBuilder();
        condition =
            RelOptUtil.andJoinFilters(
                rexBuilder,
                origCondition,
                condition);

        // create the new join tree with the factor pushed down
        return createJoinSubtree(
            multiJoin,
            left,
            right,
            condition,
            joinType,
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
        LoptJoinTree rightTree =
            new LoptJoinTree(
                semiJoinOpt.getChosenSemiJoin(factorToAdd),
                factorToAdd);
        RexNode condition =
            addFilters(
                multiJoin,
                joinTree,
                rightTree,
                filtersToAdd,
                false);

        // if the factor being added is null-generating, create the join
        // as a left outer join since it's being added to the RHS side of
        // the join; createJoinSubTree may swap the inputs and therefore
        // convert the left outer join to a right outer join; if the original
        // MultiJoinRel was a full outer join, these should be the only
        // factors in the join, so create the join as a full outer join
        JoinRelType joinType;
        if (multiJoin.getMultiJoinRel().isFullOuterJoin()) {
            assert(multiJoin.getNumJoinFactors() == 2);
            joinType = JoinRelType.FULL;
        } else if (multiJoin.isNullGenerating(factorToAdd)) {
            joinType = JoinRelType.LEFT;
        } else {
            joinType = JoinRelType.INNER;
        }
        return
            createJoinSubtree(
                multiJoin,
                joinTree,
                rightTree,
                condition,
                joinType,
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
        while (filterIter.hasNext()) {
            RexNode joinFilter = filterIter.next();
            BitSet filterBitmap =
                multiJoin.getFactorsRefByJoinFilter(joinFilter);

            BitSet childFactors = new BitSet(nJoinFactors);
            multiJoin.getChildFactors(leftTree, childFactors);
            multiJoin.getChildFactors(rightTree, childFactors);

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
                            multiJoin.getJoinFields(leftTree, rightTree),
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
            if (newJoinOrder.get(newPos) != factorAdded) {
                for (int oldPos = 0; oldPos < origJoinOrder.size(); oldPos++) {
                    if (newJoinOrder.get(newPos)
                        == origJoinOrder.get(oldPos)) {
                        break;
                    }
                    nFieldsOld +=
                        multiJoin.getNumFieldsInJoinFactor(
                            origJoinOrder.get(oldPos));
                }
                if ((-nFieldsOld + nFieldsNew) != 0) {
                    needAdjust = true;
                    for (int i = 0;
                         i
                         < multiJoin.getNumFieldsInJoinFactor(
                             newJoinOrder.get(newPos)); i++) {
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
                        multiJoin.getJoinFields(left, right),
                        adjustments));
        }

        return condition;
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
     *
     * @return created JoinRel
     */
    private LoptJoinTree createJoinSubtree(
        LoptMultiJoin multiJoin,
        LoptJoinTree left,
        LoptJoinTree right,
        RexNode condition,
        JoinRelType joinType,
        boolean fullAdjust)
    {
        RexBuilder rexBuilder =
            multiJoin.getMultiJoinRel().getCluster().getRexBuilder();

        // swap the inputs if beneficial
        if (swapInputs(multiJoin, left, right, condition)) {
            LoptJoinTree tmp = right;
            right = left;
            left = tmp;
            if (!fullAdjust) {
                condition = swapFilter(
                    rexBuilder,
                    multiJoin,
                    right,
                    left,
                    condition);
            }
            if (joinType != JoinRelType.INNER && joinType != JoinRelType.FULL) {
                joinType = (joinType == JoinRelType.LEFT) ?
                    JoinRelType.RIGHT : JoinRelType.LEFT;
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
                            multiJoin.getJoinFields(left, right),
                            adjustments));
            }
        }

        JoinRel joinTree =
            new JoinRel(
                multiJoin.getMultiJoinRel().getCluster(),
                left.getJoinTree(),
                right.getJoinTree(),
                condition,
                joinType,
                Collections.EMPTY_SET,
                true,
                true);
        return
            new LoptJoinTree(
                joinTree,
                left.getFactorTree(),
                right.getFactorTree());
    }

    /**
     * Swaps the operands to a join, if in the join condition, the RHS
     * references more columns than the right. This is done so queries like
     * (select * from A,B where A.A between B.X and B.Y) will result in B being
     * on the left. If both sides have the same number of references in the join
     * condition, then the smaller input is put on the right.
     *
     * @param multiJoin join factors being optimized
     * @param left left side of join tree
     * @param right right hand side of join tree
     * @param condition join condition between left and right
     *
     * @return true if swapping should be done
     */
    private boolean swapInputs(
        LoptMultiJoin multiJoin,
        LoptJoinTree left,
        LoptJoinTree right,
        RexNode condition)
    {
        boolean swap = false;

        // determine how many fields within the join condition each side
        // of the join references
        int nTotalFields = multiJoin.getNumTotalFields();
        BitSet leftFields = new BitSet(nTotalFields);
        multiJoin.setFieldBitmap(left, leftFields);

        BitSet rightFields = new BitSet(nTotalFields);
        multiJoin.setFieldBitmap(right, rightFields);

        // all fields referenced in the join condition
        BitSet filterRefs = new BitSet(nTotalFields);
        condition.accept(new RelOptUtil.InputFinder(filterRefs));

        // count how many fields each side of the join references by AND'ing
        // the bits referenced in the filter with the bits corresponding to
        // all fields on each side of the join
        leftFields.and(filterRefs);
        rightFields.and(filterRefs);

        if (rightFields.cardinality() > leftFields.cardinality()) {
            swap = true;
        } else if (rightFields.cardinality() == leftFields.cardinality()) {
            Double leftRowCount =
                RelMetadataQuery.getRowCount(left.getJoinTree());
            Double rightRowCount =
                RelMetadataQuery.getRowCount(right.getJoinTree());
            if ((leftRowCount != null) && (rightRowCount != null)
                && (leftRowCount < rightRowCount)) {
                swap = true;
            }
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
