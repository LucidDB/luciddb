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
package com.lucidera.lcs;

import java.util.*;

import net.sf.farrago.fem.med.*;
import net.sf.farrago.query.*;
import net.sf.farrago.type.*;

import org.eigenbase.rel.*;
import org.eigenbase.rel.rules.*;
import org.eigenbase.relopt.*;
import org.eigenbase.reltype.*;
import org.eigenbase.rex.*;


/**
 * LcsIndexSemiJoinRule implements the rule for converting a semijoin expression
 * into the actual operations used to execute the semijoin. Specfically,
 *
 * <pre>
 * SemiJoinRel(LcsRowScanRel, D) -> 
 *     LcsRowScanRel(
 *         LcsIndexMergeRel(
 *             LcsIndexSearchRel(
 *                 LcsFennelSortRel(
 *                     AggregateRel(
 *                         ProjectRel(D))))))
 * </pre>
 * 
 * <p>Note that this rule assumes that no projections have been pushed into
 * the LcsRowScanRels.
 *
 * @author Zelaine Fong
 * @version $Id$
 */
public class LcsIndexSemiJoinRule
    extends RelOptRule
{
    // ~ Constructors ----------------------------------------------------------

    public LcsIndexSemiJoinRule(RelOptRuleOperand rule, String id)
    {
        // This rule is fired for either of the following three patterns:
        //
        // RelOptRuleOperand(
        //    SemiJoinRel.class,
        //    new RelOptRuleOperand [] {
        //        new RelOptRuleOperand(LcsRowScanRel.class, null),
        //    })
        // or
        //
        // RelOptRuleOperand(
        //     SemiJoinRel.class,
        //     new RelOptRuleOperand [] {
        //         new RelOptRuleOperand(LcsRowScanRel.class,
        //         new RelOptRuleOperand [] {
        //             new RelOptRuleOperand(LcsIndexIntersectRel.class, null)
        //     })})
        // or
        //
        // RelOptRuleOperand(
        //     SemiJoinRel.class,
        //     new RelOptRuleOperand [] {
        //         new RelOptRuleOperand(LcsRowScanRel.class,
        //         new RelOptRuleOperand [] {
        //             new RelOptRuleOperand(LcsIndexSearchRel.class, null)
        //     })})
        // or
        //
        // RelOptRuleOperand(
        //     SemiJoinRel.class,
        //     new RelOptRuleOperand [] {
        //         new RelOptRuleOperand(LcsRowScanRel.class,
        //         new RelOptRuleOperand [] {
        //             new RelOptRuleOperand(LcsIndexMergeRel.class,
        //             new RelOptRuleOperand [] {
        //                 new RelOptRuleOperand(LcsIndexSearchRel.class, null)
        //     })})})

        super(rule);
        description = "LcsIndexSemiJoinRule: " + id;
    }

    //~ Methods ----------------------------------------------------------------

    // implement RelOptRule
    public void onMatch(RelOptRuleCall call)
    {
        SemiJoinRel semiJoin = (SemiJoinRel) call.rels[0];
        LcsRowScanRel origRowScan = (LcsRowScanRel) call.rels[1];

        // if the rowscan has an intersect or merge child, then let those
        // rules handle those cases
        if ((call.rels.length == 2) && (origRowScan.getInputs().length == 1)
            && !origRowScan.hasExtraFilter) {
            return;
        }
        RelNode rightRel = semiJoin.getRight();

        // loop through the indexes and either find the one that has the
        // longest matching keys, or the first one that matches all the
        // join keys
        LcsIndexGuide indexGuide = origRowScan.getIndexGuide();
        List<Integer> bestKeyOrder = new ArrayList<Integer>();
        FemLocalIndex bestIndex =
            indexGuide.findSemiJoinIndex(
                semiJoin.getLeftKeys(),
                bestKeyOrder);

        if (bestIndex != null) {
            transformSemiJoin(
                semiJoin,
                origRowScan,
                bestIndex,
                bestKeyOrder,
                rightRel,
                call);
        }
    }

    /**
     * Converts the semijoin expression once a valid index has been found
     *
     * @param semiJoin the semijoin expression to be converted
     * @param origRowScan original row scan on the left hand side of the
     * semijoin
     * @param index index to be used to scan the left hand side of the semijoin
     * @param keyOrder positions of the keys that match the index, in the order
     * of match
     * @param rightRel right hand side of the semijoin
     * @param call rule call
     */
    private void transformSemiJoin(
        SemiJoinRel semiJoin,
        LcsRowScanRel origRowScan,
        FemLocalIndex index,
        List<Integer> keyOrder,
        RelNode rightRel,
        RelOptRuleCall call)
    {
        // create a projection on the join columns from the right input,
        // matching the order of the index keys; also determine if a
        // cast is required
        List<Integer> leftKeys = semiJoin.getLeftKeys();
        List<Integer> rightKeys = semiJoin.getRightKeys();
        RelDataTypeField [] leftFields = origRowScan.getRowType().getFields();
        RelDataTypeField [] rightFields = rightRel.getRowType().getFields();
        RexBuilder rexBuilder = rightRel.getCluster().getRexBuilder();
        int nKeys = keyOrder.size();
        String [] fieldNames = new String[nKeys];
        RexNode [] projExps = new RexNode[nKeys];
        boolean castRequired = false;

        Integer [] rightOrdinals = new Integer[nKeys];
        for (int i = 0; i < nKeys; i++) {
            rightOrdinals[i] = rightKeys.get(keyOrder.get(i));
            RelDataTypeField rightField = rightFields[rightOrdinals[i]];
            projExps[i] =
                rexBuilder.makeInputRef(
                    rightField.getType(),
                    rightOrdinals[i]);
            fieldNames[i] = rightField.getName();

            RelDataTypeField leftField =
                leftFields[leftKeys.get(keyOrder.get(i))];
            if (!leftField.getType().equals(rightField.getType())) {
                castRequired = true;
            }
        }

        // create a cast on the projected columns if the types of the
        // left join keys don't match the right
        RexNode [] castExps;
        if (castRequired) {
            FarragoPreparingStmt stmt =
                FennelRelUtil.getPreparingStmt(origRowScan);
            FarragoTypeFactory typeFactory = stmt.getFarragoTypeFactory();
            castExps =
                castJoinKeys(
                    leftKeys,
                    leftFields,
                    nKeys,
                    keyOrder,
                    rexBuilder,
                    projExps,
                    typeFactory);
        } else {
            castExps = projExps;
        }

        // filter out null search keys, since they never match, and use
        // that filter result as the input into the projection/cast
        RelNode nullFilterRel =
            RelOptUtil.createNullFilter(rightRel, rightOrdinals);
        ProjectRel projectRel =
            (ProjectRel) CalcRel.createProject(
                nullFilterRel,
                castExps,
                fieldNames);
        RelNode distInput =
            mergeTraitsAndConvert(
                semiJoin.getTraits(),
                FennelRel.FENNEL_EXEC_CONVENTION,
                projectRel);

        // create a distinct agg on top of the project to remove duplicate
        // keys
        AggregateRel distinctRel =
            (AggregateRel) RelOptUtil.createDistinctRel(distInput);
        
        // then sort the result so we will search the index in key order
        FennelSortRel sort =
            new FennelSortRel(
                origRowScan.getCluster(),
                distinctRel,
                FennelRelUtil.newIotaProjection(nKeys),
                false);

        // create a merge and index search on top of the index scan
        boolean needIntersect = (call.rels.length > 2);
        FennelRelImplementor relImplementor =
            FennelRelUtil.getRelImplementor(origRowScan);
        RelNode [] inputRels =
            createMergeIdxSearches(
                relImplementor,
                call,
                sort,
                origRowScan,
                index,
                needIntersect);

        LcsRowScanRel newRowScan =
            new LcsRowScanRel(
                origRowScan.getCluster(),
                inputRels,
                origRowScan.lcsTable,
                origRowScan.clusteredIndexes,
                origRowScan.getConnection(),
                origRowScan.projectedColumns,
                false,
                origRowScan.hasExtraFilter,
                origRowScan.residualColumns);

        call.transformTo(newRowScan);
    }

    /**
     * Casts the types of the join keys from the right hand side of the join to
     * the types of the left hand side
     *
     * @param leftKeys left hand side join keys
     * @param leftFields fields corresponding to the left hand side of the join
     * @param nKeys number of keys to be cast
     * @param keyOrder positions of the keys that match the index, in the order
     * of match
     * @param rexBuilder rex builder from right hand side of join
     * @param rhsExps right hand side expressions that need to be cast
     * @param typeFactory type factory
     *
     * @return cast expression
     */
    private RexNode [] castJoinKeys(List<Integer> leftKeys,
        RelDataTypeField [] leftFields,
        int nKeys,
        List<Integer> keyOrder,
        RexBuilder rexBuilder,
        RexNode [] rhsExps,
        FarragoTypeFactory typeFactory)
    {
        RelDataType [] leftType = new RelDataType[nKeys];
        String [] leftFieldNames = new String[nKeys];
        for (int i = 0; i < nKeys; i++) {
            leftType[i] = leftFields[leftKeys.get(keyOrder.get(i))].getType();
            leftFieldNames[i] =
                leftFields[leftKeys.get(keyOrder.get(i))].getName();
        }
        RelDataType leftStructType =
            typeFactory.createStructType(leftType, leftFieldNames);
        RexNode [] castExps =
            RexUtil.generateCastExpressions(
                rexBuilder,
                leftStructType,
                rhsExps);
        return castExps;
    }

    /**
     * Creates a merge on top of an index search on top of a sort. If necessary,
     * creates an intersect on top of the merges. This will then feed into the
     * rowscan.
     *
     * @param relImplementor for allocating dynamic parameters
     * @param call inputs into this rule
     * @param sort distinct, sorted input into index search
     * @param indexScan index scan to be used with search
     * @param rowScan the original row scan
     * @param needIntersect true if the row scan requires more than 1 index
     * search and therefore, an intersect needs to feed into the scan
     *
     * @return input into the row scan
     */
    private RelNode [] createMergeIdxSearches(
        FennelRelImplementor relImplementor,
        RelOptRuleCall call,
        FennelSortRel sort,
        LcsRowScanRel rowScan,
        FemLocalIndex index,
        boolean needIntersect)
    {
        // if there already is a child underneath the rowscan, then we'll
        // need to create an intersect; for the intersect, we'll need dynamic
        // parameters; either create new ones if there is no intersect yet or
        // reuse the existing ones
        FennelRelParamId startRidParamId;
        FennelRelParamId rowLimitParamId;
        if (needIntersect) {
            if (call.rels[2] instanceof LcsIndexIntersectRel) {
                startRidParamId =
                    ((LcsIndexIntersectRel) call.rels[2]).getStartRidParamId();
                rowLimitParamId =
                    ((LcsIndexIntersectRel) call.rels[2]).getRowLimitParamId();
            } else {
                startRidParamId = relImplementor.allocateRelParamId();
                rowLimitParamId = relImplementor.allocateRelParamId();
            }
        } else {
            startRidParamId = null;
            rowLimitParamId = null;
        }

        // directives don't need to be passed into the index search
        // because we are doing an equijoin where the sort feeds in
        // the search values; also no need to set the dynamic parameters
        // since those are set in the parent index merge rel
        LcsIndexSearchRel indexSearch =
            new LcsIndexSearchRel(
                rowScan.getCluster(),
                sort,
                rowScan.lcsTable,
                index,
                false,
                null,
                true,
                false,
                null,
                null,
                null,
                null,
                null);

        // create a merge on top of the index search
        LcsIndexMergeRel merge =
            new LcsIndexMergeRel(
                rowScan.lcsTable,
                indexSearch,
                startRidParamId,
                rowLimitParamId,
                relImplementor.allocateRelParamId());

        // finally create the new row scan
        int inputLen = rowScan.getInputs().length;
        if (rowScan.isFullScan) {
            inputLen++;
        }
        RelNode [] inputRels = new RelNode[inputLen];
        if (needIntersect) {
            LcsIndexIntersectRel intersectRel =
                addIntersect(
                    relImplementor,
                    call,
                    rowScan,
                    call.rels[2],
                    merge,
                    startRidParamId,
                    rowLimitParamId);
            inputRels[0] = intersectRel;
        } else {
            inputRels[0] = merge;
        }
        
        if (rowScan.hasExtraFilter) {
            System.arraycopy(
                rowScan.getInputs(),
                (rowScan.isFullScan) ? 0 : 1,
                inputRels, 1, inputLen - 1);
        }

        return inputRels;
    }

    /**
     * Creates an intersect of an existing set of merge/index search/sort
     * RelNodes strung together with a new merge/index search/sort RelNode
     * chain. In doing so, make sure all children of the intersect have the
     * correct dynamic parameters.
     *
     * @param rowScan rowScan underneath this intersect
     * @param existingMerges existing merges
     * @param newInput new merge to be intersected with the existing
     * @param startRidParamId id for startRid parameter
     * @param rowLimitParamId id for rowLimit parameter
     *
     * @return new intersect relnode
     */
    private LcsIndexIntersectRel addIntersect(
        FennelRelImplementor relImplementor,
        RelOptRuleCall call,
        LcsRowScanRel rowScan,
        RelNode existingMerges,
        RelNode newMerge,
        FennelRelParamId startRidParamId,
        FennelRelParamId rowLimitParamId)
    {
        RelNode [] mergeRels;
        int nMergeRels;

        // if there's already an intersect, get the existing children
        // of that intersect
        if (existingMerges instanceof LcsIndexIntersectRel) {
            nMergeRels = existingMerges.getInputs().length;
            mergeRels = new RelNode[nMergeRels + 1];
            for (int i = 0; i < nMergeRels; i++) {
                mergeRels[i] = existingMerges.getInputs()[i];
            }
        } else {
            nMergeRels = 1;
            mergeRels = new RelNode[2];

            if (existingMerges instanceof LcsIndexMergeRel) {
                // recreate the index merge rel with the appropriate dynamic
                // params
                mergeRels[0] =
                    new LcsIndexMergeRel(
                        rowScan.lcsTable,
                        (LcsIndexSearchRel) call.rels[3],
                        startRidParamId,
                        rowLimitParamId,
                        relImplementor.allocateRelParamId());
            } else {
                // recreate the index search with the appropriate dynamic
                // params
                assert (existingMerges instanceof LcsIndexSearchRel);
                LcsIndexSearchRel indexSearch =
                    (LcsIndexSearchRel) call.rels[2];
                mergeRels[0] =
                    new LcsIndexSearchRel(
                        indexSearch.getCluster(),
                        indexSearch.getChild(),
                        indexSearch.lcsTable,
                        indexSearch.index,
                        false,
                        null,
                        indexSearch.isUniqueKey(),
                        indexSearch.isOuter(),
                        indexSearch.getInputKeyProj(),
                        indexSearch.getInputJoinProj(),
                        indexSearch.getInputDirectiveProj(),
                        startRidParamId,
                        rowLimitParamId);
            }
        }
        mergeRels[nMergeRels] = newMerge;

        LcsIndexIntersectRel intersectRel =
            new LcsIndexIntersectRel(
                rowScan.getCluster(),
                mergeRels,
                rowScan.lcsTable,
                startRidParamId,
                rowLimitParamId);

        return intersectRel;
    }
}

// End LcsIndexSemiJoinRule.java
