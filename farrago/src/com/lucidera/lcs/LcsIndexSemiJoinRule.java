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
package com.lucidera.lcs;

import java.util.*;
import java.util.logging.*;

import net.sf.farrago.fem.med.*;
import net.sf.farrago.query.*;
import net.sf.farrago.type.*;

import org.eigenbase.rel.*;
import org.eigenbase.rel.metadata.*;
import org.eigenbase.rel.rules.*;
import org.eigenbase.relopt.*;
import org.eigenbase.reltype.*;
import org.eigenbase.rex.*;
import org.eigenbase.sql.*;
import org.eigenbase.trace.*;


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
 * <p>Note that this rule assumes that no projections have been pushed into the
 * LcsRowScanRels.
 *
 * @author Zelaine Fong
 * @version $Id$
 */
public class LcsIndexSemiJoinRule
    extends RelOptRule
{
    // ~ Constructors ----------------------------------------------------------

    //~ Constructors -----------------------------------------------------------

    public LcsIndexSemiJoinRule(RelOptRuleOperand rule, String id)
    {
        // This rule is fired for either of the following 4 patterns:
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

        // if the rowscan is already being used with an index, then let one
        // of the other rules handle those cases
        if ((call.rels.length == 2) && !origRowScan.isFullScan) {
            return;
        }
        RelNode rightRel = semiJoin.getRight();

        // find the best index to filter the LHS of a SemiJoinRel
        List<Integer> bestKeyOrder = new ArrayList<Integer>();

        LcsIndexOptimizer indexOptimizer = new LcsIndexOptimizer(origRowScan);
        FemLocalIndex bestIndex =
            indexOptimizer.findSemiJoinIndexByCost(
                rightRel,
                semiJoin.getLeftKeys(),
                semiJoin.getRightKeys(),
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

        // Add the new semi join filtering index to the access path.
        int rowScanRelPosInCall = 1;

        RelNode [] inputRels =
            addNewIndexAccessRel(
                call,
                rowScanRelPosInCall,
                sort,
                index);

        Double rowScanInputSelectivity =
            RelMdUtil.computeSemiJoinSelectivity(semiJoin);

        LcsRowScanRel newRowScan =
            new LcsRowScanRel(
                origRowScan.getCluster(),
                inputRels,
                origRowScan.lcsTable,
                origRowScan.clusteredIndexes,
                origRowScan.getConnection(),
                origRowScan.projectedColumns,
                false,
                origRowScan.hasResidualFilter,
                origRowScan.residualColumns,
                rowScanInputSelectivity);

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
    private RexNode [] castJoinKeys(
        List<Integer> leftKeys,
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
     * @param call call this rule is matched against
     * @param rowScanRelPosInCall the position(start from 0) of the
     * LcsRowScanRel in the sequence of rels matched by this rule
     * @param sort input to the index search rel to be created
     * @param index the index to use in the index search rel
     *
     * @return the new input rels, after adding the new index search rel, to the
     * row scan rel.
     */
    private static RelNode [] addNewIndexAccessRel(
        RelOptRuleCall call,
        int rowScanRelPosInCall,
        FennelSortRel sort,
        FemLocalIndex index)
    {
        assert (call.rels[rowScanRelPosInCall] instanceof LcsRowScanRel);
        LcsRowScanRel origRowScanRel =
            (LcsRowScanRel) call.rels[rowScanRelPosInCall];

        RelNode newIndexAccessRel = null;

        // AND the INDEX rels together.
        if (!origRowScanRel.isFullScan) {
            assert (call.rels.length > (rowScanRelPosInCall + 1));
            newIndexAccessRel = call.rels[rowScanRelPosInCall + 1];
        }

        // Always require merge for the index access to be added
        boolean requireMerge = true;

        // No input key proj required
        // An equality condition using the input sort values is implied.
        Integer [] inputKeyProj = null;
        Integer [] inputDirectiveProj = null;

        newIndexAccessRel =
            LcsIndexOptimizer.addNewIndexAccessRel(
                newIndexAccessRel,
                call,
                rowScanRelPosInCall,
                index,
                sort,
                inputKeyProj,
                inputDirectiveProj,
                requireMerge);

        // Number of existing residual filters
        int origResidualColumnCount = 0;
        if (origRowScanRel.hasResidualFilter) {
            origResidualColumnCount = origRowScanRel.residualColumns.length;
        }

        int origIndexRelCount =
            origRowScanRel.getInputs().length - origResidualColumnCount;
        int indexRelCount = 1;

        RelNode [] rowScanInputRels =
            new RelNode[indexRelCount
                + origResidualColumnCount];

        // finally create the new row scan
        rowScanInputRels[0] = newIndexAccessRel;

        if (origRowScanRel.hasResidualFilter) {
            System.arraycopy(
                origRowScanRel.getInputs(),
                origIndexRelCount,
                rowScanInputRels,
                indexRelCount,
                origResidualColumnCount);
        }

        return rowScanInputRels;
    }
}

// End LcsIndexSemiJoinRule.java
