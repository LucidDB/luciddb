/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2006-2009 LucidEra, Inc.
// Copyright (C) 2006-2009 The Eigenbase Project
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

import com.lucidera.lcs.*;
import com.lucidera.query.*;

import java.util.*;

import org.eigenbase.rel.*;
import org.eigenbase.rel.metadata.*;
import org.eigenbase.relopt.*;
import org.eigenbase.reltype.*;
import org.eigenbase.rex.*;
import org.eigenbase.sql.fun.*;


/**
 * LoptRemoveSelfJoinRule implements a rule that converts removable self-joins
 * into a scan on the common underlying row scan table. The projection and
 * filtering applied on each join input are combined into one.
 *
 * <p>This rule only needs to look for a very specific RelNode pattern because
 * it assumes that {@link LoptModifyRemovableSelfJoinRule} has already been
 * applied.
 *
 * @author Zelaine Fong
 * @version $Id$
 */
public class LoptRemoveSelfJoinRule
    extends RelOptRule
{
    public static final LoptRemoveSelfJoinRule instance =
        new LoptRemoveSelfJoinRule();

    //~ Constructors -----------------------------------------------------------

    /**
     * @deprecated use {@link #instance} instead
     */
    public LoptRemoveSelfJoinRule()
    {
        super(
            new RelOptRuleOperand(
                JoinRel.class,
                new RelOptRuleOperand(
                    ProjectRel.class,
                    new RelOptRuleOperand(
                        FilterRel.class,
                        new RelOptRuleOperand(LcsRowScanRel.class))),
                new RelOptRuleOperand(
                    ProjectRel.class,
                    new RelOptRuleOperand(
                        FilterRel.class,
                        new RelOptRuleOperand(LcsRowScanRel.class)))));
    }

    //~ Methods ----------------------------------------------------------------

    // implement RelOptRule
    public void onMatch(RelOptRuleCall call)
    {
        JoinRel joinRel = (JoinRel) call.rels[0];
        if (!LoptOptimizeJoinRule.isRemovableSelfJoin(joinRel)) {
            return;
        }

        ProjectRel leftProject = (ProjectRel) call.rels[1];
        ProjectRel rightProject = (ProjectRel) call.rels[4];
        FilterRel leftFilter = (FilterRel) call.rels[2];
        FilterRel rightFilter = (FilterRel) call.rels[5];
        LcsRowScanRel leftRowScan = (LcsRowScanRel) call.rels[3];
        LcsRowScanRel rightRowScan = (LcsRowScanRel) call.rels[6];

        // This rule needs to be applied before projections and filters
        // are pushed into the row scan
        assert (leftRowScan.getProjectedColumns() == null);
        assert (rightRowScan.getProjectedColumns() == null);
        assert (!leftRowScan.hasResidualFilters());
        assert (!rightRowScan.hasResidualFilters());

        // Combine the row scans into one
        LcsRowScanRel newRowScan = combineRowScans(leftRowScan, rightRowScan);

        // Combine the filters on top of the row scans
        RelNode newFilter = combineFilters(leftFilter, rightFilter, newRowScan);

        // Combine the projections on top of the filters
        ProjectRel newProject =
            combineProjects(leftProject, rightProject, newFilter);

        // Remove the join, replacing it, as needed, with whatever filtering
        // still needs to be applied
        RelNode joinReplacement = createJoinReplacement(joinRel, newProject);

        call.transformTo(joinReplacement);
    }

    /**
     * Combines two row scans into one by taking the intersection of the inputs
     * into each row scan. It's assumed that each row scan has no more than one
     * input.
     *
     * @param leftRowScan left row scan
     * @param rightRowScan right row scan
     *
     * @return the combined row scan
     */
    private LcsRowScanRel combineRowScans(
        LcsRowScanRel leftRowScan,
        LcsRowScanRel rightRowScan)
    {
        RelNode [] leftRowScanInputs = leftRowScan.getInputs();
        RelNode [] rightRowScanInputs = rightRowScan.getInputs();
        assert (leftRowScanInputs.length <= 1);
        assert (rightRowScanInputs.length <= 1);

        int nInputs = leftRowScanInputs.length + rightRowScanInputs.length;
        if (nInputs > 1) {
            // When taking the intersection of the two inputs, note that
            // dynamic parameters are not being created.  So, we won't take
            // advantage of skipping past rids for this topmost intersect.
            RelNode [] newRowScanInputs =
                new RelNode[] { leftRowScanInputs[0], rightRowScanInputs[0] };
            LcsIndexIntersectRel intersect =
                new LcsIndexIntersectRel(
                    leftRowScan.getCluster(),
                    newRowScanInputs,
                    (LcsTable) leftRowScan.getTable(),
                    null,
                    null);
            return new LcsRowScanRel(
                leftRowScan.getCluster(),
                new RelNode[] { intersect },
                (LcsTable) leftRowScan.getTable(),
                leftRowScan.getClusteredIndexes(),
                leftRowScan.getConnection(),
                null,
                false,
                new Integer[] {},
                leftRowScan.getInputSelectivity()
                * rightRowScan.getInputSelectivity());
        } else if (leftRowScanInputs.length == 1) {
            return new LcsRowScanRel(
                leftRowScan.getCluster(),
                new RelNode[] { leftRowScanInputs[0] },
                (LcsTable) leftRowScan.getTable(),
                leftRowScan.getClusteredIndexes(),
                leftRowScan.getConnection(),
                null,
                false,
                new Integer[] {},
                leftRowScan.getInputSelectivity());
        } else if (rightRowScanInputs.length == 1) {
            return new LcsRowScanRel(
                leftRowScan.getCluster(),
                new RelNode[] { rightRowScanInputs[0] },
                (LcsTable) leftRowScan.getTable(),
                leftRowScan.getClusteredIndexes(),
                leftRowScan.getConnection(),
                null,
                false,
                new Integer[] {},
                rightRowScan.getInputSelectivity());
        } else {
            return leftRowScan;
        }
    }

    /**
     * Combines filters on top of row scans into a single filter. If either
     * filter is always true, then don't include it in the combined filter.
     *
     * @param leftFilter filter on top of the original left row scan
     * @param rightFilter filter on top of the original right row scan
     * @param newRowScan the new, combined row scan
     *
     * @return a FilterRel containing the combined filters on top of the new row
     * scan; if both filters are always true, then the combined row scan is
     * returned instead
     */
    private RelNode combineFilters(
        FilterRel leftFilter,
        FilterRel rightFilter,
        LcsRowScanRel newRowScan)
    {
        RexNode leftCondition = leftFilter.getCondition();
        RexNode rightCondition = rightFilter.getCondition();
        if (leftCondition.isAlwaysTrue()) {
            if (rightCondition.isAlwaysTrue()) {
                return newRowScan;
            } else {
                return CalcRel.createFilter(newRowScan, rightCondition);
            }
        } else if (rightCondition.isAlwaysTrue()) {
            return CalcRel.createFilter(newRowScan, leftCondition);
        } else {
            return CalcRel.createFilter(
                newRowScan,
                leftFilter.getCluster().getRexBuilder().makeCall(
                    SqlStdOperatorTable.andOperator,
                    leftCondition,
                    rightCondition));
        }
    }

    /**
     * Combines projections on top of filters that are top of row scans into a
     * single projection. Since the row scans are identical and have not yet
     * been projected, the original expressions in both projections will be the
     * same in the combined projection.
     *
     * @param leftProject the left projection
     * @param rightProject the right projection
     * @param projInput the input into the combined projection
     *
     * @return the resulting combined projection
     */
    private ProjectRel combineProjects(
        ProjectRel leftProject,
        ProjectRel rightProject,
        RelNode projInput)
    {
        RexNode [] leftProjExprs = leftProject.getProjectExps();
        RexNode [] rightProjExprs = rightProject.getProjectExps();
        int nLeftProjExprs = leftProjExprs.length;
        int nRightProjExprs = rightProjExprs.length;
        RexNode [] newProjExprs = new RexNode[nLeftProjExprs + nRightProjExprs];
        String [] fieldNames = new String[nLeftProjExprs + nRightProjExprs];

        for (int i = 0; i < nLeftProjExprs; i++) {
            newProjExprs[i] = leftProjExprs[i];
            fieldNames[i] = leftProject.getRowType().getFields()[i].getName();
        }
        for (int i = 0; i < nRightProjExprs; i++) {
            newProjExprs[i + nLeftProjExprs] = rightProjExprs[i];
            fieldNames[i + nLeftProjExprs] =
                rightProject.getRowType().getFields()[i].getName();
        }

        return CalcRel.createProject(projInput, newProjExprs, fieldNames);
    }

    /**
     * Replaces the join with whatever additional filters need to be applied as
     * a result of removing the join.
     *
     * @param joinRel the original join
     * @param joinInput the new input into the join
     *
     * @return a FilterRel containing whatever additional filters need to be
     * applied; if no additional filtering is required, then the new join input
     * is returned
     */
    private RelNode createJoinReplacement(
        JoinRel joinRel,
        ProjectRel joinInput)
    {
        // Determine if there are excess filters beyond the equality join
        // condition
        List<Integer> leftKeys = new ArrayList<Integer>();
        List<Integer> rightKeys = new ArrayList<Integer>();
        RexNode extraFilters =
            RelOptUtil.splitJoinCondition(
                joinRel.getLeft(),
                joinRel.getRight(),
                joinRel.getCondition(),
                leftKeys,
                rightKeys);

        // Then, see if any of the join keys are nullable types.  If there
        // are any, then those are excess keys that are not really unique.
        // So, add them back in as IS NOT NULL filters.
        RelDataTypeField [] fields = joinRel.getLeft().getRowType().getFields();
        RexBuilder rexBuilder = joinRel.getCluster().getRexBuilder();
        List<RexNode> extraFilterList = new ArrayList<RexNode>();
        for (Integer key : leftKeys) {
            // Handle the rid key as a special case -- it's always unique
            // and non-nullable
            RelColumnOrigin colOrigin =
                LoptMetadataProvider.getSimpleColumnOrigin(
                    joinRel.getLeft(),
                    key);
            if ((colOrigin != null)
                && LucidDbSpecialOperators.isLcsRidColumnId(
                    colOrigin.getOriginColumnOrdinal()))
            {
                continue;
            }
            if (fields[key].getType().isNullable()) {
                RexNode [] expr =
                    new RexNode[] { RelOptUtil.createInputRef(joinInput, key) };
                extraFilterList.add(
                    rexBuilder.makeCall(
                        SqlStdOperatorTable.isNotNullOperator,
                        expr));
            }
        }
        if (!extraFilterList.isEmpty()) {
            if (!extraFilters.isAlwaysTrue()) {
                extraFilterList.add(extraFilters);
            }
            extraFilters = RexUtil.andRexNodeList(rexBuilder, extraFilterList);
        }

        if (extraFilters.isAlwaysTrue()) {
            return joinInput;
        } else {
            return CalcRel.createFilter(joinInput, extraFilters);
        }
    }
}

// End LoptRemoveSelfJoinRule.java
