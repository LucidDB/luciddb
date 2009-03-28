/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2006-2009 The Eigenbase Project
// Copyright (C) 2009-2009 SQLstream, Inc.
// Copyright (C) 2006-2009 LucidEra, Inc.
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
package net.sf.farrago.fennel.rel;

import java.util.*;

import net.sf.farrago.query.*;

import org.eigenbase.rel.*;
import org.eigenbase.rel.metadata.*;
import org.eigenbase.relopt.*;
import org.eigenbase.rex.*;
import org.eigenbase.reltype.RelDataTypeField;


/**
 * LhxJoinRule implements the planner rule for converting a JoinRel with join
 * condition into a LhxJoinRel (hash join).
 *
 * @author Rushan Chen
 * @version $Id$
 */
public class LhxJoinRule
    extends RelOptRule
{
    public static final LhxJoinRule instance =
        new LhxJoinRule();

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a LhxJoinRule.
     */
    private LhxJoinRule()
    {
        super(new RelOptRuleOperand(JoinRel.class, ANY));
    }

    //~ Methods ----------------------------------------------------------------

    // implement RelOptRule
    public CallingConvention getOutConvention()
    {
        return FennelRel.FENNEL_EXEC_CONVENTION;
    }

    // implement RelOptRule
    public void onMatch(RelOptRuleCall call)
    {
        JoinRel joinRel = (JoinRel) call.rels[0];

        RelNode leftRel = joinRel.getLeft();
        RelNode rightRel = joinRel.getRight();
        RexNode nonEquiCondition = null;

        // determine if we have a valid join condition
        List<Integer> leftKeys = new ArrayList<Integer>();
        List<Integer> rightKeys = new ArrayList<Integer>();

        List<RexNode> leftJoinKeys = new ArrayList<RexNode>();
        List<RexNode> rightJoinKeys = new ArrayList<RexNode>();
        List<Integer> filterNulls = new ArrayList<Integer>();

        nonEquiCondition =
            RelOptUtil.splitJoinCondition(
                Collections.<RelDataTypeField>emptyList(),
                leftRel,
                rightRel,
                joinRel.getCondition(),
                leftJoinKeys,
                rightJoinKeys,
                filterNulls,
                null);

        if ((nonEquiCondition != null)
            && (joinRel.getJoinType() != JoinRelType.INNER))
        {
            // Cannot use hash outer join types if there're non-equi join
            // conditions.
            // Note this type of join cannot be implemented by cartesian
            // product either.
            return;
        }

        if (!joinRel.getVariablesStopped().isEmpty()) {
            return;
        }

        if (leftJoinKeys.size() == 0) {
            // should use cartesian product instead of hash join
            return;
        }

        List<Integer> outputProj = new ArrayList<Integer>();

        RelNode [] inputRels = new RelNode[] { leftRel, rightRel };

        RelOptUtil.projectJoinInputs(
            inputRels,
            leftJoinKeys,
            rightJoinKeys,
            0,
            leftKeys,
            rightKeys,
            outputProj);

        // the new leftRel and new rightRel, after projection is added.
        leftRel = inputRels[0];
        rightRel = inputRels[1];

        RelNode fennelLeft =
            mergeTraitsAndConvert(
                joinRel.getTraits(),
                FennelRel.FENNEL_EXEC_CONVENTION,
                leftRel);

        if (fennelLeft == null) {
            return;
        }

        RelNode fennelRight =
            mergeTraitsAndConvert(
                joinRel.getTraits(),
                FennelRel.FENNEL_EXEC_CONVENTION,
                rightRel);

        if (fennelRight == null) {
            return;
        }

        Double numBuildRows = RelMetadataQuery.getRowCount(fennelRight);
        if (numBuildRows == null) {
            numBuildRows = -1.0;
        }

        // Derive cardinality of RHS join keys.
        Double cndBuildKey;
        BitSet joinKeyMap = new BitSet();

        // since rightJoinKeys can be more than simply inputrefs
        // assume the cardinality of the key to be the cardinality of all
        // referenced fields.

        for (int i = 0; i < rightKeys.size(); i++) {
            joinKeyMap.set(rightKeys.get(i));
        }

        cndBuildKey =
            RelMetadataQuery.getPopulationSize(
                fennelRight,
                joinKeyMap);

        if (cndBuildKey == null) {
            cndBuildKey = -1.0;
        }

        boolean isSetop = false;

        // pass in null for the fieldNameList so proper names can be derived
        // when the left and right hand side have overlapping names
        RelNode rel =
            new LhxJoinRel(
                joinRel.getCluster(),
                fennelLeft,
                fennelRight,
                LhxJoinRelType.getLhxJoinType(joinRel.getJoinType()),
                isSetop,
                leftKeys,
                rightKeys,
                filterNulls,
                null,
                numBuildRows.longValue(),
                cndBuildKey.longValue());

        // Need to project the new output(left+key+right+key) to the original
        // join output(left+right).
        // The projection needs to happen before additional filtering since
        // filtering condition references the original output ordinals.
        rel = RelOptUtil.createProjectJoinRel(outputProj, rel);

        transformCall(call, rel, nonEquiCondition);
    }

    private void transformCall(
        RelOptRuleCall call,
        RelNode rel,
        RexNode extraFilter)
    {
        if (extraFilter != null) {
            rel =
                new FilterRel(
                    rel.getCluster(),
                    rel,
                    extraFilter);
        }
        call.transformTo(rel);
    }
}

// End LhxJoinRule.java
