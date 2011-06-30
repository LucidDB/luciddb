/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2010 The Eigenbase Project
// Copyright (C) 2010 SQLstream, Inc.
// Copyright (C) 2010 Dynamo BI Corporation
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
package org.eigenbase.rel.rules;

import java.util.*;

import org.eigenbase.rel.*;
import org.eigenbase.relopt.*;
import org.eigenbase.rex.*;
import org.eigenbase.util.mapping.*;


/**
 * Planner rule that converts a {@link JoinRel} with system fields into a join
 * without system fields, if a {@link ProjectRel} above the join indicates that
 * the system fields are not needed.
 *
 * <p>This rule is needed only for sub-projects whose SQL-to-Relnode
 * translation process introduces a join with system fields.
 *
 * @see MergeCalcJoinEliminatingSystemFieldsRule
 *
 * @author Julian Hyde
 * @version $Id$
 */
public final class MergeProjectJoinEliminatingSystemFieldsRule
    extends RelOptRule
{
    public static final MergeProjectJoinEliminatingSystemFieldsRule instance =
        new MergeProjectJoinEliminatingSystemFieldsRule();

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a MergeProjectJoinEliminatingSystemFieldsRule.
     */
    private MergeProjectJoinEliminatingSystemFieldsRule()
    {
        super(
            new RelOptRuleOperand(
                ProjectRel.class,
                new RelOptRuleOperand(
                    JoinRel.class,
                    RelOptRuleOperand.hasSystemFields(true),
                    ANY)));
    }

    //~ Methods ----------------------------------------------------------------

    // implement RelOptRule
    public void onMatch(RelOptRuleCall call)
    {
        ProjectRel projRel = (ProjectRel) call.rels[0];
        JoinRel joinRel = (JoinRel) call.rels[1];

        assert !joinRel.getSystemFieldList().isEmpty()
            : "Operand predicate should require sys fields";

        // Are we projecting any of the system fields?
        final int fieldCount = joinRel.getRowType().getFieldCount();
        final BitSet fieldsUsed = new BitSet(fieldCount);
        RexUtil.apply(
            new RelOptUtil.InputFinder(fieldsUsed),
            projRel.getChildExps(),
            null);
        final int sysFieldCount = joinRel.getSystemFieldList().size();
        for (int i = 0; i < sysFieldCount; i++) {
            if (fieldsUsed.get(i)) {
                return;
            }
        }

        // Mapping to project away system fields.
        Mappings.TargetMapping mapping =
            (Mappings.TargetMapping)
                Mappings.create(
                    MappingType.Surjection,
                    fieldCount,
                    fieldCount - sysFieldCount);
        final RexPermuteInputsShuttle shuttle =
            new RexPermuteInputsShuttle(mapping);
        for (int i = sysFieldCount; i < fieldCount; i++) {
            mapping.set(i, i - sysFieldCount);
        }

        // Create a new join, with relocated condition, and no system fields.
        JoinRel newJoinRel =
            new JoinRel(
                joinRel.getCluster(),
                joinRel.getLeft(),
                joinRel.getRight(),
                shuttle.apply(joinRel.getCondition()),
                joinRel.getJoinType(),
                joinRel.getVariablesStopped());

        // Build new projections.
        final ProjectRel newProjRel =
            new ProjectRel(
                projRel.getCluster(),
                newJoinRel,
                RexUtil.apply(
                    shuttle, projRel.getProjectExps()),
                projRel.getRowType(),
                projRel.getFlags(),
                RexUtil.apply(mapping, projRel.getCollationList()));

        call.transformTo(newProjRel);
    }
}

// End MergeProjectJoinEliminatingSystemFieldsRule.java
