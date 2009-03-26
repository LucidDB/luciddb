/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005-2009 The Eigenbase Project
// Copyright (C) 2002-2009 SQLstream, Inc.
// Copyright (C) 2005-2009 LucidEra, Inc.
// Portions Copyright (C) 2003-2009 John V. Sichi
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

import org.eigenbase.rel.*;
import org.eigenbase.relopt.*;
import org.eigenbase.reltype.*;
import org.eigenbase.rex.*;


/**
 * Rule which, given a {@link ProjectRel} node which merely returns its input,
 * converts the node into its child.
 *
 * <p>For example, <code>ProjectRel(ArrayReader(a), {$input0})</code> becomes
 * <code>ArrayReader(a)</code>.</p>
 *
 * @see org.eigenbase.rel.rules.RemoveTrivialCalcRule
 */
public class RemoveTrivialProjectRule
    extends RelOptRule
{
    //~ Static fields/initializers ---------------------------------------------

    public static final RemoveTrivialProjectRule instance =
        new RemoveTrivialProjectRule();

    //~ Constructors -----------------------------------------------------------

    private RemoveTrivialProjectRule()
    {
        super(new RelOptRuleOperand(ProjectRel.class, ANY));
    }

    //~ Methods ----------------------------------------------------------------

    public void onMatch(RelOptRuleCall call)
    {
        ProjectRel project = (ProjectRel) call.rels[0];
        RelNode child = project.getChild();
        final RelDataType childRowType = child.getRowType();
        if (!childRowType.isStruct()) {
            return;
        }
        if (!project.isBoxed()) {
            return;
        }
        if (!isIdentity(
                project.getProjectExps(),
                project.getRowType(),
                childRowType))
        {
            return;
        }
        child = call.getPlanner().register(child, project);
        child =
            convert(
                child,
                project.getTraits());
        if (child != null) {
            call.transformTo(child);
        }
    }

    public static boolean isIdentity(
        RexNode [] exps,
        RelDataType rowType,
        RelDataType childRowType)
    {
        RelDataTypeField [] fields = rowType.getFields();
        RelDataTypeField [] childFields = childRowType.getFields();
        int fieldCount = childFields.length;
        if (exps.length != fieldCount) {
            return false;
        }
        for (int i = 0; i < exps.length; i++) {
            RexNode exp = exps[i];
            if (exp instanceof RexInputRef) {
                RexInputRef var = (RexInputRef) exp;
                if (var.getIndex() != i) {
                    return false;
                }
                if (!fields[i].getName().equals(childFields[i].getName())) {
                    return false;
                }
            } else {
                return false;
            }
        }
        return true;
    }
}

// End RemoveTrivialProjectRule.java
