/*
// $Id$
// Package org.eigenbase is a class library of database components.
// Copyright (C) 2002-2004 Disruptive Tech
// Copyright (C) 2003-2004 John V. Sichi
//
// This program is free software; you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation; either version 2 of the License, or
// (at your option) any later version.
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

package org.eigenbase.rel;

import org.eigenbase.relopt.RelOptRule;
import org.eigenbase.relopt.RelOptRuleCall;
import org.eigenbase.relopt.RelOptRuleOperand;
import org.eigenbase.reltype.RelDataType;
import org.eigenbase.reltype.RelDataTypeField;
import org.eigenbase.rex.RexInputRef;
import org.eigenbase.rex.RexNode;


/**
 * Rule which, given a {@link ProjectRel} node which merely returns its input,
 * converts the node into its child.
 *
 * <p>
 * For example, <code>ProjectRel(ArrayReader(a), {$input0})</code> becomes
 * <code>ArrayReader(a)</code>.
 * </p>
 */
public class RemoveTrivialProjectRule extends RelOptRule
{
    //~ Constructors ----------------------------------------------------------

    public RemoveTrivialProjectRule()
    {
        super(new RelOptRuleOperand(ProjectRel.class, null));
    }

    //~ Methods ---------------------------------------------------------------

    public void onMatch(RelOptRuleCall call)
    {
        ProjectRel project = (ProjectRel) call.rels[0];
        RelNode child = project.child;
        final RelDataType childRowType = child.getRowType();
        if (!childRowType.isStruct()) {
            return;
        }
        if (!project.isBoxed()) {
            return;
        }
        if (!isIdentity(
                    project.exps,
                    project.getFieldNames(),
                    childRowType)) {
            return;
        }
        child = call.planner.register(child, project);
        child = convert(
                child,
                project.getConvention());
        if (child != null) {
            call.transformTo(child);
        }
    }

    private static boolean isIdentity(
        RexNode [] exps,
        String [] fieldNames,
        RelDataType childRowType)
    {
        int fieldCount = childRowType.getFieldCount();
        RelDataTypeField [] childFields = childRowType.getFields();
        if (exps.length != fieldCount) {
            return false;
        }
        for (int i = 0; i < exps.length; i++) {
            RexNode exp = exps[i];
            if (exp instanceof RexInputRef) {
                RexInputRef var = (RexInputRef) exp;
                if (var.index != i) {
                    return false;
                }
                if (fieldNames[i] != null) {
                    if (!fieldNames[i].equals(childFields[i].getName())) {
                        return false;
                    }
                }
            } else {
                return false;
            }
        }
        return true;
    }
}


// End RemoveTrivialProjectRule.java
