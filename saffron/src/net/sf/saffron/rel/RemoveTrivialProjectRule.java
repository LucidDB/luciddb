/*
// $Id$
// Saffron preprocessor and data engine
// (C) Copyright 2002-2003 Disruptive Technologies, Inc.
// (C) Copyright 2003-2004 John V. Sichi
// You must accept the terms in LICENSE.html to use this software.
//
// This program is free software; you can redistribute it and/or
// modify it under the terms of the GNU Lesser General Public License
// as published by the Free Software Foundation; either version 2.1
// of the License, or (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with this program; if not, write to the Free Software
// Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
*/

package net.sf.saffron.rel;

import net.sf.saffron.core.SaffronField;
import net.sf.saffron.core.SaffronType;
import net.sf.saffron.opt.RuleOperand;
import net.sf.saffron.opt.VolcanoRule;
import net.sf.saffron.opt.VolcanoRuleCall;
import net.sf.saffron.rex.RexInputRef;
import net.sf.saffron.rex.RexNode;

/**
 * Rule which, given a {@link ProjectRel} node which merely returns its input,
 * converts the node into its child.
 * 
 * <p>
 * For example, <code>ProjectRel(ArrayReader(a), {$input0})</code> becomes
 * <code>ArrayReader(a)</code>.
 * </p>
 */
public class RemoveTrivialProjectRule extends VolcanoRule
{
    //~ Constructors ----------------------------------------------------------

    public RemoveTrivialProjectRule()
    {
        super(new RuleOperand(ProjectRel.class,null));
    }

    //~ Methods ---------------------------------------------------------------

    public void onMatch(VolcanoRuleCall call)
    {
        ProjectRel project = (ProjectRel) call.rels[0];
        SaffronRel child = project.child;
        final SaffronType childRowType = child.getRowType();
        if (!childRowType.isProject()) {
            return;
        }
        if (!project.isBoxed()) {
            return;
        }
        if (!isIdentity(project.exps, project.getFieldNames(), childRowType)) {
            return;
        }
        child = call.planner.register(child,project);
        child = convert(child,project.getConvention());
        if (child != null) {
            call.transformTo(child);
        }
    }

    private static boolean isIdentity(
        RexNode[] exps,
        String [] fieldNames,
        SaffronType childRowType)
    {
        int fieldCount = childRowType.getFieldCount();
        SaffronField [] childFields = childRowType.getFields();
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
