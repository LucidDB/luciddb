/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2002-2005 Disruptive Tech
// Copyright (C) 2005-2005 Red Square, Inc.
// Portions Copyright (C) 2003-2005 John V. Sichi
//
// This program is free software; you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation; either version 2 of the License, or
// (at your option) any later Eigenbase-approved version.
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
import org.eigenbase.rex.RexNode;
import org.eigenbase.rex.RexUtil;


/**
 * Rule to convert a {@link ProjectRel} to a {@link CalcRel}
 *
 * <p>The rule does not fire if the child is a {@link ProjectRel},
 * {@link FilterRel} or {@link CalcRel}. If it did, then the same
 * {@link CalcRel} would be formed via several transformation paths, which
 * is a waste of effort.</p>
 *
 * @see FilterToCalcRule
 *
 * @author jhyde
 * @since Mar 7, 2004
 * @version $Id$
 **/
public class ProjectToCalcRule extends RelOptRule
{
    //~ Static fields/initializers --------------------------------------------

    public static ProjectToCalcRule instance = new ProjectToCalcRule();

    //~ Constructors ----------------------------------------------------------

    private ProjectToCalcRule()
    {
        super(new RelOptRuleOperand(
                ProjectRel.class,
                new RelOptRuleOperand [] {
                    new RelOptRuleOperand(RelNode.class, null),
                }));
    }

    //~ Methods ---------------------------------------------------------------

    public void onMatch(RelOptRuleCall call)
    {
        final ProjectRel project = (ProjectRel) call.rels[0];
        final RelNode child = call.rels[1];
        if (child instanceof FilterRel || child instanceof ProjectRel
                || child instanceof CalcRel) {
            // don't create a CalcRel if the input is, or is potentially, a
            // CalcRel
            return;
        }
        final RelDataType rowType = project.getRowType();
        final RexNode [] projectExprs = RexUtil.clone(project.exps);
        final CalcRel calc =
            new CalcRel(project.cluster, child, rowType, projectExprs, null);
        call.transformTo(calc);
    }
}


// End ProjectToCalcRule.java
