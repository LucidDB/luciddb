/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2006-2006 The Eigenbase Project
// Copyright (C) 2006-2006 Disruptive Tech
// Copyright (C) 2006-2006 LucidEra, Inc.
// Portions Copyright (C) 2006-2006 John V. Sichi
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

import org.eigenbase.rel.CalcRel;
import org.eigenbase.rel.RelNode;
import org.eigenbase.relopt.RelOptRule;
import org.eigenbase.relopt.RelOptRuleCall;
import org.eigenbase.relopt.RelOptRuleOperand;
import org.eigenbase.rex.RexProgram;

/**
 * Rule which removes a trivial {@link CalcRel}.
 *
 * <p>A {@link CalcRel} is trivial if it projects its input fields in their
 * original order, and it does not filter.
 *
 * @see org.eigenbase.rel.RemoveTrivialProjectRule
 *
 * @author Julian Hyde
 * @version $Id$
 */
public class RemoveTrivialCalcRule
    extends RelOptRule
{
    public static final RemoveTrivialCalcRule instance =
        new RemoveTrivialCalcRule();
    
    //~ Constructors -----------------------------------------------------------

    private RemoveTrivialCalcRule()
    {
        super(
            new RelOptRuleOperand(
                CalcRel.class,
                null));
    }

    //~ Methods ----------------------------------------------------------------

    // implement RelOptRule
    public void onMatch(RelOptRuleCall call)
    {
        CalcRel calc = (CalcRel) call.rels[0];
        RexProgram program = calc.getProgram();
        if (!program.isTrivial()) {
            return;
        }
        RelNode child = calc.getInput(0);
        child = call.getPlanner().register(child, calc);
        child = convert(
                child,
                calc.getTraits());
        if (child != null) {
            call.transformTo(child);
        }
    }
}

// End RemoveTrivialCalcRule.java
