/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2005 Disruptive Tech
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
package com.disruptivetech.farrago.rel;

import org.eigenbase.rel.*;
import org.eigenbase.relopt.*;
import org.eigenbase.reltype.*;
import org.eigenbase.rex.*;
import org.eigenbase.util.*;


/**
 * Rule which slices the {@link CalcRel} into sections which contain windowed
 * agg functions and sections which do not.
 *
 * <p>The sections which contain windowed agg functions become instances of
 * {@link WindowedAggregateRel}. If the {@link CalcRel} does not contain any
 * windowed agg functions, does nothing.
 *
 * @author Julian Hyde
 * @version $Id$
 * @since April 24, 2005
 */
public class WindowedAggSplitterRule
    extends RelOptRule
{
    //~ Static fields/initializers ---------------------------------------------

    /**
     * The {@link Glossary#SingletonPattern singleton} instance.
     */
    public static final WindowedAggSplitterRule instance =
        new WindowedAggSplitterRule();

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a rule.
     */
    private WindowedAggSplitterRule()
    {
        super(new RelOptRuleOperand(
                CalcRel.class,
                null));
    }

    //~ Methods ----------------------------------------------------------------

    public void onMatch(RelOptRuleCall call)
    {
        CalcRel calc = (CalcRel) call.rels[0];
        if (!RexOver.containsOver(calc.getProgram())) {
            return;
        }
        CalcRel calcClone = calc.clone();
        CalcRelSplitter transform = new WindowedAggRelSplitter(calcClone);
        RelNode newRel = transform.execute();
        call.transformTo(newRel);
    }

    //~ Inner Classes ----------------------------------------------------------

    /**
     * Splitter which distinguishes between windowed aggregation expressions
     * (calls to {@link RexOver}) and ordinary expressions.
     */
    static class WindowedAggRelSplitter
        extends CalcRelSplitter
    {
        WindowedAggRelSplitter(CalcRel calc)
        {
            super(
                calc,
                new RelType[] {
                    new CalcRelSplitter.RelType("CalcRelType") {
                        protected boolean canImplement(RexFieldAccess field)
                        {
                            return true;
                        }

                        protected boolean canImplement(RexDynamicParam param)
                        {
                            return true;
                        }

                        protected boolean canImplement(RexLiteral literal)
                        {
                            return true;
                        }

                        protected boolean canImplement(RexCall call)
                        {
                            return !(call instanceof RexOver);
                        }

                        protected RelNode makeRel(
                            RelOptCluster cluster,
                            RelTraitSet traits,
                            RelDataType rowType,
                            RelNode child,
                            RexProgram program)
                        {
                            assert !program.containsAggs();
                            return super.makeRel(
                                cluster,
                                traits,
                                rowType,
                                child,
                                program);
                        }
                    }
                    ,

                    new CalcRelSplitter.RelType("WinAggRelType") {
                        protected boolean canImplement(RexFieldAccess field)
                        {
                            return false;
                        }

                        protected boolean canImplement(RexDynamicParam param)
                        {
                            return false;
                        }

                        protected boolean canImplement(RexLiteral literal)
                        {
                            return false;
                        }

                        protected boolean canImplement(RexCall call)
                        {
                            return call instanceof RexOver;
                        }

                        protected boolean supportsCondition()
                        {
                            return false;
                        }

                        protected RelNode makeRel(
                            RelOptCluster cluster,
                            RelTraitSet traits,
                            RelDataType rowType,
                            RelNode child,
                            RexProgram program)
                        {
                            Util.permAssert(
                                program.getCondition() == null,
                                "WindowedAggregateRel cannot accept a condition");
                            return new WindowedAggregateRel(
                                cluster,
                                traits,
                                child,
                                program,
                                rowType);
                        }
                    }
                });
        }
    }
}

// End WindowedAggSplitterRule.java
