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

import org.eigenbase.rel.CalcRel;
import org.eigenbase.rel.RelNode;
import org.eigenbase.rel.WindowedAggregateRel;
import org.eigenbase.relopt.*;
import org.eigenbase.reltype.RelDataType;
import org.eigenbase.rex.*;
import org.eigenbase.util.Glossary;
import org.eigenbase.util.Util;

/**
 * Rule which slices the {@link CalcRel} into sections which contain
 * windowed agg functions and sections which do not.
 *
 * <p>The sections which contain windowed agg functions become instances of
 * {@link WindowedAggregateRel}. If the {@link CalcRel}
 * does not contain any windowed agg functions, does nothing.
 *
 * @author Julian Hyde
 * @since April 24, 2005
 * @version $Id$
 */
public class WindowedAggSplitterRule extends RelOptRule
{
    //~ Static fields/initializers --------------------------------------------
    private static final CalcRelSplitter.RelType CalcRelType =
        new CalcRelSplitter.RelType("CalcRelType");

    private static final CalcRelSplitter.RelType WinAggRelType =
        new CalcRelSplitter.RelType("WinAggRelType");

    /** The {@link Glossary#SingletonPattern singleton} instance. */
    public static final WindowedAggSplitterRule instance =
        new WindowedAggSplitterRule();

    //~ Constructors ----------------------------------------------------------

    /**
     * Creates a rule.
     */
    private WindowedAggSplitterRule()
    {
        super(new RelOptRuleOperand(
            CalcRel.class,
            new RelOptRuleOperand [] {
                new RelOptRuleOperand(RelNode.class, null)
            }));
    }

    //~ Methods ---------------------------------------------------------------

    public void onMatch(RelOptRuleCall call)
    {
        CalcRel calc = (CalcRel) call.rels[0];
        if (!RexOver.containsOver(calc.projectExprs, calc.getCondition())) {
            return;
        }
        CalcRel calcClone = (CalcRel) calc.clone();
        CalcRelSplitter transform = new WindowedAggRelSplitter(calcClone);
        RelNode newRel = transform.execute();
        call.transformTo(newRel);
    }

    //~ Inner Classes ---------------------------------------------------------

    /**
     * Splitter which distinguishes between windowed aggregation expressions
     * (calls to {@link RexOver}) and ordinary expressions.
     */
    static class WindowedAggRelSplitter extends CalcRelSplitter
    {
        WindowedAggRelSplitter(CalcRel calc)
        {
            super(calc, CalcRelType, WinAggRelType);
        }

        protected boolean canImplementAs(RexCall call, RelType relType)
        {
            return call instanceof RexOver ?
                relType == WinAggRelType :
                relType == CalcRelType;
        }

        protected boolean canImplementAs(RexDynamicParam param, RelType relType)
        {
            return relType == CalcRelType;
        }

        protected boolean canImplementAs(RexFieldAccess field, RelType relType)
        {
            return relType == CalcRelType;
        }

        protected RelNode makeRel(
            RelType relType,
            RelOptCluster cluster,
            RelTraitSet traits,
            RelDataType rowType,
            RelNode child,
            RexNode[] exprs,
            RexNode conditionExpr)
        {
            if (relType == CalcRelType) {
                RelNode node = super.makeRel(
                    relType, cluster, traits, rowType, child, exprs,
                    conditionExpr);
                assert node instanceof CalcRel;
                ((CalcRel) node).setAggs(true);
                return node;
            } else {
                Util.permAssert(conditionExpr == null,
                    "FennelWindowRel cannot accept a condition");
                return new WindowedAggregateRel(
                    cluster, traits, child, exprs, rowType);
            }
        }
    }
}

// End WindowedAggSplitterRule.java
