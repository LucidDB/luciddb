/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005-2007 The Eigenbase Project
// Copyright (C) 2005-2007 SQLstream, Inc.
// Copyright (C) 2005-2007 LucidEra, Inc.
// Portions Copyright (C) 2003-2007 John V. Sichi
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
package org.eigenbase.rel;

import org.eigenbase.relopt.*;
import org.eigenbase.reltype.*;
import org.eigenbase.rex.*;


/**
 * A relational expression representing a set of window aggregates.
 *
 * <p>Rules:
 *
 * <ul>
 * <li>Created by {@link
 * com.disruptivetech.farrago.rel.WindowedAggSplitterRule}.
 * <li>Triggers {@link com.disruptivetech.farrago.rel.FennelWindowRule}.
 */
public final class WindowedAggregateRel
    extends SingleRel
{
    //~ Instance fields --------------------------------------------------------

    public final RexProgram program;

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a WindowedAggregateRel.
     *
     * @param cluster
     * @param traits
     * @param child
     * @param program Program containing an array of expressions. The program
     * must not have a condition, and each expression must be either a {@link
     * RexLocalRef}, or a {@link RexOver} whose arguments are all {@link
     * RexLocalRef}.
     * @param rowType
     */
    public WindowedAggregateRel(
        RelOptCluster cluster,
        RelTraitSet traits,
        RelNode child,
        RexProgram program,
        RelDataType rowType)
    {
        super(cluster, traits, child);
        this.rowType = rowType;
        this.program = program;
        assert isValid(true);
    }

    //~ Methods ----------------------------------------------------------------

    public boolean isValid(boolean fail)
    {
        if (!program.isValid(fail)) {
            return false;
        }
        if (program.getCondition() != null) {
            assert !fail : "Agg program must not have condition";
            return false;
        }
        int i = -1;
        for (RexNode agg : program.getExprList()) {
            ++i;
            if (agg instanceof RexOver) {
                RexOver over = (RexOver) agg;
                for (int j = 0; j < over.operands.length; j++) {
                    RexNode operand = over.operands[j];
                    if (!(operand instanceof RexLocalRef)) {
                        assert !fail : "aggs[" + i + "].operand[" + j
                            + "] is not a RexLocalRef";
                        return false;
                    }
                }
            } else if (agg instanceof RexInputRef) {
                ;
            } else {
                assert !fail : "aggs[" + i + "] is a " + agg.getClass()
                    + ", expecting RexInputRef or RexOver";
            }
        }
        return true;
    }

    public RexProgram getProgram()
    {
        return program;
    }

    public void explain(RelOptPlanWriter pw)
    {
        program.explainCalc(this, pw);
    }

    public WindowedAggregateRel clone()
    {
        return new WindowedAggregateRel(
            getCluster(),
            traits,
            getChild(),
            program,
            rowType);
    }
}

// End WindowedAggregateRel.java
