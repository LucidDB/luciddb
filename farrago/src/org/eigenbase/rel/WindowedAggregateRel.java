/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2005-2005 Disruptive Tech
// Copyright (C) 2005-2005 Red Square, Inc.
// Portions Copyright (C) 2003-2005 John V. Sichi
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

import org.eigenbase.relopt.RelOptCluster;
import org.eigenbase.relopt.RelTraitSet;
import org.eigenbase.rex.RexInputRef;
import org.eigenbase.rex.RexNode;
import org.eigenbase.rex.RexOver;
import org.eigenbase.reltype.RelDataType;
import org.eigenbase.util.Util;

/**
 * A relational expression representing a set of window aggregates.
 *
 * <p>Rules:<ul>
 * <li>Created by {@link com.disruptivetech.farrago.rel.WindowedAggSplitterRule}.
 * <li>Triggers {@link com.disruptivetech.farrago.rel.FennelWindowRule}.
 */
public class WindowedAggregateRel extends SingleRel
{
    public final RexNode[] aggs;

    /**
     * Creates a WindowedAggregateRel.
     *
     * @param cluster
     * @param traits
     * @param child
     * @param aggs Array of expressions. Each expression must be
     *            either a {@link RexInputRef},  or a {@link RexOver} whose
     *            arguments are all {@link RexInputRef}.
     * @param rowType
     */
    public WindowedAggregateRel(
        RelOptCluster cluster,
        RelTraitSet traits,
        RelNode child,
        RexNode[] aggs,
        RelDataType rowType)
    {
        super(cluster, traits, child);
        this.rowType = rowType;
        this.aggs = aggs;
        for (int i = 0; i < aggs.length; i++) {
            RexNode agg = aggs[i];
            if (agg instanceof RexOver) {
                RexOver over = (RexOver) agg;
                for (int j = 0; j < over.operands.length; j++) {
                    RexNode operand = over.operands[j];
                    Util.pre(operand instanceof RexInputRef,
                        "aggs[i].operand[j] instanceof RexInputRef");
                }
            } else if (agg instanceof RexInputRef) {
                ;
            } else {
                Util.pre(false,
                    "aggs[i] instanceof RexInputRef ||" +
                    "aggs[i] instanceof RexOver");
            }
        }
    }

    public Object clone()
    {
        return new WindowedAggregateRel(cluster, traits, child, aggs, rowType);
    }
}

// End WindowedAggregateRel.java
