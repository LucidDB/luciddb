/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005 The Eigenbase Project
// Copyright (C) 2002 SQLstream, Inc.
// Copyright (C) 2005 Dynamo BI Corporation
// Portions Copyright (C) 2003 John V. Sichi
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

import java.util.*;

import org.eigenbase.relopt.*;


/**
 * <code>AggregateRel</code> is a relational operator which eliminates
 * duplicates and computes totals.
 *
 * <p>Rules:
 *
 * <ul>
 * <li>{@link org.eigenbase.rel.rules.PullConstantsThroughAggregatesRule}
 * <li>{@link org.eigenbase.rel.rules.RemoveDistinctAggregateRule}
 * <li>{@link org.eigenbase.rel.rules.ReduceAggregatesRule}.
 *
 * @author jhyde
 * @version $Id$
 * @since 3 February, 2002
 */
public final class AggregateRel
    extends AggregateRelBase
{
    //~ Constructors -----------------------------------------------------------

    /**
     * Creates an AggregateRel.
     *
     * @param cluster {@link RelOptCluster}  this relational expression belongs
     * to
     * @param child input relational expression
     * @param groupCount Number of columns to group on
     * @param aggCalls Array of aggregates to compute
     *
     * @pre aggCalls != null
     */
    public AggregateRel(
        RelOptCluster cluster,
        RelNode child,
        int groupCount,
        List<AggregateCall> aggCalls)
    {
        super(
            cluster,
            new RelTraitSet(CallingConvention.NONE),
            child,
            groupCount,
            aggCalls);
    }

    //~ Methods ----------------------------------------------------------------

    public AggregateRel clone()
    {
        AggregateRel clone =
            new AggregateRel(
                getCluster(),
                getChild().clone(),
                groupCount,
                aggCalls);
        clone.inheritTraitsFrom(this);
        return clone;
    }
}

// End AggregateRel.java
