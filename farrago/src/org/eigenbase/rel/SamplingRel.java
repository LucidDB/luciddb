/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2007-2007 The Eigenbase Project
// Copyright (C) 2007-2007 Disruptive Tech
// Copyright (C) 2007-2007 LucidEra, Inc.
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


/**
 * SamplingRel represents the TABLESAMPLE BERNOULLI or SYSTEM keyword applied to
 * a table, view or subquery.
 *
 * @author Stephan Zuercher
 */
public class SamplingRel
    extends SingleRel
{
    //~ Instance fields --------------------------------------------------------

    private final RelOptSamplingParameters params;

    //~ Constructors -----------------------------------------------------------

    public SamplingRel(
        RelOptCluster cluster,
        RelNode child,
        RelOptSamplingParameters params)
    {
        super(cluster, new RelTraitSet(CallingConvention.NONE), child);

        this.params = params;
    }

    //~ Methods ----------------------------------------------------------------

    public RelNode clone()
    {
        SamplingRel clone = new SamplingRel(getCluster(), getChild(), params);
        clone.inheritTraitsFrom(this);
        return clone;
    }

    /**
     * Retrieve the sampling parameters for this SamplingRel.
     */
    public RelOptSamplingParameters getSamplingParameters()
    {
        return params;
    }

    // implement RelNode
    public void explain(RelOptPlanWriter pw)
    {
        pw.explain(
            this,
            new String[] { "child", "mode", "rate", "repeatableSeed" },
            new Object[] {
                params.isBernoulli() ? "bernoulli" : "system",
                params.getSamplingPercentage(),
                params.isRepeatable() ? params.getRepeatableSeed() : "-"
            });
    }
}

// End SamplingRel.java
