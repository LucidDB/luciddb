/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2002-2005 Disruptive Tech
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
import org.eigenbase.relopt.RelOptCost;
import org.eigenbase.relopt.RelOptPlanner;
import org.eigenbase.relopt.RelOptUtil;


// REVIEW jvs 15-Nov-2003:  Is there a good reason for this to exist?  It's
// redundant with an AggregateRel with no aggCalls.

/**
 * <code>DistinctRel</code> is a {@link RelNode} which eliminates
 * duplicates from its input.
 *
 * @author jhyde
 * @version $Id$
 *
 * @since 30 September, 2001
 */
public class DistinctRel extends SingleRel
{
    //~ Constructors ----------------------------------------------------------

    public DistinctRel(
        RelOptCluster cluster,
        RelNode child)
    {
        super(cluster, child);
    }

    //~ Methods ---------------------------------------------------------------

    public boolean isDistinct()
    {
        return true;
    }

    public Object clone()
    {
        return new DistinctRel(
            cluster,
            RelOptUtil.clone(child));
    }

    public RelOptCost computeSelfCost(RelOptPlanner planner)
    {
        return planner.makeTinyCost();
    }
}


// End DistinctRel.java
