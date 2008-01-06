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
import org.eigenbase.reltype.*;

/**
 * <code>EmptyRel</code> represents a relational expression with zero rows.
 *
 * <p>EmptyRel can not be implemented, but serves as a token for rules to
 * match so that empty sections of queries can be eliminated.
 *
 * <p>Rules:
 * <ul>
 * <li>Created by {@link net.sf.farrago.query.FarragoReduceValuesRule}</li>
 * <li>Triggers {@link org.eigenbase.rel.rules.RemoveEmptyRule}</li>
 * </ul>
 *
 * @see org.eigenbase.rel.ValuesRel
 *
 * @author jhyde
 * @version $Id$
 */
public class EmptyRel
    extends AbstractRelNode
{
    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a new EmptyRel.
     *
     * @param cluster Cluster
     * @param rowType row type for tuples which would be produced by this rel
     * if it actually produced any, but it doesn't (see,
     * philosophy is good for something after all!)
     */
    public EmptyRel(
        RelOptCluster cluster,
        RelDataType rowType)
    {
        super(
            cluster,
            new RelTraitSet(CallingConvention.NONE));
        this.rowType = rowType;
    }

    // override Object
    public EmptyRel clone()
    {
        // immutable with no children
        return this;
    }

    // implement RelNode
    protected RelDataType deriveRowType()
    {
        return rowType;
    }

    // implement RelNode
    public RelOptCost computeSelfCost(RelOptPlanner planner)
    {
        return planner.makeZeroCost();
    }

    // implement RelNode
    public double getRows()
    {
        return 0.0;
    }
}

// End EmptyRel.java
