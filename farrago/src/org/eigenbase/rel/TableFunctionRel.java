/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2006-2006 The Eigenbase Project
// Copyright (C) 2006-2006 Disruptive Tech
// Copyright (C) 2006-2006 LucidEra, Inc.
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

import org.eigenbase.rex.*;
import org.eigenbase.relopt.*;
import org.eigenbase.reltype.*;
import org.eigenbase.sql.type.*;

/**
 * <code>TableFunctionRel</code> represents a call to a function which returns
 * a result set.  Currently, it can only appear as a leaf in
 * a query tree, but eventually we will extend it to take relational
 * inputs.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class TableFunctionRel extends TableFunctionRelBase
{
    //~ Constructors ----------------------------------------------------------
    
    /**
     * Creates a <code>TableFunctionRel</code>.
     *
     * @param cluster {@link RelOptCluster} this relational expression
     *        belongs to
     *
     * @param rexCall function invocation expression
     *
     * @param rowType row type produced by function
     */
    public TableFunctionRel(
        RelOptCluster cluster, RexNode rexCall, RelDataType rowType)
    {
        super(
            cluster,
            new RelTraitSet(CallingConvention.NONE),
            rexCall,
            rowType);
    }

    public RelOptCost computeSelfCost(RelOptPlanner planner)
    {
        // REVIEW jvs 8-Jan-2006:  what is supposed to be here
        // for an abstract rel?
        return planner.makeHugeCost();
    }
}

// End TableFunctionRel.java
