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
 * <code>TableFunctionRelBase</code> is an abstract base class for
 * implementations of {@link TableFunctionRel}.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public abstract class TableFunctionRelBase extends AbstractRelNode
{
    private final RexNode rexCall;

    private final RelDataType rowType;
    
    //~ Constructors ----------------------------------------------------------
    
    /**
     * Creates a <code>TableFunctionRelBase</code>.
     *
     * @param cluster {@link RelOptCluster} this relational expression
     *        belongs to
     *
     * @param rexCall function invocation expression
     *
     * @param rowType row type produced by function
     */
    protected TableFunctionRelBase(
        RelOptCluster cluster,
        RelTraitSet traits,
        RexNode rexCall, RelDataType rowType)
    {
        super(cluster, traits);
        this.rexCall = rexCall;
        this.rowType = rowType;
    }
    
    //~ Methods ---------------------------------------------------------------

    public RexNode [] getChildExps()
    {
        return new RexNode [] 
            {
                rexCall
            };
    }

    public RexNode getCall()
    {
        return rexCall;
    }

    public void explain(RelOptPlanWriter pw)
    {
        pw.explain(this, new String [] { "invocation" });
    }
    
    public Object clone()
    {
        return this;
    }

    protected RelDataType deriveRowType()
    {
        return rowType;
    }
}

// End TableFunctionRelBase.java
