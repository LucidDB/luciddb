/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005-2009 The Eigenbase Project
// Copyright (C) 2002-2009 SQLstream, Inc.
// Copyright (C) 2005-2009 LucidEra, Inc.
// Portions Copyright (C) 2003-2009 John V. Sichi
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
import org.eigenbase.rex.*;


/**
 * A <code>FilterRel</code> is a relational expression which iterates over its
 * input, and returns elements for which <code>condition</code> evaluates to
 * <code>true</code>.
 */
public final class FilterRel
    extends FilterRelBase
{
    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a filter.
     *
     * @param cluster {@link RelOptCluster}  this relational expression belongs
     * to
     * @param child input relational expression
     * @param condition boolean expression which determines whether a row is
     * allowed to pass
     */
    public FilterRel(
        RelOptCluster cluster,
        RelNode child,
        RexNode condition)
    {
        super(
            cluster,
            new RelTraitSet(CallingConvention.NONE),
            child,
            condition);
    }

    //~ Methods ----------------------------------------------------------------

    public FilterRel clone()
    {
        FilterRel clone =
            new FilterRel(
                getCluster(),
                getChild().clone(),
                getCondition().clone());
        clone.inheritTraitsFrom(this);
        return clone;
    }
}

// End FilterRel.java
