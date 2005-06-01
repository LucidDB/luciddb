/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2002-2005 Disruptive Tech
// Copyright (C) 2005-2005 LucidEra, Inc.
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

import org.eigenbase.relopt.*;
import org.eigenbase.reltype.*;

/**
 * <code>UnionRel</code> returns the union of the rows of its inputs,
 * optionally eliminating duplicates.
 *
 * @author jhyde
 * @version $Id$
 *
 * @since 23 September, 2001
 */
public final class UnionRel extends UnionRelBase
{
    //~ Constructors ----------------------------------------------------------

    public UnionRel(
        RelOptCluster cluster,
        RelNode [] inputs,
        boolean all)
    {
        super(cluster, new RelTraitSet(CallingConvention.NONE), inputs, all);
    }

    //~ Methods ---------------------------------------------------------------

    public Object clone()
    {
        UnionRel clone = new UnionRel(
            getCluster(),
            RelOptUtil.clone(inputs),
            all);
        clone.inheritTraitsFrom(this);
        return clone;
    }
}


// End UnionRel.java
