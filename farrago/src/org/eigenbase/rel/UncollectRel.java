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
import org.eigenbase.relopt.RelOptUtil;
import org.eigenbase.reltype.RelDataType;

/**
 * A relational expression which extract "collapses"
 * of multiply rows from one row
 *
 * @author Wael Chatila 
 * @since Dec 12, 2004
 * @version $Id$
 */
public class UncollectRel extends SingleRel {

    public UncollectRel(RelOptCluster cluster, RelNode child) {
        super(cluster, child);
    }

    // override Object (public, does not throw CloneNotSupportedException)
    public Object clone() {
        return new UncollectRel(cluster, RelOptUtil.clone(child));
    }

    protected RelDataType deriveRowType()
    {
        return deriveUncollectRowType(this);
    }

    public static RelDataType deriveUncollectRowType(SingleRel rel)
    {
        RelDataType inputType = rel.child.getRowType();
        assert(inputType.isStruct());
        assert(1 == inputType.getFields().length);
        RelDataType ret =
            inputType.getFields()[0].getType().getComponentType();
        assert(null != ret);
        assert(ret.isStruct());
        return ret;
    }
}
