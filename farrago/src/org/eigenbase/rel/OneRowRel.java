/*
// $Id$
// Package org.eigenbase is a class library of database components.
// Copyright (C) 2002-2004 Disruptive Tech
// Copyright (C) 2003-2004 John V. Sichi
//
// This program is free software; you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation; either version 2 of the License, or
// (at your option) any later version.
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
 * <code>OneRowRel</code> always returns one row, one column (containing
 * the value 0).
 *
 * @author jhyde
 * @version $Id$
 *
 * @since 23 September, 2001
 */
public class OneRowRel extends AbstractRelNode
{
    //~ Constructors ----------------------------------------------------------

    /**
     * Creates an <code>OneRow</code>.
     *
     * @param cluster {@link RelOptCluster} this relational expression
     *        belongs to
     */
    public OneRowRel(RelOptCluster cluster)
    {
        super(cluster);
    }

    //~ Methods ---------------------------------------------------------------

    public Object clone()
    {
        return new OneRowRel(cluster);
    }

    public RelOptCost computeSelfCost(RelOptPlanner planner)
    {
        return planner.makeTinyCost();
    }

    protected RelDataType deriveRowType()
    {
        return cluster.typeFactory.createProjectType(
            new RelDataType [] { cluster.typeFactory.createJavaType(
                    Integer.TYPE) },
            new String [] { "ZERO" });
    }
}


// End OneRowRel.java
