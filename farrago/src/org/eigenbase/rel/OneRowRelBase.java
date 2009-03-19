/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005-2009 The Eigenbase Project
// Copyright (C) 2005-2009 SQLstream, Inc.
// Copyright (C) 2005-2009 LucidEra, Inc.
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
import org.eigenbase.sql.type.*;


/**
 * <code>OneRowRelBase</code> is an abstract base class for implementations of
 * {@link OneRowRel}.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public abstract class OneRowRelBase
    extends AbstractRelNode
{
    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a <code>OneRowRelBase</code> with specific traits.
     *
     * @param cluster {@link RelOptCluster}  this relational expression belongs
     * to
     * @param traits for this rel
     */
    protected OneRowRelBase(RelOptCluster cluster, RelTraitSet traits)
    {
        super(cluster, traits);
    }

    //~ Methods ----------------------------------------------------------------

    public OneRowRelBase clone()
    {
        return this;
    }

    public RelOptCost computeSelfCost(RelOptPlanner planner)
    {
        return planner.makeTinyCost();
    }

    protected RelDataType deriveRowType()
    {
        return deriveOneRowType(getCluster().getTypeFactory());
    }

    public static RelDataType deriveOneRowType(RelDataTypeFactory typeFactory)
    {
        return typeFactory.createStructType(
            new RelDataType[] {
                typeFactory.createSqlType(
                    SqlTypeName.INTEGER)
            },
            new String[] { "ZERO" });
    }
}

// End OneRowRelBase.java
