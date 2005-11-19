/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2005-2005 Disruptive Tech
// Copyright (C) 2005-2005 LucidEra, Inc.
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
 * <code>UnionRelBase</code> is an abstract base class for implementations
 * of {@link UnionRel}.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public abstract class UnionRelBase extends SetOpRel
{
    protected UnionRelBase(
        RelOptCluster cluster,
        RelTraitSet traits,
        RelNode [] inputs,
        boolean all)
    {
        super(cluster, traits, inputs, all);
    }
    
    // implement RelNode
    public double getRows()
    {
        double dRows = estimateRowCount(this);
        if (isDistinct()) {
            dRows *= 0.5;
        }
        return dRows;
    }

    /**
     * Helper method for computing row count for UNION ALL.
     *
     * @param rel node representing UNION ALL
     *
     * @return estimated row count for rel
     */
    public static double estimateRowCount(RelNode rel)
    {
        double dRows = 0;
        for (int i = 0; i < rel.getInputs().length; i++) {
            dRows += rel.getInputs()[i].getRows();
        }
        return dRows;
    }
}

// End UnionRelBase.java
