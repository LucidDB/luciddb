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

import java.util.*;

import org.eigenbase.relopt.*;
import org.eigenbase.reltype.*;
import org.eigenbase.rex.*;

/**
 * A SemiJoinRel represents two relational expressions joined according to
 * some condition, where the output only contains the columns from the
 * left join input.
 *
 * @version $Id$
 * @author Zelaine Fong
 */
public final class SemiJoinRel extends JoinRelBase
{

    //~ Instance fields -------------------------------------------------------

    private int rightOrdinal;
    
    //~ Constructors ----------------------------------------------------------

    /**
     * @param cluster cluster that join belongs to
     * @param left left join input
     * @param right right join input
     * @param condition join condition
     * @param rightOrdinal ordinal from the right input that participates in
     * the join
     */
    public SemiJoinRel(
        RelOptCluster cluster,
        RelNode left,
        RelNode right,
        RexNode condition,
        int rightOrdinal)
    {
        super(
            cluster, new RelTraitSet(CallingConvention.NONE), left, right,
            condition, JoinRelType.INNER, Collections.EMPTY_SET);
        this.rightOrdinal = rightOrdinal;
    }

    //~ Methods ---------------------------------------------------------------

    public Object clone()
    {
        SemiJoinRel clone = new SemiJoinRel(
            getCluster(),
            RelOptUtil.clone(left),
            RelOptUtil.clone(right),
            RexUtil.clone(condition),
            getRightOrdinal());
        clone.inheritTraitsFrom(this);
        return clone;
    }
    
    /**
     * @return returns rowtype representing only the left join input
     */
    public RelDataType deriveRowType()
    {
        return deriveJoinRowType(
            left.getRowType(), null, JoinRelType.INNER,
            getCluster().getTypeFactory(), null);
    }
    
    public int getRightOrdinal()
    {
        return rightOrdinal;
    }

}

// End SemiJoinRel.java
