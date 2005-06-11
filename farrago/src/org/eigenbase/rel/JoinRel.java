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
import org.eigenbase.util.*;

/**
 * A JoinRel represents two relational expressions joined according to some
 * condition.
 */
public final class JoinRel extends JoinRelBase
{
    //~ Constructors ----------------------------------------------------------

    public JoinRel(
        RelOptCluster cluster,
        RelNode left,
        RelNode right,
        RexNode condition,
        int joinType,
        Set variablesStopped)
    {
        super(
            cluster, new RelTraitSet(CallingConvention.NONE), left, right,
            condition, joinType, variablesStopped);
    }

    //~ Methods ---------------------------------------------------------------

    public Object clone()
    {
        JoinRel clone = new JoinRel(
            getCluster(),
            RelOptUtil.clone(left),
            RelOptUtil.clone(right),
            RexUtil.clone(condition),
            joinType,
            new HashSet(variablesStopped));
        clone.inheritTraitsFrom(this);
        return clone;
    }
}

// End JoinRel.java
