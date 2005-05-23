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

import org.eigenbase.relopt.RelOptRule;
import org.eigenbase.relopt.RelOptRuleCall;
import org.eigenbase.relopt.RelOptRuleOperand;


/**
 * <code>UnionToDistinctRule</code> translates a distinct {@link UnionRel}
 * (<code>all</code> = <code>false</code>) into a {@link DistinctRel} on top
 * of a non-distinct {@link UnionRel} (<code>all</code> = <code>true</code>).
 */
public class UnionToDistinctRule extends RelOptRule
{
    //~ Constructors ----------------------------------------------------------

    public UnionToDistinctRule()
    {
        super(new RelOptRuleOperand(UnionRel.class, null));
    }

    //~ Methods ---------------------------------------------------------------

    public void onMatch(RelOptRuleCall call)
    {
        UnionRel union = (UnionRel) call.rels[0];
        if (union.getClass() != UnionRel.class) {
            return; // require precise class, otherwise we loop
        }
        if (union.all) {
            return; // nothing to do
        }
        UnionRel unionAll = new UnionRel(union.cluster, union.inputs, true);
        call.transformTo(new DistinctRel(union.cluster, unionAll));
    }
}


// End UnionToDistinctRule.java
