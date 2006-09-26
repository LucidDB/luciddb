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
package org.eigenbase.rel.rules;

import org.eigenbase.rel.*;
import org.eigenbase.relopt.*;
import org.eigenbase.reltype.*;
import org.eigenbase.rex.*;


/**
 * A MultiJoinRel represents a join of N inputs, whereas other join relnodes
 * represent strictly binary joins.
 *
 * @author Zelaine Fong
 * @version $Id$
 */
public final class MultiJoinRel
    extends AbstractRelNode
{

    //~ Instance fields --------------------------------------------------------

    private RelNode [] inputs;
    RexNode joinFilter;
    RelDataType rowType;

    //~ Constructors -----------------------------------------------------------

    /**
     * @param cluster cluster that join belongs to
     * @param inputs inputs into this multirel join
     * @param joinFilters join filters applicable to this join node
     * @param rowType row type of the join result of this node
     */
    public MultiJoinRel(
        RelOptCluster cluster,
        RelNode [] inputs,
        RexNode joinFilter,
        RelDataType rowType)
    {
        super(
            cluster,
            new RelTraitSet(CallingConvention.NONE));
        this.inputs = inputs;
        this.joinFilter = joinFilter;
        this.rowType = rowType;
    }

    //~ Methods ----------------------------------------------------------------

    public MultiJoinRel clone()
    {
        MultiJoinRel clone =
            new MultiJoinRel(
                getCluster(),
                RelOptUtil.clone(inputs),
                joinFilter.clone(),
                rowType);
        clone.inheritTraitsFrom(this);
        return clone;
    }

    public void explain(RelOptPlanWriter pw)
    {
        String [] terms = new String[inputs.length + 1];
        for (int i = 0; i < inputs.length; i++) {
            terms[i] = "input#" + i;
        }
        terms[inputs.length] = "joinFilter";
        pw.explain(
            this,
            terms,
            new Object[] {});
    }

    public RelDataType deriveRowType()
    {
        return rowType;
    }

    public RelNode [] getInputs()
    {
        return inputs;
    }

    public RexNode [] getChildExps()
    {
        return new RexNode[] { joinFilter };
    }

    public void replaceInput(
        int ordinalInParent,
        RelNode p)
    {
        inputs[ordinalInParent] = p;
    }

    /**
     * Returns join filters associated with this MultiJoinRel
     */
    public RexNode getJoinFilter()
    {
        return joinFilter;
    }
}

// End MultiJoinRel.java
