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

import java.util.*;

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
    private RexNode joinFilter;
    private RelDataType rowType;
    private boolean isFullOuterJoin;
    private RexNode [] outerJoinConditions;
    private JoinRelType [] joinTypes;

    //~ Constructors -----------------------------------------------------------

    /**
     * Constructs a MultiJoinRel object
     * 
     * @param cluster cluster that join belongs to
     * @param inputs inputs into this multirel join
     * @param joinFilters join filters applicable to this join node
     * @param rowType row type of the join result of this node
     * @param isFullOuterJoin true if the join is a full outer join
     * @param outerJoinConditions outer join condition associated with each
     * join input, if the input is null-generating in a left or right outer
     * join; null otherwise
     * @param joinTypes the join type corresponding to each input; if an input
     * is null-generating in a left or right outer join, the entry indicates
     * the type of outer join; otherwise, the entry is set to INNER
     */
    public MultiJoinRel(
        RelOptCluster cluster,
        RelNode [] inputs,
        RexNode joinFilter,
        RelDataType rowType,
        boolean isFullOuterJoin,
        RexNode [] outerJoinConditions,
        JoinRelType [] joinTypes)
    {
        super(
            cluster,
            new RelTraitSet(CallingConvention.NONE));
        this.inputs = inputs;
        this.joinFilter = joinFilter;
        this.rowType = rowType;
        this.isFullOuterJoin = isFullOuterJoin;
        this.outerJoinConditions = outerJoinConditions;
        this.joinTypes = joinTypes;
    }

    //~ Methods ----------------------------------------------------------------

    public MultiJoinRel clone()
    {
        MultiJoinRel clone =
            new MultiJoinRel(
                getCluster(),
                RelOptUtil.clone(inputs),
                joinFilter.clone(),
                rowType,
                isFullOuterJoin,
                RexUtil.clone(outerJoinConditions),
                joinTypes.clone());
        clone.inheritTraitsFrom(this);
        return clone;
    }

    public void explain(RelOptPlanWriter pw)
    {
        int nInputs = inputs.length;
        String [] terms = new String[nInputs + 4];
        for (int i = 0; i < nInputs; i++) {
            terms[i] = "input#" + i;
        }
        terms[nInputs] = "joinFilter";
        terms[nInputs + 1] = "isFullOuterJoin";
        terms[nInputs + 2] = "joinTypes";
        terms[nInputs + 3] = "outerJoinConditions";
        List<String> joinTypeNames = new ArrayList<String>();
        for (int i = 0; i < nInputs; i++) {
            joinTypeNames.add(joinTypes[i].name());
        }
        List<String> outerJoinConds = new ArrayList<String>();
        for (int i = 0; i < nInputs; i++) {
            if (outerJoinConditions[i] == null) {
                outerJoinConds.add("NULL");
            } else {
                outerJoinConds.add(outerJoinConditions[i].toString());
            }
        }
        pw.explain(
            this,
            terms,
            new Object[] {
                isFullOuterJoin,
                joinTypeNames,
                outerJoinConds
            });
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
     * @return join filters associated with this MultiJoinRel
     */
    public RexNode getJoinFilter()
    {
        return joinFilter;
    }
    
    /**
     * @return true if the MultiJoinRel corresponds to a full outer join.
     */
    public boolean isFullOuterJoin()
    {
        return isFullOuterJoin;
    }
    
    /**
     * @return outer join conditions for null-generating inputs
     */
    public RexNode [] getOuterJoinConditions()
    {
        return outerJoinConditions;
    }
    
    /**
     * @return join types of each input
     */
    public JoinRelType [] getJoinTypes()
    {
        return joinTypes;
    }
}

// End MultiJoinRel.java
