/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005-2007 The Eigenbase Project
// Copyright (C) 2002-2007 Disruptive Tech
// Copyright (C) 2005-2007 LucidEra, Inc.
// Portions Copyright (C) 2003-2007 John V. Sichi
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

import java.util.*;

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
    private RexNode joinFilter;
    private RelDataType rowType;
    private boolean isFullOuterJoin;
    private RexNode [] outerJoinConditions;
    private JoinRelType [] joinTypes;
    private BitSet [] projFields;
    private Map<Integer, int[]> joinFieldRefCountsMap;
    private RexNode postJoinFilter;

    //~ Constructors -----------------------------------------------------------

    /**
     * Constructs a MultiJoinRel.
     *
     * @param cluster cluster that join belongs to
     * @param inputs inputs into this multirel join
     * @param joinFilter join filter applicable to this join node
     * @param rowType row type of the join result of this node
     * @param isFullOuterJoin true if the join is a full outer join
     * @param outerJoinConditions outer join condition associated with each join
     * input, if the input is null-generating in a left or right outer join;
     * null otherwise
     * @param joinTypes the join type corresponding to each input; if an input
     * is null-generating in a left or right outer join, the entry indicates the
     * type of outer join; otherwise, the entry is set to INNER
     * @param projFields fields that will be projected from each input; if null,
     * projection information is not available yet so it's assumed that all
     * fields from the input are projected
     * @param joinFieldRefCountsMap counters of the number of times each field
     * is referenced in join conditions, indexed by the input #
     * @param postJoinFilter filter to be applied after the joins are executed
     */
    public MultiJoinRel(
        RelOptCluster cluster,
        RelNode [] inputs,
        RexNode joinFilter,
        RelDataType rowType,
        boolean isFullOuterJoin,
        RexNode [] outerJoinConditions,
        JoinRelType [] joinTypes,
        BitSet [] projFields,
        Map<Integer, int[]> joinFieldRefCountsMap,
        RexNode postJoinFilter)
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
        this.projFields = projFields;
        this.joinFieldRefCountsMap = joinFieldRefCountsMap;
        this.postJoinFilter = postJoinFilter;
    }
    
    /*
     * @deprecated
     */
    public MultiJoinRel(
        RelOptCluster cluster,
        RelNode [] inputs,
        RexNode joinFilter,
        RelDataType rowType,
        boolean isFullOuterJoin,
        RexNode [] outerJoinConditions,
        JoinRelType [] joinTypes,
        BitSet [] projFields,
        Map<Integer, int[]> joinFieldRefCountsMap)
    {
        this(
            cluster,
            inputs,
            joinFilter,
            rowType,
            isFullOuterJoin,
            outerJoinConditions,
            joinTypes,
            projFields,
            joinFieldRefCountsMap,
            null);
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
                joinTypes.clone(),
                projFields.clone(),
                cloneJoinFieldRefCountsMap(),
                postJoinFilter);
        clone.inheritTraitsFrom(this);
        return clone;
    }

    /**
     * Returns a deep copy of {@link #joinFieldRefCountsMap}.
     */
    private Map<Integer, int[]> cloneJoinFieldRefCountsMap()
    {
        Map<Integer, int[]> clonedMap = new HashMap<Integer, int[]>();
        for (int i = 0; i < inputs.length; i++) {
            clonedMap.put(i, joinFieldRefCountsMap.get(i).clone());
        }
        return clonedMap;
    }

    public void explain(RelOptPlanWriter pw)
    {
        int nInputs = inputs.length;
        int nExtraTerms = (postJoinFilter != null) ? 6 : 5;
        String [] terms = new String[nInputs + nExtraTerms];
        for (int i = 0; i < nInputs; i++) {
            terms[i] = "input#" + i;
        }
        terms[nInputs] = "joinFilter";
        terms[nInputs + 1] = "isFullOuterJoin";
        terms[nInputs + 2] = "joinTypes";
        terms[nInputs + 3] = "outerJoinConditions";
        terms[nInputs + 4] = "projFields";
        if (postJoinFilter != null) {
            terms[nInputs + 5] = "postJoinFilter";
        }
        List<String> joinTypeNames = new ArrayList<String>();
        List<String> outerJoinConds = new ArrayList<String>();
        List<String> projFieldObjects = new ArrayList<String>();
        for (int i = 0; i < nInputs; i++) {
            joinTypeNames.add(joinTypes[i].name());
            if (outerJoinConditions[i] == null) {
                outerJoinConds.add("NULL");
            } else {
                outerJoinConds.add(outerJoinConditions[i].toString());
            }
            if (projFields[i] == null) {
                projFieldObjects.add("ALL");
            } else {
                projFieldObjects.add(projFields[i].toString());
            }
        }

        // Note that we don't need to include the join field reference counts
        // in the digest because that field does not change for a given set
        // of inputs
        Object[] objects = new Object[nExtraTerms - 1];
        objects[0] = isFullOuterJoin;
        objects[1] = joinTypeNames;
        objects[2] = outerJoinConds;
        objects[3] = projFieldObjects;
        if (postJoinFilter != null) {
            objects[4] = postJoinFilter;
        }   
        
        pw.explain(this, terms, objects);
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

    /**
     * @return bitmaps representing the fields projected from each input; if an
     * entry is null, all fields are projected
     */
    public BitSet [] getProjFields()
    {
        return projFields;
    }

    /**
     * @return the map of reference counts for each input, representing the
     * fields accessed in join conditions
     */
    public Map<Integer, int[]> getJoinFieldRefCountsMap()
    {
        return joinFieldRefCountsMap;
    }

    /**
     * @return a copy of the map of reference counts for each input,
     * representing the fields accessed in join conditions
     */
    public Map<Integer, int[]> getCopyJoinFieldRefCountsMap()
    {
        return cloneJoinFieldRefCountsMap();
    }
    
    /**
     * @return post-join filter associated with this MultiJoinRel
     */
    public RexNode getPostJoinFilter()
    {
        return postJoinFilter;
    }
}

// End MultiJoinRel.java
