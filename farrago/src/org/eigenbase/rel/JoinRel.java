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
package org.eigenbase.rel;

import java.util.*;

import org.eigenbase.relopt.*;
import org.eigenbase.rex.*;


/**
 * A JoinRel represents two relational expressions joined according to some
 * condition.
 *
 * <p>Some rules:
 *
 * <ul>
 * <li>{@link ExtractJoinFilterRule} converts an {@link JoinRel inner join} to a
 * {@link FilterRel filter} on top of a {@link JoinRel cartesian inner join}.
 * <li>{@link net.sf.farrago.query.FennelCartesianJoinRule} implements a JoinRel
 * as a cartesian product.
 * </ul>
 *
 * @author jhyde
 * @version $Id$
 */
public final class JoinRel
    extends JoinRelBase
{
    //~ Instance fields --------------------------------------------------------

    // NOTE jvs 14-Mar-2006:  Normally we don't use state like this
    // to control rule firing, but due to the non-local nature of
    // semijoin optimizations, it's pretty much required.
    private final boolean semiJoinDone;
    
    /**
     * True if the join is a removable self-join
     */
    private final boolean removableSelfJoin;

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a JoinRel.
     *
     * @param cluster Cluster
     * @param left Left input
     * @param right Right input
     * @param condition Join condition
     * @param joinType Join type
     * @param variablesStopped Set of names of variables which are set by the
     * LHS and used by the RHS and are not available to nodes above this JoinRel
     * in the tree
     */
    public JoinRel(
        RelOptCluster cluster,
        RelNode left,
        RelNode right,
        RexNode condition,
        JoinRelType joinType,
        Set<String> variablesStopped)
    {
        this(
            cluster,
            left,
            right,
            condition,
            joinType,
            variablesStopped,
            false,
            false);
    }

    /**
     * Creates a JoinRel, flagged with whether it has been translated to a
     * semi-join.
     *
     * @param cluster Cluster
     * @param left Left input
     * @param right Right input
     * @param condition Join condition
     * @param joinType Join type
     * @param variablesStopped Set of names of variables which are set by the
     * LHS and used by the RHS and are not available to nodes above this JoinRel
     * in the tree
     * @param semiJoinDone Whether this join has been translated to a semi-join
     *
     * @see #isSemiJoinDone()
     */
    public JoinRel(
        RelOptCluster cluster,
        RelNode left,
        RelNode right,
        RexNode condition,
        JoinRelType joinType,
        Set<String> variablesStopped,
        boolean semiJoinDone)
    {
        this(
            cluster,
            left,
            right,
            condition,
            joinType,
            variablesStopped,
            semiJoinDone,
            false);
    }
    
    /**
     * Creates a JoinRel, flagged with whether it has been translated to a
     * semi-join, and whether it is a removable self-join.
     *
     * @param cluster Cluster
     * @param left Left input
     * @param right Right input
     * @param condition Join condition
     * @param joinType Join type
     * @param variablesStopped Set of names of variables which are set by the
     * LHS and used by the RHS and are not available to nodes above this JoinRel
     * in the tree
     * @param semiJoinDone Whether this join has been translated to a semi-join
     * @param removableSelfJoin true if the join is a removable self-join
     *
     * @see #isSemiJoinDone()
     */
    public JoinRel(
        RelOptCluster cluster,
        RelNode left,
        RelNode right,
        RexNode condition,
        JoinRelType joinType,
        Set<String> variablesStopped,
        boolean semiJoinDone,
        boolean removableSelfJoin)
    {
        super(
            cluster,
            new RelTraitSet(CallingConvention.NONE),
            left,
            right,
            condition,
            joinType,
            variablesStopped);
        this.semiJoinDone = semiJoinDone;
        this.removableSelfJoin = removableSelfJoin;
    }

    //~ Methods ----------------------------------------------------------------

    public JoinRel clone()
    {
        JoinRel clone =
            new JoinRel(
                getCluster(),
                left.clone(),
                right.clone(),
                condition.clone(),
                joinType,
                new HashSet<String>(variablesStopped),
                isSemiJoinDone(),
                isRemovableSelfJoin());
        clone.inheritTraitsFrom(this);
        return clone;
    }

    public void explain(RelOptPlanWriter pw)
    {
        // NOTE jvs 14-Mar-2006: Do it this way so that semijoin state
        // don't clutter things up in optimizers that don't use semijoins
        // or removable self-joins
        if (!semiJoinDone && !removableSelfJoin) {
            super.explain(pw);
            return;
        }
        String[] names;
        Object[] objects;
        if (removableSelfJoin) {
            names = new String[6];
            objects = new Object[3];
            names[5] = "removableSelfJoin";
            objects[2] = removableSelfJoin;
        } else {
            names = new String[5];
            objects = new Object[2];
        }
        names[0] = "left";
        names[1] = "right";
        names[2] = "condition";
        names[3] = "joinType";
        names[4] = "semiJoinDone";
        objects[0] = joinType.name().toLowerCase();
        objects[1] = semiJoinDone;
        pw.explain(this, names, objects);
    }

    /**
     * Returns whether this JoinRel has already spawned a {@link
     * org.eigenbase.rel.rules.SemiJoinRel} via {@link
     * org.eigenbase.rel.rules.AddRedundantSemiJoinRule}.
     */
    public boolean isSemiJoinDone()
    {
        return semiJoinDone;
    }
    
    /**
     * @return true if the join is flagged as a self-join
     */
    public boolean isRemovableSelfJoin()
    {
        return removableSelfJoin;
    }
}

// End JoinRel.java
