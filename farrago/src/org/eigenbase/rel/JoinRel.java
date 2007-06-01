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

    // Likewise for multiJoinDone.  This boolean indicates that this JoinRel
    // has already been converted to a MultiJoinRel and now is being
    // converted back to a JoinRel and therefore should not undergo another
    // transformation back to a MultiJoinRel.
    private final boolean multiJoinDone;

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
     * semi-join or multi-join.
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
     * @param multiJoinDone Whether this join has been translated to a
     * multi-join
     *
     * @see #isSemiJoinDone()
     * @see #isMultiJoinDone()
     */
    public JoinRel(
        RelOptCluster cluster,
        RelNode left,
        RelNode right,
        RexNode condition,
        JoinRelType joinType,
        Set<String> variablesStopped,
        boolean semiJoinDone,
        boolean multiJoinDone)
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
        this.multiJoinDone = multiJoinDone;
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
                isMultiJoinDone());
        clone.inheritTraitsFrom(this);
        return clone;
    }

    public void explain(RelOptPlanWriter pw)
    {
        // NOTE jvs 14-Mar-2006: Do it this way so that semijoin/multijoin state
        // don't clutter things up in optimizers that don't use semijoins or
        // multijoins.
        if (!semiJoinDone && !multiJoinDone) {
            super.explain(pw);
            return;
        }
        pw.explain(
            this,
            new String[] {
                "left", "right", "condition", "joinType", "semiJoinDone",
                "multiJoinDone"
            },
            new Object[] {
                joinType.name().toLowerCase(), semiJoinDone, multiJoinDone
            });
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
     * Returns whether this JoinRel has already been converted to a {@link
     * org.eigenbase.rel.rules.MultiJoinRel}.
     */
    public boolean isMultiJoinDone()
    {
        return multiJoinDone;
    }
}

// End JoinRel.java
