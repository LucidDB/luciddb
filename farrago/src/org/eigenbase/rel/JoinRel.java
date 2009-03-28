/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005-2009 The Eigenbase Project
// Copyright (C) 2002-2009 SQLstream, Inc.
// Copyright (C) 2005-2009 LucidEra, Inc.
// Portions Copyright (C) 2003-2009 John V. Sichi
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
import org.eigenbase.reltype.RelDataTypeField;


/**
 * A JoinRel represents two relational expressions joined according to some
 * condition.
 *
 * <p>Some rules:
 *
 * <ul> <li>{@link org.eigenbase.rel.rules.ExtractJoinFilterRule} converts an
 * {@link JoinRel inner join} to a {@link FilterRel filter} on top of a {@link
 * JoinRel cartesian inner join}.  <li>{@link
 * net.sf.farrago.fennel.rel.FennelCartesianJoinRule} implements a JoinRel as a
 * cartesian product.  </ul>
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

    private List<RelDataTypeField> systemFieldList;

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
            Collections.<RelDataTypeField>emptyList());
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
     * @param systemFieldList List of system fields that will be prefixed to
     * output row type; typically empty but must not be null
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
        List<RelDataTypeField> systemFieldList)
    {
        super(
            cluster,
            new RelTraitSet(CallingConvention.NONE),
            left,
            right,
            condition,
            joinType,
            variablesStopped);
        assert systemFieldList != null;
        this.semiJoinDone = semiJoinDone;
        this.systemFieldList = systemFieldList;
    }

    //~ Methods ----------------------------------------------------------------

    @SuppressWarnings({"CloneDoesntCallSuperClone"})
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
                systemFieldList);
        clone.inheritTraitsFrom(this);
        return clone;
    }

    public void explain(RelOptPlanWriter pw)
    {
        // NOTE jvs 14-Mar-2006: Do it this way so that semijoin state
        // don't clutter things up in optimizers that don't use semijoins
        if (!semiJoinDone) {
            super.explain(pw);
            return;
        }
        pw.explain(
            this,
            new String[] {
                "left", "right", "condition", "joinType", "semiJoinDone"
            },
            new Object[] {
                joinType.name().toLowerCase(), semiJoinDone
            });
    }

    /**
     * Returns whether this JoinRel has already spawned a {@link
     * org.eigenbase.rel.rules.SemiJoinRel} via {@link
     * org.eigenbase.rel.rules.AddRedundantSemiJoinRule}.
     *
     * @return whether this join has already spawned a semi join
     */
    public boolean isSemiJoinDone()
    {
        return semiJoinDone;
    }

    public List<RelDataTypeField> getSystemFieldList()
    {
        return systemFieldList;
    }
}

// End JoinRel.java
