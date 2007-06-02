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
import org.eigenbase.reltype.*;
import org.eigenbase.rex.*;


/**
 * A <code>CorrelatorRel</code> behaves like a kind of {@link JoinRel}, but
 * works by setting variables in its environment and restarting its right-hand
 * input.
 *
 * <p>A CorrelatorRel is used to represent a correlated query. One
 * implementation strategy is to de-correlate the expression.
 *
 * @author jhyde
 * @version $Id$
 * @since 23 September, 2001
 */
public final class CorrelatorRel
    extends JoinRelBase
{
    //~ Instance fields --------------------------------------------------------

    protected final List<Correlation> correlations;

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a Correlator.
     *
     * @param cluster {@link RelOptCluster}  this relational expression belongs
     * to
     * @param left left input relational expression
     * @param right right input relational expression
     * @param correlations set of expressions to set as variables each time a
     * row arrives from the left input
     * @param joinType
     */
    public CorrelatorRel(
        RelOptCluster cluster,
        RelNode left,
        RelNode right,
        RexNode joinCond,
        List<Correlation> correlations,
        JoinRelType joinType)
    {
        super(
            cluster,
            new RelTraitSet(CallingConvention.NONE),
            left,
            right,
            joinCond,
            joinType,
            (Set<String>) Collections.EMPTY_SET);
        this.correlations = correlations;
        assert (joinType == JoinRelType.LEFT)
            || (joinType == JoinRelType.INNER);
    }

    public CorrelatorRel(
        RelOptCluster cluster,
        RelNode left,
        RelNode right,
        List<Correlation> correlations,
        JoinRelType joinType)
    {
        this(
            cluster,
            left,
            right,
            cluster.getRexBuilder().makeLiteral(true),
            correlations,
            joinType);
    }

    //~ Methods ----------------------------------------------------------------

    public CorrelatorRel clone()
    {
        CorrelatorRel clone =
            new CorrelatorRel(
                getCluster(),
                left.clone(),
                right.clone(),
                cloneCorrelations(),
                joinType);
        clone.inheritTraitsFrom(this);
        return clone;
    }

    protected RelDataType deriveRowType()
    {
        return super.deriveRowType();
    }

    public void explain(RelOptPlanWriter pw)
    {
        pw.explain(
            this,
            new String[] {
                "left", "right", "condition", "joinType", "correlations"
            },
            new Object[] {
                joinType.name().toLowerCase(),
                correlations
            });
    }

    public List<Correlation> getCorrelations()
    {
        return correlations;
    }

    public List<Correlation> cloneCorrelations()
    {
        return new ArrayList<Correlation>(correlations);
    }

    //~ Inner Classes ----------------------------------------------------------

    /**
     * Describes the neccessary parameters for an implementation in order to
     * identify and set dynamic variables
     */
    public static class Correlation
        implements Cloneable,
            Comparable<Correlation>
    {
        private final int id;
        private final int offset;

        public Correlation(int id, int offset)
        {
            this.id = id;
            this.offset = offset;
        }

        public int getId()
        {
            return id;
        }

        public int getOffset()
        {
            return offset;
        }

        public String toString()
        {
            return "var" + id + "=offset" + offset;
        }

        public int compareTo(Correlation other)
        {
            return (id - other.id);
        }
    }
}

// End CorrelatorRel.java
