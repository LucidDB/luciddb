/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005 The Eigenbase Project
// Copyright (C) 2002 SQLstream, Inc.
// Copyright (C) 2005 Dynamo BI Corporation
// Portions Copyright (C) 2003 John V. Sichi
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
     * Creates a CorrelatorRel.
     *
     * @param cluster cluster this relational expression belongs to
     * @param left left input relational expression
     * @param right right input relational expression
     * @param joinCond join condition
     * @param correlations set of expressions to set as variables each time a
     * row arrives from the left input
     * @param joinType join type
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
            Collections.<String>emptySet());
        this.correlations = correlations;
        assert (joinType == JoinRelType.LEFT)
            || (joinType == JoinRelType.INNER);
    }

    /**
     * Creates a CorrelatorRel with no join condition.
     *
     * @param cluster cluster this relational expression belongs to
     * @param left left input relational expression
     * @param right right input relational expression
     * @param correlations set of expressions to set as variables each time a
     * row arrives from the left input
     * @param joinType join type
     */
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
                new ArrayList<Correlation>(correlations),
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

    /**
     * Returns the correlating expressions.
     *
     * @return correlating expressions
     */
    public List<Correlation> getCorrelations()
    {
        return correlations;
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

        /**
         * Creates a correlation.
         *
         * @param id Identifier
         * @param offset Offset
         */
        public Correlation(int id, int offset)
        {
            this.id = id;
            this.offset = offset;
        }

        /**
         * Returns the identifier.
         *
         * @return identifier
         */
        public int getId()
        {
            return id;
        }

        /**
         * Returns this correlation's offset.
         *
         * @return offset
         */
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
