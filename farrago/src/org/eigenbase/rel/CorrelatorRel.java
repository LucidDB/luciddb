/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2002-2005 Disruptive Tech
// Copyright (C) 2005-2005 Red Square, Inc.
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

import java.util.Collections;
import java.util.List;
import java.util.ArrayList;

import openjava.ptree.Expression;

import org.eigenbase.relopt.RelOptCluster;
import org.eigenbase.relopt.RelOptUtil;
import org.eigenbase.relopt.RelOptPlanWriter;
import org.eigenbase.util.Util;
import org.eigenbase.reltype.RelDataType;


/**
 * A <code>CorrelatorRel</code> behaves like a kind of {@link JoinRel}, but
 * works by setting variables in its environment and restarting its
 * right-hand input.  It is used to represent a correlated query; one
 * implementation option is to de-correlate the expression.
 *
 * @author jhyde
 * @version $Id$
 *
 * @since 23 September, 2001
 */
public class CorrelatorRel extends JoinRel
{
    //~ Instance fields -------------------------------------------------------

    protected ArrayList correlations;

    //~ Inner Classes ---------------------------------------------------------

    /**
     * Describes the neccessary parameters for an implementation in order
     * to identify and set dynamic variables
     */
    public static class Correlation implements Cloneable {
        public final int id;
        public final int offset;

        public Correlation(int id, int offset)
        {
            this.id = id;
            this.offset = offset;
        }
    }
    //~ Constructors ----------------------------------------------------------

    /**
     * Creates a Correlator.
     *
     * @param cluster {@link RelOptCluster} this relational expression
     *        belongs to
     * @param left left input relational expression
     * @param right right  input relational expression
     * @param correlations set of expressions to set as variables each time a
     *        row arrives from the left input
     */
    public CorrelatorRel(
        RelOptCluster cluster,
        RelNode left,
        RelNode right,
        ArrayList correlations)
    {
        super(cluster, left, right,
            cluster.rexBuilder.makeLiteral(true), JoinType.LEFT,
            Collections.EMPTY_SET);
        this.correlations = correlations;
    }

    //~ Methods ---------------------------------------------------------------

    public Object clone()
    {
        return new CorrelatorRel(
            cluster,
            RelOptUtil.clone(left),
            RelOptUtil.clone(right),
            (ArrayList) correlations.clone());
    }

    protected RelDataType deriveRowType()
    {
        return super.deriveRowType();
    }

    public void explain(RelOptPlanWriter pw)
    {
        // todo wael: add var and col descriptions;
        pw.explain(
            this,
            new String [] { "left", "right", "condition", "joinType" },
            new Object [] { JoinType.toString(joinType) });
    }

    public ArrayList getCorrelations()
    {
        return correlations;
    }
}


// End CorrelatorRel.java
