/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2002-2005 Disruptive Tech
// Copyright (C) 2005-2005 Red Square, Inc.
// Portions Copyright (C) 2003-2005 John V. Sichi
//
// This program is free software; you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation; either version 2 of the License, or
// (at your option) any later Eigenbase-approved version.
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

package org.eigenbase.relopt;

import java.util.logging.Logger;

import org.eigenbase.oj.rel.JavaRelImplementor;
import org.eigenbase.rel.RelNode;
import org.eigenbase.relopt.CallingConvention;
import org.eigenbase.relopt.RelOptCost;
import org.eigenbase.relopt.RelOptRule;
import org.eigenbase.trace.EigenbaseTrace;


/**
 * A <code>RelOptPlanner</code> is a query optimizer: it transforms a
 * relational expression into a semantically equivalent relational
 * expression, according to a given set of rules and a cost model.
 */
public interface RelOptPlanner
{
    //~ Static fields/initializers --------------------------------------------

    static final Logger tracer = EigenbaseTrace.getPlannerTracer();

    //~ Methods ---------------------------------------------------------------

    void setRoot(RelNode rel);

    RelNode getRoot();

    /**
     * Registers a calling convention. If the calling convention has already
     * been registered, does nothing.
     *
     * @return whether the calling convention was added, as per
     *   {@link java.util.Collection#add}
     */
    boolean addCallingConvention(CallingConvention convention);

    /**
     * Registers a rule. If the calling convention has already
     * been registered, does nothing.
     *
     * @return whether the rule was added, as per
     *   {@link java.util.Collection#add}
     */
    boolean addRule(RelOptRule rule);

    /**
     * Changes a relational expression to an equivalent one of a different
     * calling convention. The return is never null, but may be abstract.
     *
     * @pre rel.getConvention() != toConvention
     * @post return != null
     */
    RelNode changeConvention(
        RelNode rel,
        CallingConvention toConvention);

    /**
     * Negotiates an appropriate planner to deal with distributed queries.
     * The idea is that the schemas decide among themselves which has the
     * most knowledge.  Right now, the local planner retains control.
     */
    RelOptPlanner chooseDelegate();

    /**
     * Find the most efficient expression to implement this query.
     */
    RelNode findBestExp();

    /**
     * Create a cost object.
     */
    RelOptCost makeCost(
        double dRows,
        double dCpu,
        double dIo);

    /**
     * Create a cost object representing an enormous non-infinite cost.
     */
    RelOptCost makeHugeCost();

    /**
     * Create a cost object representing infinite cost.
     */
    RelOptCost makeInfiniteCost();

    /**
     * Create a cost object representing a small positive cost.
     */
    RelOptCost makeTinyCost();

    /**
     * Create a cost object representing zero cost.
     */
    RelOptCost makeZeroCost();

    /**
     * Registers a relational expression in the expression bank. After it has
     * been registered, you may not modify it.
     *
     * @param rel Relational expression to register
     * @param equivRel Relational expression it is equivalent to (may be null)
     *
     * @return the same expression, or an equivalent existing expression
     */
    RelNode register(
        RelNode rel,
        RelNode equivRel);

    /**
     * Determines whether a relational expression has been registered yet.
     *
     * @param rel expression to test
     *
     * @return whether rel has been registered
     */
    boolean isRegistered(RelNode rel);

    /**
     * Tells this planner that a schema exists. This is the schema's chance to
     * tell the planner about all of the special transformation rules.
     */
    void registerSchema(RelOptSchema schema);

    /**
     * Retrieve an implementor appropriate for the context in which
     * this planner was created.
     */
    JavaRelImplementor getJavaRelImplementor(RelNode rel);

    /**
     * Adds a listener to this planner.  
     *
     * @param newListener new listener to be notified of events
     */
    void addListener(RelOptListener newListener);
}


// End RelOptPlanner.java
