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

package org.eigenbase.relopt;

import org.eigenbase.oj.rel.JavaRelImplementor;
import org.eigenbase.rel.RelNode;
import org.eigenbase.trace.EigenbaseTrace;

import java.util.logging.Logger;


/**
 * A <code>RelOptPlanner</code> is a query optimizer: it transforms a
 * relational expression into a semantically equivalent relational
 * expression, according to a given set of rules and a cost model.
 */
public interface RelOptPlanner
{
    //~ Static fields/initializers --------------------------------------------

    public static final Logger tracer = EigenbaseTrace.getPlannerTracer();

    //~ Methods ---------------------------------------------------------------

    public void setRoot(RelNode rel);

    public RelNode getRoot();

    /**
     * Registers a rel trait definition. If the {@link RelTraitDef} has already
     * been registered, does nothing.
     *
     * @return whether the RelTraitDef was added, as per
     *   {@link java.util.Collection#add}
     */
    public boolean addRelTraitDef(RelTraitDef relTraitDef);

    /**
     * Registers a rule. If the rule has already been registered, does nothing.
     * This method should determine if the given rule is a
     * {@link org.eigenbase.rel.convert.ConverterRule} and pass the
     * ConverterRule to all {@link #addRelTraitDef(RelTraitDef) registered}
     * RelTraitDef instances.
     *
     * @return whether the rule was added, as per
     *   {@link java.util.Collection#add}
     */
    public boolean addRule(RelOptRule rule);

    /**
     * Removes a rule.
     *
     * @return true if the rule was present, as per
     *   {@link java.util.Collection#remove(Object)}
     */
    boolean removeRule(RelOptRule rule);

    /**
     * Changes a relational expression to an equivalent one with a different
     * set of traits.  The return is never null, but may be abstract.
     *
     * @pre rel.getTraits() != toTraits
     * @post return != null
     */
    public RelNode changeTraits(RelNode rel, RelTraitSet toTraits);

    /**
     * Negotiates an appropriate planner to deal with distributed queries.
     * The idea is that the schemas decide among themselves which has the
     * most knowledge.  Right now, the local planner retains control.
     */
    public RelOptPlanner chooseDelegate();

    /**
     * Finds the most efficient expression to implement this query.
     */
    public RelNode findBestExp();

    /**
     * Creates a cost object.
     */
    public RelOptCost makeCost(
        double dRows,
        double dCpu,
        double dIo);

    /**
     * Creates a cost object representing an enormous non-infinite cost.
     */
    public RelOptCost makeHugeCost();

    /**
     * Creates a cost object representing infinite cost.
     */
    public RelOptCost makeInfiniteCost();

    /**
     * Creates a cost object representing a small positive cost.
     */
    public RelOptCost makeTinyCost();

    /**
     * Creates a cost object representing zero cost.
     */
    public RelOptCost makeZeroCost();

    /**
     * Computes the cost of a RelNode.
     */
    public RelOptCost getCost(RelNode rel);

    /**
     * Registers a relational expression in the expression bank. After it has
     * been registered, you may not modify it.
     *
     * @param rel Relational expression to register
     * @param equivRel Relational expression it is equivalent to (may be null)
     *
     * @return the same expression, or an equivalent existing expression
     */
    public RelNode register(
        RelNode rel,
        RelNode equivRel);

    /**
     * Determines whether a relational expression has been registered yet.
     *
     * @param rel expression to test
     *
     * @return whether rel has been registered
     */
    public boolean isRegistered(RelNode rel);

    /**
     * Tells this planner that a schema exists. This is the schema's chance to
     * tell the planner about all of the special transformation rules.
     */
    public void registerSchema(RelOptSchema schema);

    /**
     * Retrieves an implementor appropriate for the context in which
     * this planner was created.
     */
    public JavaRelImplementor getJavaRelImplementor(RelNode rel);

    /**
     * Adds a listener to this planner.
     *
     * @param newListener new listener to be notified of events
     */
    public void addListener(RelOptListener newListener);
}


// End RelOptPlanner.java
