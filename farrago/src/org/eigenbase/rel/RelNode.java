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

import java.util.Set;

import org.eigenbase.relopt.CallingConvention;
import org.eigenbase.relopt.RelOptCluster;
import org.eigenbase.relopt.RelOptCost;
import org.eigenbase.relopt.RelOptPlanWriter;
import org.eigenbase.relopt.RelOptPlanner;
import org.eigenbase.relopt.RelOptQuery;
import org.eigenbase.relopt.RelOptTable;
import org.eigenbase.reltype.RelDataType;
import org.eigenbase.rex.RexNode;


/**
 * A <code>RelNode</code> is a relational expression.  It is NOT an
 * {@link openjava.ptree.Expression}.
 *
 * <p>
 * If this type of relational expression has some particular rules, it should
 * implement the <em>public static</em> method {@link AbstractRelNode#register}.
 * </p>
 *
 * <p>When a relational expression comes to be implemented, the system
 * allocates a {@link org.eigenbase.relopt.RelImplementor} to manage the
 * process. Every implementable relational expression has a
 * {@link CallingConvention} describing how it passes data to its consuming
 * relational expression.</p>
 *
 * <p>For each calling-convention, there is a corresponding sub-interface of
 * RelNode. For example, {@link org.eigenbase.oj.rel.JavaRel} has
 * operations to manage the conversion to a graph of
 * {@link CallingConvention#JAVA Java calling-convention}, and it interacts
 * with a {@link org.eigenbase.oj.rel.JavaRelImplementor}.</p>
 *
 * <p>A relational expression is only required to implement its
 * calling-convention's interface when it is actually implemented, that is,
 * converted into a plan/program. This means that relational expressions which
 * cannot be implemented, such as converters, are not required to implement
 * their convention's interface.</p>
 *
 * <p>Every relational expression must derive from {@link AbstractRelNode}.
 * (Why have the <code>RelNode</code> interface, then? We need a root
 * interface, because an interface can only derive from an interface.)</p>
 *
 * @author jhyde
 * @since May 24, 2004
 * @version $Id$
 **/
public interface RelNode
{
    //~ Instance fields -------------------------------------------------------

    AbstractRelNode [] emptyArray = new AbstractRelNode[0];

    //~ Methods ---------------------------------------------------------------

    /**
     * Returns whether this relational expression is an access to
     * <code>table</code>.
     */
    boolean isAccessTo(RelOptTable table);

    /**
     * Returns an array of this relational expression's child expressions
     * (not including the inputs returned by {@link #getInputs}.  If there
     * are no child expressions, returns an empty array, not
     * <code>null</code>.
     */
    RexNode [] getChildExps();

    RelOptCluster getCluster();

    /**
     * Returns a value from {@link CallingConvention}.
     */
    CallingConvention getConvention();

    void setCorrelVariable(String correlVariable);

    String getCorrelVariable();

    /**
     * Returns whether the same value will not come out twice. Default value
     * is <code>false</code>, derived classes should override.
     */
    boolean isDistinct();

    int getId();

    /**
     * Get the <code>i</code><sup>th</sup> input.
     */
    RelNode getInput(int i);

    /**
     * Returns a variable with which to reference the current row of this
     * relational expression as a correlating variable. Creates a variable if
     * none exists.
     */
    String getOrCreateCorrelVariable();

    RelOptQuery getQuery();

    /**
     * Returns the type of the rows returned by this relational expression.
     */
    RelDataType getRowType();

    /**
     * Returns the type of the rows expected for an input.  Defaults to
     * {@link #getRowType}.
     *
     * @param ordinalInParent input's 0-based ordinal with respect to
     * this parent rel
     *
     * @return expected row type
     */
    RelDataType getExpectedInputRowType(int ordinalInParent);

    /**
     * Returns an array of this relational expression's inputs.  If there
     * are no inputs, returns an empty array, not <code>null</code>.
     */
    RelNode [] getInputs();

    /**
     * Returns an estimate of the number of rows this relational expression
     * will return.
     */
    double getRows();

    /**
     * Returns the names of variables which are set in this relational
     * expression but also used and therefore not available to parents of
     * this relational expression.
     *
     * <p>
     * By default, returns the empty set. Derived classes may override this
     * method.
     * </p>
     */
    Set getVariablesStopped();

    /**
     * Interacts with the {@link RelVisitor} in a
     * {@link org.eigenbase.util.Glossary#VisitorPattern visitor pattern} to
     * traverse the tree of relational expressions.
     */
    void childrenAccept(RelVisitor visitor);

    /**
     * Returns the cost of this plan (not including children). The base
     * implementation throws an error; derived classes should override.
     */
    RelOptCost computeSelfCost(RelOptPlanner planner);

    void explain(RelOptPlanWriter pw);

    /**
     * This method is called just before the expression is registered.  The
     * implementation of this method must at least register all child
     * expressions.
     */
    void onRegister(RelOptPlanner planner);

    /**
     * Computes the digest, assigns it, and returns it. For internal use only.
     */
    String recomputeDigest();

    /**
     * Registers a correlation variable.
     *
     * @see #getVariablesStopped
     */
    void registerCorrelVariable(String correlVariable);

    /**
     * Replaces the <code>ordinalInParent</code><sup>th</sup> input.  You must
     * override this method if you override {@link #getInputs}.
     */
    void replaceInput(
        int ordinalInParent,
        RelNode p);

    /**
     * If this relational expression represents an access to a table, returns
     * that table, otherwise returns null.
     */
    RelOptTable getTable();

    /**
     * Returns the name of this relational expression's class, sans package
     * name, for use in {@link #explain}.  For example, for a
     * <code>org.eigenbase.rel.ArrayRel.ArrayReader</code>, this method
     * returns "ArrayReader".
     */
    String getRelTypeName();
}


// End RelNode.java
