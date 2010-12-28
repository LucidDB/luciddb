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

import org.eigenbase.rel.metadata.*;
import org.eigenbase.relopt.*;
import org.eigenbase.reltype.*;
import org.eigenbase.rex.*;


/**
 * A <code>RelNode</code> is a relational expression. It is NOT an {@link
 * openjava.ptree.Expression}.
 *
 * <p>If this type of relational expression has some particular rules, it should
 * implement the <em>public static</em> method {@link
 * AbstractRelNode#register}.</p>
 *
 * <p>When a relational expression comes to be implemented, the system allocates
 * a {@link org.eigenbase.relopt.RelImplementor} to manage the process. Every
 * implementable relational expression has a {@link RelTraitSet} describing its
 * physical attributes. The RelTraitSet always contains a {@link
 * CallingConvention} describing how the expression passes data to its consuming
 * relational expression, but may contain other traits, including some applied
 * externally. Because traits can be applied externally, implementaitons of
 * RelNode should never assume the size or contents of their trait set (beyond
 * those traits configured by the RelNode itself).</p>
 *
 * <p>For each calling-convention, there is a corresponding sub-interface of
 * RelNode. For example, {@link org.eigenbase.oj.rel.JavaRel} has operations to
 * manage the conversion to a graph of {@link CallingConvention#JAVA Java
 * calling-convention}, and it interacts with a {@link
 * org.eigenbase.oj.rel.JavaRelImplementor}.</p>
 *
 * <p>A relational expression is only required to implement its
 * calling-convention's interface when it is actually implemented, that is,
 * converted into a plan/program. This means that relational expressions which
 * cannot be implemented, such as converters, are not required to implement
 * their convention's interface.</p>
 *
 * <p>Every relational expression must derive from {@link AbstractRelNode}. (Why
 * have the <code>RelNode</code> interface, then? We need a root interface,
 * because an interface can only derive from an interface.)</p>
 *
 * @author jhyde
 * @version $Id$
 * @since May 24, 2004
 */
public interface RelNode
    extends Cloneable
{
    //~ Instance fields --------------------------------------------------------

    AbstractRelNode [] emptyArray = new AbstractRelNode[0];

    //~ Methods ----------------------------------------------------------------

    /**
     * Returns whether this relational expression is an access to <code>
     * table</code>.
     */
    public boolean isAccessTo(RelOptTable table);

    /**
     * Returns an array of this relational expression's child expressions (not
     * including the inputs returned by {@link #getInputs}. If there are no
     * child expressions, returns an empty array, not <code>null</code>.
     */
    public RexNode [] getChildExps();

    /**
     * Returns the cluster this relational expression belongs to.
     *
     * @return cluster
     */
    public RelOptCluster getCluster();

    /**
     * Return the CallingConvention trait from this RelNode's {@link
     * #getTraits() trait set}.
     *
     * @return this RelNode's CallingConvention
     */
    public CallingConvention getConvention();

    /**
     * Retrieves this RelNode's traits. Note that although the RelTraitSet
     * returned is modifiable, it <b>must not</b> be modified during
     * optimization. It is legal to modify the traits of a RelNode before or
     * after optimization, although doing so could render a tree of RelNodes
     * unimplementable. If a RelNode's traits need to be modified during
     * optimization, clone the RelNode and change the clone's traits.
     *
     * @return this RelNode's trait set
     */
    public RelTraitSet getTraits();

    /**
     * Sets the name of the variable which is to be implicitly set at runtime
     * each time a row is returned from this relational expression
     *
     * @param correlVariable Name of correlating variable
     */
    public void setCorrelVariable(String correlVariable);

    /**
     * Returns the name of the variable which is to be implicitly set at runtime
     * each time a row is returned from this relational expression; or null if
     * there is no variable.
     *
     * @return Name of correlating variable, or null
     */
    public String getCorrelVariable();

    /**
     * Returns whether the same value will not come out twice. Default value is
     * <code>false</code>, derived classes should override.
     */
    public boolean isDistinct();

    /**
     * Returns the ID of this relational expression, unique among all relational
     * expressions created since the server was started.
     *
     * @return Unique ID
     */
    public int getId();

    /**
     * Returns the <code>i</code><sup>th</sup> input relational expression.
     *
     * @param i Ordinal of input
     *
     * @return <code>i</code><sup>th</sup> input
     */
    public RelNode getInput(int i);

    /**
     * Returns a variable with which to reference the current row of this
     * relational expression as a correlating variable. Creates a variable if
     * none exists.
     */
    public String getOrCreateCorrelVariable();

    /**
     * Returns the sub-query this relational expression belongs to. A sub-query
     * determines the scope for correlating variables (see {@link
     * #setCorrelVariable(String)}).
     *
     * @return Sub-query
     */
    public RelOptQuery getQuery();

    /**
     * Returns the type of the rows returned by this relational expression.
     */
    public RelDataType getRowType();

    /**
     * Returns the type of the rows expected for an input. Defaults to {@link
     * #getRowType}.
     *
     * @param ordinalInParent input's 0-based ordinal with respect to this
     * parent rel
     *
     * @return expected row type
     */
    public RelDataType getExpectedInputRowType(int ordinalInParent);

    /**
     * Returns an array of this relational expression's inputs. If there are no
     * inputs, returns an empty array, not <code>null</code>.
     */
    public RelNode [] getInputs();

    /**
     * Returns an estimate of the number of rows this relational expression will
     * return.
     *
     * <p>NOTE jvs 29-Mar-2006: Don't call this method directly. Instead, use
     * {@link RelMetadataQuery#getRowCount}, which gives plugins a chance to
     * override the rel's default ideas about row count.
     */
    public double getRows();

    /**
     * Returns the names of variables which are set in this relational
     * expression but also used and therefore not available to parents of this
     * relational expression.
     *
     * <p>By default, returns the empty set. Derived classes may override this
     * method.</p>
     */
    public Set<String> getVariablesStopped();

    /**
     * Collects variables known to be used by this expression or its
     * descendants. By default, no such information is available and must be
     * derived by analyzing sub-expressions, but some optimizer implementations
     * may insert special expressions which remember such information.
     *
     * @param variableSet receives variables used
     */
    public void collectVariablesUsed(Set<String> variableSet);

    /**
     * Collects variables set by this expression.
     *
     * @param variableSet receives variables known to be set by
     */
    public void collectVariablesSet(Set<String> variableSet);

    /**
     * Interacts with the {@link RelVisitor} in a {@link
     * org.eigenbase.util.Glossary#VisitorPattern visitor pattern} to traverse
     * the tree of relational expressions.
     */
    public void childrenAccept(RelVisitor visitor);

    /**
     * Returns the cost of this plan (not including children). The base
     * implementation throws an error; derived classes should override.
     *
     * <p>NOTE jvs 29-Mar-2006: Don't call this method directly. Instead, use
     * {@link RelMetadataQuery#getNonCumulativeCost}, which gives plugins a
     * chance to override the rel's default ideas about cost.
     */
    public RelOptCost computeSelfCost(RelOptPlanner planner);

    public void explain(RelOptPlanWriter pw);

    /**
     * Receives notification that this expression is about to be registered. The
     * implementation of this method must at least register all child
     * expressions.
     */
    public void onRegister(RelOptPlanner planner);

    /**
     * Computes the digest, assigns it, and returns it. For planner use only.
     */
    public String recomputeDigest();

    /**
     * Registers a correlation variable.
     *
     * @see #getVariablesStopped
     */
    public void registerCorrelVariable(String correlVariable);

    /**
     * Replaces the <code>ordinalInParent</code><sup>th</sup> input. You must
     * override this method if you override {@link #getInputs}.
     */
    public void replaceInput(
        int ordinalInParent,
        RelNode p);

    /**
     * If this relational expression represents an access to a table, returns
     * that table, otherwise returns null.
     */
    public RelOptTable getTable();

    /**
     * Returns the name of this relational expression's class, sans package
     * name, for use in {@link #explain}. For example, for a <code>
     * org.eigenbase.rel.ArrayRel.ArrayReader</code>, this method returns
     * "ArrayReader".
     */
    public String getRelTypeName();

    /**
     * Returns whether this relational expression is valid.
     *
     * <p>If assertions are enabled, this method is typically called with <code>
     * fail</code> = <code>true</code>, as follows:
     *
     * <blockquote>
     * <pre>assert rel.isValid(true)</pre>
     * </blockquote>
     *
     * This signals that the method can throw an {@link AssertionError} if it is
     * not valid.
     *
     * @param fail Whether to fail if invalid
     *
     * @return Whether relational expression is valid
     *
     * @throws AssertionError if this relational expression is invalid and
     * fail=true and assertions are enabled
     */
    public boolean isValid(boolean fail);

    /**
     * Returns a description of the physical ordering (or orderings) of this
     * relational expression.
     *
     * @post return != null
     */
    public List<RelCollation> getCollationList();

    /**
     * Returns a string which concisely describes the definition of this
     * relational expression. Two relational expressions are equivalent if and
     * only if their digests are the same.
     *
     * <p>The digest does not contain the relational expression's identity --
     * that would prevent similar relational expressions from ever comparing
     * equal -- but does include the identity of children (on the assumption
     * that children have already been normalized).
     *
     * <p>If you want a descriptive string which contains the identity, call
     * {@link Object#toString()}, which always returns "rel#{id}:{digest}".
     */
    String getDigest();

    /**
     * Returns a string which describes the relational expression and, unlike
     * {@link #getDigest()}, also includes the identity. Typically returns
     * "rel#{id}:{digest}".
     */
    String getDescription();

    /**
     * Clones this RelNode.
     *
     * <p>Traits of the RelNode must be explicitly cloned, using {@link
     * AbstractRelNode#inheritTraitsFrom(AbstractRelNode)}, as the RelNode may
     * have traits of which it has no knowledge. Example implementation:
     *
     * <pre>
     *     public MyRelNode clone()
     *     {
     *         MyRelNode clone = new MyRelNode(...);
     *         clone.inheritTraitsFrom(this);
     *         return clone;
     *     }
     * </pre>
     * <b>N.B.:</b> This method must be overridden whenever an existing,
     * concrete RelNode is extended. Otherwise, calling clone() will produce a
     * differently typed RelNode, resulting in invalid or incorrect query plans.
     *
     * @return a clone of this RelNode
     */
    RelNode clone();
}

// End RelNode.java
