/*
// $Id$
// Saffron preprocessor and data engine
// (C) Copyright 2002-2003 Disruptive Technologies, Inc.
// (C) Copyright 2003-2004 John V. Sichi
// You must accept the terms in LICENSE.html to use this software.
//
// This program is free software; you can redistribute it and/or
// modify it under the terms of the GNU Lesser General Public License
// as published by the Free Software Foundation; either version 2.1
// of the License, or (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with this program; if not, write to the Free Software
// Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
*/
package net.sf.saffron.rel;

import net.sf.saffron.core.SaffronTable;
import net.sf.saffron.core.SaffronType;
import net.sf.saffron.core.SaffronPlanner;
import net.sf.saffron.core.PlanWriter;
import net.sf.saffron.rex.RexNode;
import net.sf.saffron.opt.VolcanoCluster;
import net.sf.saffron.opt.CallingConvention;
import net.sf.saffron.opt.VolcanoQuery;
import net.sf.saffron.opt.PlanCost;

import java.util.Set;

/**
 * A <code>SaffronRel</code> is a relational expression.  It is NOT an
 * {@link openjava.ptree.Expression}.
 * 
 * <p>
 * If this type of relational expression has some particular rules, it should
 * implement the <em>public static</em> method {@link SaffronBaseRel#register}.
 * </p>
 *
 * <p>When a relational expression comes to be implemented, the system
 * allocates a {@link net.sf.saffron.opt.RelImplementor} to manage the
 * process. Every implementable relational expression has a
 * {@link CallingConvention} describing how it passes data to its consuming
 * relational expression.</p>
 *
 * <p>For each calling-convention, there is a corresponding sub-interface of
 * SaffronRel. For example, {@link net.sf.saffron.oj.rel.JavaRel} has
 * operations to manage the conversion to a graph of
 * {@link CallingConvention#JAVA Java calling-convention}, and it interacts
 * with a {@link net.sf.saffron.oj.rel.JavaRelImplementor}.</p>
 *
 * <p>A relational expression is only required to implement its
 * calling-convention's interface when it is actually implemented, that is,
 * converted into a plan/program. This means that relational expressions which
 * cannot be implemented, such as converters, are not required to implement
 * their convention's interface.</p>
 *
 * <p>Every relational expression must derive from {@link SaffronBaseRel}.
 * (Why have the <code>SaffronRel</code> interface, then? We need a root
 * interface, because an interface can only derive from an interface.)</p>
 *
 * @author jhyde
 * @since May 24, 2004
 * @version $Id$
 **/
public interface SaffronRel {
    SaffronBaseRel [] emptyArray = new SaffronBaseRel[0];

    /**
     * Returns whether this relational expression is an access to
     * <code>table</code>.
     */
    boolean isAccessTo(SaffronTable table);

    /**
     * Returns an array of this relational expression's child expressions
     * (not including the inputs returned by {@link #getInputs}.  If there
     * are no child expressions, returns an empty array, not
     * <code>null</code>.
     */
    RexNode [] getChildExps();

    VolcanoCluster getCluster();

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
    SaffronRel getInput(int i);

    /**
     * Returns a variable with which to reference the current row of this
     * relational expression as a correlating variable. Creates a variable if
     * none exists.
     */
    String getOrCreateCorrelVariable();

    VolcanoQuery getQuery();

    /**
     * Returns the type of the rows returned by this relational expression.
     */
    SaffronType getRowType();

    /**
     * Returns the type of the rows expected for an input.  Defaults to
     * {@link #getRowType}.
     *
     * @param ordinalInParent input's 0-based ordinal with respect to
     * this parent rel
     *
     * @return expected row type
     */
    SaffronType getExpectedInputRowType(int ordinalInParent);

    /**
     * Returns an array of this relational expression's inputs.  If there
     * are no inputs, returns an empty array, not <code>null</code>.
     */
    SaffronRel [] getInputs();

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
     * {@link net.sf.saffron.util.Glossary#VisitorPattern visitor pattern} to
     * traverse the tree of relational expressions.
     */
    void childrenAccept(RelVisitor visitor);

    /**
     * Returns the cost of this plan (not including children). The base
     * implementation throws an error; derived classes should override.
     */
    PlanCost computeSelfCost(SaffronPlanner planner);

    void explain(PlanWriter pw);

    /**
     * This method is called just before the expression is registered.  The
     * implementation of this method must at least register all child
     * expressions.
     */
    void onRegister(SaffronPlanner planner);

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
    void replaceInput(int ordinalInParent,SaffronRel p);

    /**
     * If this relational expression represents an access to a table, returns
     * that table, otherwise returns null.
     */
    SaffronTable getTable();

    /**
     * Returns the name of this relational expression's class, sans package
     * name, for use in {@link #explain}.  For example, for a
     * <code>net.sf.saffron.rel.ArrayRel.ArrayReader</code>, this method
     * returns "ArrayReader".
     */
    String getRelTypeName();
}

// End SaffronRel.java
