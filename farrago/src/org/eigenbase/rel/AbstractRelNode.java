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

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Collections;
import java.util.Set;
import java.util.logging.Logger;

import org.eigenbase.relopt.*;
import org.eigenbase.reltype.*;
import org.eigenbase.rex.RexNode;
import org.eigenbase.rex.RexUtil;
import org.eigenbase.trace.*;
import org.eigenbase.util.Util;


/**
 * Base class for every relational expression ({@link RelNode}).
 */
public abstract class AbstractRelNode implements RelNode
{
    //~ Static fields/initializers --------------------------------------------

    // TODO jvs 10-Oct-2003:  Make this thread safe.  Either synchronize, or
    // keep this per-VolcanoPlanner.

    /** generator for {@link #id} values */
    static int nextId = 0;
    private static final Logger tracer = EigenbaseTrace.getPlannerTracer();

    //~ Instance fields -------------------------------------------------------

    /** cached type of this relational expression */
    public RelDataType rowType;

    /**
     * A short description of this relational expression's type, inputs, and
     * other properties. The string uniquely identifies the node; another
     * node is equivalent if and only if it has the same value. Computed by
     * {@link #computeDigest}, assigned by {@link #onRegister}, returned by
     * {@link #toString}.
     */
    protected String digest;
    protected RelOptCluster cluster;

    /** unique id of this object -- for debugging */
    protected int id;

    /**
     * The variable by which to refer to rows from this relational expression,
     * as correlating expressions; null if this expression is not correlated
     * on.
     */
    private String correlVariable;

    /**
     * The RelTraitSet that describes the traits of this RelNode.
     */
    protected RelTraitSet traits;

    //~ Constructors ----------------------------------------------------------

    /**
     * Creates a <code>AbstractRelNode</code>.
     *
     * @pre cluster != null
     */
    public AbstractRelNode(RelOptCluster cluster, RelTraitSet traits)
    {
        super();
        assert (cluster != null);
        this.cluster = cluster;
        this.traits = traits;
        this.id = nextId++;
        this.digest = getRelTypeName() + "#" + id;
        tracer.finest("new " + digest);
    }

    //~ Methods ---------------------------------------------------------------

    /**
     * Clone this RelNode.  Traits of the RelNode must be explicitly cloned
     * as the RelNode may have traits of which it has now knowledge.
     * Example implementation:
     *
     * <pre>
     *     public Object clone()
     *     {
     *         MyRelNode clone = new MyRelNode(...);
     *         clone.traits = this.cloneTraits();
     *         return clone;
     *     }
     * </pre>
     *
     * @return a clone of this RelNode
     */
    public abstract Object clone();

    public boolean isAccessTo(RelOptTable table)
    {
        return getTable() == table;
    }

    public RexNode [] getChildExps()
    {
        return RexUtil.emptyExpressionArray;
    }

    public RelOptCluster getCluster()
    {
        return cluster;
    }

    public final CallingConvention getConvention()
    {
        return
            (CallingConvention)traits.getTrait(
                CallingConventionTraitDef.instance);
    }

    public RelTraitSet getTraits()
    {
        return traits;
    }

    /**
     * Returns a clone of this RelNode's traits.  Useful for
     * implementing {@link #clone()}.
     *
     * @return a clone of this RelNode's traits.
     */
    protected RelTraitSet cloneTraits()
    {
        return (RelTraitSet)traits.clone();
    }

    public void setCorrelVariable(String correlVariable)
    {
        this.correlVariable = correlVariable;
    }

    public String getCorrelVariable()
    {
        return correlVariable;
    }

    public boolean isDistinct()
    {
        return false;
    }

    public int getId()
    {
        return id;
    }

    public RelNode getInput(int i)
    {
        RelNode [] inputs = getInputs();
        return inputs[i];
    }

    public String getOrCreateCorrelVariable()
    {
        if (correlVariable == null) {
            correlVariable = getQuery().createCorrel();
            getQuery().mapCorrel(correlVariable, this);
        }
        return correlVariable;
    }

    public RelOptQuery getQuery()
    {
        return cluster.query;
    }

    /**
     * Registers any special rules specific to this kind of relational
     * expression.
     *
     * <p>
     * The planner calls this method this first time that it sees a relational
     * expression of this class. The derived class should call {@link
     * RelOptPlanner#addRule} for each rule, and then call {@link #register}
     * on its base class.
     * </p>
     */
    public static void register(RelOptPlanner planner)
    {
        Util.discard(planner);
    }

    public final String getRelTypeName()
    {
        String className = getClass().getName();
        int i = className.lastIndexOf("$");
        if (i >= 0) {
            return className.substring(i + 1);
        }
        i = className.lastIndexOf(".");
        if (i >= 0) {
            return className.substring(i + 1);
        }
        return className;
    }

    public final RelDataType getRowType()
    {
        if (rowType == null) {
            rowType = deriveRowType();
        }
        return rowType;
    }

    protected RelDataType deriveRowType()
    {
        // This method is only called if rowType is null, so you don't NEED to
        // implement it if rowType is always set.
        throw new UnsupportedOperationException();
    }

    public RelDataType getExpectedInputRowType(int ordinalInParent)
    {
        return getRowType();
    }

    public RelNode [] getInputs()
    {
        return emptyArray;
    }

    public String getQualifier()
    {
        return "";
    }

    public double getRows()
    {
        return 1.0;
    }

    public Set getVariablesStopped()
    {
        return Collections.EMPTY_SET;
    }

    public void childrenAccept(RelVisitor visitor)
    {
        RelNode [] inputs = getInputs();
        for (int i = 0; i < inputs.length; i++) {
            visitor.visit(inputs[i], i, this);
        }
    }

    public RelOptCost computeSelfCost(RelOptPlanner planner)
    {
        throw Util.newInternal("todo: implement " + getClass()
            + ".computeSelfCost");
    }

    public void explain(RelOptPlanWriter pw)
    {
        pw.explain(this, Util.emptyStringArray, Util.emptyObjectArray);
    }

    public void onRegister(RelOptPlanner planner)
    {
        RelNode [] inputs = getInputs();
        for (int i = 0; i < inputs.length; i++) {
            RelNode e = planner.register(inputs[i], null);
            if (e != inputs[i]) {
                replaceInput(i, e);
            }
        }
        digest = computeDigest();
        assert (digest != null);
    }

    public String recomputeDigest()
    {
        return digest = computeDigest();
    }

    public void registerCorrelVariable(String correlVariable)
    {
        assert (this.correlVariable == null);
        this.correlVariable = correlVariable;
        getQuery().mapCorrel(correlVariable, this);
    }

    public void replaceInput(
        int ordinalInParent,
        RelNode p)
    {
        throw Util.newInternal("replaceInput called on " + this);
    }

    public String toString()
    {
        return digest;
    }

    public RelOptTable getTable()
    {
        return null;
    }

    /**
     * Computes the digest. Does not modify this object.
     */
    protected String computeDigest()
    {
        StringWriter sw = new StringWriter();
        RelOptPlanWriter pw =
            new RelOptPlanWriter(new PrintWriter(sw)) {
                public void explain(
                    RelNode rel,
                    String [] terms,
                    Object [] values)
                {
                    RelNode [] inputs = rel.getInputs();
                    RexNode [] childExps = rel.getChildExps();
                    assert terms.length == (inputs.length + childExps.length
                        + values.length) : "terms.length=" + terms.length
                    + " inputs.length=" + inputs.length + " childExps.length="
                    + childExps.length + " values.length=" + values.length;
                    write(getRelTypeName());
                    write("(");
                    int j = 0;
                    for (int i = 0; i < inputs.length; i++) {
                        if (j > 0) {
                            write(",");
                        }
                        write(terms[j++] + "=" + inputs[i].toString());
                    }
                    for (int i = 0; i < childExps.length; i++) {
                        if (j > 0) {
                            write(",");
                        }
                        RexNode childExp = childExps[i];
                        write(terms[j++] + "=" + childExp.toString());
                    }
                    for (int i = 0; i < values.length; i++) {
                        Object value = values[i];
                        if (j > 0) {
                            write(",");
                        }
                        write(terms[j++] + "=" + value.toString());
                    }
                    write(")");
                }
            };
        explain(pw);
        pw.flush();
        return sw.toString();
    }
}


// End AbstractRelNode.java
