/*
// $Id$
// Saffron preprocessor and data engine
// (C) Copyright 2002-2003 Disruptive Technologies, Inc.
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

import net.sf.saffron.core.PlanWriter;
import net.sf.saffron.core.SaffronPlanner;
import net.sf.saffron.core.SaffronTable;
import net.sf.saffron.core.SaffronType;
import net.sf.saffron.opt.*;
import net.sf.saffron.rex.RexNode;
import net.sf.saffron.rex.RexUtil;
import net.sf.saffron.util.Util;
import openjava.tools.DebugOut;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Collections;
import java.util.Set;


/**
 * A <code>SaffronRel</code> is a relational expression.  It is NOT an {@link
 * openjava.ptree.Expression}.
 * 
 * <p>
 * If this type of relational expression has some particular rules, it should
 * implement the <em>public static</em> method {@link #register}.
 * </p>
 */
public abstract class SaffronRel
{
    //~ Static fields/initializers --------------------------------------------

    // TODO jvs 10-Oct-2003:  Make this thread safe.  Either synchronize, or
    // keep this per-VolcanoPlanner.
    /** generator for {@link #id} values */
    static int nextId = 0;
    
    static final SaffronRel [] emptyArray = new SaffronRel[0];

    //~ Instance fields -------------------------------------------------------

    /** cached type of this relational expression */
    public SaffronType rowType;

    /**
     * A short description of this relational expression's type, inputs, and
     * other properties. The string uniquely identifies the node; another
     * node is equivalent if and only if it has the same value. Computed by
     * {@link #computeDigest}, assigned by {@link #onRegister}, returned by
     * {@link #toString}.
     */
    protected String digest;
    protected VolcanoCluster cluster;

    /** unique id of this object -- for debugging */
    protected int id;

    /**
     * The variable by which to refer to rows from this relational expression,
     * as correlating expressions; null if this expression is not correlated
     * on.
     */
    private String correlVariable;

    //~ Constructors ----------------------------------------------------------

    /**
     * Creates a <code>SaffronRel</code>.
     *
     * @pre cluster != null
     */
    public SaffronRel(VolcanoCluster cluster)
    {
        super();
        assert(cluster != null);
        this.cluster = cluster;
        this.id = nextId++;
        this.digest = getRelTypeName() + "#" + id;
        DebugOut.println("new " + digest);
    }

    //~ Methods ---------------------------------------------------------------

    // override Object (public, does not throw CloneNotSupportedException)
    public abstract Object clone();

    /**
     * Returns whether this relational expression is an access to
     * <code>table</code>.
     */
    public boolean isAccessTo(SaffronTable table)
    {
        return getTable() == table;
    }

    /**
     * Returns an array of this <code>SaffronRel</code>'s child expressions
     * (not including the inputs returned by {@link #getInputs}.  If there
     * are no child expressions, returns an empty array, not
     * <code>null</code>.
     */
    public RexNode [] getChildExps()
    {
        return RexUtil.emptyExpressionArray;
    }

    public VolcanoCluster getCluster()
    {
        return cluster;
    }

    /**
     * Returns a value from {@link net.sf.saffron.opt.CallingConvention}.
     */
    public CallingConvention getConvention()
    {
        return CallingConvention.NONE;
    }

    public void setCorrelVariable(String correlVariable)
    {
        this.correlVariable = correlVariable;
    }

    public String getCorrelVariable()
    {
        return correlVariable;
    }

    /**
     * Returns whether the same value will not come out twice. Default value
     * is <code>false</code>, derived classes should override.
     */
    public boolean isDistinct()
    {
        return false;
    }

    public int getId()
    {
        return id;
    }

    /**
     * Get the <code>i</code><sup>th</sup> input.
     */
    public SaffronRel getInput(int i)
    {
        SaffronRel [] inputs = getInputs();
        return inputs[i];
    }

    /**
     * Returns a variable with which to reference the current row of this
     * relational expression as a correlating variable. Creates a variable if
     * none exists.
     */
    public String getOrCreateCorrelVariable()
    {
        if (correlVariable == null) {
            correlVariable = getQuery().createCorrel();
            getQuery().mapCorrel(correlVariable,this);
        }
        return correlVariable;
    }

    public VolcanoQuery getQuery()
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
     * SaffronPlanner#addRule} for each rule, and then call {@link #register}
     * on its base class.
     * </p>
     */
    public static void register(SaffronPlanner planner)
    {
        Util.discard(planner);
    }

    /**
     * Returns the name of this <code>SaffronRel</code>'s class, sans package
     * name, for use in {@link #explain}.  For example, for a
     * <code>saffron.ArrayRel.ArrayReader</code>, returns "ArrayReader".
     */
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

    /**
     * Returns the type of the rows returned by this relational expression.
     */
    public final SaffronType getRowType()
    {
        if (rowType == null) {
            rowType = deriveRowType();
        }
        return rowType;
    }

    /**
     * Returns the type of the rows expected for an input.  Defaults to
     * getRowType().
     *
     * @param ordinalInParent input's 0-based ordinal with respect to
     * this parent rel
     *
     * @return expected row type
     */
    public SaffronType getExpectedInputRowType(int ordinalInParent)
    {
        return getRowType();
    }

    /**
     * Returns an array of this <code>SaffronRel</code>'s inputs.  If there
     * are no inputs, returns an empty array, not <code>null</code>.
     */
    public SaffronRel [] getInputs()
    {
        return emptyArray;
    }

    public String getQualifier()
    {
        return "";
    }

    /**
     * Returns an estimate of the number of rows this relational expression
     * will return.
     */
    public double getRows()
    {
        return 1.0;
    }

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
    public Set getVariablesStopped()
    {
        return Collections.EMPTY_SET;
    }

    public void childrenAccept(RelVisitor visitor)
    {
        SaffronRel [] inputs = getInputs();
        for (int i = 0; i < inputs.length; i++) {
            visitor.visit(inputs[i],i,this);
        }
    }

    /**
     * Returns the cost of this plan (not including children). The base
     * implementation throws an error; derived classes should override.
     */
    public PlanCost computeSelfCost(SaffronPlanner planner)
    {
        throw Util.newInternal(
            "todo: implement " + getClass() + ".computeSelfCost");
    }

    public void explain(PlanWriter pw)
    {
        pw.explain(this,Util.emptyStringArray,Util.emptyObjectArray);
    }

    /**
     * Create a plan for this expression according to a calling convention.
     *
     * @param implementor implementor
     * @param ordinal indicates our position in the pre-, in- and postfix walk
     *        over the tree; <code>ordinal</code> is -1 when called from the
     *        parent, and <code>i</code> when called from the
     *        <code>i</code><sup>th</sup> child.
     *
     * @throws UnsupportedOperationException if this expression cannot be
     *         implemented
     */
    public Object implement(RelImplementor implementor,int ordinal)
    {
        throw new UnsupportedOperationException();
    }

    /**
     * Burrows into a synthetic record and returns the underlying relation
     * which provides the field called <code>fieldName</code>.
     */
    public SaffronRel implementFieldAccess(
        RelImplementor implementor,
        String fieldName)
    {
        return null;
    }

    /**
     * This method is called just before the expression is registered.  The
     * implementation of this method must at least register all child
     * expressions.
     */
    public void onRegister(SaffronPlanner planner)
    {
        SaffronRel [] inputs = getInputs();
        for (int i = 0; i < inputs.length; i++) {
            SaffronRel e = planner.register(inputs[i],null);
            if (e != inputs[i]) {
                replaceInput(i,e);
            }
        }
        digest = computeDigest();
        assert(digest != null);
    }

    /**
     * Computes the digest, assigns it, and returns it. For internal use only.
     */
    public String recomputeDigest()
    {
        return digest = computeDigest();
    }

    public void registerCorrelVariable(String correlVariable)
    {
        assert(this.correlVariable == null);
        this.correlVariable = correlVariable;
        getQuery().mapCorrel(correlVariable,this);
    }

    /**
     * Replaces the <code>ordinalInParent</code><sup>th</sup> input.  You must
     * override this method if you override {@link #getInputs}.
     */
    public void replaceInput(int ordinalInParent,SaffronRel p)
    {
        throw Util.newInternal("replaceInput called on " + this);
    }

    public String toString()
    {
        return digest;
    }

    /**
     * If this relational expression represents an access to a table, returns
     * that table, otherwise returns null.
     */
    public SaffronTable getTable()
    {
        return null;
    }

    /**
     * Computes the digest. Does not modify this object.
     */
    protected String computeDigest()
    {
        StringWriter sw = new StringWriter();
        PlanWriter pw =
            new PlanWriter(new PrintWriter(sw)) {
                public void explain(
                    SaffronRel rel,
                    String [] terms,
                    Object [] values)
                {
                    write(getRelTypeName());
                    write("(");
                    int j = 0;
                    final SaffronRel [] inputs = getInputs();
                    for (int i = 0; i < inputs.length; i++) {
                        if (j > 0) {
                            write(",");
                        }
                        write(terms[j++] + "=" + inputs[i].toString());
                    }
                    final RexNode [] childExps = getChildExps();
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

    protected abstract SaffronType deriveRowType();
}


// End SaffronRel.java
