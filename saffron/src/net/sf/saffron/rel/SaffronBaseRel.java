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

import net.sf.saffron.trace.*;
import net.sf.saffron.trace.SaffronTrace;
import net.sf.saffron.core.*;
import net.sf.saffron.opt.CallingConvention;
import net.sf.saffron.opt.PlanCost;
import net.sf.saffron.opt.VolcanoCluster;
import net.sf.saffron.opt.VolcanoQuery;
import net.sf.saffron.rex.RexNode;
import net.sf.saffron.rex.RexUtil;
import net.sf.saffron.util.Util;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Collections;
import java.util.Set;
import java.util.logging.Logger;


/**
 * Base class for every relational expression ({@link SaffronRel}).
 */
public abstract class SaffronBaseRel implements SaffronRel {
    //~ Static fields/initializers --------------------------------------------

    // TODO jvs 10-Oct-2003:  Make this thread safe.  Either synchronize, or
    // keep this per-VolcanoPlanner.
    /** generator for {@link #id} values */
    static int nextId = 0;

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
    private static final Logger tracer = SaffronTrace.getPlannerTracer();

    //~ Constructors ----------------------------------------------------------

    /**
     * Creates a <code>SaffronBaseRel</code>.
     *
     * @pre cluster != null
     */
    public SaffronBaseRel(VolcanoCluster cluster)
    {
        super();
        assert(cluster != null);
        this.cluster = cluster;
        this.id = nextId++;
        this.digest = getRelTypeName() + "#" + id;
        tracer.finest("new " + digest);
    }

    //~ Methods ---------------------------------------------------------------

    // override Object (public, does not throw CloneNotSupportedException)
    public abstract Object clone();


    public boolean isAccessTo(SaffronTable table)
    {
        return getTable() == table;
    }

    public RexNode [] getChildExps()
    {
        return RexUtil.emptyExpressionArray;
    }

    public VolcanoCluster getCluster()
    {
        return cluster;
    }

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

    public boolean isDistinct()
    {
        return false;
    }

    public int getId()
    {
        return id;
    }

    public SaffronRel getInput(int i)
    {
        SaffronRel [] inputs = getInputs();
        return inputs[i];
    }

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

    public final SaffronType getRowType()
    {
        if (rowType == null) {
            rowType = deriveRowType();
        }
        return rowType;
    }

    protected SaffronType deriveRowType() {
        // This method is only called if rowType is null, so you don't NEED to
        // implement it if rowType is always set.
        throw new UnsupportedOperationException();
    }

    public SaffronType getExpectedInputRowType(int ordinalInParent)
    {
        return getRowType();
    }

    public SaffronRel [] getInputs()
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
        SaffronRel [] inputs = getInputs();
        for (int i = 0; i < inputs.length; i++) {
            visitor.visit(inputs[i],i,this);
        }
    }

    public PlanCost computeSelfCost(SaffronPlanner planner)
    {
        throw Util.newInternal(
            "todo: implement " + getClass() + ".computeSelfCost");
    }

    public void explain(PlanWriter pw)
    {
        pw.explain(this,Util.emptyStringArray,Util.emptyObjectArray);
    }

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

    public void replaceInput(int ordinalInParent,SaffronRel p)
    {
        throw Util.newInternal("replaceInput called on " + this);
    }

    public String toString()
    {
        return digest;
    }

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
                    SaffronRel[] inputs = rel.getInputs();
                    RexNode[] childExps = rel.getChildExps();
                    assert terms.length ==
                            inputs.length + childExps.length + values.length :
                            "terms.length=" + terms.length +
                            " inputs.length=" + inputs.length +
                            " childExps.length=" + childExps.length +
                            " values.length=" + values.length;
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


// End SaffronBaseRel.java
