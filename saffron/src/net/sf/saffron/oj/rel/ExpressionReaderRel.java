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

package net.sf.saffron.oj.rel;

import net.sf.saffron.core.PlanWriter;
import net.sf.saffron.core.SaffronPlanner;
import net.sf.saffron.core.SaffronType;
import net.sf.saffron.oj.util.JavaRowExpression;
import net.sf.saffron.oj.util.OJUtil;
import net.sf.saffron.oj.OJTypeFactory;
import net.sf.saffron.opt.CallingConvention;
import net.sf.saffron.opt.PlanCost;
import net.sf.saffron.opt.RelImplementor;
import net.sf.saffron.opt.VolcanoCluster;
import net.sf.saffron.rel.SaffronRel;
import net.sf.saffron.rex.RexNode;
import net.sf.saffron.rex.RexUtil;
import net.sf.saffron.util.Util;
import openjava.mop.OJClass;
import openjava.ptree.*;

import java.util.HashSet;


/**
 * An <code>ExpressionReaderRel</code> is a relational expression node which
 * evaluates an expression and returns the results as a relation. Cases:
 * 
 * <ol>
 * <li>
 * If the expression is an array <code>T[]</code>, the each result row is of
 * type <code>T</code>.
 * </li>
 * <li>
 * If the expression implements {@link java.util.Map} or derives from {@link
 * java.util.Hashtable}, each result row contains fields <code>{Object key;
 * Object value;}</code>.
 * </li>
 * <li>
 * If the expression implements {@link java.util.Iterator} or derives from
 * {@link java.util.Enumeration}, each result row is an {@link Object}.
 * </li>
 * </ol>
 * 
 * <p>
 * NOTE: We support {@link java.util.Hashtable}, {@link java.util.Enumeration}
 * and {@link java.util.Vector} explicitly because {@link java.util.Map},
 * {@link java.util.Iterator} and {@link java.util.Map} does not exist until
 * JDK 1.2.
 * </p>
 * 
 * <p>
 * Example accessing an array:
 * <blockquote>
 * <pre>Emp[] emps;
 * Emp[] males = (select from emps as emp where emp.gender.equals("M"));</pre>
 * </blockquote>
 * </p>
 * 
 * <p>
 * The following example shows how you can cast the values returned from a
 * {@link java.util.Hashtable}:
 * <blockquote>
 * <pre>Hashtable nameToDeptno = new Hashtable();
 * nameToDeptno.put("Fred", new Integer(20));
 * nameToDeptno.put("Eric", new Integer(10));
 * nameToDeptno.put("Bill", new Integer(10));
 * for (i in (select {(String) key, ((Integer) value).intValue() as value}
 *            from nameToDeptno)) {
 *   print(i.key + " is in dept " + i.value);
 * }</pre>
 * </blockquote>
 * </p>
 * 
 * <p>
 * Here we access a value in a {@link java.util.Hashtable} by key:
 * <blockquote>
 * <pre>Object fredDeptno =
 *    (select value from nameToDeptno where key.equals("Fred");</pre>
 * </blockquote>
 * Because the Hashtable is being accessed via its key, the optimizer will
 * probably optimize this code to <code>nameToDeptno.get("Fred")</code>.
 * </p>
 *
 * @author jhyde
 * @version $Id$
 *
 * @since 8 December, 2001
 */
public class ExpressionReaderRel extends SaffronRel
{
    //~ Instance fields -------------------------------------------------------

    protected CallingConvention convention;
    protected RexNode exp;

    /** Whether the rows are distinct; set by {@link #deriveRowType}. */
    private boolean distinct;

    //~ Constructors ----------------------------------------------------------

    /**
     * Creates an <code>ExpressionReaderRel</code>.
     *
     * @param cluster {@link VolcanoCluster} this relational expression
     *        belongs to
     * @param exp expression to evaluate
     * @param rowType row type of the expression; if null, the row type is
     *        deduced from the expression (for example, the row type of a
     *        {@link java.util.Iterator} is {@link java.lang.Object}.
     *        Note that actual row type of the relational expression is
     *        always a record type with a single field. For example, if you
     *        supply an expression of type "java.util.Iterator" and specify that
     *        the row type is "java.lang.String" then the row type will be
     *        "Record{$f0:String}". 
     */
    public ExpressionReaderRel(VolcanoCluster cluster, RexNode exp, SaffronType rowType)
    {
        super(cluster);
        if (rowType != null) {
            exp = cluster.rexBuilder.makeCast(rowType.getArrayType(), exp);
        }
        this.exp = exp;
        this.convention = chooseConvention(exp);
        Util.discard(getRowType()); // force derivation of row-type
    }

    //~ Methods ---------------------------------------------------------------

    public RexNode [] getChildExps()
    {
        return new RexNode[] { exp };
    }

    public CallingConvention getConvention()
    {
        return convention;
    }

    public boolean isDistinct()
    {
        Util.discard(getRowType());
        return distinct;
    }

    public RexNode getExp()
    {
        return exp;
    }

    public Object clone()
    {
        return new ExpressionReaderRel(cluster,RexUtil.clone(exp), rowType);
    }

    public PlanCost computeSelfCost(SaffronPlanner planner)
    {
        int arrayLength = 50; // a guess
        double dRows = arrayLength;
        double dCpu = arrayLength;
        double dIo = 0;
        return planner.makeCost(dRows,dCpu,dIo);
    }

    public void explain(PlanWriter pw)
    {
        pw.explain(this,new String [] { "expression" });
    }

    public Object implement(RelImplementor implementor,int ordinal)
    {
        switch (ordinal) {
        case -1: // called from parent
            Expression exp2 = implementor.translate(this,exp);
            return exp2;
        default:
            throw Util.newInternal("implement: ordinal=" + ordinal);
        }
    }

    protected SaffronType deriveRowType()
    {
        final SaffronType type = exp.getType();
        final SaffronType componentType = type.getComponentType();
        if (componentType == null) {
            throw Util.newInternal(
                "expression " + exp + " is not relational "
                + "(array, iterator, enumeration, map or hashtable)");
        }
        distinct = isDistinct(exp);
        if (false) {
            return cluster.typeFactory.createProjectType(
                    new SaffronType[] {componentType}, new String[] {"$f0"});
        } else {
            return componentType;
        }
    }

    private boolean isDistinct(RexNode exp) {
        final SaffronType type = exp.getType();
        final OJClass ojClass = ((OJTypeFactory) cluster.typeFactory).toOJClass(null,type);
        if (Util.clazzSet.isAssignableFrom(ojClass)) {
            return true;
        } else if (exp instanceof JavaRowExpression &&
                ((JavaRowExpression) exp).expression instanceof
                ArrayAllocationExpression) {
            ArrayAllocationExpression arrayAlloc =
                (ArrayAllocationExpression)
                    ((JavaRowExpression) exp).expression;
            return isDistinct(arrayAlloc);
        } else {
            return false;
        }
    }

    /**
     * Returns true if the array allocation expression consists of distinct
     * literals, false otherwise. For example, <code>isDistinct(new String[]
     * { "a", "b"})</code> returns true, <code>isDistinct(new int[] {1,
     * 1+1})</code> returns false.
     */
    private static boolean isDistinct(ArrayAllocationExpression arrayAlloc)
    {
        HashSet values = new HashSet();
        final ArrayInitializer initializer = arrayAlloc.getInitializer();
        for (int i = 0,n = initializer.size(); i < n; i++) {
            final VariableInitializer variableInitializer = initializer.get(i);
            if (!(variableInitializer instanceof Literal)) {
                return false; // value is not a literal
            }
            Literal literal = (Literal) variableInitializer;
            if (!values.add(literal)) {
                return false; // literal is already in the set
            }
        }
        return true;
    }

    private CallingConvention chooseConvention(RexNode exp)
    {
        final SaffronType saffronType = exp.getType();
        OJClass clazz = OJUtil.typeToOJClass(saffronType);
        if (clazz.getComponentType() != null) {
            return CallingConvention.ARRAY;
        } else if (Util.clazzCollection.isAssignableFrom(clazz)) {
            return CallingConvention.COLLECTION;
        } else if (Util.clazzIterable.isAssignableFrom(clazz)) {
            return CallingConvention.ITERABLE; // preferable to ITERATOR
        } else if (Util.clazzVector.isAssignableFrom(clazz)) {
            return CallingConvention.VECTOR;
        } else if (Util.clazzIterator.isAssignableFrom(clazz)) {
            return CallingConvention.ITERATOR;
        } else if (Util.clazzEnumeration.isAssignableFrom(clazz)) {
            return CallingConvention.ENUMERATION;
        } else if (Util.clazzMap.isAssignableFrom(clazz)) {
            return CallingConvention.MAP;
        } else if (Util.clazzHashtable.isAssignableFrom(clazz)) {
            return CallingConvention.HASHTABLE;
        } else if (Util.clazzResultSet.isAssignableFrom(clazz)) {
            return CallingConvention.RESULT_SET;
        } else {
            throw Util.newInternal("bad type " + clazz);
        }
    }
}


// End ExpressionReaderRel.java
