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

package net.sf.saffron.oj.xlat;

import org.eigenbase.reltype.*;
import org.eigenbase.relopt.*;
import net.sf.saffron.trace.*;
import net.sf.saffron.oj.rel.ExpressionReaderRel;
import net.sf.saffron.oj.rel.ForTerminatorRel;
import org.eigenbase.oj.util.JavaRexBuilder;
import org.eigenbase.oj.util.OJUtil;
import org.eigenbase.relopt.CallingConvention;
import org.eigenbase.oj.rel.JavaRelImplementor;
import org.eigenbase.oj.rel.JavaRel;
import org.eigenbase.rel.*;
import org.eigenbase.rex.RexNode;
import org.eigenbase.util.Util;
import openjava.mop.Environment;
import openjava.mop.OJClass;
import openjava.mop.QueryEnvironment;
import openjava.mop.Toolbox;
import openjava.ptree.*;
import openjava.ptree.util.QueryExpander;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * <code>OJQueryExpander</code> passes over a parse tree, and converts
 * relational expressions into regular Java constructs.
 *
 * @author jhyde
 * @version $Id$
 *
 * @since 22 September, 2001
 */
public class OJQueryExpander extends QueryExpander
{
    private static final Logger tracer = SaffronTrace.getQueryExpanderTracer();

    //~ Instance fields -------------------------------------------------------

    private RelDataType rootRowType;
    private final RelOptConnection connection;

    //~ Constructors ----------------------------------------------------------

    public OJQueryExpander(Environment env, RelOptConnection connection)
    {
        super(env);
        this.connection = connection;
    }

    //~ Methods ---------------------------------------------------------------

    /**
     * @return the row Type for the root relational expression encountered
     *         during last expansion, or null if none
     */
    public RelDataType getRootRowType()
    {
        return rootRowType;
    }

    /**
     * Converts an expression to a relational expression according to a
     * particular calling convention, and optimizes it.
     *
     * @param exp expression to convert
     * @param eager whether to try to convert expressions which might not be
     *        relational, for example arrays
     * @param convention calling convention (see {@link
     *        org.eigenbase.relopt.CallingConvention})
     * @param variable iterator variable (for <code>for</code>-loop)
     * @param body statement list to add code to
     *
     * @return the optimal relational expression
     */
    public JavaRel convertExpToOptimizedRel(
        Expression exp,
        boolean eager,
        CallingConvention convention,
        Variable variable,
        StatementList body)
    {
        RelNode rel = convertExpToUnoptimizedRel(exp,eager,null,null);
        if (rel == null) {
            return null;
        }
        // Project out the one and only field.
        if (rel.getRowType().getFieldCount() == 1) {
            final RelDataTypeField field0 = rel.getRowType().getFields()[0];
            rel = new ProjectRel(rel.getCluster(),
                    rel,
                    new RexNode[] {rel.getCluster().rexBuilder.makeInputRef(
                            field0.getType(),0)},
                    new String[]{field0.getName()},
                    ProjectRel.Flags.None);
        } else {
            //assert false;
        }
        RelOptPlanner planner = rel.getCluster().getPlanner();
        planner.setRoot(rel);
        if (rel.getConvention() != convention) {
            RelNode previous = rel;
            try {
                rel = planner.changeConvention(rel,convention);
                assert(rel != null);
                planner.setRoot(rel);
            } catch (Throwable e) {
                throw Util.newInternal(
                    e,
                    "Error while converting relational expression ["
                    + previous + "] to calling convention [" + convention
                    + "]");
            }
            if (convention == CallingConvention.JAVA) {
                String label = Variable.generateUniqueVariable().toString();
                rel = new ForTerminatorRel(
                        rel.getCluster(),
                        rel,
                        variable,
                        body,
                        label);
            }
            tracer.log(Level.FINE,
                "Change convention: rel#" + previous.getId() + ":"
                + previous.getConvention() + " to rel#" + rel.getId()
                + ":" + rel.getConvention());
            assert(rel.getConvention() == convention);
        }
        planner = planner.chooseDelegate();
        RelNode bestExp = planner.findBestExp();
        assert(bestExp != null) : "could not implement exp";
        return (JavaRel) bestExp;
    }

    // implement QueryExpander
    protected Expression expandExpression(Expression exp)
    {
        while (true) {
            Expression exp0 = exp;
            exp = expandExpression_(exp0,null,CallingConvention.ARRAY);
            assert(exp != null);
            if (exp == exp0) {
                return exp;
            }
        }
    }

    /**
     * Converts an {@link Expression} of the form <code>for (<i>variable</i>
     * in <i>exp</i>) {<i>body</i>}</code> into a regular java {@link
     * openjava.ptree.Statement}.
     */

    // implement QueryExpander
    protected Statement expandForIn(
        Variable variable,
        Expression exp,
        StatementList body)
    {
        boolean eager = true;
        CallingConvention convention = CallingConvention.JAVA;
        JavaRel best = convertExpToOptimizedRel(
            exp,eager,convention,variable,body);

        // spit out java statement block
        JavaRelImplementor implementor = new JavaRelImplementor(best.getCluster().rexBuilder);
        Object o = implementor.implementRoot(best);
        return new Block((StatementList) o);
    }

    /**
     * Convert an expression to a relation, if it is relational, otherwise
     * return null.  This method only deals with relational expressions which
     * can occur outside a query; joins, in particular, are only dealt with
     * inside queries.
     *
     * @param exp expression to translate
     * @param eager whether to translate expressions which may or may not be
     *        (such as references to arrays) into relational expressions
     * @param queryInfo context to convert expressions into results of other
     *        queries; null if this expression is not inside a query
     * @param desiredRowType row type required; if this is null, leave the
     *        results as they are
     *
     * @return a relational expression, or null if the expression was not
     *         relational
     */
    public RelNode convertExpToUnoptimizedRel(
        Expression exp,
        boolean eager,
        QueryInfo queryInfo,
        OJClass desiredRowType)
    {
        if (exp instanceof QueryExpression) {
            QueryExpression queryExp = (QueryExpression) exp;

            // Yeah, if we're being called from QueryExpander.evaluateDown we
            // will already have pushed a QueryEnvironment but another can't do
            // any harm
            QueryEnvironment qenv =
                new QueryEnvironment(getEnvironment(),queryExp);
            QueryInfo newQueryInfo =
                new QueryInfo(queryInfo,qenv,this,queryExp);
            return newQueryInfo.convertQueryToRel(queryExp);
        }
        if (exp instanceof BinaryExpression) {
            BinaryExpression binaryExp = (BinaryExpression) exp;
            switch (binaryExp.getOperator()) {
            case BinaryExpression.UNION:
            case BinaryExpression.EXCEPT:
            case BinaryExpression.INTERSECT:
                Expression leftExp = binaryExp.getLeft();
                Expression rightExp = binaryExp.getRight();
                if (queryInfo == null) {
                    queryInfo =
                        new QueryInfo(queryInfo,getEnvironment(),this,exp);
                }
                RelNode left = convertExpToUnoptimizedRel(
                    leftExp,true,queryInfo,null);
                RelNode right = convertExpToUnoptimizedRel(
                    rightExp,true,queryInfo,null);
                RelOptCluster cluster =
                    QueryInfo.createCluster(queryInfo,getEnvironment());
                cluster.originalExpression = ((JavaRexBuilder)
                        cluster.rexBuilder).makeJava(cluster.env, exp);
                switch (binaryExp.getOperator()) {
                case BinaryExpression.UNION:
                    return new UnionRel(
                        cluster,
                        new RelNode [] { left,right },
                        false);
                case BinaryExpression.EXCEPT:
                    return new MinusRel(cluster,left,right);
                case BinaryExpression.INTERSECT:
                    return new IntersectRel(cluster,left,right);
                default:
                    throw Util.newInternal("bad case");
                }
            default:
                return null;
            }
        }

        // We only translate arrays, vectors, collections into relational
        // expressions if they occur with a select statement.
        if (!eager) {
            return null;
        }
tryit:
        if (exp instanceof MethodCall) {
            MethodCall call = (MethodCall) exp;
            String name = call.getName();
            if (!name.equals("contentsAsArray")) {
                break tryit;
            }
            final Expression refexpr = call.getReferenceExpr();
            if (refexpr == null) {
                break tryit;
            }
            ExpressionList args = call.getArguments();
            if (args.size() != 2) {
                break tryit;
            }
            Expression arg = args.get(0);
            if (!(arg instanceof Literal)) {
                break tryit;
            }
            Literal literal = (Literal) arg;
            String schemaName;
            if (literal.getLiteralType() == Literal.NULL) {
                schemaName = null;
            } else {
                if (literal.getLiteralType() != Literal.STRING) {
                    break tryit;
                }
                schemaName = Literal.stripString(literal.toString());
            }
            arg = args.get(1);
            if (!(arg instanceof Literal)) {
                break tryit;
            }
            literal = (Literal) arg;
            if (literal.getLiteralType() != Literal.STRING) {
                break tryit;
            }
            String tableName = Literal.stripString(literal.toString());
            final Environment env = getEnvironment();
            RelOptTable table =
                Toolbox.getTable(env,refexpr,schemaName,tableName);
            if (table == null) {
                break tryit;
            }
            RelNode rel = table.toRel(queryInfo.cluster, connection);
            if (rel == null) {
                return null;
            }
            queryInfo.leaves.add(rel);
            return rel;
        }

        OJClass clazz = Util.getType(getEnvironment(),exp);

        // An array cast is not meant literally -- it tells us what the row
        // type is.
        if (exp instanceof CastExpression) {
            CastExpression castExp = (CastExpression) exp;
            if (clazz.isArray() && (desiredRowType == null)) {
                desiredRowType = clazz.getComponentType();
                return convertExpToUnoptimizedRel(
                    castExp.getExpression(),
                    eager,
                    queryInfo,
                    desiredRowType);
            }
        }

        if (isRelational(clazz)) {
            RelOptCluster cluster;
            RexNode rexExp = null;
            if (queryInfo != null) {
                rexExp = queryInfo.convertExpToInternal(
                        exp,
                        new RelNode [] {  });
                cluster = queryInfo.cluster;
            } else {
                // there's no query current, but we still need a cluster
                cluster = QueryInfo.createCluster(queryInfo,getEnvironment());
                rexExp = ((JavaRexBuilder) cluster.rexBuilder).makeJava(
                        getEnvironment(), exp);
                cluster.originalExpression = rexExp;
            }
            final RelDataType rowType =
                    OJUtil.ojToType(cluster.typeFactory,desiredRowType);
            RelNode rel = new ExpressionReaderRel(cluster,rexExp,rowType);
            if (queryInfo != null) {
                queryInfo.leaves.add(rel);
            }
            return rel;
        }

        throw Toolbox.newInternal("exp is not relational: " + exp);
    }

    private Expression convertRelToExp(JavaRel rel)
    {
        // spit out java statement block
        JavaRelImplementor implementor =
                new JavaRelImplementor(rel.getCluster().rexBuilder);
        Object o = implementor.implementRoot(rel);
        return (Expression) o;
    }

    private Expression expandExists(Expression exp)
    {
        boolean eager = true;
        CallingConvention convention = CallingConvention.EXISTS;
        JavaRel rel = convertExpToOptimizedRel(
            exp,eager,convention,null,null);
        return convertRelToExp(rel);
    }

    /**
     * Converts an {@link Expression} into a java expression which returns an
     * array.  Examples:
     *
     * <ol>
     * <li>
     * Query as expression
     * <blockquote>
     * <pre>Emp[] femaleEmps = (select emp from emps as emp
     *                     where emp.gender.equals("F"));</pre>
     * </blockquote>
     * becomes
     * <blockquote>
     * <pre>Emp[] femaleEmps = new Object() {
     *         public Emp[] asArray() {
     *             Vector v = new Vector();
     *             for (i in (select emp from emps as emp
     *                        where emp.gender.equals("F"))) {
     *                 v.addElement(i);
     *             }
     *             Emp[] a = new Emp[v.size()];
     *             v.copyInto(a);
     *             return a;
     *         }
     *     }.asArray();</pre>
     * </blockquote>
     * </li>
     * </ol>
     *
     *
     * @param desiredRowType row type required; if this is null, leave the
     *        results as they are
     * @param convention calling convention of the result
     */
    private Expression expandExpression_(
        Expression exp,
        OJClass desiredRowType,
        CallingConvention convention)
    {
        if (exp instanceof CastExpression) {
            // see if this is something like "(Emp[]) vector" or "(Emp[])
            // connection.contentsAsArray("emps")" or "(Iterator) (select from
            // emps)"
            CastExpression castExp = (CastExpression) exp;
            Environment env = getEnvironment();
            OJClass clazz = Util.getType(env,castExp);
            boolean retainCast;
            if (clazz.isArray()) {
                if (desiredRowType == null) {
                    desiredRowType = clazz.getComponentType();
                }
            }
            if (clazz.isArray()) {
                convention = CallingConvention.ARRAY;
                retainCast = false;
            } else if (clazz.isAssignableFrom(Toolbox.clazzIterable)) {
                // we prefer ITERABLE to ITERATOR
                convention = CallingConvention.ITERABLE;
                retainCast = false;
            } else if (clazz.isAssignableFrom(Toolbox.clazzIterator)) {
                convention = CallingConvention.ITERATOR;
                retainCast = false;
            } else if (clazz.isAssignableFrom(Toolbox.clazzCollection)) {
                convention = CallingConvention.COLLECTION;
                retainCast = false;
            } else if (clazz.isAssignableFrom(Toolbox.clazzVector)) {
                convention = CallingConvention.VECTOR;
                retainCast = false;
            } else if (clazz.isAssignableFrom(Toolbox.clazzEnumeration)) {
                convention = CallingConvention.ENUMERATION;
                retainCast = false;
            } else if (clazz.isAssignableFrom(Toolbox.clazzResultSet)) {
                convention = CallingConvention.RESULT_SET;
                retainCast = false;
            } else {
                retainCast = true;
            }
            Expression expanded =
                expandExpression_(
                    castExp.getExpression(),
                    desiredRowType,
                    convention);
            if (retainCast) {
                expanded =
                    new CastExpression(castExp.getTypeSpecifier(),expanded);
            }
            return expanded;
        }

        boolean eager = false;
        JavaRel rel = convertExpToOptimizedRel(
            exp,eager,convention,null,null);
        if (exp instanceof QueryExpression) {
            assert(rel != null);
        }
        if (rel != null) {
            if (tracer.isLoggable(Level.FINE)) {
                final StringWriter sw = new StringWriter();
                final RelOptPlanWriter pw =
                    new RelOptPlanWriter(new PrintWriter(sw));
                rel.explain(pw);
                pw.flush();
                tracer.log(Level.FINE,
                        "Converted expression {0} into rel expression {1}",
                        new Object[] {exp, sw.toString()});
            }
            rootRowType = rel.getRowType();
            return convertRelToExp(rel);
        } else if (exp instanceof BinaryExpression) {
            BinaryExpression binaryExp = (BinaryExpression) exp;
            if (binaryExp.getOperator() == BinaryExpression.IN) {
                // "in" outside a query context, e.g. "if (1 in new int[]
                // {1,2,3})"
                return expandIn(binaryExp.getLeft(),binaryExp.getRight());
            }
        } else if (exp instanceof UnaryExpression) {
            UnaryExpression unaryExp = (UnaryExpression) exp;
            if (unaryExp.getOperator() == UnaryExpression.EXISTS) {
                // "exists" outside a query context, e.g. "if (exists (select
                // {} from emp))"
                return expandExists(unaryExp.getExpression());
            }
        }
        return exp;
    }

    /**
     * Translates an expression involving <code>in</code>.  For example,
     * <blockquote>
     * <pre>if ("Fred" in (select name from emps as emp)) {
     *     yes();
     * }</pre>
     * </blockquote>
     * becomes <code>in</code> within a query
     * <blockquote>
     * <pre>if (exists (
     *         select where "Joe" in (select name from emps as emp))) {
     *     yes();
     * }</pre>
     * </blockquote>
     */
    private Expression expandIn(Expression seekExp,Expression queryExp)
    {
        return expandExists(
            new QueryExpression(
                null,
                true,
                null,
                null,
                new BinaryExpression(seekExp,BinaryExpression.IN,queryExp),
                null));
    }

    /**
     * Returns whether an expression <code>clazz</code> can be read as a
     * relation.
     */
    private static boolean isRelational(OJClass clazz)
    {
        return (clazz.getComponentType() != null)
            || Util.clazzVector.isAssignableFrom(clazz)
            || Util.clazzCollection.isAssignableFrom(clazz)
            || Util.clazzIterable.isAssignableFrom(clazz)
            || Util.clazzIterator.isAssignableFrom(clazz)
            || Util.clazzEnumeration.isAssignableFrom(clazz)
            || Util.clazzMap.isAssignableFrom(clazz)
            || Util.clazzHashtable.isAssignableFrom(clazz)
            || Util.clazzResultSet.isAssignableFrom(clazz);
    }
}


// End OJQueryExpander.java
