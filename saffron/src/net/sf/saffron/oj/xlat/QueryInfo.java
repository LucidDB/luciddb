/*
// Saffron preprocessor and data engine.
// Copyright (C) 2002-2004 Disruptive Tech
//
// This program is free software; you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation; either version 2 of the License, or
// (at your option) any later version.
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

package net.sf.saffron.oj.xlat;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.logging.Level;
import java.util.logging.Logger;

import net.sf.saffron.oj.*;
import net.sf.saffron.oj.rel.ExpressionReaderRel;
import net.sf.saffron.trace.SaffronTrace;

import openjava.mop.Environment;
import openjava.mop.OJClass;
import openjava.mop.QueryEnvironment;
import openjava.mop.Toolbox;
import openjava.ptree.*;
import openjava.tools.parser.ParserConstants;

import org.eigenbase.oj.*;
import org.eigenbase.oj.util.JavaRexBuilder;
import org.eigenbase.oj.util.OJUtil;
import org.eigenbase.rel.*;
import org.eigenbase.relopt.RelOptCluster;
import org.eigenbase.relopt.RelOptQuery;
import org.eigenbase.relopt.RelOptUtil;
import org.eigenbase.reltype.RelDataType;
import org.eigenbase.reltype.RelDataTypeFactory;
import org.eigenbase.reltype.RelDataTypeFactoryImpl;
import org.eigenbase.rex.RexNode;
import org.eigenbase.util.Util;

/**
 * A <code>QueryInfo</code> holds all the information about a {@link
 * QueryExpression} while it is being translated into a collection of {@link
 * org.eigenbase.rel.RelNode}s.
 *
 * <p>
 * Those <code>RelNode</code>s will all belong to the same {@link
 * RelOptCluster}, but <code>RelOptCluster</code> is much simpler.  Please
 * put the stuff which is only needed at translation time into
 * <code>QueryInfo</code>, and keep <code>RelOptCluster</code> simple.
 * </p>
 */
class QueryInfo
{
    ArrayList leaves = new ArrayList();
    Environment env;
    OJQueryExpander expander;
    QueryInfo parent;
    RelOptCluster cluster;
    private RelNode root;
    final JavaRexBuilder rexBuilder;
    private static final Logger tracer = SaffronTrace.getQueryExpanderTracer();

    /**
     * Creates a <code>QueryInfo</code>
     *
     * @param env environment of the query that this expression was contained
     *        in - gives us access to the from list of that query
     */
    QueryInfo(
        QueryInfo parent,
        Environment env,
        OJQueryExpander expander,
        Expression exp)
    {
        this.parent = parent;
        this.env = env;
        this.expander = expander;
        this.cluster = createCluster(
                parent,
                env.getParent());
        this.rexBuilder = (JavaRexBuilder) cluster.rexBuilder;
        if (exp != null) {
            cluster.originalExpression = rexBuilder.makeJava(env, exp);
        }
    }

    /**
     * Translates an expression into one which references only internal
     * variables.  Clones the expression first.
     *
     * @param exp expression to translate
     *
     * @return converted expression
     */
    public RexNode convertExpToInternal(Expression exp)
    {
        return convertExpToInternal(
            exp,
            new RelNode [] { getRoot() });
    }

    public RexNode convertExpToInternal(
        Expression exp,
        RelNode [] inputs)
    {
        InternalTranslator translator =
            new InternalTranslator(this, inputs, rexBuilder);
        return translator.go(exp);
    }

    /**
     * Translates an aggregate expression into one which references only
     * variables. Expressions inside aggregate functions are added to
     * <code>aggList</code>, if they are not already present.
     *
     * @param exp expression to translate
     * @param groups expressions from the <code>group by</code> clause
     * @param aggInputList expressions inside aggregations; expressions are
     *        added to this list if an aggregate needs one and it is not
     *        present
     * @param aggCallVector calls to aggregates; {@link
     *        org.eigenbase.rel.AggregateRel.Call}s are added to this list if they
     *        are needed but are not present
     */
    public RexNode convertGroupExpToInternal(
        Expression exp,
        Expression [] groups,
        ArrayList aggInputList,
        ArrayList aggCallVector)
    {
        AggInternalTranslator translator =
            new AggInternalTranslator(this, new RelNode [] { getRoot() },
                groups, aggInputList, aggCallVector, rexBuilder);
        RexNode translated;
        try {
            translated = translator.go(exp);
        } catch (NotAGroupException e) {
            Util.discard(e);
            throw Util.newInternal("Not a group-by expression: " + exp);
        }
        return translator.unpickle(translated);
    }

    static RelOptCluster createCluster(
        QueryInfo queryInfo,
        Environment env)
    {
        RelOptQuery query;
        if (queryInfo == null) {
            query = new RelOptQuery(
                OJPlannerFactory.threadInstance().newPlanner());
        } else {
            query = queryInfo.cluster.query;
        }
        final RelDataTypeFactory typeFactory =
            OJUtil.threadTypeFactory();
        return query.createCluster(
            env,
            typeFactory,
            new JavaRexBuilder(typeFactory));
    }

    void setRoot(RelNode root)
    {
        this.root = root;
    }

    RelNode getRoot()
    {
        return root;
    }

    /**
     * Converts an expression into a relational expression.  For example, an
     * array becomes an {@link ExpressionReaderRel}.  Leaf nodes are
     * registered in {@link #leaves}.
     *
     * @param exp the expression to convert
     *
     * @return the equivalent relational expression
     */
    RelNode convertFromExpToRel(Expression exp)
    {
        if (exp == null) {
            RelNode rel = new OneRowRel(cluster);
            leaves.add(rel);
            return rel;
        } else if (exp instanceof JoinExpression) {
            JoinExpression joinExp = (JoinExpression) exp;
            Expression leftExp = joinExp.getLeft();
            Expression rightExp = joinExp.getRight();
            Expression conditionExp = joinExp.getCondition();
            int saffronJoinType = joinExp.getJoinType();
            int joinType = convertJoinType(saffronJoinType);
            RelNode left = convertFromExpToRel(leftExp);
            RelNode right = convertFromExpToRel(rightExp);

            // Deal with any forward-references.
            if (!cluster.query.mapDeferredToCorrel.isEmpty()) {
                Iterator lookups =
                    cluster.query.mapDeferredToCorrel.keySet().iterator();
                while (lookups.hasNext()) {
                    DeferredLookup lookup = (DeferredLookup) lookups.next();
                    String correlName =
                        (String) cluster.query.mapDeferredToCorrel.get(lookup);

                    // as a side-effect, this associates correlName with rel
                    LookupResult lookupResult =
                        lookup.lookup(
                            new RelNode [] { left, right },
                            correlName);
                    assert (lookupResult != null);
                }
                cluster.query.mapDeferredToCorrel.clear();
            }

            // Make sure that left does not depend upon a correlating variable
            // coming from right. We'll swap them before we create a
            // JavaNestedLoopJoin.
            String [] variablesL2R =
                RelOptUtil.getVariablesSetAndUsed(right, left);
            String [] variablesR2L =
                RelOptUtil.getVariablesSetAndUsed(left, right);
            if ((variablesL2R.length > 0) && (variablesR2L.length > 0)) {
                throw Util.newInternal(
                    "joined expressions must not be mutually dependent: "
                    + exp);
            }
            HashSet variablesStopped = new HashSet();
            for (int i = 0; i < variablesL2R.length; i++) {
                variablesStopped.add(variablesL2R[i]);
            }
            for (int i = 0; i < variablesR2L.length; i++) {
                variablesStopped.add(variablesR2L[i]);
            }
            RexNode conditionExp1 =
                convertExpToInternal(
                    conditionExp,
                    new RelNode [] { left, right });
            return new JoinRel(cluster, left, right, conditionExp1, joinType,
                variablesStopped);
        } else if (exp instanceof AliasedExpression) {
            AliasedExpression aliasedExp = (AliasedExpression) exp;
            return convertFromExpToRel(aliasedExp.getExpression());
        } else if (exp instanceof QueryExpression) {
            QueryExpression queryExp = (QueryExpression) exp;
            QueryEnvironment qenv = new QueryEnvironment(env, queryExp);
            QueryInfo newQueryInfo = new QueryInfo(this, qenv, expander, exp);
            RelNode rel = newQueryInfo.convertQueryToRel(queryExp);
            leaves.add(rel);
            return rel;
        } else {
            // finally, look for relational expressions which can occur
            // anywhere, and fail if we're not looking at one
            return expander.convertExpToUnoptimizedRel(exp, true, this, null);
        }
    }

    private static int convertJoinType(int saffronJoinType)
    {
        switch (saffronJoinType) {
        case ParserConstants.INNER:
            return JoinRel.JoinType.INNER;
        case ParserConstants.LEFT:
            return JoinRel.JoinType.LEFT;
        case ParserConstants.RIGHT:
            return JoinRel.JoinType.RIGHT;
        case ParserConstants.FULL:
            return JoinRel.JoinType.FULL;
        default:
            throw Util.newInternal("Unknown join type " + saffronJoinType);
        }
    }

    /**
     * Converts a {@link QueryExpression} into a {@link RelNode}. Capture
     * occurs when a query is converted into relational expressions. The
     * scalar expressions in the query reference (a) rows from their own
     * query, (b) rows from an enclosing query, (c) variables from the
     * enclosing environment. We have already dealt with (a), and we can
     * leave environmental references (c) as they are.  So, we deal with
     * references to rows in this query now.  References to queries inside or
     * outside this query will happen in due course.
     */
    RelNode convertQueryToRel(QueryExpression queryExp)
    {
        tracer.log(Level.FINE,
            "convertQueryToRel: queryExp=[" + queryExp + "] recurse [");

        Expression fromList = queryExp.getFrom();
        setRoot(convertFromExpToRel(fromList));

        // Examples in the following refer to the query
        //   select 1 + sum(x + 2)
        //   group by y
        //   from t
        //   where min(z) > 4
        ExpressionList groupList = queryExp.getGroupList();
        if (groupList != null) {
            // "aggInputList" is the input expressions to the aggregation:
            // group expressions first, then expressions within aggregate
            // functions, for example {y, x + 2, z}.  "preGroups" and
            // "postGroups" are the group expressions before and after
            // translation.
            ArrayList aggInputList = new ArrayList();
            Expression [] preGroups = Util.toArray(groupList);
            RexNode [] postGroups = new RexNode[preGroups.length];
            for (int i = 0; i < preGroups.length; i++) {
                Expression preGroup = preGroups[i];
                RexNode postGroup = convertExpToInternal(preGroup);
                aggInputList.add(postGroup);
                postGroups[i] = postGroup;
            }
            Expression whereClause = queryExp.getWhere();

            // "aggCallVector" is the aggregate expressions, for example
            // {sum(#1), min(#2)}.
            ArrayList aggCallVector = new ArrayList();
            RexNode rexWhereClause = null;
            if (whereClause != null) {
                rexWhereClause =
                    convertGroupExpToInternal(whereClause, preGroups,
                        aggInputList, aggCallVector);
                whereClause = removeSubqueries(whereClause);
            }
            Expression [] selects = Util.toArray(queryExp.getSelectList());
            String [] aliases = new String[selects.length];
            RexNode [] rexSelects = new RexNode[selects.length];
            for (int i = 0; i < selects.length; i++) {
                aliases[i] = Util.getAlias(selects[i]);
                rexSelects[i] =
                    convertGroupExpToInternal(selects[i], preGroups,
                        aggInputList, aggCallVector);
            }
            AggregateRel.Call [] aggCalls =
                (AggregateRel.Call []) aggCallVector.toArray(
                    new AggregateRel.Call[aggCallVector.size()]);
            RexNode [] aggInputs =
                (RexNode []) aggInputList.toArray(
                    new RexNode[aggInputList.size()]);
            setRoot(
                new ProjectRel(
                    cluster,
                    getRoot(),
                    aggInputs,
                    null,
                    ProjectRel.Flags.Boxed));
            setRoot(
                new AggregateRel(
                    cluster,
                    getRoot(),
                    preGroups.length,
                    aggCalls));
            if (whereClause != null) {
                setRoot(new FilterRel(
                        cluster,
                        getRoot(),
                        rexWhereClause));
            }
            setRoot(
                new ProjectRel(
                    cluster,
                    getRoot(),
                    rexSelects,
                    aliases,
                    queryExp.isBoxed() ? ProjectRel.Flags.Boxed : 0));

            ExpressionList sortList = queryExp.getSort();
            Expression [] sorts = Util.toArray(sortList);
            if ((sorts != null) && (sorts.length > 0)) {
                throw Util.newInternal("sort not implemented");
            }
            RelDataType relRowType = getRoot().getRowType();
            OJClass queryRowClass = queryExp.getRowType(env);
            final RelDataType queryRowType =
                OJUtil.ojToType(
                    cluster.getTypeFactory(),
                    queryRowClass);
            tracer.log(Level.FINE,
                "] return [" + getRoot() + "] rowType=[" + relRowType + "]");
            assert (relRowType == queryRowType);
            return getRoot();
        }

        Expression whereClause = queryExp.getWhere();
        if (whereClause != null) {
            RexNode rexWhereClause = convertExpToInternal(whereClause);
            whereClause = removeSubqueries(whereClause);
            setRoot(new FilterRel(
                    cluster,
                    getRoot(),
                    rexWhereClause));
        }

        Expression [] selects = Util.toArray(queryExp.getSelectList());
        String [] aliases = new String[selects.length];
        RexNode [] rexSelects = new RexNode[selects.length];
        for (int i = 0; i < selects.length; i++) {
            aliases[i] = Util.getAlias(selects[i]);
            rexSelects[i] = convertExpToInternal(selects[i]);
        }
        setRoot(
            new ProjectRel(
                cluster,
                getRoot(),
                rexSelects,
                aliases,
                ProjectRel.Flags.Boxed));

        ExpressionList sortList = queryExp.getSort();
        Expression [] sorts = Util.toArray(sortList);
        if ((sorts != null) && (sorts.length > 0)) {
            throw Util.newInternal("sort not implemented");

            //          Parameter parameter = new Parameter("p", rel.getRowType(), null);
            //              ExpReplacer replacer = new OrdinalRef.Replacer(0, parameter);
            //              Sort[] compiledSorts = new Sort[sorts.length];
            //              for (int i = 0; i < sorts.length; i++) {
            //                  compiledSorts[i] = new Sort(
            //                      (Exp) replacer.go(sorts[i].safeClone()),
            //                      sorts[i].ascending);
            //              }
            //              rel = new Sort(env, rel, sorts, parameter);
        }
        RelDataType relRowType = getRoot().getRowType();
        tracer.log(Level.FINE,
            "] return [" + getRoot() + "] rowType=[" + relRowType + "]");
        RelDataType fieldType = relRowType;

        /*
        if (relRowType.getFieldCount() == 1) {
            fieldType = relRowType.getFields()[0].getType();
        } else if (relRowType.getFieldCount() == 0) {
            fieldType = relRowType; // ?why
        } else {
            throw Util.newInternal(
                    "rel row type (" + relRowType +
                    ") should be a record with 1 field");
        }
        */
        final OJClass queryRowClass = queryExp.getRowType(env);
        RelDataType queryRowType =
            OJUtil.ojToType(
                cluster.getTypeFactory(),
                queryRowClass);
        if (fieldType != queryRowType) {
            throw Util.newInternal("rel row type (" + fieldType
                + ") should equal the " + "row type (" + queryRowType
                + ") of the query it was " + "translated from");
        }
        return getRoot();
    }

    /**
     * Returns the number of columns in this input.
     */
    int countColumns(RelNode rel)
    {
        return rel.getRowType().getFieldCount();
    }

    /**
     * Creates an expression with which to reference <code>expression</code>,
     * whose offset in its from-list is <code>offset</code>.
     */
    LookupResult lookup(
        int offset,
        RelNode [] inputs,
        boolean isParent,
        String varName)
    {
        final ArrayList relList = flatten(inputs);
        if ((offset < 0) || (offset >= relList.size())) {
            throw Util.newInternal("could not find input");
        }
        int fieldOffset = 0;
        for (int i = 0; i < offset; i++) {
            final RelNode rel = (RelNode) relList.get(i);
            fieldOffset += rel.getRowType().getFieldCount();
        }
        RelNode rel = (RelNode) relList.get(offset);
        if (isParent) {
            if (varName == null) {
                varName = rel.getOrCreateCorrelVariable();
            } else {
                // we are resolving a forward reference
                rel.registerCorrelVariable(varName);
            }
            return new CorrelLookupResult(varName);
        } else {
            return new LocalLookupResult(
                fieldOffset,
                rel.getRowType());
        }
    }

    /**
     * Goes through an expression looking for sub-queries (<code>in</code> or
     * <code>exists</code>). If it finds one, it joins the query to the from
     * clause, replaces the condition, and returns true. Examples:
     *
     * <ul>
     * <li>
     * <code>exists</code> becomes a reference to a indicator query:
     * <blockquote>
     * <pre>select from dept
     * where dept.location.equals("SF") && exists (
     *   select from emp
     *   where emp.deptno == dept.deptno && emp.gender.equals("F"))</pre>
     * </blockquote>
     * becomes
     * <blockquote>
     * <pre>select from dept cross join (
     *   select distinct from emp
     *   where emp.deptno == dept.deptno && emp.gender.equals("F"))
     * where dept.location.equals("SF") && indicator != null</pre>
     * </blockquote>
     *
     * <p>
     * The reference to 'dept' from within a join query is illegal -- but we
     * haven't de-correlated yet.
     * </p>
     * </li>
     * <li>
     * <code>in</code> becomes a test to see whether an outer-joined query met
     * the join condition:
     * <blockquote>
     * <pre>select from emp
     * where emp.deptno in (
     *   select dept.deptno from dept where dept.location.equals("SF"))
     * && emp.gender.equals("F")</pre>
     * </blockquote>
     * becomes
     * <blockquote>
     * <pre>select from emp <i>left</i> join (
     *   select dept.deptno from dept where dept.location.equals("SF")) q1
     * on emp.deptno == q1.deptno
     * where <i>q1 is not null &&</i> emp.gender.equals("F")</pre>
     * </blockquote>
     * Optimization #1: If the <code>in</code> condition is
     * <code>&amp;&amp;</code>ed into the where clause, then make the join
     * into a full join, and omit the condition.  Hence,
     * <blockquote>
     * <pre>select from emp join (
     *   select dept.deptno from dept where dept.location.equals("SF")) q1
     * on emp.deptno == q1.deptno
     * where emp.gender.equals("F")</pre>
     * </blockquote>
     * </li>
     * </ul>
     *
     *
     * @param exp Expression
     *
     * @return expression with subqueries removed
     */
    Expression removeSubqueries(Expression exp)
    {
        while (true) {
            RelNode oldFrom = getRoot();
            SubqueryFinder subqueryFinder = new SubqueryFinder(this, env);
            Expression oldExp = exp;
            exp = OJUtil.go(subqueryFinder, oldExp);
            if (oldFrom == getRoot()) {
                return exp;
            }
        }
    }

    private ArrayList flatten(RelNode [] rels)
    {
        ArrayList list = new ArrayList();
        flatten(rels, list);
        return list;
    }

    private void flatten(
        RelNode [] rels,
        ArrayList list)
    {
        for (int i = 0; i < rels.length; i++) {
            RelNode rel = rels[i];
            if (leaves.contains(rel)) {
                list.add(rel);
            } else {
                flatten(
                    rel.getInputs(),
                    list);
            }
        }
    }

    static abstract class LookupResult
    {
    }

    static class LocalLookupResult extends LookupResult
    {
        /** The offset of the field in the input relation which corresponds to
         * the first field in the relation we were seeking. */
        final int offset;

        /** The record type of the relation we were seeking. */
        final RelDataType rowType;

        LocalLookupResult(
            int offset,
            RelDataType rowType)
        {
            this.offset = offset;
            this.rowType = rowType;
        }
    }

    static class CorrelLookupResult extends LookupResult
    {
        final String varName;

        CorrelLookupResult(String varName)
        {
            super();
            this.varName = varName;
        }
    }
}


/**
 * Contains the information necessary to repeat a call to {@link
 * QueryInfo#lookup(int,RelNode[],boolean,String)}.
 */
class DeferredLookup
{
    QueryInfo queryInfo;
    boolean isParent;
    int offset;

    DeferredLookup(
        QueryInfo queryInfo,
        int offset,
        boolean isParent)
    {
        this.queryInfo = queryInfo;
        this.offset = offset;
        this.isParent = isParent;
    }

    QueryInfo.LookupResult lookup(
        RelNode [] inputs,
        String varName)
    {
        return queryInfo.lookup(offset, inputs, isParent, varName);
    }
}


// End QueryInfo.java
