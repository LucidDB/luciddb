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

import java.util.Collections;
import java.util.logging.Level;
import java.util.logging.Logger;

import net.sf.saffron.trace.SaffronTrace;

import openjava.mop.Environment;
import openjava.mop.OJClass;
import openjava.mop.Toolbox;
import openjava.ptree.*;
import openjava.ptree.util.ScopeHandler;

import org.eigenbase.rel.JoinRel;
import org.eigenbase.rel.ProjectRel;
import org.eigenbase.rel.RelNode;
import org.eigenbase.relopt.RelOptUtil;
import org.eigenbase.rex.RexNode;
import org.eigenbase.rex.RexUtil;
import org.eigenbase.oj.util.*;
import org.eigenbase.reltype.RelDataTypeFactory;


/**
 * Visitor which walks over an {@link ParseTree OpenJava parse tree} looking
 * for sub-queries. When it finds one, it throws a
 * {@link OJUtil.StopIterationException}.
 *
 * $Id$
 */
class SubqueryFinder extends ScopeHandler
{
    QueryInfo queryInfo;
    private static final Logger tracer = SaffronTrace.getQueryExpanderTracer();

    SubqueryFinder(
        QueryInfo queryInfo,
        Environment env)
    {
        super(env);
        this.queryInfo = queryInfo;
    }

    public void visit(BinaryExpression p)
        throws ParseTreeException
    {
        if (p.getOperator() == BinaryExpression.IN) {
            Expression left = p.getLeft();
            Expression right = p.getRight();
            tracer.log(Level.FINE,
                "SubqueryFinder: found IN: left=[" + left + "], right=["
                + right + "]");
            RelNode oldFrom = queryInfo.getRoot();
            RexNode rexLeft = queryInfo.convertExpToInternal(left);
            left = new RexExpression(rexLeft);
            RelNode rightRel = queryInfo.convertFromExpToRel(right);
            OJClass rightRowType = Toolbox.getRowType(queryInfo.env, right);
            boolean wrap = rightRowType.isPrimitive();
            final RelDataTypeFactory typeFactory = queryInfo.cluster.getTypeFactory();
            if (wrap) {
                final RexNode[] exprs = {
                    queryInfo.rexBuilder.makeJava(
                        getEnvironment(),
                        OJUtil.box(
                            rightRowType,
                            RelOptUtil.makeReference(0)))
                };
                rightRel =
                    new ProjectRel(
                        queryInfo.cluster,
                        rightRel,
                        exprs,
                        RexUtil.createStructType(
                            typeFactory, exprs),
                        ProjectRel.Flags.None);
            }

            boolean isNullable = false;
            Expression v; // variable with which to refer to the value
            Expression condition;
            if (!isNullable) {
                v = RelOptUtil.makeReference(1);
                condition =
                    new BinaryExpression(
                        RelOptUtil.makeReference(0),
                        BinaryExpression.NOTEQUAL,
                        Literal.constantNull());
            } else {
                // The value may be null, so we have to wrap it as {value,
                // true} and outer join to that.
                final RexNode[] exprs = {
                    queryInfo.rexBuilder.makeRangeReference(
                        rightRel.getRowType(),
                        0),
                    queryInfo.rexBuilder.makeLiteral(true)
                };
                final String[] fieldNames = { "v", "b" };
                rightRel =
                    new ProjectRel(
                        queryInfo.cluster,
                        rightRel,
                        exprs,
                        RexUtil.createStructType(typeFactory, exprs, fieldNames),
                        ProjectRel.Flags.Boxed);
                v = RelOptUtil.makeFieldAccess(1, 0);
                condition =
                    new FieldAccess(
                        RelOptUtil.makeFieldAccess(0, 1),
                        OJSyntheticClass.makeField(1));
            }
            RelNode rightDistinct =
                RelOptUtil.createDistinctRel(rightRel);
            queryInfo.leaves.add(rightDistinct);

            // Join the sub-query from the 'in' to the tree built up from
            // the 'from' clause
            JoinRel join =
                new JoinRel(queryInfo.cluster, oldFrom, rightDistinct,
                    queryInfo.rexBuilder.makeJava(
                        getEnvironment(),
                        Toolbox.makeEquals(
                            left,
                            Toolbox.castObject(
                                v,
                                rightRowType.primitiveWrapper(),
                                rightRowType),
                            rightRowType,
                            true)), JoinRel.JoinType.LEFT,
                    Collections.EMPTY_SET);
            queryInfo.setRoot(join);

            // Replace the 'in' condition with '$input0.$f1.$f1'
            p.replace(condition);

            // signal that we want to stop iteration (not really an error)
            throw new OJUtil.StopIterationException();
        } else {
            super.visit(p);
        }
    }

    /**
     * Replaces an <code>exists</code> expression. For example,
     * <blockquote>
     * <pre>select from dept
     * where dept.state.equals("CA") && <b>exists (
     *  select from emp where emp.deptno == dept.deptno)</b></pre>
     * </blockquote>
     * becomes
     * <blockquote>
     * <pre>select from dept <b>left join (
     *  select distinct true from (
     *     select from emp where emp.deptno == dept.deptno)) as test</b>
     * where dept.state.equals("CA") && <b>test</b></pre>
     * </blockquote>
     */
    public void visit(UnaryExpression p)
        throws ParseTreeException
    {
        if (p.getOperator() == UnaryExpression.EXISTS) {
            Expression right = p.getExpression();
            tracer.log(Level.FINE,
                "SubqueryFinder: found EXISTS: expr=[" + right + "]");
            RelNode oldFrom = queryInfo.getRoot();
            RelNode rightRel = queryInfo.convertFromExpToRel(right);
            final RexNode[] exprs = { queryInfo.rexBuilder.makeLiteral(true) };
            RelNode rightProject =
                new ProjectRel(queryInfo.cluster, rightRel,
                    exprs,
                    RexUtil.createStructType(
                        queryInfo.cluster.getTypeFactory(), exprs),
                    ProjectRel.Flags.None);
            RelNode rightDistinct =
                RelOptUtil.createDistinctRel(rightProject);
            queryInfo.leaves.add(rightDistinct);
            JoinRel join =
                new JoinRel(queryInfo.cluster, oldFrom, rightDistinct,
                    queryInfo.rexBuilder.makeLiteral(true),
                    JoinRel.JoinType.LEFT, Collections.EMPTY_SET);
            queryInfo.setRoot(join);

            // Replace the 'exists' with a test as to whether the single
            // boolean column of the indicator relation is true.
            Expression condition = RelOptUtil.makeFieldAccess(0, 1);
            p.replace(condition);

            // signal that we want to stop iteration (not really an error)
            throw new OJUtil.StopIterationException();
        } else {
            super.visit(p);
        }
    }

    /**
     * A {@link RexNode Row-expression} wrapped to look like a
     * {@link Expression Openjava expression}.
     */
    private static class RexExpression extends Literal
    {
        private final RexNode rexNode;

        public RexExpression(RexNode rexNode)
        {
            super(Literal.STRING, "isParent");
            this.rexNode = rexNode;
        }
    }
}


// End SubqueryFinder.java
