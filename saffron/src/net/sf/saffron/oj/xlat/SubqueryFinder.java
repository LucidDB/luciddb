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

import net.sf.saffron.opt.OptUtil;
import net.sf.saffron.rel.DistinctRel;
import net.sf.saffron.rel.JoinRel;
import net.sf.saffron.rel.ProjectRel;
import net.sf.saffron.rel.SaffronRel;
import net.sf.saffron.rex.RexNode;
import openjava.mop.Environment;
import openjava.mop.OJClass;
import openjava.mop.Toolbox;
import openjava.ptree.*;
import openjava.ptree.util.ScopeHandler;
import openjava.ptree.util.SyntheticClass;
import openjava.tools.DebugOut;

import java.util.Collections;


class SubqueryFinder extends ScopeHandler
{
    //~ Instance fields -------------------------------------------------------

    QueryInfo queryInfo;

    //~ Constructors ----------------------------------------------------------

    SubqueryFinder(QueryInfo queryInfo, Environment env)
    {
        super(env);
        this.queryInfo = queryInfo;
    }

    //~ Methods ---------------------------------------------------------------

    public void visit(BinaryExpression p) throws ParseTreeException
    {
        if (p.getOperator() == BinaryExpression.IN) {
            Expression left = p.getLeft();
            Expression right = p.getRight();
            DebugOut.println(
                "SubqueryFinder: found IN: left=[" + left + "], right=["
                + right + "]");
            SaffronRel oldFrom = queryInfo.getRoot();
            RexNode rexLeft = queryInfo.convertExpToInternal(left);
            left = new RexExpression(rexLeft);
            SaffronRel rightRel = queryInfo.convertFromExpToRel(right);
            OJClass rightRowType = Toolbox.getRowType(queryInfo.env,right);
            boolean wrap = rightRowType.isPrimitive();
            if (wrap) {
                rightRel =
                    new ProjectRel(
                        queryInfo.cluster,
                        rightRel,
                        new RexNode [] {
                            queryInfo.rexBuilder.makeJava(
                                    getEnvironment(),
                                    Toolbox.box(rightRowType,OptUtil.makeReference(0)))
                        },
                        new String [] { null },
                        ProjectRel.Flags.None);
            }

            boolean isNullable = false;
            Expression v; // variable with which to refer to the value
            Expression condition;
            if (!isNullable) {
                v = OptUtil.makeReference(1);
                condition =
                    new BinaryExpression(
                        OptUtil.makeReference(0),
                        BinaryExpression.NOTEQUAL,
                        Literal.constantNull());
            } else {
                // The value may be null, so we have to wrap it as {value,
                // true} and outer join to that.
                rightRel =
                    new ProjectRel(
                        queryInfo.cluster,
                        rightRel,
                        new RexNode [] {
                            queryInfo.rexBuilder.makeRangeReference(rightRel.getRowType(), 0),
                            queryInfo.rexBuilder.makeLiteral(true)
                        },
                        new String [] { "v","b" },
                        ProjectRel.Flags.Boxed);
                v = OptUtil.makeFieldAccess(1,0);
                condition =
                    new FieldAccess(
                        OptUtil.makeFieldAccess(0,1),
                        SyntheticClass.makeField(1));
            }
            SaffronRel rightDistinct =
                new DistinctRel(queryInfo.cluster,rightRel);
            queryInfo.leaves.add(rightDistinct);

            // Join the sub-query from the 'in' to the tree built up from
            // the 'from' clause
            JoinRel join =
                    new JoinRel(
                            queryInfo.cluster,
                            oldFrom,
                            rightDistinct,
                            queryInfo.rexBuilder.makeJava(
                                    getEnvironment(),
                                    Toolbox.makeEquals(
                                            left,
                                            Toolbox.castObject(
                                                    v,
                                                    rightRowType.primitiveWrapper(),
                                                    rightRowType),
                                            rightRowType,
                                            true)),
                            JoinRel.JoinType.LEFT,
                            Collections.EMPTY_SET);
            queryInfo.setRoot(join);

            // Replace the 'in' condition with '$input0.$f1.$f1'
            p.replace(condition);

            // signal that we want to stop iteration (not really an error)
            throw new Toolbox.StopIterationException();
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
     * 	select from emp where emp.deptno == dept.deptno)) as test</b>
     * where dept.state.equals("CA") && <b>test</b></pre>
     * </blockquote>
     */
    public void visit(UnaryExpression p) throws ParseTreeException
    {
        if (p.getOperator() == UnaryExpression.EXISTS) {
            Expression right = p.getExpression();
            DebugOut.println(
                "SubqueryFinder: found EXISTS: expr=[" + right + "]");
            SaffronRel oldFrom = queryInfo.getRoot();
            SaffronRel rightRel = queryInfo.convertFromExpToRel(right);
            SaffronRel rightProject =
                new ProjectRel(
                    queryInfo.cluster,
                    rightRel,
                    new RexNode[] { queryInfo.rexBuilder.makeLiteral(true) },
                    null,
                    ProjectRel.Flags.None);
            SaffronRel rightDistinct =
                new DistinctRel(queryInfo.cluster,rightProject);
            queryInfo.leaves.add(rightDistinct);
            JoinRel join =
                new JoinRel(
                    queryInfo.cluster,
                    oldFrom,
                    rightDistinct,
                        queryInfo.rexBuilder.makeLiteral(true),
                    JoinRel.JoinType.LEFT,
                    Collections.EMPTY_SET);
            queryInfo.setRoot(join);

            // Replace the 'exists' with a test as to whether the single
            // boolean column of the indicator relation is true.
            Expression condition = OptUtil.makeFieldAccess(0,1);
            p.replace(condition);

            // signal that we want to stop iteration (not really an error)
            throw new Toolbox.StopIterationException();
        } else {
            super.visit(p);
        }
    }

    /**
     * A {@link RexNode Row-expression} wrapped to look like a
     * {@link Expression Openjava expression}.
     */
    private static class RexExpression extends Literal {
        private final RexNode rexNode;

        public RexExpression(RexNode rexNode) {
            super(Literal.STRING, "isParent");
            this.rexNode = rexNode;
        }
    }
}


// End SubqueryFinder.java
