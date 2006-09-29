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

package net.sf.saffron.oj.rel;

import openjava.mop.OJClass;
import openjava.mop.OJField;
import openjava.mop.Toolbox;
import openjava.ptree.*;

import org.eigenbase.oj.rel.*;
import org.eigenbase.oj.util.OJUtil;
import org.eigenbase.rel.*;
import org.eigenbase.relopt.CallingConvention;
import org.eigenbase.relopt.RelOptCluster;
import org.eigenbase.relopt.RelOptCost;
import org.eigenbase.relopt.RelOptPlanner;
import org.eigenbase.relopt.RelTraitSet;
import org.eigenbase.util.Util;


/**
 * <code>JavaAggregateRel</code> implements the {@link AggregateRel} relational
 * operator by generating code. The code looks like this:
 *
 * <blockquote>
 * <pre>HashMap h = new HashMap();
 * <i>for each child</i>
 *   HashableArray groups = new HashableArray(
 *     new Object[] {
 *        <i>child</i>.field4,
 *        <i>child</i>.field2});
 *   Object[] aggs = h.get(groups);
 *   if (aggs == null) {
 *     aggs = new Object[<<aggCount>>];
 *     aggs[0] = <i>agg start code</i>;
 *     aggs[1] = <i>agg start code</i>;
 *     h.put(groups, aggs);
 *   }
 *   <i>agg inc code {aggs[0], childRow}<i/>
 *   <i>agg inc code {aggs[1], childRow}<i/>
 * <i>end for</i>
 * Iterator keys = h.keys();
 * while (keys.hasNext()) {
 *      T row = new T();
 *      HashableArray groups = (HashableArray) keys.next();
 *      row.c0 = groups[0];
 *      row.c1 = groups[1];
 *      Object[] aggs = h.get(groups);
 *      row.c2 = <i>agg result code {aggs[0]}</i>
 *      row.c3 = <i>agg result code {aggs[1]}</i>
 *   <i>emit row to parent</i>
 * }</pre>
 * </blockquote>
 * Simplifications:
 *
 * <ul>
 * <li>
 * If there is onle one group column, the <code>HashableArray</code> is
 * simplified to <code>Object</code>.
 * </li>
 * <li>
 * If there is only one aggregate, the <code>Object[]</code> are simplified to
 * <code>Object</code>.
 * </li>
 * <li>
 * todo: Write an efficient implementation if there are no group columns.
 * </li>
 * <li>
 * If there are no aggregations, {@link JavaDistinctRel} is more efficient.
 * </li>
 * </ul>
 */
public class JavaAggregateRel extends AggregateRelBase implements JavaLoopRel
{
    Variable var_h;

    public JavaAggregateRel(
        RelOptCluster cluster,
        RelNode child,
        int groupCount,
        Call [] aggCalls)
    {
        super(
            cluster, new RelTraitSet(CallingConvention.JAVA), child,
            groupCount, aggCalls);
    }

    // implement RelNode
    public JavaAggregateRel clone()
    {
        JavaAggregateRel clone =
            new JavaAggregateRel(
                getCluster(), getChild(), groupCount, aggCalls);
        clone.inheritTraitsFrom(this);
        return clone;
    }

    public RelOptCost computeSelfCost(RelOptPlanner planner)
    {
        double dRows = getChild().getRows();
        double 
        // reflects memory cost also
        dCpu = Util.nLogN(dRows) + (aggCalls.length * 4);
        double dIo = 0;
        return planner.makeCost(dRows, dCpu, dIo);
    }

    public ParseTree implement(JavaRelImplementor implementor)
    {
        // Generate
        //     HashMap h = new HashMap();
        //     <<child>>
        //     Iterator keys = h.keySet().iterator();
        //     while (keys.hasNext()) {
        //       T row = new T();
        //       HashableArray groups = (HashableArray) keys.next();
        //       row.c0 = groups[0];
        //       row.c1 = groups[1];
        // (or, if groupCount == 1,
        //       Object groups = keys.next();
        //       row.c0 = (T1) groups;
        // )
        //       Object[] aggs = h.get(groups);
        //       row.c2 = <<agg result code {aggs[0]}>>
        //       row.c3 = <<agg result code {aggs[1]}>>
        // (or, if aggCount == 1
        //       Object agg = h.get(groups);
        //       row.c2 = <<agg result code {agg}>>
        // )
        //       <<emit row to parent>>
        //     }
        StatementList stmtList = implementor.getStatementList();
        this.var_h = implementor.newVariable();
        Variable var_keys = implementor.newVariable();
        stmtList.add(
            new VariableDeclaration(
                new TypeName("java.util.HashMap"),
                var_h.toString(),
                new AllocationExpression(
                    new TypeName("java.util.HashMap"),
                    null)));
        Expression o = implementor.visitJavaChild(
            this, 0, (JavaRel) getChild());
        assert (o == null);
        stmtList.add(
            new VariableDeclaration(
                new TypeName("java.util.Iterator"),
                var_keys.toString(),
                new MethodCall(
                    new MethodCall(var_h, "keySet", null),
                    "iterator",
                    null)));
        StatementList stmtList2 = new StatementList();
        stmtList.add(
            new WhileStatement(
                new MethodCall(var_keys, "hasNext", null),
                stmtList2));

        //       T row = new T();
        OJClass rowType = OJUtil.typeToOJClass(
            getRowType(),
            implementor.getTypeFactory());
        Variable var_row = implementor.newVariable();
        stmtList2.add(
            new VariableDeclaration(
                TypeName.forOJClass(rowType),
                var_row.toString(),
                new AllocationExpression(
                    TypeName.forOJClass(rowType),
                    null)));
        Variable var_groups = implementor.newVariable();
        if (groupCount == 1) {
            //       Object groups = keys.next();
            //       row.c0 = (T0) groups;
            int i = 0;
            OJField field = rowType.getDeclaredFields()[i];
            stmtList2.add(
                new VariableDeclaration(
                    TypeName.forOJClass(Toolbox.clazzObject),
                    var_groups.toString(),
                    new MethodCall(var_keys, "next", null)));
            stmtList2.add(
                new ExpressionStatement(
                    new AssignmentExpression(
                        new FieldAccess(
                            var_row,
                            field.getName()),
                        AssignmentExpression.EQUALS,
                        Toolbox.castObject(
                            var_groups,
                            Toolbox.clazzObject,
                            field.getType()))));
        } else {
            // HashableArray groups = (HashableArray) keys.next();
            // row.c0 = (T0) groups[0];
            // row.c1 = (T1) groups[1];
            throw Util.newInternal("todo: implement");
        }
        Variable var_aggs = implementor.newVariable();
        if (aggCalls.length == 1) {
            //       Object agg = h.get(groups);
            //       row.c2 = <<agg result code {agg}>>
            stmtList2.add(
                new VariableDeclaration(
                    TypeName.forOJClass(Toolbox.clazzObject),
                    var_aggs.toString(),
                    new MethodCall(
                        var_h,
                        "get",
                        new ExpressionList(var_groups))));
            int i = 0;
            OJField field = rowType.getDeclaredFields()[groupCount + i];
            stmtList2.add(
                new ExpressionStatement(
                    new AssignmentExpression(
                        new FieldAccess(
                            var_row,
                            field.getName()),
                        AssignmentExpression.EQUALS,
                        implementor.implementResult(aggCalls[i], var_aggs))));
        } else {
            //       Object[] aggs = (Object[]) h.get(groups);
            //       row.c2 = <<agg result code {aggs[0]}>>
            //       row.c3 = <<agg result code {aggs[1]}>>
            stmtList2.add(
                new VariableDeclaration(
                    TypeName.forOJClass(Toolbox.clazzObjectArray),
                    var_aggs.toString(),
                    new CastExpression(
                        Toolbox.clazzObjectArray,
                        new MethodCall(
                            var_h,
                            "get",
                            new ExpressionList(var_groups)))));
            OJField [] fields = rowType.getDeclaredFields();
            for (int i = 0; i < aggCalls.length; i++) {
                OJField field = fields[groupCount + i];
                stmtList2.add(
                    new ExpressionStatement(
                        new AssignmentExpression(
                            new FieldAccess(
                                var_row,
                                field.getName()),
                            AssignmentExpression.EQUALS,
                            implementor.implementResult(
                                aggCalls[i],
                                new ArrayAccess(
                                    var_aggs,
                                    Literal.makeLiteral(i))))));
            }
        }
        implementor.bind(this, var_row);
        implementor.generateParentBody(this, stmtList2);
        return null;
    }

    public void implementJavaParent(
        JavaRelImplementor implementor,
        int ordinal)
    {
        assert ordinal == 0;

        // Generate
        //   HashableArray groups = new HashableArray(
        //     new Object[] {
        //       <<child variable>>.field4,
        //       <<child variable>>.field2});
        //   Object[] aggs = h.get(groups);
        //   if (aggs == null) {
        //     aggs = new Object[] {
        //       <<agg[0] start code>>,
        //       <<agg[1] start code>>};
        //     h.put(groups, aggs);
        //   } else {
        //     <<agg inc code {aggs[0], childRow}>>
        //     <<agg inc code {aggs[1], childRow}>>
        //   }
        StatementList stmtList = implementor.getStatementList();
        Variable var_groups = implementor.newVariable();
        if (groupCount == 1) {
            //   Object groups = <<child variable>>.field4;
            int i = 0;
            stmtList.add(
                new VariableDeclaration(
                    TypeName.forOJClass(Toolbox.clazzObject),
                    var_groups.toString(),
                    implementor.translateInputField(this, 0, i)));
        } else {
            //   HashableArray groups = new HashableArray(
            //     new Object[] {
            //       <<child variable>>.field4,
            //       <<child variable>>.field2});
            throw Util.newInternal("todo:");
        }
        Variable var_aggs = implementor.newVariable();
        if (aggCalls.length == 1) {
            //   Object aggs = h.get(groups);
            stmtList.add(
                new VariableDeclaration(
                    TypeName.forOJClass(Toolbox.clazzObject),
                    var_aggs.toString(),
                    new MethodCall(
                        var_h,
                        "get",
                        new ExpressionList(var_groups))));
        } else {
            //   Object[] aggs = (Object[]) h.get(groups);
            stmtList.add(
                new VariableDeclaration(
                    TypeName.forOJClass(Toolbox.clazzObjectArray),
                    var_aggs.toString(),
                    new CastExpression(
                        Toolbox.clazzObjectArray,
                        new MethodCall(
                            var_h,
                            "get",
                            new ExpressionList(var_groups)))));
        }

        //   if (aggs == null) ...
        StatementList ifBlock = new StatementList();

        //   if (aggs == null) ...
        StatementList elseBlock = new StatementList();
        stmtList.add(
            new IfStatement(
                new BinaryExpression(
                    var_aggs,
                    BinaryExpression.EQUAL,
                    Literal.constantNull()),
                ifBlock,
                elseBlock));
        implementor.pushStatementList(ifBlock);
        if (aggCalls.length == 1) {
            //     aggs = <<aggs[0] start code>>;
            int i = 0;
            ifBlock.add(
                new ExpressionStatement(
                    new AssignmentExpression(
                        var_aggs,
                        AssignmentExpression.EQUALS,
                        implementor.implementStartAndNext(aggCalls[i], this))));
        } else {
            //     aggs = new Object[] {
            //       <<aggs[0] start code>>,
            //       <<aggs[1] start code>>};
            ExpressionList startList = new ExpressionList();
            for (int i = 0; i < aggCalls.length; i++) {
                startList.add(
                    implementor.implementStartAndNext(aggCalls[i], this));
            }
            ifBlock.add(
                new ExpressionStatement(
                    new AssignmentExpression(
                        var_aggs,
                        AssignmentExpression.EQUALS,
                        new ArrayAllocationExpression(
                            TypeName.forOJClass(Toolbox.clazzObjectArray),
                            null,
                            new ArrayInitializer(startList)))));
        }

        //     h.put(groups, aggs);
        ifBlock.add(
            new ExpressionStatement(
                new MethodCall(
                    var_h,
                    "put",
                    new ExpressionList(var_groups, var_aggs))));
        implementor.popStatementList(ifBlock);

        // <<agg inc code {aggs[0], child row}>>
        // <<agg inc code {aggs[1], child row}>>
        implementor.pushStatementList(elseBlock);
        for (int i = 0; i < aggCalls.length; i++) {
            assert (aggCalls[i].getArgs().length == 1);
            implementor.implementNext(
                aggCalls[i],
                this,
                (aggCalls.length == 1) ?
                (Expression) var_aggs :
                (Expression) new ArrayAccess(
                    var_aggs,
                    Literal.makeLiteral(i)));
        }
        implementor.popStatementList(elseBlock);
    }
}


// End JavaAggregateRel.java
