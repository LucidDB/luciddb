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

package net.sf.saffron.test;

import java.lang.reflect.Field;
import java.util.Collections;

import net.sf.saffron.ext.ExtentRel;
import net.sf.saffron.ext.ExtentTable;
import net.sf.saffron.ext.ReflectSchema;
import net.sf.saffron.oj.rel.ExpressionReaderRel;
import net.sf.saffron.oj.stmt.OJStatement;

import openjava.ptree.FieldAccess;
import openjava.ptree.MethodCall;

import org.eigenbase.oj.util.JavaRexBuilder;
import org.eigenbase.rel.*;
import org.eigenbase.relopt.*;
import org.eigenbase.reltype.*;
import org.eigenbase.rex.*;
import org.eigenbase.sql.type.*;
import org.eigenbase.util.Util;


/**
 * A <code>ObjectSchemaTest</code> is a test harness for {@link
 * net.sf.saffron.ext.ObjectSchema}. Called from {@link
 * net.sf.saffron.ext.ObjectSchema#suite} via reflection.
 *
 * @author jhyde
 * @version $Id$
 *
 * @since May 3, 2002
 */
public class ObjectSchemaTest extends SaffronTestCase
{
    private static JavaReflectConnection reflect;

    public ObjectSchemaTest(String s)
        throws Exception
    {
        super(s);
    }

    public RelOptConnection getConnection()
    {
        return reflect;
    }

    // ------------------------------------------------------------------------
    // tests follow
    public void _testFields()
    {
        // impossible to implement
        Object o = runQuery("select from reflect.fields as field");
        assertTrue(o instanceof Field []);
        Field [] fields = (Field []) o;
        assertEquals(10, fields.length);
    }

    public void testFilter()
    {
        Object o =
            runQuery("select from reflect.classes as clazz "
                + "where clazz.getName().equals(\"java\" + \".lang.Object\")");
        assertTrue(o instanceof Class []);
        Class [] classes = (Class []) o;
        assertEquals(1, classes.length);
        assertEquals(Object.class, classes[0]);
    }

    public void testFilterNegative()
    {
        Object o =
            runQuery("select from reflect.classes as clazz "
                + "where clazz.getName().equals(\"non.existent.class\")");
        assertTrue(o instanceof Class []);
        Class [] classes = (Class []) o;
        assertEquals(0, classes.length);
    }

    public void testJoin0()
    {
        Object o =
            runQuery("select from reflect.fields as field "
                + "where field.getDeclaringClass() == java.lang.System.class");
        assertTrue(o instanceof Field []);
        Field [] fields = (Field []) o;
        assertEquals(3, fields.length); // fields are 'in', 'out', 'err'
    }

    public void testJoin3()
    {
        Object o =
            runQuery("select field.getName() " + nl
                + "from reflect.fields as field" + nl
                + " join reflect.classes as clazz" + nl
                + " on field.getDeclaringClass() == clazz" + nl
                + "where clazz in new Class[] {" + nl
                + "   java.lang.Object.class," + nl
                + "   java.lang.String.class," + nl
                + "   java.util.Hashtable.class} &&" + nl
                + " field.getType() == int.class");
        assertTrue(o instanceof Field []);
        Field [] fields = (Field []) o;
        assertEquals(10, fields.length);
    }

    protected Object runQuery(String query)
    {
        return super.runQuery(
            query,
            new OJStatement.Argument [] {
                new OJStatement.Argument(
                    "reflect",
                    getReflect())
            });
    }

    private static synchronized JavaReflectConnection getReflect()
    {
        if (reflect == null) {
            reflect = new JavaReflectConnection();
        }
        return reflect;
    }

    /**
     * Matches <code>expr == $0</code> or <code>expr.equals($0)</code> or
     * <code>$0.equals(expr)</code>.
     */
    public static class EqualsPattern implements RexPattern
    {
        RexNode target;

        public EqualsPattern(RexNode target)
        {
            this.target = target;
        }

        public void match(
            RexNode rex,
            RexAction action)
        {
            if (rex.isA(RexKind.Equals)) {
                RexCall call = (RexCall) rex;

                // Note that if each operand matches, we call the rule twice. 
                for (int i = 0; i < 2; i++) {
                    if (call.operands[i].equals(target)) {
                        action.onMatch(new RexNode [] { call.operands[i] });
                    }
                }
            }
        }
    }

    /**
     * Rule to translate an {@link net.sf.saffron.ext.ExtentRel} equi-joined to
     * anything, to the anything (plus a project, to provide the lost
     * column). This is valid because an extent has, by definition, one copy
     * of every possible instance of the type, and therefore has no filtering
     * effect.
     *
     * <p>For example, <code>select * from emps as e join
     * AllInstancesOf(int.class) as i on e.deptno = i</code> translates
     * to <code>select e.*, e.deptno from emps as e</code>.</p>
     */
    public static class ExtentJoinRule extends RelOptRule
    {
        public ExtentJoinRule()
        {
            super(new RelOptRuleOperand(
                    JoinRel.class,
                    new RelOptRuleOperand [] {
                        new RelOptRuleOperand(RelNode.class, null),
                        new RelOptRuleOperand(ExtentRel.class, null)
                    }));
        }

        public void onMatch(final RelOptRuleCall call)
        {
            JoinRel join = (JoinRel) call.rels[0];
            final RelNode rel = call.rels[1];
            ExtentRel extent = (ExtentRel) call.rels[2];
            Util.discard(extent);
            final RexBuilder rexBuilder = join.getCluster().getRexBuilder();
            int fieldIndex = rel.getRowType().getFieldList().size();
            final EqualsPattern equalsPattern =
                new EqualsPattern(
                    rexBuilder.makeInputRef(
                        join.getRowType().getFields()[fieldIndex].getType(),
                        fieldIndex));
            equalsPattern.match(
                join.getCondition(),
                new RexAction() {
                    public void onMatch(RexNode [] tokens)
                    {
                        // todo: we are assuming that the condition is simply
                        // t1 == t2, but it may be more complicated. We should
                        // probably convert each occurrence of t1 to t2, and
                        // apply the join condition as a filter. 't1 == t2' will
                        // become 't2 == t2', which is always true, so the filter
                        // can be removed
                        final RexNode[] exprs = new RexNode [] {
                            rexBuilder.makeRangeReference(rel.getRowType(), 0, false),
                            RexUtil.clone(tokens[0])
                        };
                        RelNode project =
                            CalcRel.createProject(
                                rel,
                                exprs,
                                null);
                        call.transformTo(project);
                    }
                });
        }
    }

    // ------------------------------------------------------------------------
    // classes follow

    /**
     * <code>JavaReflectConnection</code> is a connection to the virtual
     * database formed by the Java reflect API.
     */
    public static class JavaReflectConnection implements RelOptConnection
    {
        private static JavaReflectSchema schema = new JavaReflectSchema(null);

        public RelOptSchema getRelOptSchema()
        {
            return getRelOptSchemaStatic();
        }

        // for Connection
        public static RelOptSchema getRelOptSchemaStatic()
        {
            return schema;
        }

        public Object contentsAsArray(
            String qualifier,
            String tableName)
        {
            return null;
        }
    }

    /**
     * <code>JavaReflectConnection</code> is the schema for the virtual
     * database formed by the Java reflect API.
     */
    public static class JavaReflectSchema extends ReflectSchema
    {
        public final RelDataTypeFactory typeFactory =
            new SqlTypeFactoryImpl();
        public RelOptTable classes =
            new ExtentTable(this, "classes",
                typeFactory.createJavaType(Class.class));
        public RelOptTable fields =
            new ExtentTable(this, "fields",
                typeFactory.createJavaType(Field.class));
        private final JavaRexBuilder rexBuilder;

        JavaReflectSchema(JavaRexBuilder rexBuilder)
        {
            this.rexBuilder = rexBuilder;
        }

        public void registerRules(RelOptPlanner planner)
            throws Exception
        {
            planner.addRule(new ExtentJoinRule());
            addRelationship(planner, fields, classes, "getDeclaringClass()",
                "getFields()");

            /*
            // record that
            //   select from classes as clazz
            //   where clazz.getName().equals(<foo>)
            // can be transformed to
            //   SaffronUtil.classesForName(<foo>)
            // which is almost the same as
            //   select new Class[] {Class.forName(<foo>)}
            planner.addRule(
                new FilterRule(
                    classes,
                    new EqualsPattern(
                        rexBuilder.makeJava(
                                new MethodCall(
                            OJUtil.makeReference(0),
                            "getName",
                            null))) {
                    public void onMatch(RexNode [] tokens)
                    {
                        RexNode expression =
                            new MethodCall(
                                Util.clazzSaffronUtil,
                                "classesForName",
                                new ExpressionList((Expression) tokens[0]));
                        assert(call != null);
                        call.transformTo(
                            new ExpressionReaderRel(
                                call.rels[0].getCluster(),
                                expression,
                                null));
                    }
                });
                */
        }

        private void addLink(
            RelOptPlanner planner,
            RelOptTable fromTable,
            RelOptTable toTable,
            String fieldName,
            final String replaceFieldName,
            final boolean many)
            throws Exception
        {
            if (fieldName == null) {
                return;
            }
            final RelDataType fromClass = fromTable.getRowType();
            final RelDataType toClass = toTable.getRowType();
            Util.discard(toClass);
            RexNode seek =
                makeFieldOrMethodCall(
                    rexBuilder.makeRangeReference(
                        rexBuilder.constantNull().getType(),
                        0, false),
                    fieldName);

            // In the many-to-one case, we transform
            //
            // select
            // from fromTable
            // join toTable
            // on fromTable.method() == toTable
            //
            // e.g.
            //
            // select
            // from reflect.classes as clazz
            // join reflect.fields as field
            // on field.getDeclaringClass() == clazz
            //
            // into the equivalent
            //
            // select {fromTable, x}
            // from fromTable
            // join fromTable.method() as x
            //
            // e.g.
            //
            // select {clazz, field}
            // from reflect.classes as clazz
            // join clazz.getFields() as field
            //
            // In the one-to-many case,
            //
            // select
            // from emp
            // join dept
            // on emp == dept.getEmp()
            //
            // is equivalent to
            //
            // select {dept.getEmp(), dept}
            // from dept
            //
            // because 'emp' is an extent, and therefore is guaranteed to
            // exist. (todo: Loosen up this rule, so body expressions don't
            // have to be tables.)
            planner.addRule(
                new JoinRule(
                    fromTable,
                    toTable,
                    new EqualsPattern(seek)) {
                    public void onMatch(RexNode [] tokens)
                    {
                        RexNode replaceExpr;
                        final JoinRel oldJoin = (JoinRel) call.rels[0];
                        final RelNode manyRel = call.rels[1]; // e.g. "fields"
                        final RelNode oneRel = call.rels[2]; // e.g. "classes"
                        String correl = oneRel.getOrCreateCorrelVariable();
                        RexNode correlVar =
                            rexBuilder.makeCorrel(null, correl);
                        replaceExpr =
                            makeFieldOrMethodCall(correlVar, replaceFieldName);
                        if (many) {
                            throw Util.newInternal("todo:");
                        } else {
                            final RelOptCluster cluster = oldJoin.getCluster();
                            final RelDataType rowType = manyRel.getRowType();
                            final ExpressionReaderRel expressionReader =
                                new ExpressionReaderRel(cluster, replaceExpr,
                                    rowType);
                            final JoinRel newJoin =
                                new JoinRel(
                                    cluster,
                                    expressionReader,
                                    oneRel,
                                    rexBuilder.makeLiteral(true),
                                    JoinRelType.INNER,
                                    Collections.EMPTY_SET);
                            newJoin.registerStoppedVariable(correl);
                            call.transformTo(newJoin);
                        }
                    }
                });

            // record that
            //   select from fromTable
            //   where fromTable.method() == v
            // e.g.
            //   select from fields as field
            //   where field.getDeclaringClass() == x
            // can be transformed to
            //   select from x.getFields()
            planner.addRule(
                new FilterRule(
                    fromTable,
                    new EqualsPattern(seek)) {
                    public void onMatch(RexNode [] tokens)
                    {
                        assert (call != null);
                        call.transformTo(
                            new ExpressionReaderRel(
                                call.rels[0].getCluster(),
                                makeFieldOrMethodCall(tokens[0],
                                    replaceFieldName),
                                fromClass));
                    }
                });
        }

        private void addRelationship(
            RelOptPlanner planner,
            RelOptTable fromTable,
            RelOptTable toTable,
            String fromField,
            String toField)
            throws Exception
        {
            addLink(planner, fromTable, toTable, fromField, toField, false);
            addLink(planner, toTable, fromTable, toField, fromField, true);
        }

        /**
         * Creates a {@link FieldAccess} "target.field" or a {@link
         * MethodCall} "target.method()", depending upon whether
         * <code>fieldName</code> ends with "()"
         *
         * @param target Expression for object to apply to
         * @param fieldName Field name, or method name followed by "()"
         *
         * @return A {@link FieldAccess} or {@link MethodCall}
         */
        private RexNode makeFieldOrMethodCall(
            RexNode target,
            String fieldName)
        {
            if (fieldName.endsWith("()")) {
                //String methodName =
                //    fieldName.substring(0,fieldName.length() - 2);
                //return new MethodCall(target,methodName,null);
                throw Util.needToImplement(this);
            } else {
                return rexBuilder.makeFieldAccess(target, fieldName);
            }
        }
    }

    /**
     * <code>FilterRule</code> is a template for a rule. It applies the parse
     * tree <code>pattern</code> to the filter condition, and each time the
     * pattern matches, fires the {@link #onMatch(RexNode[])} method (which
     * is abstract in this class).
     */
    static abstract class FilterRule extends RelOptRule implements RexAction
    {
        RexPattern pattern;
        RelOptTable table;
        RelOptRuleCall call;

        public FilterRule(
            RelOptTable table,
            RexPattern pattern)
        {
            super(new RelOptRuleOperand(
                    FilterRel.class, // todo: constructor which takes a table
                    new RelOptRuleOperand [] {
                        new RelOptRuleOperand(RelNode.class, null)
                    }));
            this.table = table;
            this.pattern = pattern;
        }

        public void onMatch(RelOptRuleCall call)
        {
            FilterRel filter = (FilterRel) call.rels[0];
            RelNode rel = call.rels[1];
            if (!rel.isAccessTo(table)) {
                return;
            }
            RexNode condition = filter.getCondition();
            this.call = call; // non-reentrant!! but okay
            pattern.match(condition, this);
        }
    }

    /**
     * <code>JoinRule</code> is a template for a rule. It applies the parse
     * tree <code>pattern</code> to the join condition, and each time the
     * pattern matches, fires the {@link #onMatch(RexNode[])} method (which
     * is abstract in this class).
     */
    static abstract class JoinRule extends RelOptRule implements RexAction
    {
        RexPattern pattern;
        RelOptTable fromTable;
        RelOptTable toTable;
        RelOptRuleCall call;

        public JoinRule(
            RelOptTable fromTable,
            RelOptTable toTable,
            RexPattern pattern)
        {
            super(new RelOptRuleOperand(
                    JoinRel.class, // todo: constructor which takes a table
                    new RelOptRuleOperand [] {
                        new RelOptRuleOperand(RelNode.class, null),
                        new RelOptRuleOperand(RelNode.class, null)
                    }));
            this.fromTable = fromTable;
            this.toTable = toTable;
            this.pattern = pattern;
        }

        public void onMatch(RelOptRuleCall call)
        {
            JoinRel join = (JoinRel) call.rels[0];
            RelNode left = call.rels[1];
            RelNode right = call.rels[2];
            if (left.isAccessTo(fromTable) && right.isAccessTo(toTable)) {
                ;
            } else {
                return;
            }
            RexNode condition = join.getCondition();
            this.call = call; // non-reentrant!! but okay
            pattern.match(condition, this);
        }
    }
}


// End ObjectSchemaTest.java
