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

package net.sf.saffron.oj.convert;

import net.sf.saffron.core.*;
import net.sf.saffron.oj.util.*;
import net.sf.saffron.opt.*;
import net.sf.saffron.rel.*;
import net.sf.saffron.rel.convert.*;
import net.sf.saffron.runtime.*;
import net.sf.saffron.util.*;

import openjava.mop.*;

import openjava.ptree.*;

import java.util.HashMap;


/**
 * <code>JavaConverterRel</code> converts a plan from
 * <code>inConvention</code> to {@link
 * net.sf.saffron.opt.CallingConvention#JAVA_ORDINAL}.
 */
public class JavaConverterRel extends ConverterRel
{
    /**
     * Maps {@link OJClass} to the name of a JDBC getter method. For example,
     * {@link OJSystem#INT} maps to "getInt".
     */
    private static final HashMap jdbcGetterMap = createJdbcGetterMap();

    //~ Constructors ----------------------------------------------------------

    public JavaConverterRel(VolcanoCluster cluster,SaffronRel child)
    {
        super(cluster,child);
    }

    //~ Methods ---------------------------------------------------------------

    public CallingConvention getConvention()
    {
        return CallingConvention.JAVA;
    }

    public Object clone()
    {
        return new JavaConverterRel(cluster,child);
    }

    public static void init(SaffronPlanner planner)
    {
        final ConverterFactory factory =
            new ConverterFactory() {
                public CallingConvention getConvention()
                {
                    return CallingConvention.JAVA;
                }

                public ConverterRel convert(SaffronRel rel)
                {
                    return new JavaConverterRel(rel.getCluster(),rel);
                }
            };
        planner.addRule(
            new FactoryConverterRule(factory,CallingConvention.ARRAY));
        planner.addRule(
            new FactoryConverterRule(factory,CallingConvention.ITERATOR));
        planner.addRule(
            new FactoryConverterRule(factory,CallingConvention.ENUMERATION));
        planner.addRule(
            new FactoryConverterRule(factory,CallingConvention.VECTOR));
        planner.addRule(
            new FactoryConverterRule(factory,CallingConvention.MAP));
        planner.addRule(
            new FactoryConverterRule(factory,CallingConvention.HASHTABLE));
        planner.addRule(
            new FactoryConverterRule(factory,CallingConvention.ITERABLE));
        planner.addRule(
            new FactoryConverterRule(factory,CallingConvention.RESULT_SET));
    }

    public Object implement(RelImplementor implementor,int ordinal)
    {
        switch (inConvention.ordinal_) {
        case CallingConvention.ITERATOR_ORDINAL:
            return implementIterator(implementor,ordinal);
        case CallingConvention.ITERABLE_ORDINAL:
            return implementIterable(implementor,ordinal);
        case CallingConvention.VECTOR_ORDINAL:
            return implementVector(implementor,ordinal);
        case CallingConvention.ARRAY_ORDINAL:
            return implementArray(implementor,ordinal);
        case CallingConvention.ENUMERATION_ORDINAL:
            return implementEnumeration(implementor,ordinal);
        case CallingConvention.MAP_ORDINAL:
            return implementMap(implementor,ordinal);
        case CallingConvention.HASHTABLE_ORDINAL:
            return implementHashtable(implementor,ordinal);
        case CallingConvention.RESULT_SET_ORDINAL:
            return implementResultSet(implementor,ordinal);
        default:
            return super.implement(implementor,ordinal);
        }
    }

    private Object implementArray(RelImplementor implementor,int ordinal)
    {
        assert (ordinal == -1) : "Cannot implement callback from child";

        // Generate
        //   V[] array = <<exp>>;
        //   for (int i = 0; i < array.length; i++) {
        //     V row = array[i];
        //     <<parent>>
        //   }
        StatementList stmtList = implementor.getStatementList();
        Variable variable_array = implementor.newVariable();
        Variable variable_i = implementor.newVariable();
        StatementList forBody = new StatementList();
        Expression exp = (Expression) implementor.implementChild(this,0,child);
        stmtList.add(
            new VariableDeclaration(
                TypeName.forOJClass(OJUtil.ojClassForExpression(this,exp)),
                variable_array.toString(),
                exp));
        stmtList.add(
            new ForStatement(
                new TypeName("int"),
                new VariableDeclarator [] {
                    new VariableDeclarator(
                        variable_i.toString(),
                        Literal.constantZero())
                },
                new BinaryExpression(
                    variable_i,
                    BinaryExpression.LESS,
                    new FieldAccess(variable_array,"length")),
                new ExpressionList(
                    new UnaryExpression(
                        UnaryExpression.POST_INCREMENT,
                        variable_i)),
                forBody));
        Variable variable_row =
            implementor.bind(
                this,
                forBody,
                new ArrayAccess(variable_array,variable_i));
        Util.discard(variable_row);
        implementor.generateParentBody(this,forBody);
        return null;
    }

    private Object implementEnumeration(
        RelImplementor implementor,
        int ordinal)
    {
        assert (ordinal == -1) : "Cannot implement callback from child";

        // Generate
        //   for (Enumeration enum = <<exp>>; enum.hasMoreElements();)
        //   {
        //     Row row = enum.nextElement();
        //     <<parent>>
        //   }
        StatementList stmtList = implementor.getStatementList();
        Variable variable_enum = implementor.newVariable();
        StatementList forBody = new StatementList();
        Expression exp = (Expression) implementor.implementChild(this,0,child);
        stmtList.add(
            new ForStatement(
                TypeName.forOJClass(Util.clazzEnumeration),
                new VariableDeclarator [] {
                    new VariableDeclarator(variable_enum.toString(),exp)
                },
                new MethodCall(variable_enum,"hasMoreElements",null),
                new ExpressionList(),
                forBody));
        Variable variable_row =
            implementor.bind(
                this,
                forBody,
                new MethodCall(variable_enum,"nextElement",null));
        Util.discard(variable_row);
        implementor.generateParentBody(this,forBody);
        return null;
    }

    private Object implementHashtable(RelImplementor implementor,int ordinal)
    {
        assert (ordinal == -1) : "Cannot implement callback from child";

        // Generate
        //   Hashtable h = <<exp>>;
        //   for (Enumeration keys = h.keys(); keys.hasMoreElements();) {
        //     Object key = keys.nextElement();
        //     Object value = h.get(key);
        //     Row row = new Row(key, value);
        //     <<parent>>
        //   }
        StatementList stmtList = implementor.getStatementList();
        Variable variable_h = implementor.newVariable();
        Variable variable_keys = implementor.newVariable();
        Variable variable_key = implementor.newVariable();
        Variable variable_value = implementor.newVariable();
        StatementList forBody = new StatementList();
        Expression exp = (Expression) implementor.implementChild(this,0,child);
        stmtList.add(
            new VariableDeclaration(
                TypeName.forOJClass(Util.clazzHashtable),
                variable_h.toString(),
                exp));
        stmtList.add(
            new ForStatement(
                TypeName.forOJClass(Util.clazzEnumeration),
                new VariableDeclarator [] {
                    new VariableDeclarator(
                        variable_keys.toString(),
                        new MethodCall(variable_h,"keys",null))
                },
                new MethodCall(variable_keys,"hasMoreElements",null),
                new ExpressionList(),
                forBody));
        forBody.add(
            new VariableDeclaration(
                TypeName.forOJClass(Toolbox.clazzObject),
                new VariableDeclarator(
                    variable_key.toString(),
                    new MethodCall(variable_keys,"nextElement",null))));
        forBody.add(
            new VariableDeclaration(
                TypeName.forOJClass(Toolbox.clazzObject),
                new VariableDeclarator(
                    variable_value.toString(),
                    new MethodCall(
                        variable_h,
                        "get",
                        new ExpressionList(variable_key)))));
        OJClass rowType = OJUtil.typeToOJClass(getRowType());
        Variable variable_row =
            implementor.bind(
                this,
                forBody,
                new AllocationExpression(
                    TypeName.forOJClass(rowType),
                    new ExpressionList(variable_key,variable_value)));
        Util.discard(variable_row);
        implementor.generateParentBody(this,forBody);
        return null;
    }

    private Object implementIterable(RelImplementor implementor,int ordinal)
    {
        assert (ordinal == -1) :  "Cannot implement callback from child";

        // Generate
        //   Iterator iter = <<exp>>.iterator();
        //   while (iter.hasNext()) {
        //     V row = (Type) iter.next();
        //     <<body>>
        //   }
        //
        StatementList stmtList = implementor.getStatementList();

        // Generate
        //   Iterator iter = <<exp>>.iterator();
        //   while (iter.hasNext()) {
        //     V row = (Type) iter.next();
        //     <<body>>
        //   }
        //
        StatementList whileBody = new StatementList();
        Variable variable_iter = implementor.newVariable();
        Expression exp = (Expression) implementor.implementChild(this,0,child);
        stmtList.add(
            new VariableDeclaration(
                new TypeName("java.util.Iterator"),
                variable_iter.toString(),
                exp));
        stmtList.add(
            new WhileStatement(
                new MethodCall(variable_iter,"hasNext",null),
                whileBody));
        OJClass rowType = OJUtil.typeToOJClass(child.getRowType());
        Variable variable_row =
            implementor.bind(
                this,
                whileBody,
                Util.castObject(
                    new MethodCall(variable_iter,"next",null),
                    Toolbox.clazzObject,
                    rowType));
        Util.discard(variable_row);
        implementor.generateParentBody(this,whileBody);
        return null;
    }

    private Object implementIterator(RelImplementor implementor,int ordinal)
    {
        assert (ordinal == -1) : "Cannot implement callback from child";

        // Generate
        //   Iterator iter = <<exp>>;
        //   while (iter.hasNext()) {
        //     V row = (Type) iter.next();
        //     <<body>>
        //   }
        //
        StatementList stmtList = implementor.getStatementList();

        // Generate
        //   Iterator iter = <<exp>>;
        //   while (iter.hasNext()) {
        //     V row = (Type) iter.next();
        //     <<body>>
        //   }
        //
        StatementList whileBody = new StatementList();
        Variable variable_iter = implementor.newVariable();
        Expression exp = (Expression) implementor.implementChild(this,0,child);
        stmtList.add(
            new VariableDeclaration(
                new TypeName("java.util.Iterator"),
                variable_iter.toString(),
                exp));
        stmtList.add(
            new WhileStatement(
                new MethodCall(variable_iter,"hasNext",null),
                whileBody));
        Variable variable_row =
            implementor.bind(
                this,
                whileBody,
                Util.castObject(
                    new MethodCall(variable_iter,"next",null),
                    Toolbox.clazzObject,
                    OJUtil.typeToOJClass(child.getRowType())));
        Util.discard(variable_row);
        implementor.generateParentBody(this,whileBody);
        return null;
    }

    private Object implementMap(RelImplementor implementor,int ordinal)
    {
        assert (ordinal == -1) : "Cannot implement callback from child";

        // Generate
        //   Map m = <<exp>>;
        //   for (Iterator entries = m.entrySet().iterator();
        //        entries.hasNext();) {
        //     Map.Entry entry = (Map.Entry) keys.next();
        //     Row row = new Row(entry.getKey(), entry.getValue());
        //     <<parent>>
        //   }
        StatementList stmtList = implementor.getStatementList();
        Variable variable_m = implementor.newVariable();
        Variable variable_entries = implementor.newVariable();
        Variable variable_entry = implementor.newVariable();
        StatementList forBody = new StatementList();
        Expression exp = (Expression) implementor.implementChild(this,0,child);
        stmtList.add(
            new VariableDeclaration(
                TypeName.forOJClass(Util.clazzMap),
                variable_m.toString(),
                exp));
        stmtList.add(
            new ForStatement(
                TypeName.forOJClass(Util.clazzIterator),
                new VariableDeclarator [] {
                    new VariableDeclarator(
                        variable_entries.toString(),
                        new MethodCall(
                            new MethodCall(variable_m,"entrySet",null),
                            "iterator",
                            null))
                },
                new MethodCall(variable_entries,"hasNext",null),
                new ExpressionList(),
                forBody));
        forBody.add(
            new VariableDeclaration(
                TypeName.forOJClass(Toolbox.clazzMapEntry),
                new VariableDeclarator(
                    variable_entry.toString(),
                    new CastExpression(
                        TypeName.forOJClass(Toolbox.clazzMapEntry),
                        new MethodCall(variable_entries,"next",null)))));
        OJClass rowType = OJUtil.typeToOJClass(getRowType());
        Variable variable_row =
            implementor.bind(
                this,
                forBody,
                new AllocationExpression(
                    TypeName.forOJClass(rowType),
                    new ExpressionList(
                        new MethodCall(variable_entry,"getKey",null),
                        new MethodCall(variable_entry,"getValue",null))));
        Util.discard(variable_row);
        implementor.generateParentBody(this,forBody);
        return null;
    }

    private Object implementResultSet(RelImplementor implementor,int ordinal)
    {
        assert (ordinal == -1) : "Cannot implement callback from child";

        // Generate
        //
        // java.sql.ResultSet resultSet = null;
        // try {
        //    resultSet = <<child>>;
        //    while (resultSet.next()) {
        //      Emp emp = new Emp();
        //      emp.empno = resultSet.getInt(1);
        //      ...
        //      emp.gender = resultSet.getString(4);
        //      <<parent>>
        //    }
        // } catch (java.sql.SQLException e) {
        //    throw new saffron.runtime.SaffronError(e);
        // } finally {
        //    if (resultSet != null) {
        //       try {
        //          resultSet.close();
        //       } catch {}
        //    }
        // }
        // TODO jvs 16-May-2003 -- In cases where parent doesn't need distinct
        // instances, move the row constructor outside the loop.
        StatementList stmtList = implementor.getStatementList();
        Variable varRs = implementor.newVariable();
        Variable varRow = implementor.newVariable();
        Variable varEx = implementor.newVariable();

        final OJClass clazz = OJUtil.typeToOJClass(getRowType());
        StatementList whileBody =
            new StatementList(
                
            // Emp emp = new Emp();
            //      emp.empno = resultSet.getInt(1);
            //      ...
            //      emp.gender = resultSet.getString(4);
            // <<parent>>
            new VariableDeclaration(
                    TypeName.forOJClass(clazz),
                    varRow.toString(),
                    new AllocationExpression(clazz,new ExpressionList())));

        // REVIEW jvs 7-May-2003 -- The javadoc for getDeclaredFields says they
        // come back in arbitrary order, but the implementation gives them back
        // in declaration order.   If this is not reliable, need to figure
        // out something else.  (getAllFields definitely scrambled them, so
        // this cannot be used with classes from a hierarchy.)
        OJField [] fields = clazz.getDeclaredFields();
        for (int i = 0; i < fields.length; ++i) {
            int iField = i + 1;

            final OJClass ojClass = fields[i].getType();
            String getterMethodName = getJdbcGetterName(ojClass);
            whileBody.add(
                new ExpressionStatement(
                    new AssignmentExpression(
                        new FieldAccess(varRow,fields[i].getName()),
                        AssignmentExpression.EQUALS,
                        new MethodCall(
                            varRs,
                            getterMethodName,
                            new ExpressionList(Literal.makeLiteral(iField))))));
        }
        implementor.bind(this,varRow);
        implementor.generateParentBody(this,whileBody);

        stmtList.add(
            
        // java.sql.ResultSet resultSet = null;
        new VariableDeclaration(
                new TypeName("java.sql.ResultSet"),
                varRs.toString(),
                Literal.constantNull()));

        stmtList.add(
            new TryStatement(
                
        // try {
        new StatementList(
                    
        // resultSet = <<child>>;
        new ExpressionStatement(
                        new AssignmentExpression(
                            varRs,
                            AssignmentExpression.EQUALS,
                            (Expression) implementor.implementChild(
                                this,
                                0,
                                child))),
                    
        // while (resultSet.next()) {
        new WhileStatement(new MethodCall(varRs,"next",null),
        // Emp emp = new Emp(resultSet);
        // <<body>>
        whileBody)),
                new CatchList(
                    
        // catch (java.sql.SQLException e) {
        //   throw new saffron.runtime.SaffronError(e);
        // }
        new CatchBlock(new Parameter(
                            new TypeName("java.sql.SQLException"),
                            varEx.toString()),
                        new StatementList(
                            new ThrowStatement(
                                new AllocationExpression(
                                    TypeName.forClass(SaffronError.class),
                                    new ExpressionList(varEx)))))),
                
        // finally {
        //    if (resultSet != null) {
        //       try {
        //          resultSet.close();
        //       } catch (java.sql.SQLException e) {}
        //    }
        // }
        new StatementList(
                    new IfStatement(
                        new BinaryExpression(
                            varRs,
                            BinaryExpression.NOTEQUAL,
                            Literal.constantNull()),
                        new StatementList(
                            new TryStatement(
                                new StatementList(
                                    new ExpressionStatement(
                                        new MethodCall(varRs,"close",null))),
                                new CatchList(
                                    new CatchBlock(
                                        new Parameter(
                                            new TypeName(
                                                "java.sql.SQLException"),
                                            varEx.toString()),
                                        new StatementList()))))))));
        return null;
    }

    private static String getJdbcGetterName(final OJClass ojClass) {
        final String getter = (String) jdbcGetterMap.get(ojClass);
        if (getter == null) {
            return "getObject";
        } else {
            return getter;
        }
    }

    private static HashMap createJdbcGetterMap() {
        HashMap map = new HashMap();
        map.put(OJSystem.INT, "getInt");
        map.put(OJSystem.BOOLEAN, "getBoolean");
        map.put(OJSystem.BYTE, "getByte");
        map.put(OJSystem.CHAR, "getChar");
        map.put(OJSystem.DOUBLE, "getDouble");
        map.put(OJSystem.FLOAT, "getFloat");
        map.put(OJSystem.LONG, "getLong");
        map.put(OJSystem.OBJECT, "getObject");
        map.put(OJSystem.SHORT, "getShort");
        map.put(OJSystem.STRING, "getString");
        return map;
    }

    private Object implementVector(RelImplementor implementor,int ordinal)
    {
        assert (ordinal == -1) : "Cannot implement callback from child";

        // Generate
        //   for (Enumeration e = <<exp>>.elements(); e.hasMoreElements();) {
        //     Row row = (Row) e.nextElement();
        //     <<parent>>
        //   }
        StatementList stmtList = implementor.getStatementList();
        Variable variable_enum = implementor.newVariable();
        StatementList forBody = new StatementList();
        Expression exp = (Expression) implementor.implementChild(this,0,child);
        OJClass rowType = OJUtil.typeToOJClass(child.getRowType());
        stmtList.add(
            new ForStatement(
                TypeName.forOJClass(Util.clazzEnumeration),
                new VariableDeclarator [] {
                    new VariableDeclarator(
                        variable_enum.toString(),
                        new MethodCall(exp,"elements",new ExpressionList()))
                },
                new MethodCall(variable_enum,"hasMoreElements",null),
                new ExpressionList(),
                forBody));
        Variable variable_row =
            implementor.bind(
                this,
                forBody,
                Util.castObject(
                    new MethodCall(variable_enum,"nextElement",null),
                    Toolbox.clazzObject,
                    rowType));
        Util.discard(variable_row);
        implementor.generateParentBody(this,forBody);
        return null;
    }
}


// End JavaConverterRel.java
