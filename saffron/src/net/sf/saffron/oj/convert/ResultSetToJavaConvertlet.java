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

package net.sf.saffron.oj.convert;

import java.util.HashMap;

import net.sf.saffron.runtime.SaffronError;

import openjava.mop.OJClass;
import openjava.mop.OJField;
import openjava.mop.OJSystem;
import openjava.ptree.*;

import org.eigenbase.oj.rel.JavaRel;
import org.eigenbase.oj.rel.JavaRelImplementor;
import org.eigenbase.oj.util.OJUtil;
import org.eigenbase.rel.convert.ConverterRel;
import org.eigenbase.relopt.CallingConvention;


/**
 * Thunk to convert between {@link CallingConvention#RESULT_SET result-set}
 * and {@link CallingConvention#JAVA java} calling-conventions.
 *
 * @author jhyde
 * @since May 27, 2004
 * @version $Id$
 **/
public class ResultSetToJavaConvertlet extends JavaConvertlet
{
    /**
     * Maps {@link OJClass} to the name of a JDBC getter method. For example,
     * {@link OJSystem#INT} maps to "getInt".
     */
    private static final HashMap jdbcGetterMap = createJdbcGetterMap();

    public ResultSetToJavaConvertlet()
    {
        super(CallingConvention.RESULT_SET, CallingConvention.JAVA);
    }

    public ParseTree implement(
        JavaRelImplementor implementor,
        ConverterRel converter)
    {
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

        final OJClass clazz = OJUtil.typeToOJClass(converter.getRowType());
        StatementList whileBody =
            new StatementList(
            // Emp emp = new Emp();
            //      emp.empno = resultSet.getInt(1);
            //      ...
            //      emp.gender = resultSet.getString(4);
            // <<parent>>
            new VariableDeclaration(TypeName.forOJClass(clazz),
                    varRow.toString(),
                    new AllocationExpression(
                        clazz,
                        new ExpressionList())));

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
                        new FieldAccess(
                            varRow,
                            fields[i].getName()),
                        AssignmentExpression.EQUALS,
                        new MethodCall(
                            varRs,
                            getterMethodName,
                            new ExpressionList(Literal.makeLiteral(iField))))));
        }
        implementor.bind(converter, varRow);
        implementor.generateParentBody(converter, whileBody);

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
                            implementor.visitJavaChild(converter, 0,
                                (JavaRel) converter.child))),
                    
        // while (resultSet.next()) {
        new WhileStatement(new MethodCall(varRs, "next", null),
                        
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
                                        new MethodCall(varRs, "close", null))),
                                new CatchList(
                                    new CatchBlock(
                                        new Parameter(
                                            new TypeName(
                                                "java.sql.SQLException"),
                                            varEx.toString()),
                                        new StatementList()))))))));
        return null;
    }

    private static String getJdbcGetterName(final OJClass ojClass)
    {
        final String getter = (String) jdbcGetterMap.get(ojClass);
        if (getter == null) {
            return "getObject";
        } else {
            return getter;
        }
    }

    private static HashMap createJdbcGetterMap()
    {
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
}


// End ResultSetToJavaConvertlet.java
