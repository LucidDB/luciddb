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

import openjava.mop.OJClass;
import openjava.mop.OJField;
import openjava.ptree.*;

import org.eigenbase.oj.rel.JavaRel;
import org.eigenbase.oj.rel.JavaRelImplementor;
import org.eigenbase.oj.util.OJUtil;
import org.eigenbase.rel.convert.ConverterRel;
import org.eigenbase.relopt.CallingConvention;
import org.eigenbase.runtime.ResultSetIterator;
import org.eigenbase.util.Util;


/**
 * Thunk to convert between {@link CallingConvention#RESULT_SET result-set}
 * and {@link CallingConvention#ITERATOR iterator} calling-conventions.
 *
 * @author jhyde
 * @since May 27, 2004
 * @version $Id$
 **/
public class ResultSetToIteratorConvertlet extends JavaConvertlet
{
    public ResultSetToIteratorConvertlet()
    {
        super(CallingConvention.RESULT_SET, CallingConvention.ITERATOR);
    }

    public ParseTree implement(
        JavaRelImplementor implementor,
        ConverterRel converter)
    {
        Object o =
            implementor.visitJavaChild(converter, 0, (JavaRel) converter.child);
        StatementList methodBody = new StatementList();
        Variable varResultSet = new Variable("resultSet");
        OJClass rowClass = OJUtil.typeToOJClass(converter.rowType);
        if (converter.rowType.isProject()) {
            Variable varRow = implementor.newVariable();
            methodBody.add(
                new VariableDeclaration(
                    TypeName.forOJClass(rowClass),
                    varRow.toString(),
                    new AllocationExpression(
                        rowClass,
                        new ExpressionList())));

            // <<RowType>> row = new <<RowType>>();
            // row.fieldA = resultSet.get<<Type>>(1);
            // ...
            // return row;
            OJField [] fields = rowClass.getDeclaredFields();
            for (int i = 0; i < fields.length; i++) {
                OJField field = fields[i];
                methodBody.add(
                    new ExpressionStatement(
                        new AssignmentExpression(
                            new FieldAccess(
                                varRow,
                                field.getName()),
                            AssignmentExpression.EQUALS,
                            new MethodCall(
                                varResultSet,
                                getResultSetAccessorMethod(field.getType()),
                                new ExpressionList(Literal.makeLiteral(i + 1))))));
            }
            methodBody.add(new ReturnStatement(varRow));
        } else {
            methodBody.add(
                new ReturnStatement(
                    new AllocationExpression(
                        rowClass,
                        new ExpressionList(varResultSet))));
        }
        return new AllocationExpression(
            TypeName.forOJClass(OJClass.forClass(ResultSetIterator.class)),
            new ExpressionList(
                new CastExpression(
                    TypeName.forOJClass(Util.clazzResultSet),
                    (Expression) o)),
            new MemberDeclarationList(
                new MethodDeclaration(
                    new ModifierList(ModifierList.PUBLIC),
                    TypeName.forOJClass(Util.clazzObject),
                    "makeRow",
                    new ParameterList(),
                    new TypeName [] { TypeName.forOJClass(
                            Util.clazzSQLException) },
                    methodBody)));
    }

    /**
     * Returns the name of the method in {@link java.sql.ResultSet} for
     * retrieving a particular type. For example, <code>getInt</code>
     * accesses <code>int</code>, <code>getString</code> accesses
     * <code>String</code>, <code>getObject</code> accesses most other kinds
     * of Object.
     */
    private static String getResultSetAccessorMethod(OJClass clazz)
    {
        if (clazz.isPrimitive()) {
            return "get" + toInitcap(clazz.getName());
        } else if (Util.clazzString.isAssignableFrom(clazz)) {
            return "getString";
        } else {
            return "getObject";
        }
    }

    private static String toInitcap(String s)
    {
        return s.substring(0, 1).toUpperCase() + s.substring(1);
    }
}


// End ResultSetToIteratorConvertlet.java
