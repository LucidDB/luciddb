/*
// $Id$
// Package org.eigenbase is a class library of database components.
// Copyright (C) 2002-2004 Disruptive Tech
// Copyright (C) 2003-2004 John V. Sichi
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

package org.eigenbase.sql;

import org.eigenbase.sql.type.SqlTypeName;
import org.eigenbase.util.BarfingInvocationHandler;
import org.eigenbase.util.Util;

import java.lang.reflect.Proxy;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;


/**
 * Contains utility functions related to SQL parsing, all static.
 *
 * @author jhyde
 * @since Nov 26, 2003
 * @version $Id$
 */
public abstract class SqlUtil
{
    //~ Static fields/initializers --------------------------------------------

    /**
     * A {@link SqlDialect} useful for generating generic SQL. If you need to
     * do something database-specific like quoting identifiers, don't rely on
     * this dialect to do what you want.
     */
    public static final SqlDialect dummyDialect =
        new SqlDialect(dummyDatabaseMetaData());

    //~ Methods ---------------------------------------------------------------

    static SqlNode andExpressions(
        SqlNode node1,
        SqlNode node2)
    {
        if (node1 == null) {
            return node2;
        }
        ArrayList list = new ArrayList();
        if (node1.isA(SqlKind.And)) {
            list.addAll(Arrays.asList(((SqlCall) node1).operands));
        } else {
            list.add(node1);
        }
        if (node2.isA(SqlKind.And)) {
            list.addAll(Arrays.asList(((SqlCall) node2).operands));
        } else {
            list.add(node2);
        }
        return SqlOperatorTable.std().andOperator.createCall((SqlNode []) list
                .toArray(new SqlNode[list.size()]), null);
    }

    static ArrayList flatten(SqlNode node)
    {
        ArrayList list = new ArrayList();
        flatten(node, list);
        return list;
    }

    /**
     * Returns the <code>n</code>th (0-based) input to a join expression.
     */
    public static SqlNode getFromNode(
        SqlSelect query,
        int ordinal)
    {
        ArrayList list = flatten(query.getFrom());
        return (SqlNode) list.get(ordinal);
    }

    private static void flatten(
        SqlNode node,
        ArrayList list)
    {
        switch (node.getKind().getOrdinal()) {
        case SqlKind.JoinORDINAL:
            SqlJoin join = (SqlJoin) node;
            flatten(
                join.getLeft(),
                list);
            flatten(
                join.getRight(),
                list);
            return;
        case SqlKind.AsORDINAL:
            SqlCall call = (SqlCall) node;
            flatten(call.operands[0], list);
            return;
        default:
            list.add(node);
            return;
        }
    }

    /**
     * Returns whether a node represents the NULL value.
     *
     * <p>Examples:<ul>
     * <li>For {@link SqlLiteral} Unknown, returns false.
     * <li>For <code>CAST(NULL AS <i>type</i>)</code>, returns true if
     *     <code>allowCast</code> is true, false otherwise.
     * <li>For <code>CAST(CAST(NULL AS <i>type</i>) AS <i>type</i>))</code>,
     *     returns false.
     * </ul>
     */
    public static boolean isNullLiteral(
        SqlNode node,
        boolean allowCast)
    {
        if (node instanceof SqlLiteral) {
            SqlLiteral literal = (SqlLiteral) node;
            if (literal.typeName == SqlTypeName.Null) {
                assert (null == literal.getValue());
                return true;
            } else {
                // We don't regard UNKNOWN -- SqlLiteral(null,Boolean) -- as NULL.
                return false;
            }
        }
        if (allowCast) {
            if (node.isA(SqlKind.Cast)) {
                SqlCall call = (SqlCall) node;
                if (isNullLiteral(call.operands[0], false)) {
                    // node is "CAST(NULL as type)"
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * Returns whether a node is a literal.
     *
     * <p>Many constructs which require literals also accept
     * <code>CAST(NULL AS <i>type</i>)</code>. This method does not accept
     * casts, so you should call {@link #isNullLiteral} first.
     *
     * @param node The node, never null.
     * @return Whether the node is a literal
     * @pre node != null
     */
    public static boolean isLiteral(SqlNode node)
    {
        Util.pre(node != null, "node != null");
        return node instanceof SqlLiteral;
    }

    /**
     * Unparse a call to an operator which has function syntax.
     *
     * @param operator The operator
     * @param writer Writer
     * @param operands List of 0 or more operands
     * @param emptyParens Whether to print parentheses if there are 0 operands
     */
    public static void unparseFunctionSyntax(
        SqlOperator operator,
        SqlWriter writer,
        SqlNode [] operands,
        boolean emptyParens)
    {
        writer.print(operator.name);
        if (operands.length == 0 && !emptyParens) {
            // For example, the "LOCALTIME" function appears as "LOCALTIME"
            // when it has 0 args, not "LOCALTIME()".
            return;
        }
        writer.print('(');
        for (int i = 0; i < operands.length; i++) {
            SqlNode operand = operands[i];
            if (i > 0) {
                writer.print(", ");
            }
            operand.unparse(writer,0,0);
        }
        writer.print(')');
    }

    public static void unparseBinarySyntax(
        SqlOperator operator,
        SqlNode[] operands,
        SqlWriter writer,
        int leftPrec,
        int rightPrec)
    {
        SqlBinaryOperator binop = (SqlBinaryOperator) operator;
        assert operands.length == 2;
        operands[0].unparse(writer,leftPrec,binop.leftPrec);
        if (binop.needsSpace()) {
            writer.print(' ');
            writer.print(binop.name);
            writer.print(' ');
        } else {
            writer.print(binop.name);
        }
        operands[1].unparse(writer,binop.rightPrec,rightPrec);
    }

    /**
     * Creates a {@link DatabaseMetaData} object good enough to create a
     * {@link SqlDialect} object with, but not good for much else.
     */
    private static DatabaseMetaData dummyDatabaseMetaData()
    {
        return (DatabaseMetaData) Proxy.newProxyInstance(
            null,
            new Class [] { DatabaseMetaData.class },
            new DatabaseMetaDataInvocationHandler());
    }

    /**
     * Concatenates string literals.
     *
     * <p>This method takes an array of arguments,
     * since pairwise concatenation means too much string copying.
     *
     * @param lits an array of {@link SqlLiteral}, not empty, all of the same
     *     class
     * @return a new {@link SqlLiteral}, of that same class, whose value is the
     *     string concatenation of the values of the literals
     * @throws ClassCastException if the lits are not homogeneous.
     * @throws ArrayIndexOutOfBoundsException if lits is an empty array.
     */
    public static SqlLiteral concatenateLiterals(SqlLiteral [] lits)
    {
        if (lits.length == 1) {
            return lits[0]; // nothing to do
        }
        return ((SqlAbstractStringLiteral) lits[0]).concat1(lits);
    }

    //~ Inner Classes ---------------------------------------------------------

    /**
     * Handles particular {@link DatabaseMetaData} methods; invocations of
     * other methods will fall through to the base class, {@link
     * BarfingInvocationHandler}, which will throw an error.
     */
    public static class DatabaseMetaDataInvocationHandler
        extends BarfingInvocationHandler
    {
        public String getDatabaseProductName()
            throws SQLException
        {
            return "fooBar";
        }

        public String getIdentifierQuoteString()
            throws SQLException
        {
            return "`";
        }
    }
}


// End SqlUtil.java
