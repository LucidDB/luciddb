/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2002-2005 Disruptive Tech
// Copyright (C) 2005-2005 LucidEra, Inc.
// Portions Copyright (C) 2003-2005 John V. Sichi
//
// This program is free software; you can redistribute it and/or modify it
// under the terms of the GNU General Public License as published by the Free
// Software Foundation; either version 2 of the License, or (at your option)
// any later version approved by The Eigenbase Project.
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

import org.eigenbase.sql.fun.*;
import org.eigenbase.sql.type.SqlTypeName;
import org.eigenbase.sql.parser.SqlParserPos;
import org.eigenbase.sql.validate.SqlValidatorException;
import org.eigenbase.util.*;
import org.eigenbase.reltype.*;
import org.eigenbase.resource.EigenbaseResource;

import java.lang.reflect.Proxy;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.util.*;
import java.text.*;

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
        return SqlStdOperatorTable.andOperator.createCall(
            (SqlNode []) list.toArray(new SqlNode[list.size()]),
            SqlParserPos.ZERO);
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
     * Converts an SqlNode array to a SqlNodeList
     */
    public static SqlNodeList toNodeList(SqlNode[] operands) {
        SqlNodeList ret = new SqlNodeList(SqlParserPos.ZERO);
        for (int i = 0; i < operands.length; i++) {
            SqlNode node = operands[i];
            ret.add(node);
        }
        return ret;
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
            if (literal.getTypeName() == SqlTypeName.Null) {
                assert (null == literal.getValue());
                return true;
            } else {
                // We don't regard UNKNOWN -- SqlLiteral(null,Boolean) -- as
                // NULL.
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
     * Returns whether a node represents the NULL value or a series of nested
     * CAST(NULL as <TYPE>) calls
     * <br>
     * For Example:<br>
     * isNull(CAST(CAST(NULL as INTEGER) AS VARCHAR(1))) returns true
     */
    public static boolean isNull(SqlNode node)
    {
        return isNullLiteral(node, false)
            || ((node.getKind() == SqlKind.Cast)
            && isNull(((SqlCall) node).operands[0]));
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
     * Unparses a call to an operator which has function syntax.
     *
     * @param operator The operator
     * @param writer Writer
     * @param operands List of 0 or more operands
     * @param emptyParens Whether to print parentheses if there are 0 operands
     * @param quantifier
     */
    public static void unparseFunctionSyntax(SqlOperator operator,
        SqlWriter writer,
        SqlNode[] operands,
        boolean emptyParens,
        SqlLiteral quantifier)
    {
        if (operator instanceof SqlFunction) {
            SqlFunction function = (SqlFunction) operator;

            if (function.getFunctionType()
                == SqlFunctionCategory.UserDefinedSpecificFunction)
            {
                writer.print("SPECIFIC ");
            }
            SqlIdentifier id = function.getSqlIdentifier();
            if (id == null) {
                writer.print(operator.getName());
            } else {
                id.unparse(writer, 0, 0);
            }
        } else {
            writer.print(operator.getName());
        }
        if (operands.length == 0 && !emptyParens) {
            // For example, the "LOCALTIME" function appears as "LOCALTIME"
            // when it has 0 args, not "LOCALTIME()".
            return;
        }
        writer.print('(');
        if (null != quantifier) {
            quantifier.unparse(writer,0,0);
            writer.print(" ");
        }
        for (int i=0; i < operands.length; i++) {
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
        operands[0].unparse(writer,leftPrec,binop.getLeftPrec());
        if (binop.needsSpace()) {
            writer.print(' ');
            writer.print(binop.getName());
            writer.print(' ');
        } else {
            writer.print(binop.getName());
        }
        operands[1].unparse(writer,binop.getRightPrec(),rightPrec);
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

    /**
     * Looks up a (possibly overloaded) routine based on name
     * and argument types.
     *
     * @param opTab operator table to search
     *
     * @param funcName name of function being invoked
     *
     * @param argTypes argument types
     *
     * @param category whether a function or a procedure.
     *       (If a procedure is being invoked, the overload rules are simpler.)
     *
     * @return matching routine, or null if none found
     *
     * @sql.99 Part 2 Section 10.4
     */
    public static SqlFunction lookupRoutine(
        SqlOperatorTable opTab,
        SqlIdentifier funcName,
        RelDataType [] argTypes,
        SqlFunctionCategory category)
    {
        List list = lookupSubjectRoutines(
            opTab, funcName, argTypes, category);
        if (list.isEmpty()) {
            return null;
        } else {
            // return first on schema path
            return (SqlFunction) list.get(0);
        }
    }

    /**
     * Looks up all subject routines matching the given name
     * and argument types.
     *
     * @param opTab operator table to search
     *
     * @param funcName name of function being invoked
     *
     * @param argTypes argument types
     *
     * @param category category of routine to look up
     *
     * @return list of matching routines
     *
     * @sql.99 Part 2 Section 10.4
     */
    public static List lookupSubjectRoutines(
        SqlOperatorTable opTab,
        SqlIdentifier funcName,
        RelDataType [] argTypes,
        SqlFunctionCategory category)
    {
        // start with all routines matching by name
        List routines = opTab.lookupOperatorOverloads(
            funcName, category, SqlSyntax.Function);

        // first pass:  eliminate routines which don't accept the given
        // number of arguments
        filterRoutinesByParameterCount(routines, argTypes);

        // NOTE: according to SQL99, procedures are NOT overloaded on type,
        // only on number of arguments.
        if (category == SqlFunctionCategory.UserDefinedProcedure) {
            return routines;
        }

        // second pass:  eliminate routines which don't accept the given
        // argument types
        filterRoutinesByParameterType(routines, argTypes);

        // see if we can stop now; this is necessary for the case
        // of builtin functions where we don't have param type info
        if (routines.size() < 2) {
            return routines;
        }

        // third pass:  for each parameter from left to right, eliminate
        // all routines except those with the best precedence match for
        // the given arguments
        filterRoutinesByTypePrecedence(routines, argTypes);

        return routines;
    }

    private static void filterRoutinesByParameterCount(
        List routines,
        RelDataType [] argTypes)
    {
        Iterator iter = routines.iterator();
        while (iter.hasNext()) {
            SqlFunction function = (SqlFunction) iter.next();
            SqlOperandCountRange od =
                function.getOperandCountRange();
            if (!od.isVariadic()
                && !od.getAllowedList().contains(
                    new Integer(argTypes.length)))
            {
                iter.remove();
            }
        }
    }

    /**
     * @sql.99 Part 2 Section 10.4 Syntax Rule 6.b.iii.2.B
     */
    private static void filterRoutinesByParameterType(
        List routines,
        RelDataType [] argTypes)
    {
        Iterator iter = routines.iterator();
        while (iter.hasNext()) {
            SqlFunction function = (SqlFunction) iter.next();
            RelDataType [] paramTypes = function.getParamTypes();
            if (paramTypes == null) {
                // no parameter information for builtins; keep for now
                continue;
            }
            assert(paramTypes.length == argTypes.length);
            boolean keep = true;
            for (int i = 0; i < paramTypes.length; ++i) {
                RelDataTypePrecedenceList precList =
                    argTypes[i].getPrecedenceList();
                if (!precList.containsType(paramTypes[i])) {
                    keep = false;
                    break;
                }
            }
            if (!keep) {
                iter.remove();
            }
        }
    }

    /**
     * @sql.99 Part 2 Section 9.4
     */
    private static void filterRoutinesByTypePrecedence(
        List routines,
        RelDataType [] argTypes)
    {
        for (int i = 0; i < argTypes.length; ++i) {
            RelDataTypePrecedenceList precList =
                argTypes[i].getPrecedenceList();
            RelDataType bestMatch = null;
            Iterator iter = routines.iterator();
            while (iter.hasNext()) {
                SqlFunction function = (SqlFunction) iter.next();
                RelDataType [] paramTypes = function.getParamTypes();
                if (paramTypes == null) {
                    continue;
                }
                if (bestMatch == null) {
                    bestMatch = paramTypes[i];
                } else {
                    int c = precList.compareTypePrecedence(
                        bestMatch, paramTypes[i]);
                    if (c < 0) {
                        bestMatch = paramTypes[i];
                    }
                }
            }
            if (bestMatch != null) {
                iter = routines.iterator();
                while (iter.hasNext()) {
                    SqlFunction function = (SqlFunction) iter.next();
                    RelDataType [] paramTypes = function.getParamTypes();
                    int c;
                    if (paramTypes == null) {
                        c = -1;
                    } else {
                        c = precList.compareTypePrecedence(
                            paramTypes[i], bestMatch);
                    }
                    if (c < 0) {
                        iter.remove();
                    }
                }
            }
        }
    }

    /**
     * Returns the <code>i</code>th select-list item of a query.
     */
    public static SqlNode getSelectListItem(SqlNode query, int i) {
        if (query instanceof SqlSelect) {
            SqlSelect select = (SqlSelect) query;
            final SqlNode from = select.getFrom();
            if (from.isA(SqlKind.Values)) {
                // They wrote "VALUES (x, y)", but the validator has
                // converted this into "SELECT * FROM VALUES (x, y)".
                return getSelectListItem(from, i);
            }
            final SqlNodeList fields = select.getSelectList();
            // Range check the index to avoid index out of range.  This
            // could be expanded to actually check to see if the select
            // list is a "*"
            if (i >= fields.size()) {
                i = 0;
            }
            return fields.get(i);
        } else if (query.isA(SqlKind.Values)) {
            SqlCall call = (SqlCall) query;
            Util.permAssert(call.operands.length > 0,
                "VALUES must have at least one operand");
            final SqlCall row = (SqlCall) call.operands[0];
            Util.permAssert(row.operands.length > i, "VALUES has too few columns");
            return row.operands[i];
        } else {
            // Unexpected type of query.
            throw Util.needToImplement(query);
        }
    }

    /**
     * If an identifier is a legitimate call to a function which has no
     * arguments and requires no parentheses (for example "CURRENT_USER"),
     * returns a call to that function, otherwise returns null.
     */
    public static SqlCall makeCall(
        SqlOperatorTable opTab,
        SqlIdentifier id)
    {
        if (id.names.length == 1) {
            List list = opTab.lookupOperatorOverloads(
                id, null, SqlSyntax.Function);
            for (int i = 0; i < list.size(); i++) {
                SqlOperator operator = (SqlOperator) list.get(i);
                if (operator.getSyntax() == SqlSyntax.FunctionId) {
                    // Even though this looks like an identifier, it is a
                    // actually a call to a function. Construct a fake
                    // call to this function, so we can use the regular
                    // operator validation.
                    return new SqlCall(operator, SqlNode.emptyArray,
                        id.getParserPosition());
                }
            }
        }
        return null;
    }

    public static String deriveAliasFromOrdinal(int ordinal)
    {
        // Use a '$' so that queries can't easily reference the
        // generated name.
        return "EXPR$" + ordinal;
    }

    /**
     * Constructs an operator signature from a type list.
     *
     * @param op operator
     *
     * @param typeList list of types to use for operands
     *
     * @return constructed signature
     */
    public static String getOperatorSignature(SqlOperator op, List typeList)
    {
        return getAliasedSignature(op, op.getName(), typeList);
    }

    /**
     * Constructs an operator signature from a type list, substituting an
     * alias for the operator name.
     *
     * @param op operator
     *
     * @param opName name to use for operator
     *
     * @param typeList list of types to use for operands
     *
     * @return constructed signature
     */
    public static String getAliasedSignature(
        SqlOperator op,
        String opName, List typeList)
    {
        StringBuffer ret = new StringBuffer();
        String template = op.getSignatureTemplate(typeList.size());
        if (null == template) {
            ret.append("'");
            ret.append(opName);
            ret.append("(");
            for (int i = 0; i < typeList.size(); i++) {
                if (i > 0) {
                    ret.append(", ");
                }
                ret.append(
                    "<" + typeList.get(i).toString().toUpperCase() + ">");
            }
            ret.append(")'");
        } else {
            Object [] values = new Object[typeList.size() + 1];
            values[0] = opName;
            ret.append("'");
            for (int i = 0; i < typeList.size(); i++) {
                values[i + 1] = "<" +
                    typeList.get(i).toString().toUpperCase() + ">";
            }
            ret.append(MessageFormat.format(template, values));
            ret.append("'");
            assert (typeList.size() + 1) == values.length;
        }

        return ret.toString();
    }

    /**
     * Wraps an exception with context.
     */
    public static EigenbaseException newContextException(
        final SqlParserPos pos,
        Throwable e)
    {
        int line = pos.getLineNum();
        int col = pos.getColumnNum();
        int endLine = pos.getEndLineNum();
        int endCol = pos.getEndColumnNum();
        return newContextException(line, col, endLine, endCol, e);
    }

    /**
     * Wraps an exception with context.
     */
    public static EigenbaseException newContextException(
        int line,
        int col,
        int endLine,
        int endCol,
        Throwable e)
    {
        EigenbaseContextException contextExcn =
            line == endLine && col == endCol ?
            EigenbaseResource.instance().newValidatorContextPoint(
                new Integer(line),
                new Integer(col),
                e) :
            EigenbaseResource.instance().newValidatorContext(
                new Integer(line),
                new Integer(col),
                new Integer(endLine),
                new Integer(endCol),
                e);
        contextExcn.setPosition(line, col, endLine, endCol);
        return contextExcn;
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
