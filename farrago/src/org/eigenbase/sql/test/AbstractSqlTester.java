/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005-2007 The Eigenbase Project
// Copyright (C) 2002-2007 SQLstream, Inc.
// Copyright (C) 2005-2007 LucidEra, Inc.
// Portions Copyright (C) 2003-2007 John V. Sichi
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
package org.eigenbase.sql.test;

import junit.framework.*;

import org.eigenbase.reltype.*;
import org.eigenbase.sql.*;
import org.eigenbase.sql.type.*;


/**
 * Abstract implementation of {@link SqlTester}. A derived class only needs to
 * implement {@link #check}, {@link #checkColumnType} and {@link #checkFails}.
 *
 * @author wael
 * @version $Id$
 * @since May 22, 2004
 */
public abstract class AbstractSqlTester
    implements SqlTester
{
    //~ Static fields/initializers ---------------------------------------------

    // ~ Constants ------------------------------------------------------------
    public static final TypeChecker IntegerTypeChecker =
        new SqlTypeChecker(SqlTypeName.INTEGER);

    public static final TypeChecker BooleanTypeChecker =
        new SqlTypeChecker(SqlTypeName.BOOLEAN);

    /**
     * Checker which allows any type.
     */
    public static final TypeChecker AnyTypeChecker =
        new TypeChecker() {
            public void checkType(RelDataType type)
            {
            }
        };

    //~ Instance fields --------------------------------------------------------

    // ~ Members --------------------------------------------------------------

    private SqlOperator operator;

    //~ Methods ----------------------------------------------------------------

    public void setFor(SqlOperator operator, VmName ... unimplementedVmNames)
    {
        if ((operator != null) && (this.operator != null)) {
            throw new AssertionFailedError("isFor() called twice");
        }
        this.operator = operator;
    }

    public void checkAgg(
        String expr,
        String [] inputValues,
        Object result,
        double delta)
    {
        String query = generateAggQuery(expr, inputValues);
        check(query, AnyTypeChecker, result, delta);
    }

    public void checkWinAgg(
        String expr,
        String [] inputValues,
        String windowSpec,
        String type,
        Object result,
        double delta)
    {
        // Windowed aggregation is not implemented in eigenbase. We cannot
        // evaluate the query to check results. The best we can do is to check
        // the type.
        String query = generateWinAggQuery(expr, windowSpec, inputValues);
        checkColumnType(query, type);
    }

    /**
     * Helper function to get the string representation of a RelDataType
     * (include precision/scale but no charset or collation)
     *
     * @param sqlType Type
     *
     * @return String representation of type
     */
    public static String getTypeString(RelDataType sqlType)
    {
        switch (sqlType.getSqlTypeName()) {
        case VARCHAR:
            String actual = "VARCHAR(" + sqlType.getPrecision() + ")";
            return sqlType.isNullable() ? actual : (actual + " NOT NULL");
        case CHAR:
            actual = "CHAR(" + sqlType.getPrecision() + ")";
            return sqlType.isNullable() ? actual : (actual + " NOT NULL");
        default:

            // Get rid of the verbose charset/collation stuff.
            // TODO: There's probably a better way to do this.
            final String s = sqlType.getFullTypeString();
            return s.replace(
                " CHARACTER SET \"ISO-8859-1\" COLLATE \"ISO-8859-1$en_US$primary\"",
                "");
        }
    }

    public static String generateAggQuery(String expr, String [] inputValues)
    {
        StringBuilder buf = new StringBuilder();
        buf.append("SELECT ").append(expr).append(" FROM (");
        for (int i = 0; i < inputValues.length; i++) {
            if (i > 0) {
                buf.append(" UNION ALL ");
            }
            buf.append("SELECT ");
            String inputValue = inputValues[i];
            buf.append(inputValue).append(" AS x FROM (VALUES (1))");
        }
        buf.append(")");
        return buf.toString();
    }

    public static String generateWinAggQuery(
        String expr,
        String windowSpec,
        String [] inputValues)
    {
        StringBuilder buf = new StringBuilder();
        buf.append("SELECT ").append(expr).append(" OVER (").append(windowSpec)
        .append(") FROM (");
        for (int i = 0; i < inputValues.length; i++) {
            if (i > 0) {
                buf.append(" UNION ALL ");
            }
            buf.append("SELECT ");
            String inputValue = inputValues[i];
            buf.append(inputValue).append(" AS x FROM (VALUES (1))");
        }
        buf.append(")");
        return buf.toString();
    }

    /**
     * Helper method which converts a scalar expression into a SQL query.
     *
     * <p>By default, "expr" becomes "VALUES (expr)". Derived classes may
     * override.
     *
     * @param expression Expression
     *
     * @return Query that returns expression
     */
    protected String buildQuery(String expression)
    {
        return "values (" + expression + ")";
    }

    public void checkType(
        String expression,
        String type)
    {
        checkColumnType(buildQuery(expression), type);
    }

    public void checkScalarExact(
        String expression,
        String result)
    {
        String sql = buildQuery(expression);
        check(sql, IntegerTypeChecker, result, 0);
    }

    public void checkScalarExact(
        String expression,
        String expectedType,
        String result)
    {
        String sql = buildQuery(expression);
        TypeChecker typeChecker = new StringTypeChecker(expectedType);
        check(sql, typeChecker, result, 0);
    }

    public void checkScalarApprox(
        String expression,
        String expectedType,
        double expectedResult,
        double delta)
    {
        String sql = buildQuery(expression);
        TypeChecker typeChecker = new StringTypeChecker(expectedType);
        check(
            sql,
            typeChecker,
            expectedResult,
            delta);
    }

    public void checkBoolean(
        String expression,
        Boolean result)
    {
        String sql = buildQuery(expression);
        if (null == result) {
            checkNull(expression);
        } else {
            check(
                sql,
                BooleanTypeChecker,
                result.toString(),
                0);
        }
    }

    public void checkString(
        String expression,
        String result,
        String expectedType)
    {
        String sql = buildQuery(expression);
        TypeChecker typeChecker = new StringTypeChecker(expectedType);
        check(sql, typeChecker, result, 0);
    }

    public void checkNull(String expression)
    {
        String sql = buildQuery(expression);
        check(sql, AnyTypeChecker, null, 0);
    }

    public void checkScalar(
        String expression,
        Object result,
        String resultType)
    {
        checkType(expression, resultType);
        check(
            buildQuery(expression),
            AnyTypeChecker,
            result,
            0);
    }

    /**
     * Returns the operator this test is for. Throws if no operator has been
     * set.
     *
     * @return the operator this test is for, never null
     */
    protected SqlOperator getFor()
    {
        Assert.assertNotNull("Must call setFor()", operator);
        return operator;
    }

    //~ Inner Classes ----------------------------------------------------------

    /**
     * Checks that a type matches a given SQL type. Does not care about
     * nullability.
     */
    private static class SqlTypeChecker
        implements TypeChecker
    {
        private final SqlTypeName typeName;

        SqlTypeChecker(SqlTypeName typeName)
        {
            this.typeName = typeName;
        }

        public void checkType(RelDataType type)
        {
            TestCase.assertEquals(
                typeName.toString(),
                type.toString());
        }
    }

    /**
     * Type checker which compares types to a specified string.
     *
     * <p>The string contains "NOT NULL" constraints, but does not contain
     * collations and charsets. For example,
     *
     * <ul>
     * <li><code>INTEGER NOT NULL</code></li>
     * <li><code>BOOLEAN</code></li>
     * <li><code>DOUBLE NOT NULL MULTISET NOT NULL</code></li>
     * <li><code>CHAR(3) NOT NULL</code></li>
     * <li><code>RecordType(INTEGER X, VARCHAR(10) Y)</code></li>
     * </ul>
     */
    public static class StringTypeChecker
        implements TypeChecker
    {
        private final String expected;

        public StringTypeChecker(String expected)
        {
            this.expected = expected;
        }

        public void checkType(RelDataType type)
        {
            String actual = getTypeString(type);
            TestCase.assertEquals(expected, actual);
        }
    }
}

// End AbstractSqlTester.java
