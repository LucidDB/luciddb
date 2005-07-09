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

package org.eigenbase.sql.test;

import junit.framework.TestCase;
import junit.framework.Assert;
import org.eigenbase.reltype.RelDataType;
import org.eigenbase.sql.type.SqlTypeName;
import org.eigenbase.sql.SqlOperator;


/**
 * Abstract implementation of {@link SqlTester}. A derived class only needs
 * to implement {@link #check} and {@link #checkType}.
 *
 * @author wael
 * @since May 22, 2004
 * @version $Id$
 **/
public abstract class AbstractSqlTester implements SqlTester
{
    // ~ Constants ------------------------------------------------------------
    public static final TypeChecker IntegerTypeChecker =
        new SqlTypeChecker(SqlTypeName.Integer);

    public static final TypeChecker BooleanTypeChecker =
        new SqlTypeChecker(SqlTypeName.Boolean);

    /**
     * Checker which allows any type.
     */
    public static final TypeChecker AnyTypeChecker = new TypeChecker()
    {
        public void checkType(RelDataType type)
        {
        }
    };

    // ~ Members --------------------------------------------------------------

    private SqlOperator operator;

    //~ Methods ---------------------------------------------------------------

    public void isFor(SqlOperator operator)
    {
        Assert.assertNull("isFor() called twice", operator);
        this.operator = operator;
    }

    /**
     * Helper method which converts a scalar expression into a SQL query.
     *
     * <p>By default, "expr" becomes "VALUES (expr)". Derived
     * classes may override.
     */
    protected String buildQuery(String expression)
    {
        return "values (" + expression + ")";
    }

    public void checkScalarExact(
        String expression,
        String result)
    {
        String sql = buildQuery(expression);
        check(sql, IntegerTypeChecker, result, 0);
    }

    public void checkScalarApprox(
        String expression,
        String expectedType,
        double expectedResult,
        double delta)
    {
        String sql = buildQuery(expression);
        TypeChecker typeChecker =
            expectedType.startsWith("todo:") &&
            !SqlOperatorTests.bug315Fixed  ?
            AnyTypeChecker :
            new StringTypeChecker(expectedType);
        check(sql, typeChecker, new Double(expectedResult), delta);
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
                BooleanTypeChecker, result.toString(),
                0);
        }
    }

    public void checkString(
        String expression,
        String result,
        String expectedType)
    {
        String sql = buildQuery(expression);
        TypeChecker typeChecker =
            expectedType.startsWith("todo:") &&
            !SqlOperatorTests.bug315Fixed ?
            AnyTypeChecker :
            new StringTypeChecker(expectedType);
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
        check(buildQuery(expression), AnyTypeChecker, result, 0);
    }

    /**
     * Returns the operator this test is for.
     * Throws if no operator has been set.
     */
    protected SqlOperator getFor()
    {
        Assert.assertNotNull("Must call isFor()", operator);
        return operator;
    }

    /**
     * Checks that a type matches a given SQL type. Does not care about
     * nullability.
     */
    private static class SqlTypeChecker implements TypeChecker
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

    public static class StringTypeChecker implements TypeChecker
    {
        private final String expected;

        public StringTypeChecker(String expected)
        {
            this.expected = expected;
        }

        public void checkType(RelDataType type)
        {
            String actual = type.toString();
            if (!type.isNullable()) {
                actual += " NOT NULL";
            }
            TestCase.assertEquals(expected, actual);
        }
    }
}

// End AbstractSqlTester.java
