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

import org.eigenbase.reltype.*;
import org.eigenbase.sql.*;


/**
 * SqlTester defines a callback for testing SQL queries and expressions.
 *
 * <p>The idea is that when you define an operator (or another piece of SQL
 * functionality), you can define the logical behavior of that operator once, as
 * part of that operator. Later you can define one or more physical
 * implementations of that operator, and test them all using the same set of
 * tests.
 *
 * <p>Specific implementations of <code>SqlTestser</code> might evaluate the
 * queries in different ways, for example, using a C++ versus Java calculator.
 * An implementation might even ignore certain calls altogether.
 *
 * @author Wael Chatila
 * @version $Id$
 * @since May 22, 2004
 */
public interface SqlTester
{
    //~ Methods ----------------------------------------------------------------

    /**
     * Tests that a scalar SQL expression returns the expected result and the
     * expected type. For example,
     *
     * <blockquote>
     * <pre>checkScalar("1.1 + 2.9", "4.0", "DECIMAL(2, 1) NOT NULL");</pre>
     * </blockquote>
     *
     * @param expression Scalar expression
     * @param result Expected result
     * @param resultType Expected result type
     */
    void checkScalar(
        String expression,
        Object result,
        String resultType);

    /**
     * Tests that a scalar SQL expression returns the expected exact numeric
     * result as an integer. For example,
     *
     * <blockquote>
     * <pre>checkScalarExact("1 + 2", "3");</pre>
     * </blockquote>
     *
     * @param expression Scalar expression
     * @param result Expected result
     */
    void checkScalarExact(
        String expression,
        String result);

    /**
     * Tests that a scalar SQL expression returns the expected exact numeric
     * result. For example,
     *
     * <blockquote>
     * <pre>checkScalarExact("1 + 2", "3");</pre>
     * </blockquote>
     *
     * @param expression Scalar expression
     * @param expectedType Type we expect the result to have, including
     * nullability, precision and scale, for example <code>DECIMAL(2, 1) NOT
     * NULL</code>.
     * @param result Expected result
     */
    void checkScalarExact(
        String expression,
        String expectedType,
        String result);

    /**
     * Tests that a scalar SQL expression returns expected appoximate numeric
     * result. For example,
     *
     * <blockquote>
     * <pre>checkScalarApprox("1.0 + 2.1", "3.1");</pre>
     * </blockquote>
     *
     * @param expression Scalar expression
     * @param expectedType Type we expect the result to have, including
     * nullability, precision and scale, for example <code>DECIMAL(2, 1) NOT
     * NULL</code>.
     * @param expectedResult Expected result
     * @param delta Allowed margin of error between expected and actual result
     */
    void checkScalarApprox(
        String expression,
        String expectedType,
        double expectedResult,
        double delta);

    /**
     * Tests that a scalar SQL expression returns the expected boolean result.
     * For example,
     *
     * <blockquote>
     * <pre>checkScalarExact("TRUE AND FALSE", Boolean.TRUE);</pre>
     * </blockquote>
     *
     * The expected result can be null:
     *
     * <blockquote>
     * <pre>checkScalarExact("NOT UNKNOWN", null);</pre>
     * </blockquote>
     *
     * @param expression Scalar expression
     * @param result Expected result (null signifies NULL).
     */
    void checkBoolean(
        String expression,
        Boolean result);

    /**
     * Tests that a scalar SQL expression returns the expected string result.
     * For example,
     *
     * <blockquote>
     * <pre>checkScalarExact("'ab' || 'c'", "abc");</pre>
     * </blockquote>
     *
     * @param expression Scalar expression
     * @param result Expected result
     * @param resultType
     */
    void checkString(String expression,
        String result, String resultType);

    /**
     * Tests that a SQL expression returns the SQL NULL value. For example,
     *
     * <blockquote>
     * <pre>checkNull("CHAR_LENGTH(CAST(NULL AS VARCHAR(3))");</pre>
     * </blockquote>
     *
     * @param expression Scalar expression
     */
    void checkNull(String expression);

    /**
     * Tests that a SQL expression has a given type. For example,
     *
     * <blockquote>
     * <pre>checkType("SUBSTR('hello' FROM 1 FOR 3)", "VARCHAR(3) NOT NULL");</pre>
     * </blockquote>
     *
     * This method checks length/precision, scale, and whether the type allows
     * NULL values, so is more precise than the type-checking done by methods
     * such as {@link #checkScalarExact}.
     *
     * @param expression Scalar expression
     * @param type Type string
     */
    void checkType(
        String expression,
        String type);

    /**
     * Tests that a SQL query returns a single column with the given type. For
     * example,
     *
     * <blockquote>
     * <pre>check("VALUES (1 + 2)", "3", SqlTypeName.Integer);</pre>
     * </blockquote>
     *
     * <p>If <code>result</code> is null, the expression must yield the SQL NULL
     * value. If <code>result</code> is a {@link java.util.regex.Pattern}, the
     * result must match that pattern.
     *
     * @param query SQL query
     * @param typeChecker Checks whether the result is the expected type; must
     * not be null
     * @param result Expected result
     * @param delta The acceptable tolerance between the expected and actual
     */
    void check(
        String query,
        TypeChecker typeChecker,
        Object result,
        double delta);

    /**
     * Declares that this test is for a given operator. So we can check that all
     * operators are tested.
     */
    void setFor(SqlOperator operator);

    /**
     * Checks that an aggregate expression returns the expected result.
     *
     * <p>For example, <code>checkAgg("AVG(DISTINCT x)", new String[] {"2", "3",
     * null, "3" }, new Double(2.5), 0);</code>
     *
     * @param expr Aggregate expression, e.g. <code>SUM(DISTINCT x)</code>
     * @param inputValues Array of input values, e.g. <code>["1", null,
     * "2"]</code>.
     * @param result
     * @param delta
     */
    void checkAgg(String expr,
        String [] inputValues, Object result, int delta);

    /**
     * Tests that a scalar SQL expression fails at run time.
     *
     * @param expression SQL scalar expression
     * @param expectedError Pattern for expected error. If !runtime, must
     * include an error location.
     * @param runtime If true, must fail at runtime; if false, must fail at
     * validate time
     */
    void checkFails(
        String expression,
        String expectedError,
        boolean runtime);

    //~ Inner Interfaces -------------------------------------------------------

    interface TypeChecker
    {
        void checkType(RelDataType type);
    }
}

// End SqlTester.java
