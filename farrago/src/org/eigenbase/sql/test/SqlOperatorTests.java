/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005-2007 The Eigenbase Project
// Copyright (C) 2002-2007 Disruptive Tech
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

import java.math.*;

import java.text.*;

import java.util.*;
import java.util.regex.*;

import junit.framework.*;

import org.eigenbase.sql.*;
import org.eigenbase.sql.fun.*;
import org.eigenbase.sql.parser.*;
import org.eigenbase.sql.type.*;
import org.eigenbase.test.*;
import org.eigenbase.util.*;


/**
 * Contains unit tests for all operators. Each of the methods is named after an
 * operator.
 *
 * <p>The class is abstract. It contains a test for every operator, but does not
 * provide a mechanism to execute the tests: parse, validate, and execute
 * expressions on the operators. This is left to a {@link SqlTester} object
 * which the derived class must provide.
 *
 * <p>Different implementations of {@link SqlTester} are possible, such as:
 *
 * <ul>
 * <li>Execute against a real farrago database
 * <li>Execute in pure java (parsing and validation can be done, but expression
 * evaluation is not possible)
 * <li>Generate a SQL script.
 * <li>Analyze which operators are adequately tested.
 * </ul>
 *
 * <p>A typical method will be named after the operator it is testing (say
 * <code>testSubstringFunc</code>). It first calls {@link
 * SqlTester#setFor(SqlOperator)} to declare which operator it is testing.
 * <blockqoute>
 *
 * <pre><code>
 * public void testSubstringFunc() {
 *     getTester().setFor(SqlStdOperatorTable.substringFunc);
 *     getTester().checkScalar("sin(0)", "0");
 *     getTester().checkScalar("sin(1.5707)", "1");
 * }</code></pre>
 *
 * </blockqoute> The rest of the method contains calls to the various <code>
 * checkXxx</code> methods in the {@link SqlTester} interface. For an operator
 * to be adequately tested, there need to be tests for:
 *
 * <ul>
 * <li>Parsing all of its the syntactic variants.
 * <li>Deriving the type of in all combinations of arguments.
 *
 * <ul>
 * <li>Pay particular attention to nullability. For example, the result of the
 * "+" operator is NOT NULL if and only if both of its arguments are NOT
 * NULL.</li>
 * <li>Also pay attention to precsion/scale/length. For example, the maximum
 * length of the "||" operator is the sum of the maximum lengths of its
 * arguments.</li>
 * </ul>
 * </li>
 * <li>Executing the function. Pay particular attention to corner cases such as
 * null arguments or null results.</li>
 * </ul>
 *
 * @author Julian Hyde
 * @version $Id$
 * @since October 1, 2004
 */
public abstract class SqlOperatorTests
    extends TestCase
{
    //~ Static fields/initializers ---------------------------------------------

    public static final String NL = TestUtil.NL;

    // TODO: Change message when Fnl3Fixed to something like
    // "Invalid character for cast: PC=0 Code=22018"
    public static final String invalidCharMessage =
        Bug.Fnl3Fixed ? null : "(?s).*";

    // TODO: Change message when Fnl3Fixed to something like
    // "Overflow during calculation or cast: PC=0 Code=22003"
    public static final String outOfRangeMessage =
        Bug.Fnl3Fixed ? null : "(?s).*";

    // TODO: Change message when Fnl3Fixed to something like
    // "Division by zero: PC=0 Code=22012"
    public static final String divisionByZeroMessage =
        Bug.Fnl3Fixed ? null : "(?s).*";

    // TODO: Change message when Fnl3Fixed to something like
    // "String right truncation: PC=0 Code=22001"
    public static final String stringTruncMessage =
        Bug.Fnl3Fixed ? null : "(?s).*";

    // TODO: Change message when Fnl3Fixed to something like
    // "Invalid datetime format: PC=0 Code=22007"
    public static final String badDatetimeMessage =
        Bug.Fnl3Fixed ? null : "(?s).*";

    public static final String literalOutOfRangeMessage =
        "(?s).*Numeric literal.*out of range.*";

    public static final boolean todo = false;

    /**
     * Regular expression for a SQL TIME(0) value.
     */
    public static final Pattern timePattern =
        Pattern.compile(
            "[0-9][0-9]:[0-9][0-9]:[0-9][0-9]");

    /**
     * Regular expression for a SQL TIMESTAMP(3) value.
     */
    public static final Pattern timestampPattern =
        Pattern.compile(
            "[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9] "
            + "[0-9][0-9]:[0-9][0-9]:[0-9][0-9]\\.[0-9]+");

    /**
     * Regular expression for a SQL DATE value.
     */
    public static final Pattern datePattern =
        Pattern.compile(
            "[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]");

    public static final String [] numericTypeNames =
        new String[] {
            "TINYINT", "SMALLINT", "INTEGER", "BIGINT",
            "DECIMAL(5, 2)", "REAL", "FLOAT", "DOUBLE"
        };

    // REVIEW jvs 27-Apr-2006:  for Float and Double, MIN_VALUE
    // is the smallest positive value, not the smallest negative value
    public static final String [] minNumericStrings =
        new String[] {
            Long.toString(Byte.MIN_VALUE), Long.toString(
                Short.MIN_VALUE), Long.toString(Integer.MIN_VALUE),
            Long.toString(Long.MIN_VALUE), "-999.99",

            // NOTE jvs 26-Apr-2006:  Win32 takes smaller values from
            // win32_values.h
            "1E-37", /*Float.toString(Float.MIN_VALUE)*/
            "2E-307", /*Double.toString(Double.MIN_VALUE)*/
            "2E-307" /*Double.toString(Double.MIN_VALUE)*/,
        };

    public static final String [] minOverflowNumericStrings =
        new String[] {
            Long.toString(Byte.MIN_VALUE - 1),
            Long.toString(Short.MIN_VALUE - 1),
            Long.toString((long) Integer.MIN_VALUE - 1),
            (new BigDecimal(Long.MIN_VALUE)).subtract(BigDecimal.ONE).toString(),
            "-1000.00",
            "1e-46",
            "1e-324",
            "1e-324"
        };

    public static final String [] maxNumericStrings =
        new String[] {
            Long.toString(Byte.MAX_VALUE), Long.toString(
                Short.MAX_VALUE), Long.toString(Integer.MAX_VALUE),
            Long.toString(Long.MAX_VALUE), "999.99",

            // NOTE jvs 26-Apr-2006:  use something slightly less than MAX_VALUE
            // because roundtripping string to approx to string doesn't preserve
            // MAX_VALUE on win32
            "3.4028234E38", /*Float.toString(Float.MAX_VALUE)*/
            "1.79769313486231E308", /*Double.toString(Double.MAX_VALUE)*/
            "1.79769313486231E308" /*Double.toString(Double.MAX_VALUE)*/
        };

    public static final String [] maxOverflowNumericStrings =
        new String[] {
            Long.toString(Byte.MAX_VALUE + 1),
            Long.toString(Short.MAX_VALUE + 1),
            Long.toString((long) Integer.MAX_VALUE + 1),
            (new BigDecimal(Long.MAX_VALUE)).add(BigDecimal.ONE).toString(),
            "1000.00",
            "1e39",
            "-1e309",
            "1e309"
        };
    private static final boolean [] FalseTrue = new boolean[] { false, true };

    //~ Constructors -----------------------------------------------------------

    public SqlOperatorTests(String testName)
    {
        super(testName);
    }

    //~ Methods ----------------------------------------------------------------

    /**
     * Derived class must implement this method to provide a means to validate,
     * execute various statements.
     */
    protected abstract SqlTester getTester();

    protected void setUp()
        throws Exception
    {
        getTester().setFor(null);
    }

    protected void check(String query, SqlTester.TypeChecker typeChecker,
        Object result, double delta)
    {
        getTester().check(query, typeChecker, result, delta);
    }

    protected void checkAgg(String expr, String[] inputValues, Object result, int delta)
    {
        getTester().checkAgg(expr, inputValues, result, delta);
    }

    protected void checkBoolean(String expression, Boolean result)
    {
        getTester().checkBoolean(expression, result);
    }

    protected void checkFails(String expression, String expectedError, Boolean runtime)
    {
        getTester().checkFails(expression, expectedError, runtime);
    }

    protected void checkNull(String expression)
    {
        getTester().checkNull(expression);
    }

    protected void checkScalar(String expression, Object result, String resultType)
    {
        getTester().checkScalar(expression, result, resultType);
    }

    protected void checkScalarApprox(String expression, String expectedType,
        double expectedResult, double delta)
    {
        getTester().checkScalarApprox(expression, expectedType,
            expectedResult, delta);
    }

    protected void checkScalarExact(String expression, String expectedType,
        String result)
    {
        getTester().checkScalarExact(expression, expectedType, result);
    }

    protected void checkScalarExact(String expression, String result)
    {
        getTester().checkScalarExact(expression, result);
    }

    protected void checkString(String expression, String result, String resultType)
    {
        getTester().checkString(expression, result, resultType);
    }

    protected void checkType(String expression, String type)
    {
        getTester().checkType(expression, type);
    }

    protected void setFor(SqlOperator operator)
    {
        getTester().setFor(operator);
    }

    //--- Tests -----------------------------------------------------------
    public void testBetween()
    {
        setFor(SqlStdOperatorTable.betweenOperator);
        checkBoolean("2 between 1 and 3", Boolean.TRUE);
        checkBoolean("2 between 3 and 2", Boolean.FALSE);
        checkBoolean("2 between symmetric 3 and 2", Boolean.TRUE);
        checkBoolean("3 between 1 and 3", Boolean.TRUE);
        checkBoolean("4 between 1 and 3", Boolean.FALSE);
        checkBoolean("1 between 4 and -3", Boolean.FALSE);
        checkBoolean("1 between -1 and -3", Boolean.FALSE);
        checkBoolean("1 between -1 and 3", Boolean.TRUE);
        checkBoolean("1 between 1 and 1", Boolean.TRUE);
        checkBoolean("1.5 between 1 and 3", Boolean.TRUE);
        checkBoolean("1.2 between 1.1 and 1.3", Boolean.TRUE);
        checkBoolean("1.5 between 2 and 3", Boolean.FALSE);
        checkBoolean("1.5 between 1.6 and 1.7", Boolean.FALSE);
        checkBoolean("1.2e1 between 1.1 and 1.3", Boolean.FALSE);
        checkBoolean("1.2e0 between 1.1 and 1.3", Boolean.TRUE);
        checkBoolean("1.5e0 between 2 and 3", Boolean.FALSE);
        checkBoolean("1.5e0 between 2e0 and 3e0", Boolean.FALSE);
        checkBoolean(
            "1.5e1 between 1.6e1 and 1.7e1",
            Boolean.FALSE);
        checkBoolean("x'' between x'' and x''", Boolean.TRUE);
        checkNull("cast(null as integer) between -1 and 2");
        checkNull("1 between -1 and cast(null as integer)");
        checkNull(
            "1 between cast(null as integer) and cast(null as integer)");
        checkNull("1 between cast(null as integer) and 1");
    }

    public void testBetweenTemporal()
    {
        setFor(SqlStdOperatorTable.betweenOperator);

        //TRUE
        checkBoolean(
            "interval '2' second between " +
            "interval '1' second and " +
            "interval '3' second",
            Boolean.TRUE);
        checkBoolean(
            "interval -'1.000001' second(6) between " +
            "interval -'1.000000' second(6) and " +
            "interval -'1.000002' second(6)",
            Boolean.TRUE);
        checkBoolean(
            "interval '2' minute between " +
            "interval '1' minute and " +
            "interval '3' minute",
            Boolean.TRUE);
        checkBoolean(
            "interval '-1:2' minute to second between " +
            "interval '-1:1' minute to second and " +
            "interval '-1:3' minute to second",
            Boolean.TRUE);
        checkBoolean(
            "interval '1:1.000001' minute to second(6) between " +
            "interval '1:1.000000' minute to second(6) and " +
            "interval '1:1.000002' minute to second(6)",
            Boolean.TRUE);
        checkBoolean(
            "interval -'2' hour between " +
            "interval -'1' hour and " +
            "interval -'3' hour",
            Boolean.TRUE);
        checkBoolean(
            "interval '1:2' hour to minute between " +
            "interval '1:1' hour to minute and " +
            "interval '1:3' hour to minute",
            Boolean.TRUE);
        checkBoolean(
            "interval '-1:2:3' hour to second between " +
            "interval '-1:2:2' hour to second and " +
            "interval '-1:2:4' hour to second",
            Boolean.TRUE);
        checkBoolean(
            "interval '1:2:1.000003' hour to second(6) between " +
            "interval '1:2:1.000002' hour to second(6) and " +
            "interval '1:2:1.000004' hour to second(6)",
            Boolean.TRUE);
        checkBoolean(
            "interval -'2' day between " +
            "interval -'1' day and " +
            "interval -'3' day",
            Boolean.TRUE);
        checkBoolean(
            "interval '1 2' day to hour between " +
            "interval '1 1' day to hour and " +
            "interval '1 3' day to hour",
            Boolean.TRUE);
        checkBoolean(
            "interval '-1 1:2' day to minute between " +
            "interval '-1 1:1' day to minute and " +
            "interval '-1 1:3' day to minute",
            Boolean.TRUE);
        checkBoolean(
            "interval '1 1:1:2' day to second between " +
            "interval '1 1:1:1' day to second and " +
            "interval '1 1:1:3' day to second",
            Boolean.TRUE);
        checkBoolean(
            "interval -'1 1:1:1.000002' day to second(6) between " +
            "interval -'1 1:1:1.000001' day to second(6) and " +
            "interval -'1 1:1:1.000003' day to second(6)",
            Boolean.TRUE);
        checkBoolean(
            "interval '2' month between " +
            "interval '1' month and " +
            "interval '3' month",
            Boolean.TRUE);
        checkBoolean(
            "interval '-2' year between " +
            "interval '-1' year and " +
            "interval '-3' year",
            Boolean.TRUE);
        checkBoolean(
            "interval '1-2' year to month between " +
            "interval '1-1' year to month and " +
            "interval '1-3' year to month",
            Boolean.TRUE);

        //FALSE
        checkBoolean(
            "interval '1' second between " +
            "interval '1' second and " +
            "interval '3' second",
            Boolean.FALSE);
        checkBoolean(
            "interval -'1.000001' second(6) between " +
            "interval -'1.000001' second(6) and " +
            "interval -'1.000002' second(6)",
            Boolean.FALSE);
        checkBoolean(
            "interval '2' minute between " +
            "interval '1' minute and " +
            "interval '2' minute",
            Boolean.FALSE);
        checkBoolean(
            "interval '-1:1' minute to second between " +
            "interval '-1:1' minute to second and " +
            "interval '-1:3' minute to second",
            Boolean.FALSE);
        checkBoolean(
            "interval '1:1.000001' minute to second(6) between " +
            "interval '1:1.000001' minute to second(6) and " +
            "interval '1:1.000002' minute to second(6)",
            Boolean.FALSE);
        checkBoolean(
            "interval -'2' hour between " +
            "interval -'1' hour and " +
            "interval -'2' hour",
            Boolean.FALSE);
        checkBoolean(
            "interval '1:1' hour to minute between " +
            "interval '1:1' hour to minute and " +
            "interval '1:3' hour to minute",
            Boolean.FALSE);
        checkBoolean(
            "interval '-1:2:3' hour to second between " +
            "interval '-1:2:3' hour to second and " +
            "interval '-1:2:4' hour to second",
            Boolean.FALSE);
        checkBoolean(
            "interval '1:2:1.000003' hour to second(6) between " +
            "interval '1:2:1.000002' hour to second(6) and " +
            "interval '1:2:1.000003' hour to second(6)",
            Boolean.FALSE);
        checkBoolean(
            "interval -'1' day between " +
            "interval -'1' day and " +
            "interval -'3' day",
            Boolean.FALSE);
        checkBoolean(
            "interval '1 2' day to hour between " +
            "interval '1 2' day to hour and " +
            "interval '1 3' day to hour",
            Boolean.FALSE);
        checkBoolean(
            "interval '-1 1:2' day to minute between " +
            "interval '-1 1:1' day to minute and " +
            "interval '-1 1:2' day to minute",
            Boolean.FALSE);
        checkBoolean(
            "interval '1 1:1:1' day to second between " +
            "interval '1 1:1:1' day to second and " +
            "interval '1 1:1:3' day to second",
            Boolean.FALSE);
        checkBoolean(
            "interval -'1 1:1:1.000002' day to second(6) between " +
            "interval -'1 1:1:1.000002' day to second(6) and " +
            "interval -'1 1:1:1.000003' day to second(6)",
            Boolean.FALSE);
        checkBoolean(
            "interval '2' month between " +
            "interval '1' month and " +
            "interval '2' month",
            Boolean.FALSE);
        checkBoolean(
            "interval '-1' year between " +
            "interval '-1' year and " +
            "interval '-3' year",
            Boolean.FALSE);
        checkBoolean(
            "interval '1-2' year to month between " +
            "interval '1-2' year to month and " +
            "interval '1-3' year to month",
            Boolean.FALSE);

        //time & date
        checkBoolean(
            "time '01:23:45' between " +
            "time '01:23:44' and " +
            "time '01:23:46'",
            Boolean.TRUE);
        checkBoolean(
            "time '01:23:45.000001' between " +
            "time '01:23:45.000000' and " +
            "time '01:23:45.000002'",
            Boolean.TRUE);
        checkBoolean(
            "date '2007-01-01' between " +
            "date '2006-12-31' and " +
            "date '2007-01-02'",
            Boolean.TRUE);
        checkBoolean(
            "timestamp '2007-01-01 01:23:45' between " +
            "timestamp '2007-01-01 01:23:44' and " +
            "timestamp '2007-01-01 01:23:46'",
            Boolean.TRUE);
        checkBoolean(
            "timestamp '2007-01-01 01:23:45.000001' between " +
            "timestamp '2007-01-01 01:23:45.000000' and " +
            "timestamp '2007-01-01 01:23:45.000002'",
            Boolean.TRUE);
    }

    public void testNotBetween()
    {
        setFor(SqlStdOperatorTable.notBetweenOperator);
        checkBoolean("2 not between 1 and 3", Boolean.FALSE);
        checkBoolean("3 not between 1 and 3", Boolean.FALSE);
        checkBoolean("4 not between 1 and 3", Boolean.TRUE);
        checkBoolean(
            "1.2e0 not between 1.1 and 1.3",
            Boolean.FALSE);
        checkBoolean("1.2e1 not between 1.1 and 1.3", Boolean.TRUE);
        checkBoolean("1.5e0 not between 2 and 3", Boolean.TRUE);
        checkBoolean("1.5e0 not between 2e0 and 3e0", Boolean.TRUE);
    }

    private String getCastString(
        String value,
        String targetType,
        boolean errorLoc)
    {
        if (errorLoc) {
            value = "^" + value + "^";
        }
        return "cast(" + value + " as " + targetType + ")";
    }

    private void checkCastToApproxOkay(
        String value,
        String targetType,
        double expected,
        double delta)
    {
        checkScalarApprox(
            getCastString(value, targetType, false),
            targetType + " NOT NULL",
            expected,
            delta);
    }

    private void checkCastToStringOkay(
        String value,
        String targetType,
        String expected)
    {
        checkString(
            getCastString(value, targetType, false),
            expected,
            targetType + " NOT NULL");
    }

    private void checkCastToScalarOkay(
        String value,
        String targetType,
        String expected)
    {
        checkScalarExact(
            getCastString(value, targetType, false),
            targetType + " NOT NULL",
            expected);
    }

    private void checkCastToScalarOkay(String value, String targetType)
    {
        checkCastToScalarOkay(value, targetType, value);
    }

    private void checkCastFails(
        String value,
        String targetType,
        String expectedError,
        boolean runtime)
    {
        checkFails(
            getCastString(value, targetType, !runtime),
            expectedError,
            runtime);
    }

    private void checkCastToString(String value, String type, String expected)
    {
        String spaces = "     ";
        if (expected == null) {
            expected = value.trim();
        }
        int len = expected.length();
        if (type != null) {
            value = getCastString(value, type, false);
        }

        checkCastFails(
            value,
            "VARCHAR(" + (len - 1) + ")",
            stringTruncMessage,
            true);
        checkCastToStringOkay(value, "VARCHAR(" + len + ")", expected);
        checkCastToStringOkay(value, "VARCHAR(" + (len + 5) + ")", expected);

        checkCastFails(
            value,
            "CHAR(" + (len - 1) + ")",
            stringTruncMessage,
            true);
        checkCastToStringOkay(value, "CHAR(" + len + ")", expected);
        checkCastToStringOkay(
            value,
            "CHAR(" + (len + 5) + ")",
            expected + spaces);
    }

    public void testCastChar()
    {
        setFor(SqlStdOperatorTable.castFunc);

        // "CHAR" is shorthand for "CHAR(1)"
        checkString("CAST('abc' AS CHAR)", "a", "CHAR(1) NOT NULL");
        checkString(
            "CAST('abc' AS VARCHAR)",
            "a",
            "VARCHAR(1) NOT NULL");
    }

    public void testCastExactNumerics()
    {
        setFor(SqlStdOperatorTable.castFunc);

        // Test casting for min,max, out of range for exact numeric types
        for (int i = 0; i < numericTypeNames.length; i++) {
            String type = numericTypeNames[i];

            if (type.equalsIgnoreCase("DOUBLE")
                || type.equalsIgnoreCase("FLOAT")
                || type.equalsIgnoreCase("REAL"))
            {
                // Skip approx types
                continue;
            }

            // Convert from literal to type
            checkCastToScalarOkay(maxNumericStrings[i], type);
            checkCastToScalarOkay(minNumericStrings[i], type);

            // Overflow test
            if (type.equalsIgnoreCase("BIGINT")) {
                // Literal of range
                checkCastFails(
                    maxOverflowNumericStrings[i],
                    type,
                    literalOutOfRangeMessage,
                    false);
                checkCastFails(
                    minOverflowNumericStrings[i],
                    type,
                    literalOutOfRangeMessage,
                    false);
            } else {
                checkCastFails(
                    maxOverflowNumericStrings[i],
                    type,
                    outOfRangeMessage,
                    true);
                checkCastFails(
                    minOverflowNumericStrings[i],
                    type,
                    outOfRangeMessage,
                    true);
            }

            // Convert from string to type
            checkCastToScalarOkay(
                "'" + maxNumericStrings[i] + "'",
                type,
                maxNumericStrings[i]);
            checkCastToScalarOkay(
                "'" + minNumericStrings[i] + "'",
                type,
                minNumericStrings[i]);

            checkCastFails(
                "'" + maxOverflowNumericStrings[i] + "'",
                type,
                outOfRangeMessage,
                true);
            checkCastFails(
                "'" + minOverflowNumericStrings[i] + "'",
                type,
                outOfRangeMessage,
                true);

            // Convert from type to string
            checkCastToString(maxNumericStrings[i], null, null);
            checkCastToString(maxNumericStrings[i], type, null);

            checkCastToString(minNumericStrings[i], null, null);
            checkCastToString(minNumericStrings[i], type, null);

            checkCastFails("'notnumeric'", type, invalidCharMessage, true);
        }

        checkScalarExact(
            "cast(1.0 as bigint)",
            "BIGINT NOT NULL",
            "1");
        checkScalarExact("cast(1.0 as int)", "1");
    }

    public void testCastApproxNumerics()
    {
        setFor(SqlStdOperatorTable.castFunc);

        // Test casting for min,max, out of range for approx numeric types
        for (int i = 0; i < numericTypeNames.length; i++) {
            String type = numericTypeNames[i];
            boolean isFloat;

            if (type.equalsIgnoreCase("DOUBLE")
                || type.equalsIgnoreCase("FLOAT"))
            {
                isFloat = false;
            } else if (type.equalsIgnoreCase("REAL")) {
                isFloat = true;
            } else {
                // Skip non-approx types
                continue;
            }

            // Convert from literal to type
            checkCastToApproxOkay(
                maxNumericStrings[i],
                type,
                Double.parseDouble(maxNumericStrings[i]),
                isFloat ? 1E32 : 0);
            checkCastToApproxOkay(
                minNumericStrings[i],
                type,
                Double.parseDouble(minNumericStrings[i]),
                0);

            if (isFloat) {
                checkCastFails(
                    maxOverflowNumericStrings[i],
                    type,
                    outOfRangeMessage,
                    true);
            } else {
                // Double: Literal out of range
                checkCastFails(
                    maxOverflowNumericStrings[i],
                    type,
                    literalOutOfRangeMessage,
                    false);
            }

            // Underflow: goes to 0
            checkCastToApproxOkay(minOverflowNumericStrings[i], type, 0, 0);

            // Convert from string to type
            checkCastToApproxOkay(
                "'" + maxNumericStrings[i] + "'",
                type,
                Double.parseDouble(maxNumericStrings[i]),
                isFloat ? 1E32 : 0);
            checkCastToApproxOkay(
                "'" + minNumericStrings[i] + "'",
                type,
                Double.parseDouble(minNumericStrings[i]),
                0);

            checkCastFails(
                "'" + maxOverflowNumericStrings[i] + "'",
                type,
                outOfRangeMessage,
                true);

            // Underflow: goes to 0
            checkCastToApproxOkay(
                "'" + minOverflowNumericStrings[i] + "'",
                type,
                0,
                0);

            // Convert from type to string

            // Treated as DOUBLE
            checkCastToString(
                maxNumericStrings[i],
                null,
                isFloat ? null : "1.79769313486231E308");

            /*
            // TODO: The following tests are slightly different depending on //
             whether the java or fennel calc are used. //       Try to make them
             the same            if (FennelCalc) { // Treated as FLOAT or DOUBLE
             checkCastToString(maxNumericStrings[i], type, isFloat?
             "3.402824E38": "1.797693134862316E308"); // Treated as DOUBLE
             checkCastToString(minNumericStrings[i], null,     isFloat? null:
             "4.940656458412465E-324"); // Treated as FLOAT or DOUBLE
             checkCastToString(minNumericStrings[i], type,     isFloat?
             "1.401299E-45": "4.940656458412465E-324"); } else if (JavaCalc) {
             // Treated as FLOAT or DOUBLE
             checkCastToString(maxNumericStrings[i], type,     isFloat?
             "3.402823E38": "1.797693134862316E308"); // Treated as DOUBLE
             checkCastToString(minNumericStrings[i], null,     isFloat? null:
             null); // Treated as FLOAT or DOUBLE
             checkCastToString(minNumericStrings[i], type,     isFloat?
             "1.401298E-45": null); }
             */
            checkCastFails("'notnumeric'", type, invalidCharMessage, true);
        }

        checkScalarExact(
            "cast(1.0e0 as bigint)",
            "BIGINT NOT NULL",
            "1");
        checkScalarExact("cast(1.0e0 as int)", "1");
    }

    public void testCastDecimalToInteger()
    {
        setFor(SqlStdOperatorTable.castFunc);

        // decimal to integer
        checkScalarExact("cast(1.25 as integer)", "1");
        checkScalarExact("cast(-1.25 as integer)", "-1");
        checkScalarExact("cast(1.75 as integer)", "2");
        checkScalarExact("cast(-1.75 as integer)", "-2");
        checkScalarExact("cast(1.5 as integer)", "2");
        checkScalarExact("cast(-1.5 as integer)", "-2");
    }

    public void testCastDecimalToDecimal()
    {
        setFor(SqlStdOperatorTable.castFunc);

        // decimal to decimal
        checkScalarExact(
            "cast(1.29 as decimal(2,1))",
            "DECIMAL(2, 1) NOT NULL",
            "1.3");
        checkScalarExact(
            "cast(1.25 as decimal(2,1))",
            "DECIMAL(2, 1) NOT NULL",
            "1.3");
        checkScalarExact(
            "cast(1.21 as decimal(2,1))",
            "DECIMAL(2, 1) NOT NULL",
            "1.2");
        checkScalarExact(
            "cast(-1.29 as decimal(2,1))",
            "DECIMAL(2, 1) NOT NULL",
            "-1.3");
        checkScalarExact(
            "cast(-1.25 as decimal(2,1))",
            "DECIMAL(2, 1) NOT NULL",
            "-1.3");
        checkScalarExact(
            "cast(-1.21 as decimal(2,1))",
            "DECIMAL(2, 1) NOT NULL",
            "-1.2");

        // decimal to decimal
        checkScalarExact(
            "cast(1.29 as decimal(7,5))",
            "DECIMAL(7, 5) NOT NULL",
            "1.29000");
        checkScalarExact(
            "cast(-1.21 as decimal(7,5))",
            "DECIMAL(7, 5) NOT NULL",
            "-1.21000");
        checkScalarExact(
            "cast(-1.21 as decimal)",
            "DECIMAL(19, 0) NOT NULL",
            "-1");

        // 9.99 round to 10.0, should give out of range error
        checkFails(
            "cast(9.99 as decimal(2,1))",
            outOfRangeMessage,
            true);
    }

    public void testCastDecimalToDoubleToInteger()
    {
        setFor(SqlStdOperatorTable.castFunc);

        checkScalarExact(
            "cast( cast(1.25 as double) as integer)",
            "1");
        checkScalarExact(
            "cast( cast(-1.25 as double) as integer)",
            "-1");
        checkScalarExact(
            "cast( cast(1.75 as double) as integer)",
            "2");
        checkScalarExact(
            "cast( cast(-1.75 as double) as integer)",
            "-2");
        checkScalarExact(
            "cast( cast(1.5 as double) as integer)",
            "2");
        checkScalarExact(
            "cast( cast(-1.5 as double) as integer)",
            "-2");
    }

    public void testCastToDouble()
    {
        setFor(SqlStdOperatorTable.castFunc);
        checkScalarApprox(
            "cast(1 as double)",
            "DOUBLE NOT NULL",
            1,
            0);
        checkScalarApprox(
            "cast(1.0 as double)",
            "DOUBLE NOT NULL",
            1,
            0);
        checkScalarApprox(
            "cast(-5.9 as double)",
            "DOUBLE NOT NULL",
            -5.9,
            0);
    }

    public void testCastNull()
    {
        setFor(SqlStdOperatorTable.castFunc);

        // null
        checkNull("cast(null as decimal(4,3))");
        checkNull("cast(null as double)");
        checkNull("cast(null as varchar(10))");
        checkNull("cast(null as char(10))");
    }

    public void testCastDateTime()
    {
        // Test cast for date/time/timestamp
        setFor(SqlStdOperatorTable.castFunc);

        checkScalar(
            "cast(TIMESTAMP '1945-02-24 12:42:25.34' as TIMESTAMP)",
            "1945-02-24 12:42:25.0",
            "TIMESTAMP(0) NOT NULL");

        checkScalar(
            "cast(TIME '12:42:25.34' as TIME)",
            "12:42:25",
            "TIME(0) NOT NULL");

        // test rounding
        checkScalar(
            "cast(TIME '12:42:25.9' as TIME)",
            "12:42:26",
            "TIME(0) NOT NULL");

        if (Bug.Frg282Fixed) {
            // test precision
            checkScalar(
                "cast(TIME '12:42:25.34' as TIME(2))",
                "12:42:25.34",
                "TIME(2) NOT NULL");
        }

        checkScalar(
            "cast(DATE '1945-02-24' as DATE)",
            "1945-02-24",
            "DATE NOT NULL");

        // timestamp <-> time
        checkScalar(
            "cast(TIMESTAMP '1945-02-24 12:42:25.34' as TIME)",
            "12:42:25",
            "TIME(0) NOT NULL");

        // Generate the current date as a string, e.g. "2007-04-18". The value
        // is guaranteed to be good for at least 2 minutes, which should give
        // us time to run the rest of the tests.
        final String today;
        while (true) {
            final Calendar cal = Calendar.getInstance();
            if ((cal.get(Calendar.HOUR_OF_DAY) == 23)
                && (cal.get(Calendar.MINUTE) >= 58))
            {
                try {
                    Thread.sleep(60 * 1000);
                } catch (InterruptedException e) {
                    // ignore
                }
            } else {
                today =
                    new SimpleDateFormat("yyyy-MM-dd").format(cal.getTime());
                break;
            }
        }

        // Note: Casting to time(0) should lose date info and fractional
        // seconds, then casting back to timestamp should initialize to
        // current_date.
        if (Bug.Fnl66Fixed) {
            checkScalar(
                "cast(cast(TIMESTAMP '1945-02-24 12:42:25.34' as TIME) as TIMESTAMP)",
                today + " 12:42:25.0",
                "TIMESTAMP(0) NOT NULL");

            checkScalar(
                "cast(TIME '12:42:25.34' as TIMESTAMP)",
                today + " 12:42:25.0",
                "TIMESTAMP(0) NOT NULL");
        }

        // timestamp <-> date
        checkScalar(
            "cast(TIMESTAMP '1945-02-24 12:42:25.34' as DATE)",
            "1945-02-24",
            "DATE NOT NULL");

        // Note: casting to Date discards Time fields
        checkScalar(
            "cast(cast(TIMESTAMP '1945-02-24 12:42:25.34' as DATE) as TIMESTAMP)",
            "1945-02-24 00:00:00.0",
            "TIMESTAMP(0) NOT NULL");

        // TODO: precision should not be included
        checkScalar(
            "cast(DATE '1945-02-24' as TIMESTAMP)",
            "1945-02-24 00:00:00.0",
            "TIMESTAMP(0) NOT NULL");

        // time <-> string
        checkCastToString("TIME '12:42:25'", null, "12:42:25");
        checkCastToString("TIME '12:42:25.34'", null, "12:42:25.34");

        checkScalar(
            "cast('12:42:25' as TIME)",
            "12:42:25",
            "TIME(0) NOT NULL");
        checkScalar(
            "cast('1:42:25' as TIME)",
            "01:42:25",
            "TIME(0) NOT NULL");
        checkScalar(
            "cast('1:2:25' as TIME)",
            "01:02:25",
            "TIME(0) NOT NULL");
        checkScalar(
            "cast('  12:42:25  ' as TIME)",
            "12:42:25",
            "TIME(0) NOT NULL");
        checkScalar(
            "cast('12:42:25.34' as TIME)",
            "12:42:25",
            "TIME(0) NOT NULL");

        if (Bug.Frg282Fixed) {
            checkScalar(
                "cast('12:42:25.34' as TIME(2))",
                "12:42:25.34",
                "TIME(2) NOT NULL");
        }

        checkFails(
            "cast('nottime' as TIME)",
            badDatetimeMessage,
            true);
        checkFails(
            "cast('1241241' as TIME)",
            badDatetimeMessage,
            true);
        checkFails(
            "cast('12:54:78' as TIME)",
            badDatetimeMessage,
            true);

        // timestamp <-> string
        // TODO: Java calc displays ".0" while Fennel does not
        checkCastToString(
            "TIMESTAMP '1945-02-24 12:42:25'",
            null,
            "1945-02-24 12:42:25.0");

        // TODO: casting allows one to discard precision without error
        checkCastToString(
            "TIMESTAMP '1945-02-24 12:42:25.34'",
            null,
            "1945-02-24 12:42:25.34");

        checkScalar(
            "cast('1945-02-24 12:42:25' as TIMESTAMP)",
            "1945-02-24 12:42:25.0",
            "TIMESTAMP(0) NOT NULL");
        checkScalar(
            "cast('1945-2-2 12:2:5' as TIMESTAMP)",
            "1945-02-02 12:02:05.0",
            "TIMESTAMP(0) NOT NULL");
        checkScalar(
            "cast('  1945-02-24 12:42:25  ' as TIMESTAMP)",
            "1945-02-24 12:42:25.0",
            "TIMESTAMP(0) NOT NULL");
        checkScalar(
            "cast('1945-02-24 12:42:25.34' as TIMESTAMP)",
            "1945-02-24 12:42:25.0",
            "TIMESTAMP(0) NOT NULL");

        if (Bug.Frg282Fixed) {
            checkScalar(
                "cast('1945-02-24 12:42:25.34' as TIMESTAMP(2))",
                "1945-02-24 12:42:25.34",
                "TIMESTAMP(2) NOT NULL");
        }
        checkFails(
            "cast('nottime' as TIMESTAMP)",
            badDatetimeMessage,
            true);
        checkFails(
            "cast('1241241' as TIMESTAMP)",
            badDatetimeMessage,
            true);
        checkFails(
            "cast('1945-20-24 12:42:25.34' as TIMESTAMP)",
            badDatetimeMessage,
            true);
        checkFails(
            "cast('1945-01-24 25:42:25.34' as TIMESTAMP)",
            badDatetimeMessage,
            true);

        // date <-> string
        checkCastToString("DATE '1945-02-24'", null, "1945-02-24");
        checkCastToString("DATE '1945-2-24'", null, "1945-02-24");

        checkScalar(
            "cast('1945-02-24' as DATE)",
            "1945-02-24",
            "DATE NOT NULL");
        checkScalar(
            "cast('  1945-02-24  ' as DATE)",
            "1945-02-24",
            "DATE NOT NULL");
        checkFails(
            "cast('notdate' as DATE)",
            badDatetimeMessage,
            true);
        checkFails(
            "cast('52534253' as DATE)",
            badDatetimeMessage,
            true);
        checkFails(
            "cast('1945-30-24' as DATE)",
            badDatetimeMessage,
            true);

        // cast null
        checkNull("cast(null as date)");
        checkNull("cast(null as timestamp)");
        checkNull("cast(null as time)");
        checkNull("cast(cast(null as varchar(10)) as time)");
        checkNull("cast(cast(null as varchar(10)) as date)");
        checkNull("cast(cast(null as varchar(10)) as timestamp)");
        checkNull("cast(cast(null as date) as timestamp)");
        checkNull("cast(cast(null as time) as timestamp)");
        checkNull("cast(cast(null as timestamp) as date)");
        checkNull("cast(cast(null as timestamp) as time)");
    }

    public void testCastInterval()
    {
        setFor(SqlStdOperatorTable.castFunc);
        checkScalar(
            "cast(interval '1' second  as interval second)",
            "+1",
            "INTERVAL SECOND NOT NULL");
        checkScalar(
            "cast(interval '1' second as interval minute to second)",
            "+0:01",
            "INTERVAL MINUTE TO SECOND NOT NULL");
        checkScalar(
            "cast(interval '1' second as interval hour to second)",
            "+0:00:01",
            "INTERVAL HOUR TO SECOND NOT NULL");
        checkScalar(
            "cast(interval '1' second as interval day to second)",
            "+0 0:00:01",
            "INTERVAL DAY TO SECOND NOT NULL");
        checkScalar(
            "cast(interval '1' minute as interval minute)",
            "+1",
            "INTERVAL MINUTE NOT NULL");
        checkScalar(
            "cast(interval '1' minute as interval minute to second)",
            "+1:00",
            "INTERVAL MINUTE TO SECOND NOT NULL");
        checkScalar(
            "cast(interval '1' minute as interval hour to second)",
            "+0:01:00",
            "INTERVAL HOUR TO SECOND NOT NULL");
        checkScalar(
            "cast(interval '1' minute as interval hour to minute)",
            "+0:01",
            "INTERVAL HOUR TO MINUTE NOT NULL");
        checkScalar(
            "cast(interval '1' minute as interval day to second)",
            "+0 0:01:00",
            "INTERVAL DAY TO SECOND NOT NULL");
        checkScalar(
            "cast(interval '1' minute as interval day to minute)",
            "+0 0:01",
            "INTERVAL DAY TO MINUTE NOT NULL");
        checkScalar(
            "cast(interval '1' hour as interval hour)",
            "+1",
            "INTERVAL HOUR NOT NULL");
        checkScalar(
            "cast(interval '1' hour as interval hour to second)",
            "+1:00:00",
            "INTERVAL HOUR TO SECOND NOT NULL");
        checkScalar(
            "cast(interval '1' hour as interval hour to minute)",
            "+1:00",
            "INTERVAL HOUR TO MINUTE NOT NULL");
        checkScalar(
            "cast(interval '1' hour as interval day to second)",
            "+0 1:00:00",
            "INTERVAL DAY TO SECOND NOT NULL");
        checkScalar(
            "cast(interval '1' hour as interval day to minute)",
            "+0 1:00",
            "INTERVAL DAY TO MINUTE NOT NULL");
        checkScalar(
            "cast(interval '1' hour as interval day to hour)",
            "+0 1",
            "INTERVAL DAY TO HOUR NOT NULL");
        checkScalar(
            "cast(interval '1' day as interval day)",
            "+1",
            "INTERVAL DAY NOT NULL");
        checkScalar(
            "cast(interval '1' day as interval day to second)",
            "+1 0:00:00",
            "INTERVAL DAY TO SECOND NOT NULL");
        checkScalar(
            "cast(interval '1' day as interval day to minute)",
            "+1 0:00",
            "INTERVAL DAY TO MINUTE NOT NULL");
        checkScalar(
            "cast(interval '1' day as interval day to hour)",
            "+1 0",
            "INTERVAL DAY TO HOUR NOT NULL");
        checkScalar(
            "cast(interval '1:02' minute to second as interval minute to second)",
            "+1:02",
            "INTERVAL MINUTE TO SECOND NOT NULL");
        checkScalar(
            "cast(interval '1:02' minute to second as interval hour to second)",
            "+0:01:02",
            "INTERVAL HOUR TO SECOND NOT NULL");
        checkScalar(
            "cast(interval '1:02' minute to second as interval day to second)",
            "+0 0:01:02",
            "INTERVAL DAY TO SECOND NOT NULL");
        checkScalar(
            "cast(interval '1:02:03' hour to second as interval hour to second)",
            "+1:02:03",
            "INTERVAL HOUR TO SECOND NOT NULL");
        checkScalar(
            "cast(interval '1:02:03' hour to second as interval day to second)",
            "+0 1:02:03",
            "INTERVAL DAY TO SECOND NOT NULL");
        checkScalar(
            "cast(interval '1:02' hour to minute as interval hour to second)",
            "+1:02:00",
            "INTERVAL HOUR TO SECOND NOT NULL");
        checkScalar(
            "cast(interval '1:02' hour to minute as interval hour to minute)",
            "+1:02",
            "INTERVAL HOUR TO MINUTE NOT NULL");
        checkScalar(
            "cast(interval '1:02' hour to minute as interval day to second)",
            "+0 1:02:00",
            "INTERVAL DAY TO SECOND NOT NULL");
        checkScalar(
            "cast(interval '1:02' hour to minute as interval day to minute)",
            "+0 1:02",
            "INTERVAL DAY TO MINUTE NOT NULL");
        checkScalar(
            "cast(interval '1 2:3:4' day to second as interval day to second)",
            "+1 2:03:04",
            "INTERVAL DAY TO SECOND NOT NULL");
        checkScalar(
            "cast(interval '1 2:3' day to minute as interval day to minute)",
            "+1 2:03",
            "INTERVAL DAY TO MINUTE NOT NULL");
        checkScalar(
            "cast(interval '1 2:3' day to minute as interval day to second)",
            "+1 2:03:00",
            "INTERVAL DAY TO SECOND NOT NULL");
        checkScalar(
            "cast(interval '1 2' day to hour as interval day to hour)",
            "+1 2",
            "INTERVAL DAY TO HOUR NOT NULL");
        checkScalar(
            "cast(interval '1 2' day to hour as interval day to second)",
            "+1 2:00:00",
            "INTERVAL DAY TO SECOND NOT NULL");
        checkScalar(
            "cast(interval '1 2' day to hour as interval day to minute)",
            "+1 2:00",
            "INTERVAL DAY TO MINUTE NOT NULL");

        //month and year
        checkScalar(
            "cast(interval '1' month as interval month)",
            "+1",
            "INTERVAL MONTH NOT NULL");
        checkScalar(
            "cast(interval '1' month as interval year to month)",
            "+0-1",
            "INTERVAL YEAR TO MONTH NOT NULL");
        checkScalar(
            "cast(interval '1' year as interval year)",
            "+1",
            "INTERVAL YEAR NOT NULL");
        checkScalar(
            "cast(interval '1' year as interval year to month)",
            "+1-0",
            "INTERVAL YEAR TO MONTH NOT NULL");
        checkScalar(
            "cast(interval '1-2' year to month as interval year to month)",
            "+1-2",
            "INTERVAL YEAR TO MONTH NOT NULL");

        //day hour minute second with precision
        checkScalar(
            "cast(interval '1.123' second  as interval second(6))",
            "+1.123000",
            "INTERVAL SECOND(6) NOT NULL");
        checkScalar(
            "cast(interval '1.123' second  as interval second(8,6))",
            "+1.123000",
            "INTERVAL SECOND(8, 6) NOT NULL");
        checkScalar(
            "cast(interval '1.123' second as interval minute(3) to second(6))",
            "+0:01.123000",
            "INTERVAL MINUTE(3) TO SECOND(6) NOT NULL");
        checkScalar(
            "cast(interval '1.123' second as interval hour(3) to second(6))",
            "+0:00:01.123000",
            "INTERVAL HOUR(3) TO SECOND(6) NOT NULL");
        checkScalar(
            "cast(interval '1.123' second as interval day(3) to second(6))",
            "+0 0:00:01.123000",
            "INTERVAL DAY(3) TO SECOND(6) NOT NULL");
        checkScalar(
            "cast(interval '1' minute as interval minute(3))",
            "+1",
            "INTERVAL MINUTE(3) NOT NULL");
        checkScalar(
            "cast(interval '1' minute as interval minute(3) to second(6))",
            "+1:00.000000",
            "INTERVAL MINUTE(3) TO SECOND(6) NOT NULL");
        checkScalar(
            "cast(interval '1' minute as interval hour(3) to second(6))",
            "+0:01:00.000000",
            "INTERVAL HOUR(3) TO SECOND(6) NOT NULL");
        checkScalar(
            "cast(interval '1' minute as interval hour(3) to minute)",
            "+0:01",
            "INTERVAL HOUR(3) TO MINUTE NOT NULL");
        checkScalar(
            "cast(interval '1' minute as interval day(3) to second(6))",
            "+0 0:01:00.000000",
            "INTERVAL DAY(3) TO SECOND(6) NOT NULL");
        checkScalar(
            "cast(interval '1' minute as interval day(3) to minute)",
            "+0 0:01",
            "INTERVAL DAY(3) TO MINUTE NOT NULL");
        checkScalar(
            "cast(interval '1' hour as interval hour(3))",
            "+1",
            "INTERVAL HOUR(3) NOT NULL");
        checkScalar(
            "cast(interval '1' hour as interval hour(3) to second(6))",
            "+1:00:00.000000",
            "INTERVAL HOUR(3) TO SECOND(6) NOT NULL");
        checkScalar(
            "cast(interval '1' hour as interval hour(3) to minute)",
            "+1:00",
            "INTERVAL HOUR(3) TO MINUTE NOT NULL");
        checkScalar(
            "cast(interval '1' hour as interval day(3) to second(6))",
            "+0 1:00:00.000000",
            "INTERVAL DAY(3) TO SECOND(6) NOT NULL");
        checkScalar(
            "cast(interval '1' hour as interval day(3) to minute)",
            "+0 1:00",
            "INTERVAL DAY(3) TO MINUTE NOT NULL");
        checkScalar(
            "cast(interval '1' hour as interval day(3) to hour)",
            "+0 1",
            "INTERVAL DAY(3) TO HOUR NOT NULL");
        checkScalar(
            "cast(interval '1' day as interval day(3))",
            "+1",
            "INTERVAL DAY(3) NOT NULL");
        checkScalar(
            "cast(interval '1' day as interval day(3) to second(6))",
            "+1 0:00:00.000000",
            "INTERVAL DAY(3) TO SECOND(6) NOT NULL");
        checkScalar(
            "cast(interval '1' day as interval day(3) to minute)",
            "+1 0:00",
            "INTERVAL DAY(3) TO MINUTE NOT NULL");
        checkScalar(
            "cast(interval '1' day as interval day(3) to hour)",
            "+1 0",
            "INTERVAL DAY(3) TO HOUR NOT NULL");
        checkScalar(
            "cast(interval '1:02.123' minute to second(3) as interval minute(3) to second(6))",
            "+1:02.123000",
            "INTERVAL MINUTE(3) TO SECOND(6) NOT NULL");
        checkScalar(
            "cast(interval '1:02.123' minute to second(3) as interval hour(3) to second(6))",
            "+0:01:02.123000",
            "INTERVAL HOUR(3) TO SECOND(6) NOT NULL");
        checkScalar(
            "cast(interval '1:02.123' minute to second(3) as interval day(3) to second(6))",
            "+0 0:01:02.123000",
            "INTERVAL DAY(3) TO SECOND(6) NOT NULL");
        checkScalar(
            "cast(interval '1:02:03.123' hour to second(3) as interval hour(3) to second(6))",
            "+1:02:03.123000",
            "INTERVAL HOUR(3) TO SECOND(6) NOT NULL");
        checkScalar(
            "cast(interval '1:02:03.123' hour to second(3) as interval day(3) to second(6))",
            "+0 1:02:03.123000",
            "INTERVAL DAY(3) TO SECOND(6) NOT NULL");
        checkScalar(
            "cast(interval '1:02' hour to minute as interval hour(3) to second(6))",
            "+1:02:00.000000",
            "INTERVAL HOUR(3) TO SECOND(6) NOT NULL");
        checkScalar(
            "cast(interval '1:02' hour to minute as interval hour(3) to minute)",
            "+1:02",
            "INTERVAL HOUR(3) TO MINUTE NOT NULL");
        checkScalar(
            "cast(interval '1:02' hour to minute as interval day(3) to second(6))",
            "+0 1:02:00.000000",
            "INTERVAL DAY(3) TO SECOND(6) NOT NULL");
        checkScalar(
            "cast(interval '1:02' hour to minute as interval day(3) to minute)",
            "+0 1:02",
            "INTERVAL DAY(3) TO MINUTE NOT NULL");
        checkScalar(
            "cast(interval '1 2:3:4.123' day to second(3) as interval day(3) to second(6))",
            "+1 2:03:04.123000",
            "INTERVAL DAY(3) TO SECOND(6) NOT NULL");
        checkScalar(
            "cast(interval '1 2:3' day to minute as interval day(3) to minute)",
            "+1 2:03",
            "INTERVAL DAY(3) TO MINUTE NOT NULL");
        checkScalar(
            "cast(interval '1 2:3' day to minute as interval day(3) to second(6))",
            "+1 2:03:00.000000",
            "INTERVAL DAY(3) TO SECOND(6) NOT NULL");
        checkScalar(
            "cast(interval '1 2' day to hour as interval day(3) to hour)",
            "+1 2",
            "INTERVAL DAY(3) TO HOUR NOT NULL");
        checkScalar(
            "cast(interval '1 2' day to hour as interval day(3) to second(6))",
            "+1 2:00:00.000000",
            "INTERVAL DAY(3) TO SECOND(6) NOT NULL");
        checkScalar(
            "cast(interval '1 2' day to hour as interval day(3) to minute)",
            "+1 2:00",
            "INTERVAL DAY(3) TO MINUTE NOT NULL");

        checkScalar(
            "cast(interval '1' month(3) as interval month(5))",
            "+1",
            "INTERVAL MONTH(5) NOT NULL");
        checkScalar(
            "cast(interval '1' month(3) as interval year(3) to month)",
            "+0-1",
            "INTERVAL YEAR(3) TO MONTH NOT NULL");
        checkScalar(
            "cast(interval '1' year(3) as interval year(5))",
            "+1",
            "INTERVAL YEAR(5) NOT NULL");
        checkScalar(
            "cast(interval '1' year(3) as interval year(5) to month)",
            "+1-0",
            "INTERVAL YEAR(5) TO MONTH NOT NULL");
        checkScalar(
            "cast(interval '1-2' year(3) to month as interval year(5) to month)",
            "+1-2",
            "INTERVAL YEAR(5) TO MONTH NOT NULL");

        //todo: add the following casts:

        //string as any of the above

        //exact numeric to second
        //exact numeric to minute
        //exact numeric to hour
        //exact numeric to day
        //exact numeric to month
        //exact numeric to year

        //exact numeric to second(p,s)
        //exact numeric to second(s)
        //exact numeric to minute(p)
        //exact numeric to hour(p)
        //exact numeric to day(p)
        //exact numeric to month(p)
        //exact numeric to year(p)

    }

    public void testCastExactString()
    {
        setFor(SqlStdOperatorTable.castFunc);

        // string to decimal
        checkScalarExact(
            "cast('1.29' as decimal(2,1))",
            "DECIMAL(2, 1) NOT NULL",
            "1.3");
        checkScalarExact(
            "cast(' 1.25 ' as decimal(2,1))",
            "DECIMAL(2, 1) NOT NULL",
            "1.3");
        checkScalarExact(
            "cast('1.21' as decimal(2,1))",
            "DECIMAL(2, 1) NOT NULL",
            "1.2");
        checkScalarExact(
            "cast(' -1.29 ' as decimal(2,1))",
            "DECIMAL(2, 1) NOT NULL",
            "-1.3");
        checkScalarExact(
            "cast('-1.25' as decimal(2,1))",
            "DECIMAL(2, 1) NOT NULL",
            "-1.3");
        checkScalarExact(
            "cast(' -1.21 ' as decimal(2,1))",
            "DECIMAL(2, 1) NOT NULL",
            "-1.2");
        checkFails(
            "cast(' -1.21e' as decimal(2,1))",
            invalidCharMessage,
            true);

        // decimal to string
        checkString(
            "cast(1.29 as varchar(10))",
            "1.29",
            "VARCHAR(10) NOT NULL");
        checkString(
            "cast(.48 as varchar(10))",
            ".48",
            "VARCHAR(10) NOT NULL");
        checkFails(
            "cast(2.523 as char(2))",
            stringTruncMessage,
            true);

        checkString(
            "cast(-0.29 as varchar(10))",
            "-.29",
            "VARCHAR(10) NOT NULL");
        checkString(
            "cast(-1.29 as varchar(10))",
            "-1.29",
            "VARCHAR(10) NOT NULL");

        // string to integer
        checkScalarExact("cast('6543' as integer)", "6543");
        if (Bug.Frg26Fixed) {
            checkScalarExact("cast(' -123 ' as int)", "-123");
        }
        checkScalarExact(
            "cast('654342432412312' as bigint)",
            "BIGINT NOT NULL",
            "654342432412312");

        // integer to string
        checkString(
            "cast(9354 as varchar(10))",
            "9354",
            "VARCHAR(10) NOT NULL");
    }

    public void testCastApproxString()
    {
        setFor(SqlStdOperatorTable.castFunc);

        // string to double/float/real
        checkScalarApprox(
            "cast('1' as double)",
            "DOUBLE NOT NULL",
            1,
            0);
        checkScalarApprox(
            "cast('2.3' as float)",
            "FLOAT NOT NULL",
            2.3,
            0);
        checkScalarApprox(
            "cast('-10.2' as real)",
            "REAL NOT NULL",
            -10.2,
            0);
        checkScalarApprox(
            "cast('4e2' as double)",
            "DOUBLE NOT NULL",
            400,
            0);
        checkScalarApprox(
            "cast('2.1e1' as float)",
            "FLOAT NOT NULL",
            21,
            0);
        checkScalarApprox(
            "cast('-12e-1' as real)",
            "REAL NOT NULL",
            -1.2,
            0);
        checkScalarApprox(
            "cast(' -43 ' as double)",
            "DOUBLE NOT NULL",
            -43,
            0);
        checkScalarApprox(
            "cast(' 23e-1 ' as float)",
            "FLOAT NOT NULL",
            2.3,
            0);
        checkScalarApprox(
            "cast(' 123e+1 ' as real)",
            "REAL NOT NULL",
            1230,
            0);

        // double/float/real to string
        checkString(
            "cast(0e0 as varchar(5))",
            "0E0",
            "VARCHAR(5) NOT NULL");

        checkString(
            "cast(4e1 as varchar(5))",
            "4E1",
            "VARCHAR(5) NOT NULL");

        checkString(
            "cast(45e1 as varchar(5))",
            "4.5E2",
            "VARCHAR(5) NOT NULL");

        checkString(
            "cast(4.6834e0 as varchar(50))",
            "4.6834E0",
            "VARCHAR(50) NOT NULL");

        checkString(
            "cast(4683442.3432498375e0 as varchar(20))",
            "4.683442343249838E6",
            "VARCHAR(20) NOT NULL");

        checkString(
            "cast(cast(0.1 as real) as char(10))",
            "1E-1      ",
            "CHAR(10) NOT NULL");

        checkString(
            "cast(cast(-0.0036 as float) as char(10))",
            "-3.6E-3   ",
            "CHAR(10) NOT NULL");

        checkString(
            "cast(cast(3.23e0 as real) as varchar(20))",
            "3.23E0",
            "VARCHAR(20) NOT NULL");

        checkString(
            "cast(cast(5.2365439 as real) as varchar(20))",
            "5.236544E0",
            "VARCHAR(20) NOT NULL");

        checkString(
            "cast(-1e0 as char(6))",
            "-1E0  ",
            "CHAR(6) NOT NULL");

        checkFails(
            "cast(1.3243232e0 as varchar(4))",
            stringTruncMessage,
            true);
        checkFails(
            "cast(1.9e5 as char(4))",
            stringTruncMessage,
            true);
    }

    public void testCastBooleanString()
    {
        setFor(SqlStdOperatorTable.castFunc);

        // boolean to string (char)
        checkString(
            "cast(true as char(4))",
            "TRUE",
            "CHAR(4) NOT NULL");
        checkString(
            "cast(false as char(5))",
            "FALSE",
            "CHAR(5) NOT NULL");
        checkString(
            "cast(true as char(8))",
            "TRUE    ",
            "CHAR(8) NOT NULL");
        checkString(
            "cast(false as char(8))",
            "FALSE   ",
            "CHAR(8) NOT NULL");
        checkFails(
            "cast(true as char(3))",
            invalidCharMessage,
            true);
        checkFails(
            "cast(false as char(4))",
            invalidCharMessage,
            true);

        // boolean to string (varchar)
        checkString(
            "cast(true as varchar(4))",
            "TRUE",
            "VARCHAR(4) NOT NULL");
        checkString(
            "cast(false as varchar(5))",
            "FALSE",
            "VARCHAR(5) NOT NULL");
        checkString(
            "cast(true as varchar(8))",
            "TRUE",
            "VARCHAR(8) NOT NULL");
        checkString(
            "cast(false as varchar(8))",
            "FALSE",
            "VARCHAR(8) NOT NULL");
        checkFails(
            "cast(true as varchar(3))",
            invalidCharMessage,
            true);
        checkFails(
            "cast(false as varchar(4))",
            invalidCharMessage,
            true);

        // string to boolean
        checkBoolean("cast('true' as boolean)", Boolean.TRUE);
        checkBoolean("cast('false' as boolean)", Boolean.FALSE);
        checkBoolean("cast('  trUe' as boolean)", Boolean.TRUE);
        checkBoolean("cast('  fALse' as boolean)", Boolean.FALSE);
        checkFails(
            "cast('unknown' as boolean)",
            invalidCharMessage,
            true);
        checkFails(
            "cast('blah' as boolean)",
            invalidCharMessage,
            true);

        checkBoolean(
            "cast(cast('true' as varchar(10))  as boolean)",
            Boolean.TRUE);
        checkBoolean(
            "cast(cast('false' as varchar(10)) as boolean)",
            Boolean.FALSE);
        checkFails(
            "cast(cast('blah' as varchar(10)) as boolean)",
            invalidCharMessage,
            true);
    }

    public void testCase()
    {
        setFor(SqlStdOperatorTable.caseOperator);
        checkScalarExact("case when 'a'='a' then 1 end", "1");

        checkString(
            "case 2 when 1 then 'a' when 2 then 'bcd' end",
            "bcd",
            "CHAR(3)");
        checkString(
            "case 1 when 1 then 'a' when 2 then 'bcd' end",
            "a  ",
            "CHAR(3)");
        checkString(
            "case 1 when 1 then cast('a' as varchar(1)) "
            + "when 2 then cast('bcd' as varchar(3)) end",
            "a",
            "VARCHAR(3)");

        checkScalarExact(
            "case 2 when 1 then 11.2 when 2 then 4.543 else null end",
            "DECIMAL(5, 3)",
            "4.543");
        checkScalarExact(
            "case 1 when 1 then 11.2 when 2 then 4.543 else null end",
            "DECIMAL(5, 3)",
            "11.200");
        checkScalarExact("case 'a' when 'a' then 1 end", "1");
        checkScalarApprox(
            "case 1 when 1 then 11.2e0 when 2 then cast(4 as bigint) else 3 end",
            "DOUBLE NOT NULL",
            11.2,
            0);
        checkScalarApprox(
            "case 1 when 1 then 11.2e0 when 2 then 4 else null end",
            "DOUBLE",
            11.2,
            0);
        checkScalarApprox(
            "case 2 when 1 then 11.2e0 when 2 then 4 else null end",
            "DOUBLE",
            4,
            0);
        checkScalarApprox(
            "case 1 when 1 then 11.2e0 when 2 then 4.543 else null end",
            "DOUBLE",
            11.2,
            0);
        checkScalarApprox(
            "case 2 when 1 then 11.2e0 when 2 then 4.543 else null end",
            "DOUBLE",
            4.543,
            0);
        checkNull("case 'a' when 'b' then 1 end");
        checkScalarExact(
            "case when 'a'=cast(null as varchar(1)) then 1 else 2 end",
            "2");

        if (todo) {
            checkScalar(
                "case 1 when 1 then row(1,2) when 2 then row(2,3) end",
                "ROW(INTEGER NOT NULL, INTEGER NOT NULL)",
                "row(1,2)");
            checkScalar(
                "case 1 when 1 then row('a','b') when 2 then row('ab','cd') end",
                "ROW(CHAR(2) NOT NULL, CHAR(2) NOT NULL)",
                "row('a ','b ')");
        }
        // TODO: Check case with multisets
    }

    public void testCaseType()
    {
        setFor(SqlStdOperatorTable.caseOperator);
        checkType(
            "case 1 when 1 then current_timestamp else null end",
            "TIMESTAMP(0)");
        checkType(
            "case 1 when 1 then current_timestamp else current_timestamp end",
            "TIMESTAMP(0) NOT NULL");
        checkType(
            "case when true then current_timestamp else null end",
            "TIMESTAMP(0)");
        checkType(
            "case when true then current_timestamp end",
            "TIMESTAMP(0)");
        checkType(
            "case 'x' when 'a' then 3 when 'b' then null else 4.5 end",
            "DECIMAL(11, 1)");
    }

    public void testJdbcFn()
    {
        setFor(new SqlJdbcFunctionCall("dummy"));
    }

    public void testSelect()
    {
        setFor(SqlStdOperatorTable.selectOperator);
        check(
            "select * from (values(1))",
            AbstractSqlTester.IntegerTypeChecker,
            "1",
            0);

        // check return type on scalar subquery in select list.  Note return
        // type is always nullable even if subquery select value is NOT NULL.
        if (Bug.Frg189Fixed) {
            checkType(
                "SELECT *,(SELECT * FROM (VALUES(1))) FROM (VALUES(2))",
                "RecordType(INTEGER NOT NULL EXPR$0, INTEGER EXPR$1) NOT NULL");
            checkType(
                "SELECT *,(SELECT * FROM (VALUES(CAST(10 as BIGINT)))) "
                + "FROM (VALUES(CAST(10 as bigint)))",
                "RecordType(BIGINT NOT NULL EXPR$0, BIGINT EXPR$1) NOT NULL");
            checkType(
                " SELECT *,(SELECT * FROM (VALUES(10.5))) FROM (VALUES(10.5))",
                "RecordType(DECIMAL(3, 1) NOT NULL EXPR$0, DECIMAL(3, 1) EXPR$1) NOT NULL");
            checkType(
                "SELECT *,(SELECT * FROM (VALUES('this is a char'))) "
                + "FROM (VALUES('this is a char too'))",
                "RecordType(CHAR(18) NOT NULL EXPR$0, CHAR(14) EXPR$1) NOT NULL");
            checkType(
                "SELECT *,(SELECT * FROM (VALUES(true))) FROM (values(false))",
                "RecordType(BOOLEAN NOT NULL EXPR$0, BOOLEAN EXPR$1) NOT NULL");
            checkType(
                " SELECT *,(SELECT * FROM (VALUES(cast('abcd' as varchar(10))))) "
                + "FROM (VALUES(CAST('abcd' as varchar(10))))",
                "RecordType(VARCHAR(10) NOT NULL EXPR$0, VARCHAR(10) EXPR$1) NOT NULL");
            checkType(
                "SELECT *,"
                + "  (SELECT * FROM (VALUES(TIMESTAMP '2006-01-01 12:00:05'))) "
                + "FROM (VALUES(TIMESTAMP '2006-01-01 12:00:05'))",
                "RecordType(TIMESTAMP(0) NOT NULL EXPR$0, TIMESTAMP(0) EXPR$1) NOT NULL");
        }
    }

    public void testLiteralChain()
    {
        setFor(SqlStdOperatorTable.literalChainOperator);
        checkString(
            "'buttered'\n' toast'",
            "buttered toast",
            "CHAR(14) NOT NULL");
        checkString(
            "'corned'\n' beef'\n' on'\n' rye'",
            "corned beef on rye",
            "CHAR(18) NOT NULL");
        checkString(
            "_latin1'Spaghetti'\n' all''Amatriciana'",
            "Spaghetti all'Amatriciana",
            "CHAR(25) NOT NULL");
        checkBoolean("x'1234'\n'abcd' = x'1234abcd'", Boolean.TRUE);
        checkBoolean("x'1234'\n'' = x'1234'", Boolean.TRUE);
        checkBoolean("x''\n'ab' = x'ab'", Boolean.TRUE);
    }

    public void testRow()
    {
        setFor(SqlStdOperatorTable.rowConstructor);
    }

    public void testAndOperator()
    {
        setFor(SqlStdOperatorTable.andOperator);
        checkBoolean("true and false", Boolean.FALSE);
        checkBoolean("true and true", Boolean.TRUE);
        checkBoolean(
            "cast(null as boolean) and false",
            Boolean.FALSE);
        checkBoolean(
            "false and cast(null as boolean)",
            Boolean.FALSE);
        checkNull("cast(null as boolean) and true");
        checkBoolean("true and (not false)", Boolean.TRUE);
    }

    public void testConcatOperator()
    {
        setFor(SqlStdOperatorTable.concatOperator);
        checkString(" 'a'||'b' ", "ab", "CHAR(2) NOT NULL");

        if (todo) {
            // not yet implemented
            checkString(
                " x'f'||x'f' ",
                "X'FF",
                "BINARY(1) NOT NULL");
            checkNull("x'ff' || cast(null as varbinary)");
        }
    }

    public void testDivideOperator()
    {
        setFor(SqlStdOperatorTable.divideOperator);
        checkScalarExact("10 / 5", "2");
        checkScalarExact("-10 / 5", "-2");
        checkScalarExact("1 / 3", "0");
        checkScalarApprox(
            " cast(10.0 as double) / 5",
            "DOUBLE NOT NULL",
            2.0,
            0);
        checkScalarApprox(
            " cast(10.0 as real) / 5",
            "REAL NOT NULL",
            2.0,
            0);
        checkScalarApprox(
            " 6.0 / cast(10.0 as real) ",
            "DOUBLE NOT NULL",
            0.6,
            0);
        checkScalarExact(
            "10.0 / 5.0",
            "DECIMAL(9, 6) NOT NULL",
            "2.000000");
        checkScalarExact(
            "1.0 / 3.0",
            "DECIMAL(8, 6) NOT NULL",
            "0.333333");
        checkScalarExact(
            "100.1 / 0.0001",
            "DECIMAL(14, 7) NOT NULL",
            "1001000.0000000");
        checkScalarExact(
            "100.1 / 0.00000001",
            "DECIMAL(19, 8) NOT NULL",
            "10010000000.00000000");
        checkNull("1e1 / cast(null as float)");

        checkFails(
            "100.1 / 0.00000000000000001",
            outOfRangeMessage,
            true);

        checkNull(
            "cast(null as interval month) / 2");

    }

    public void testIntervalDivide()
    {
        setFor(SqlStdOperatorTable.divideOperator);
        checkScalar(
            "interval '1:37' minute to second / 0.5",
            "3:14",
            "INTERVAL MINUTE TO SECOND NOT NULL");
        checkScalar(
            "interval '2:5:12' hour to second / 2 / -3",
            "-0:20:52",
            "INTERVAL HOUR TO SECOND NOT NULL");
        checkScalar(
            "interval '1 23:45:06' day to second / 42",
            "0 01:08:13",
            "INTERVAL DAY TO SECOND NOT NULL");
        checkScalar(
            "interval '-2:2' hour to minute / 3",
            "-0:40",
            "INTERVAL HOUR TO MINUTE NOT NULL");
        checkScalar(
            "interval '-10 23:45' day to minute / 5",
            "-2 04:45",
            "INTERVAL DAY TO MINUTE NOT NULL");
        checkScalar(
            "interval '23 8' DAY TO HOUR / 14",
            "1 16",
            "INTERVAL DAY TO HOUR NOT NULL");
        checkScalar(
            "interval '-45' second / 9",
            "-5",
            "INTERVAL SECOND NOT NULL");
        checkScalar(
            "interval '12' minute / -3",
            "-4",
            "INTERVAL MINUTE NOT NULL");
        checkScalar(
            "interval '17' hour / -4",
            "-4",
            "INTERVAL HOUR NOT NULL");
        checkScalar(
            "interval '25' day / 5",
            "5",
            "INTERVAL DAY NOT NULL");
        checkScalar(
            "interval -'10' month / 6",
            "-1",
            "INTERVAL MONTH NOT NULL");
        checkScalar(
            "interval '45' year / 15",
            "3",
            "INTERVAL YEAR NOT NULL");
        checkScalar(
            "interval '39-8' year to month / 4",
            "9-11",
            "INTERVAL YEAR TO MONTH NOT NULL");
        checkScalar(
            "interval '3-3' year to month / 15e-1",
            "+02-02",
            "INTERVAL YEAR TO MONTH NOT NULL");
        checkScalar(
            "interval '3-4' year to month / 4.5",
            "+00-08",
            "INTERVAL YEAR TO MONTH NOT NULL");

        checkNull(
            "interval '2' day / cast(null as bigint)");
    }

    public void testEqualsOperator()
    {
        setFor(SqlStdOperatorTable.equalsOperator);
        checkBoolean("1=1", Boolean.TRUE);
        checkBoolean("1=1.0", Boolean.TRUE);
        checkBoolean("1.34=1.34", Boolean.TRUE);
        checkBoolean("1=1.34", Boolean.FALSE);
        checkBoolean("1e2=100e0", Boolean.TRUE);
        checkBoolean("1e2=101", Boolean.FALSE);
        checkBoolean(
            "cast(1e2 as real)=cast(101 as bigint)",
            Boolean.FALSE);
        checkBoolean("'a'='b'", Boolean.FALSE);
        checkBoolean(
            "cast('a' as varchar(30))=cast('a' as varchar(30))",
            Boolean.TRUE);
        checkBoolean(
            "cast('a ' as varchar(30))=cast('a' as varchar(30))",
            Boolean.TRUE);
        checkBoolean(
            "cast('a' as varchar(30))=cast('b' as varchar(30))",
            Boolean.FALSE);
        checkBoolean(
            "cast('a' as varchar(30))=cast('a' as varchar(15))",
            Boolean.TRUE);
        checkNull("cast(null as boolean)=cast(null as boolean)");
        checkNull("cast(null as integer)=1");
        checkNull("cast(null as varchar(10))='a'");
    }

    public void testIntervalEqualsOperator()
    {
        setFor(SqlStdOperatorTable.equalsOperator);
        checkBoolean(
            "interval '2' day = interval '2' hour",
            Boolean.FALSE);
        checkBoolean(
            "interval '2:2:2' hour to second = interval '2' hour",
            Boolean.FALSE);
        checkBoolean(
            "interval '1' second = interval '1' second",
            Boolean.TRUE);
        checkBoolean(
            "interval '1' minute = interval '2' minute",
            Boolean.FALSE);
        checkBoolean(
            "interval '-2' hour = interval -'2' hour",
            Boolean.TRUE);
        checkBoolean(
            "interval '3' day = interval -'3' day",
            Boolean.FALSE);
        checkBoolean(
            "interval '-4' month = interval '-4' month",
            Boolean.TRUE);
        checkBoolean(
            "interval '5' year = interval '-5' year",
            Boolean.FALSE);
        checkBoolean(
            "interval '1:2' minute to second = interval '01:02' minute to second",
            Boolean.TRUE);
        checkBoolean(
            "interval '-1:2:3' hour to second = interval '1:2:3' hour to second",
            Boolean.FALSE);
        checkBoolean(
            "interval '1 2:3:4' day to second = interval '1 02:03:04' day to second",
            Boolean.TRUE);
        checkBoolean(
            "interval '1:2' hour to minute = interval '1:0' hour to minute",
            Boolean.FALSE);
        checkBoolean(
            "interval '-2 3:4' day to minute = interval '-2 03:04' day to minute",
            Boolean.TRUE);
        checkBoolean(
            "interval '-1 1' day to hour = interval -'1 1' day to hour",
            Boolean.FALSE);
        checkBoolean(
            "interval '1' month = interval '1' month",
            Boolean.TRUE);
        checkBoolean(
            "interval '-2' year = interval '2' year",
            Boolean.FALSE);
        checkBoolean(
            "interval '-1-1' year to month = interval '1-1' year to month",
            Boolean.FALSE);

        checkNull(
            "cast(null as interval hour) = interval '2' minute");
    }

    public void testGreaterThanOperator()
    {
        setFor(SqlStdOperatorTable.greaterThanOperator);
        checkBoolean("1>2", Boolean.FALSE);
        checkBoolean(
            "cast(-1 as TINYINT)>cast(1 as TINYINT)",
            Boolean.FALSE);
        checkBoolean(
            "cast(1 as SMALLINT)>cast(1 as SMALLINT)",
            Boolean.FALSE);
        checkBoolean("2>1", Boolean.TRUE);
        checkBoolean("1.1>1.2", Boolean.FALSE);
        checkBoolean("-1.1>-1.2", Boolean.TRUE);
        checkBoolean("1.1>1.1", Boolean.FALSE);
        checkBoolean("1.2>1", Boolean.TRUE);
        checkBoolean("1.1e1>1.2e1", Boolean.FALSE);
        checkBoolean(
            "cast(-1.1 as real) > cast(-1.2 as real)",
            Boolean.TRUE);
        checkBoolean("1.1e2>1.1e2", Boolean.FALSE);
        checkBoolean("1.2e0>1", Boolean.TRUE);
        checkBoolean("cast(1.2e0 as real)>1", Boolean.TRUE);
        checkBoolean("true>false", Boolean.TRUE);
        checkBoolean("true>true", Boolean.FALSE);
        checkBoolean("false>false", Boolean.FALSE);
        checkBoolean("false>true", Boolean.FALSE);
        checkNull("3.0>cast(null as double)");
    }

    public void testIntervalGreaterThan()
    {
        setFor(SqlStdOperatorTable.greaterThanOperator);
        checkBoolean(
            "interval '2' day > interval '1' day",
            Boolean.TRUE);
        checkBoolean(
            "interval '2' day > interval '5' day",
            Boolean.FALSE);
        checkBoolean(
            "interval '2 2:2:2' day to second > interval '2' day",
            Boolean.TRUE);
        checkBoolean(
            "interval '2' day > interval '2' day",
            Boolean.FALSE);
        checkBoolean(
            "interval '2' day > interval '-2' day",
            Boolean.TRUE);
        checkBoolean(
            "interval '2' day > interval '2' hour",
            Boolean.TRUE);
        checkBoolean(
            "interval '2' minute > interval '2' hour",
            Boolean.FALSE);
        checkBoolean(
            "interval '2' second > interval '2' minute",
            Boolean.FALSE);
        checkBoolean(
            "interval '10' month > interval '9' month",
            Boolean.TRUE);
        checkBoolean(
            "interval '-2' year > interval '-3' year",
            Boolean.TRUE);
        checkBoolean(
            "interval '-2-2' year to month > interval '-2-1' year to month",
            Boolean.FALSE);

        checkNull(
            "cast(null as interval hour) > interval '2' minute");
        checkNull(
            "interval '2:2' hour to minute > cast(null as interval second)");
    }

    public void testIsDistinctFromOperator()
    {
        setFor(SqlStdOperatorTable.isDistinctFromOperator);
        checkBoolean("1 is distinct from 1", Boolean.FALSE);
        checkBoolean("1 is distinct from 1.0", Boolean.FALSE);
        checkBoolean("1 is distinct from 2", Boolean.TRUE);
        checkBoolean(
            "cast(null as integer) is distinct from 2",
            Boolean.TRUE);
        checkBoolean(
            "cast(null as integer) is distinct from cast(null as integer)",
            Boolean.FALSE);
        checkBoolean("1.23 is distinct from 1.23", Boolean.FALSE);
        checkBoolean("1.23 is distinct from 5.23", Boolean.TRUE);
        checkBoolean(
            "-23e0 is distinct from -2.3e1",
            Boolean.FALSE);
        //checkBoolean("row(1,1) is distinct from row(1,1)",
        //Boolean.TRUE); checkBoolean("row(1,1) is distinct from
        //row(1,2)", Boolean.FALSE);

        // Intervals
        checkBoolean(
            "interval '2' day is distinct from interval '1' day",
            Boolean.TRUE);
        checkBoolean(
            "interval '10' hour is distinct from interval '10' hour",
            Boolean.FALSE);
        checkBoolean(
            "interval '1-1' year to month is distinct from interval '1-2' year to month",
            Boolean.TRUE);
        checkBoolean(
            "interval '-2' year is distinct from interval -'2' year",
            Boolean.FALSE);
    }

    public void testIsNotDistinctFromOperator()
    {
        setFor(SqlStdOperatorTable.isNotDistinctFromOperator);
        checkBoolean("1 is not distinct from 1", Boolean.TRUE);
        checkBoolean("1 is not distinct from 1.0", Boolean.TRUE);
        checkBoolean("1 is not distinct from 2", Boolean.FALSE);
        checkBoolean(
            "cast(null as integer) is not distinct from 2",
            Boolean.FALSE);
        checkBoolean(
            "cast(null as integer) is not distinct from cast(null as integer)",
            Boolean.TRUE);
        checkBoolean(
            "1.23 is not distinct from 1.23",
            Boolean.TRUE);
        checkBoolean(
            "1.23 is not distinct from 5.23",
            Boolean.FALSE);
        checkBoolean(
            "-23e0 is not distinct from -2.3e1",
            Boolean.TRUE);
        //checkBoolean("row(1,1) is not distinct from row(1,1)",
        //Boolean.FALSE); checkBoolean("row(1,1) is not distinct
        //from row(1,2)", Boolean.TRUE);

        // Intervals
        checkBoolean(
            "interval '2' day is not distinct from interval '1' day",
            Boolean.FALSE);
        checkBoolean(
            "interval '10' hour is not distinct from interval '10' hour",
            Boolean.TRUE);
        checkBoolean(
            "interval '2:2:2' hour to second is not distinct from interval '02:02:02' hour to second",
            Boolean.TRUE);
        checkBoolean(
            "interval '-1-2' year to month is not distinct from interval '-1' year",
            Boolean.FALSE);
    }

    public void testGreaterThanOrEqualOperator()
    {
        setFor(SqlStdOperatorTable.greaterThanOrEqualOperator);
        checkBoolean("1>=2", Boolean.FALSE);
        checkBoolean("-1>=1", Boolean.FALSE);
        checkBoolean("1>=1", Boolean.TRUE);
        checkBoolean("2>=1", Boolean.TRUE);
        checkBoolean("1.1>=1.2", Boolean.FALSE);
        checkBoolean("-1.1>=-1.2", Boolean.TRUE);
        checkBoolean("1.1>=1.1", Boolean.TRUE);
        checkBoolean("1.2>=1", Boolean.TRUE);
        checkBoolean("1.2e4>=1e5", Boolean.FALSE);
        checkBoolean("1.2e4>=cast(1e5 as real)", Boolean.FALSE);
        checkBoolean("1.2>=cast(1e5 as double)", Boolean.FALSE);
        checkBoolean("120000>=cast(1e5 as real)", Boolean.TRUE);
        checkBoolean("true>=false", Boolean.TRUE);
        checkBoolean("true>=true", Boolean.TRUE);
        checkBoolean("false>=false", Boolean.TRUE);
        checkBoolean("false>=true", Boolean.FALSE);
        checkNull("cast(null as real)>=999");

        // Intervals
        checkBoolean(
            "interval '2' day >= interval '1' day",
            Boolean.TRUE);
        checkBoolean(
            "interval '2' day >= interval '5' day",
            Boolean.FALSE);
        checkBoolean(
            "interval '2 2:2:2' day to second >= interval '2' day",
            Boolean.TRUE);
        checkBoolean(
            "interval '2' day >= interval '2' day",
            Boolean.TRUE);
        checkBoolean(
            "interval '2' day >= interval '-2' day",
            Boolean.TRUE);
        checkBoolean(
            "interval '2' day >= interval '2' hour",
            Boolean.TRUE);
        checkBoolean(
            "interval '2' minute >= interval '2' hour",
            Boolean.FALSE);
        checkBoolean(
            "interval '2' second >= interval '2' minute",
            Boolean.FALSE);
        checkBoolean(
            "interval '1-3' year to month >= interval '1' year",
            Boolean.TRUE);
        checkBoolean(
            "interval '-1' year >= interval '1-1' year to month",
            Boolean.FALSE);

        checkNull(
            "cast(null as interval hour) >= interval '2' minute");
        checkNull(
            "interval '2:2' hour to minute >= cast(null as interval second)");
    }

    public void testInOperator()
    {
        setFor(SqlStdOperatorTable.inOperator);
    }

    public void testOverlapsOperator()
    {
        setFor(SqlStdOperatorTable.overlapsOperator);
        if (Bug.Frg187Fixed) {
            checkBoolean(
                "(date '1-2-3', date '1-2-3') overlaps (date '1-2-3', interval '1' year)",
                Boolean.TRUE);
            checkBoolean(
                "(date '1-2-3', date '1-2-3') overlaps (date '4-5-6', interval '1' year)",
                Boolean.FALSE);
            checkBoolean(
                "(date '1-2-3', date '4-5-6') overlaps (date '2-2-3', date '3-4-5')",
                Boolean.TRUE);
            checkNull(
                "(cast(null as date), date '1-2-3') overlaps (date '1-2-3', interval '1' year)");
            checkNull(
                "(date '1-2-3', date '1-2-3') overlaps (date '1-2-3', cast(null as date))");

            checkBoolean(
                "(time '1:2:3', interval '1' second) overlaps (time '23:59:59', time '1:2:3')",
                Boolean.TRUE);
            checkBoolean(
                "(time '1:2:3', interval '1' second) overlaps (time '23:59:59', time '1:2:2')",
                Boolean.FALSE);
            checkBoolean(
                "(time '1:2:3', interval '1' second) overlaps (time '23:59:59', interval '2' hour)",
                Boolean.TRUE);
            checkNull(
                "(time '1:2:3', cast(null as time)) overlaps (time '23:59:59', time '1:2:3')");
            checkNull(
                "(time '1:2:3', interval '1' second) overlaps (time '23:59:59', cast(null as interval hour))");

            checkBoolean(
                "(timestamp '1-2-3 4:5:6', timestamp '1-2-3 4:5:6' ) overlaps (timestamp '1-2-3 4:5:6', interval '1 2:3:4.5' day to second)",
                Boolean.TRUE);
            checkBoolean(
                "(timestamp '1-2-3 4:5:6', timestamp '1-2-3 4:5:6' ) overlaps (timestamp '2-2-3 4:5:6', interval '1 2:3:4.5' day to second)",
                Boolean.FALSE);

            checkBoolean(
                "(timestamp '1-2-3 4:5:6', timestamp '2-3-4 5:6:7') overlaps (timestamp '3-4-5 6:7:8', interval '1-2' year to month)",
                Boolean.TRUE);
            checkBoolean(
                "(timestamp '1-2-3 4:5:6', interval '1-1' year to month) overlaps (timestamp '2-3-4 5:6:7', timestamp '3-4-5 6:7:8')",
                Boolean.FALSE);

            checkNull(
                "(timestamp '1-2-3 4:5:6', cast(null as interval day) ) overlaps (timestamp '1-2-3 4:5:6', interval '1 2:3:4.5' day to second)");
            checkNull(
                "(timestamp '1-2-3 4:5:6', timestamp '1-2-3 4:5:6' ) overlaps (cast(null as timestamp), interval '1 2:3:4.5' day to second)");
        }
    }

    public void testLessThanOperator()
    {
        setFor(SqlStdOperatorTable.lessThanOperator);
        checkBoolean("1<2", Boolean.TRUE);
        checkBoolean("-1<1", Boolean.TRUE);
        checkBoolean("1<1", Boolean.FALSE);
        checkBoolean("2<1", Boolean.FALSE);
        checkBoolean("1.1<1.2", Boolean.TRUE);
        checkBoolean("-1.1<-1.2", Boolean.FALSE);
        checkBoolean("1.1<1.1", Boolean.FALSE);
        checkBoolean("cast(1.1 as real)<1", Boolean.FALSE);
        checkBoolean("cast(1.1 as real)<1.1", Boolean.FALSE);
        checkBoolean(
            "cast(1.1 as real)<cast(1.2 as real)",
            Boolean.TRUE);
        checkBoolean("-1.1e-1<-1.2e-1", Boolean.FALSE);
        checkBoolean(
            "cast(1.1 as real)<cast(1.1 as double)",
            Boolean.FALSE);
        checkBoolean("true<false", Boolean.FALSE);
        checkBoolean("true<true", Boolean.FALSE);
        checkBoolean("false<false", Boolean.FALSE);
        checkBoolean("false<true", Boolean.TRUE);
        checkNull("123<cast(null as bigint)");
        checkNull("cast(null as tinyint)<123");
        checkNull("cast(null as integer)<1.32");

    }

    public void testIntervalLessThan()
    {
        setFor(SqlStdOperatorTable.lessThanOperator);
    // Intervals
        checkBoolean(
            "interval '2' day < interval '1' day",
            Boolean.FALSE);
        checkBoolean(
            "interval '2' day < interval '5' day",
            Boolean.TRUE);
        checkBoolean(
            "interval '2 2:2:2' day to second < interval '2' day",
            Boolean.FALSE);
        checkBoolean(
            "interval '2' day < interval '2' day",
            Boolean.FALSE);
        checkBoolean(
            "interval '2' day < interval '-2' day",
            Boolean.FALSE);
        checkBoolean(
            "interval '2' day < interval '2' hour",
            Boolean.FALSE);
        checkBoolean(
            "interval '2' minute < interval '2' hour",
            Boolean.TRUE);
        checkBoolean(
            "interval '2' second < interval '2' minute",
            Boolean.TRUE);
        checkBoolean(
            "interval '1-3' year to month < interval '1' year",
            Boolean.FALSE);
        checkBoolean(
            "interval '-1' year < interval '1-1' year to month",
            Boolean.TRUE);

        checkNull(
            "cast(null as interval hour) < interval '2' minute");
        checkNull(
            "interval '2:2' hour to minute < cast(null as interval second)");
    }

    public void testLessThanOrEqualOperator()
    {
        setFor(SqlStdOperatorTable.lessThanOrEqualOperator);
        checkBoolean("1<=2", Boolean.TRUE);
        checkBoolean("1<=1", Boolean.TRUE);
        checkBoolean("-1<=1", Boolean.TRUE);
        checkBoolean("2<=1", Boolean.FALSE);
        checkBoolean("1.1<=1.2", Boolean.TRUE);
        checkBoolean("-1.1<=-1.2", Boolean.FALSE);
        checkBoolean("1.1<=1.1", Boolean.TRUE);
        checkBoolean("1.2<=1", Boolean.FALSE);
        checkBoolean("1<=cast(1e2 as real)", Boolean.TRUE);
        checkBoolean("1000<=cast(1e2 as real)", Boolean.FALSE);
        checkBoolean("1.2e1<=1e2", Boolean.TRUE);
        checkBoolean("1.2e1<=cast(1e2 as real)", Boolean.TRUE);
        checkBoolean("true<=false", Boolean.FALSE);
        checkBoolean("true<=true", Boolean.TRUE);
        checkBoolean("false<=false", Boolean.TRUE);
        checkBoolean("false<=true", Boolean.TRUE);
        checkNull("cast(null as real)<=cast(1 as real)");
        checkNull("cast(null as integer)<=3");
        checkNull("3<=cast(null as smallint)");
        checkNull("cast(null as integer)<=1.32");

    }
    public void testIntervalLessThanOrEqual()
    {
        setFor(SqlStdOperatorTable.lessThanOrEqualOperator);
        checkBoolean(
            "interval '2' day <= interval '1' day",
            Boolean.FALSE);
        checkBoolean(
            "interval '2' day <= interval '5' day",
            Boolean.TRUE);
        checkBoolean(
            "interval '2 2:2:2' day to second <= interval '2' day",
            Boolean.FALSE);
        checkBoolean(
            "interval '2' day <= interval '2' day",
            Boolean.TRUE);
        checkBoolean(
            "interval '2' day <= interval '-2' day",
            Boolean.FALSE);
        checkBoolean(
            "interval '2' day <= interval '2' hour",
            Boolean.FALSE);
        checkBoolean(
            "interval '2' minute <= interval '2' hour",
            Boolean.TRUE);
        checkBoolean(
            "interval '2' second <= interval '2' minute",
            Boolean.TRUE);
        checkBoolean(
            "interval '-2' year <= interval '-3' year",
            Boolean.FALSE);
        checkBoolean(
            "interval '-2-2' year to month <= interval '-2-1' year to month",
            Boolean.TRUE);

        checkNull(
            "cast(null as interval hour) <= interval '2' minute");
        checkNull(
            "interval '2:2' hour to minute <= cast(null as interval second)");
    }

    public void testMinusOperator()
    {
        setFor(SqlStdOperatorTable.minusOperator);
        checkScalarExact("-2-1", "-3");
        checkScalarExact("-2-1-5", "-8");
        checkScalarExact("2-1", "1");
        checkScalarApprox(
            "cast(2.0 as double) -1",
            "DOUBLE NOT NULL",
            1,
            0);
        checkScalarApprox(
            "cast(1 as smallint)-cast(2.0 as real)",
            "REAL NOT NULL",
            -1,
            0);
        checkScalarApprox(
            "2.4-cast(2.0 as real)",
            "DOUBLE NOT NULL",
            0.4,
            0.00000001);
        checkScalarExact("1-2", "-1");
        checkScalarExact(
            "10.0 - 5.0",
            "DECIMAL(4, 1) NOT NULL",
            "5.0");
        checkScalarExact(
            "19.68 - 4.2",
            "DECIMAL(5, 2) NOT NULL",
            "15.48");
        checkNull("1e1-cast(null as double)");
        checkNull("cast(null as tinyint) - cast(null as smallint)");

        // TODO: Fix bug
        if (Bug.Fn25Fixed) {
            // Should throw out of range error
            checkFails(
                "cast(100 as tinyint) - cast(-100 as tinyint)",
                outOfRangeMessage,
                true);
            checkFails(
                "cast(-20000 as smallint) - cast(20000 as smallint)",
                outOfRangeMessage,
                true);
            checkFails(
                "cast(1.5e9 as integer) - cast(-1.5e9 as integer)",
                outOfRangeMessage,
                true);
            checkFails(
                "cast(-5e18 as bigint) - cast(5e18 as bigint)",
                outOfRangeMessage,
                true);
            checkFails(
                "cast(5e18 as decimal(19,0)) - cast(-5e18 as decimal(19,0))",
                outOfRangeMessage,
                true);
            checkFails(
                "cast(-5e8 as decimal(19,10)) - cast(5e8 as decimal(19,10))",
                outOfRangeMessage,
                true);
        }
    }

    public void testMinusIntervalOperator()
    {
        setFor(SqlStdOperatorTable.minusOperator);

        // Intervals
        checkScalar(
            "interval '2' day - interval '1' day",
            "+1",
            "INTERVAL DAY NOT NULL");
        checkScalar(
            "interval '2' day - interval '1' minute",
            "+1 23:59",
            "INTERVAL DAY TO MINUTE NOT NULL");
        checkScalar(
            "interval '2' year - interval '1' month",
            "+1-11",
            "INTERVAL YEAR TO MONTH NOT NULL");
        checkScalar(
            "interval '2' year - interval '1' month - interval '3' year",
            "-1-01",
            "INTERVAL YEAR TO MONTH NOT NULL");
        checkNull(
            "cast(null as interval day) + interval '2' hour");

        // Datetime minus interval
        checkScalar(
            "time '12:03:01' - interval '1:1' hour to minute",
            "11:02:01",
            "TIME(0) NOT NULL");
        checkScalar(
            "date '2005-03-02' - interval '5' day",
            "2005-02-25",
            "DATE NOT NULL");
        checkScalar(
            "timestamp '2003-08-02 12:54:01' - interval '-4 2:4' day to minute",
            "2003-08-06 14:58:01.0",
            "TIMESTAMP(0) NOT NULL");

        checkScalar(
            "date '1864-07-04' - interval '87' year",
            "1776-07-04",
            "DATE NOT NULL");
        checkScalar(
            "date '1918-11-11' - interval '-11' month",
            "1919-10-11",
            "DATE NOT NULL");
        checkScalar(
            "timestamp '2007-07-18 12:34:56' - interval '4-2' year to month",
            "2003-05-18 12:34:56.0",
            "TIMESTAMP(0) NOT NULL");
    }

    public void testMinusDateOperator()
    {
        setFor(SqlStdOperatorTable.minusDateOperator);
        checkScalar(
            "(time '12:03:34' - time '11:57:23') minute to second",
            "+6:11",
            "INTERVAL MINUTE TO SECOND NOT NULL");
        checkScalar(
            "(time '12:03:23' - time '11:57:23') minute",
            "+6",
            "INTERVAL MINUTE NOT NULL");
        checkScalar(
            "(time '12:03:34' - time '11:57:23') minute",
            "+6",
            "INTERVAL MINUTE NOT NULL");
        checkScalar(
            "(timestamp '2004-05-01 12:03:34' - timestamp '2004-04-29 11:57:23') day to second",
            "+2 00:06:11",
            "INTERVAL DAY TO SECOND NOT NULL");
        checkScalar(
            "(timestamp '2004-05-01 12:03:34' - timestamp '2004-04-29 11:57:23') day to hour",
            "+2 00",
            "INTERVAL DAY TO HOUR NOT NULL");
        checkScalar(
            "(date '2004-12-02' - date '2003-12-01') day",
            "+367",
            "INTERVAL DAY NOT NULL");
        checkNull(
            "(cast(null as date) - date '2003-12-01') day");

        checkScalar(
            "(date '1864-07-04' - date '1776-07-04') year",
            "+87",
            "INTERVAL YEAR NOT NULL");
        checkScalar(
            "(date '1968-04-15' - date '2007-08-27') month",
            "-472",
            "INTERVAL MONTH NOT NULL");
        checkScalar(
            "(date '1941-12-07' - date '1918-11-11') year to month",
            "+23-01",
            "INTERVAL YEAR TO MONTH NOT NULL");
    }

    public void testMultiplyOperator()
    {
        setFor(SqlStdOperatorTable.multiplyOperator);
        checkScalarExact("2*3", "6");
        checkScalarExact("2*-3", "-6");
        checkScalarExact("+2*3", "6");
        checkScalarExact("2*0", "0");
        checkScalarApprox(
            "cast(2.0 as float)*3",
            "FLOAT NOT NULL",
            6,
            0);
        checkScalarApprox(
            "3*cast(2.0 as real)",
            "REAL NOT NULL",
            6,
            0);
        checkScalarApprox(
            "cast(2.0 as real)*3.2",
            "DOUBLE NOT NULL",
            6.4,
            0);
        checkScalarExact(
            "10.0 * 5.0",
            "DECIMAL(5, 2) NOT NULL",
            "50.00");
        checkScalarExact(
            "19.68 * 4.2",
            "DECIMAL(6, 3) NOT NULL",
            "82.656");
        checkNull("cast(1 as real)*cast(null as real)");
        checkNull("2e-3*cast(null as integer)");
        checkNull("cast(null as tinyint) * cast(4 as smallint)");

        if (Bug.Fn25Fixed) {
            // Should throw out of range error
            checkFails(
                "cast(100 as tinyint) * cast(-2 as tinyint)",
                outOfRangeMessage,
                true);
            checkFails(
                "cast(200 as smallint) * cast(200 as smallint)",
                outOfRangeMessage,
                true);
            checkFails(
                "cast(1.5e9 as integer) * cast(-2 as integer)",
                outOfRangeMessage,
                true);
            checkFails(
                "cast(5e9 as bigint) * cast(2e9 as bigint)",
                outOfRangeMessage,
                true);
            checkFails(
                "cast(2e9 as decimal(19,0)) * cast(-5e9 as decimal(19,0))",
                outOfRangeMessage,
                true);
            checkFails(
                "cast(5e4 as decimal(19,10)) * cast(2e4 as decimal(19,10))",
                outOfRangeMessage,
                true);
        }

    }

    public void testIntervalMultiply()
    {
        setFor(SqlStdOperatorTable.multiplyOperator);
        checkScalar(
            "interval '2:2' hour to minute * 3",
            "+6:06",
            "INTERVAL HOUR TO MINUTE NOT NULL");
        checkScalar(
            "3 * 2 * interval '2:5:12' hour to second",
            "+12:31:12",
            "INTERVAL HOUR TO SECOND NOT NULL");
        checkScalar(
            "interval '1 3:46:20' day to second * -0.001",
            "-0 0:01:40",
            "INTERVAL DAY TO SECOND NOT NULL");
        checkScalar(
            "interval '-1 16' day to hour * 24",
            "-40 0",
            "INTERVAL DAY TO HOUR NOT NULL");
        checkScalar(
            "interval '23 13:47:52' day to second * 0",
            "+0 0:00:00",
            "INTERVAL DAY TO SECOND NOT NULL");

        checkNull(
            "interval '2' day * cast(null as bigint)");
        checkNull(
            "cast(null as interval month) * 2");

        checkScalar(
            "interval '3-2' year to month * 15e-1",
            "+04-09",
            "INTERVAL YEAR TO MONTH NOT NULL");
        checkScalar(
            "interval '3-4' year to month * 4.5",
            "+15-00",
            "INTERVAL YEAR TO MONTH NOT NULL");
        checkScalar(
            "interval '-44' year * 0.1",
            "-4",
            "INTERVAL YEAR NOT NULL");
        checkScalar(
            "interval '6' month * -4",
            "-24",
            "INTERVAL MONTH NOT NULL");
        checkScalar(
            "interval '17-9' year to month * 0",
            "+0-0",
            "INTERVAL YEAR TO MONTH NOT NULL");
    }

    public void testNotEqualsOperator()
    {
        setFor(SqlStdOperatorTable.notEqualsOperator);
        checkBoolean("1<>1", Boolean.FALSE);
        checkBoolean("'a'<>'A'", Boolean.TRUE);
        checkBoolean("1e0<>1e1", Boolean.TRUE);
        checkNull("'a'<>cast(null as varchar(1))");

        // Intervals
        checkBoolean(
            "interval '2' day <> interval '1' day",
            Boolean.TRUE);
        checkBoolean(
            "interval '2' day <> interval '2' day",
            Boolean.FALSE);
        checkBoolean(
            "interval '2:2:2' hour to second <> interval '2' hour",
            Boolean.TRUE);
        checkBoolean(
            "interval '-3' year <> interval '3' year",
            Boolean.TRUE);
        checkBoolean(
            "interval '-3-3' year to month <> interval '-3' year",
            Boolean.FALSE);

        checkNull(
            "cast(null as interval hour) <> interval '2' minute");
        checkNull(
            "cast(null as interval second) <> cast(null as interval second)");

        // "!=" is not an acceptable alternative to "<>"
        checkFails(
            "1 ^!^= 1",
            "(?s).*Encountered: \"!\" \\(33\\).*",
            false);
    }

    public void testOrOperator()
    {
        setFor(SqlStdOperatorTable.orOperator);
        checkBoolean("true or false", Boolean.TRUE);
        checkBoolean("false or false", Boolean.FALSE);
        checkBoolean("true or cast(null as boolean)", Boolean.TRUE);
        checkNull("false or cast(null as boolean)");
    }

    public void testPlusOperator()
    {
        setFor(SqlStdOperatorTable.plusOperator);
        checkScalarExact("1+2", "3");
        checkScalarExact("-1+2", "1");
        checkScalarExact("1+2+3", "6");
        checkScalarApprox(
            "1+cast(2.0 as double)",
            "DOUBLE NOT NULL",
            3,
            0);
        checkScalarApprox(
            "1+cast(2.0 as double)+cast(6.0 as float)",
            "DOUBLE NOT NULL",
            9,
            0);
        checkScalarExact(
            "10.0 + 5.0",
            "DECIMAL(4, 1) NOT NULL",
            "15.0");
        checkScalarExact(
            "19.68 + 4.2",
            "DECIMAL(5, 2) NOT NULL",
            "23.88");
        checkScalarExact(
            "19.68 + 4.2 + 6",
            "DECIMAL(13, 2) NOT NULL",
            "29.88");
        checkScalarApprox(
            "19.68 + cast(4.2 as float)",
            "DOUBLE NOT NULL",
            23.88,
            0);
        checkNull("cast(null as tinyint)+1");
        checkNull("1e-2+cast(null as double)");

        if (Bug.Fn25Fixed) {
            // Should throw out of range error
            checkFails(
                "cast(100 as tinyint) + cast(100 as tinyint)",
                outOfRangeMessage,
                true);
            checkFails(
                "cast(-20000 as smallint) + cast(-20000 as smallint)",
                outOfRangeMessage,
                true);
            checkFails(
                "cast(1.5e9 as integer) + cast(1.5e9 as integer)",
                outOfRangeMessage,
                true);
            checkFails(
                "cast(5e18 as bigint) + cast(5e18 as bigint)",
                outOfRangeMessage,
                true);
            checkFails(
                "cast(-5e18 as decimal(19,0)) + cast(-5e18 as decimal(19,0))",
                outOfRangeMessage,
                true);
            checkFails(
                "cast(5e8 as decimal(19,10)) + cast(5e8 as decimal(19,10))",
                outOfRangeMessage,
                true);
        }
    }

    public void testIntervalPlus()
    {
        setFor(SqlStdOperatorTable.plusOperator);

        // Intervals
        checkScalar(
            "interval '2' day + interval '1' day",
            "+3",
            "INTERVAL DAY NOT NULL");
        checkScalar(
            "interval '2' day + interval '1' minute",
            "+2 00:01",
            "INTERVAL DAY TO MINUTE NOT NULL");
        checkScalar(
            "interval '2' day + interval '5' minute + interval '-3' second",
            "+2 00:04:57",
            "INTERVAL DAY TO SECOND NOT NULL");
        checkScalar(
            "interval '2' year + interval '1' month",
            "+2-01",
            "INTERVAL YEAR TO MONTH NOT NULL");
        checkNull(
            "interval '2' year + cast(null as interval month)");

        // Datetime plus interval
        checkScalar(
            "time '12:03:01' + interval '1:1' hour to minute",
            "13:04:01",
            "TIME(0) NOT NULL");
        checkScalar(
            "interval '5' day + date '2005-03-02'",
            "2005-03-07",
            "DATE NOT NULL");
        checkScalar(
            "timestamp '2003-08-02 12:54:01' + interval '-4 2:4' day to minute",
            "2003-07-29 10:50:01.0",
            "TIMESTAMP(0) NOT NULL");

    }

    public void testDescendingOperator()
    {
        setFor(SqlStdOperatorTable.descendingOperator);
    }

    public void testIsNotNullOperator()
    {
        setFor(SqlStdOperatorTable.isNotNullOperator);
        checkBoolean("true is not null", Boolean.TRUE);
        checkBoolean(
            "cast(null as boolean) is not null",
            Boolean.FALSE);
    }

    public void testIsNullOperator()
    {
        setFor(SqlStdOperatorTable.isNullOperator);
        checkBoolean("true is null", Boolean.FALSE);
        checkBoolean("cast(null as boolean) is null",
            Boolean.TRUE);
    }

    public void testIsNotTrueOperator()
    {
        setFor(SqlStdOperatorTable.isNotTrueOperator);
        checkBoolean("true is not true", Boolean.FALSE);
        checkBoolean("false is not true", Boolean.TRUE);
        checkBoolean(
            "cast(null as boolean) is not true",
            Boolean.TRUE);
        checkFails(
            "select ^'a string' is not true^ from (values (1))",
            "(?s)Cannot apply 'IS NOT TRUE' to arguments of type '<CHAR\\(8\\)> IS NOT TRUE'. Supported form\\(s\\): '<BOOLEAN> IS NOT TRUE'.*",
            false);
    }

    public void testIsTrueOperator()
    {
        setFor(SqlStdOperatorTable.isTrueOperator);
        checkBoolean("true is true", Boolean.TRUE);
        checkBoolean("false is true", Boolean.FALSE);
        checkBoolean(
            "cast(null as boolean) is true",
            Boolean.FALSE);
    }

    public void testIsNotFalseOperator()
    {
        setFor(SqlStdOperatorTable.isNotFalseOperator);
        checkBoolean("false is not false", Boolean.FALSE);
        checkBoolean("true is not false", Boolean.TRUE);
        checkBoolean(
            "cast(null as boolean) is not false",
            Boolean.TRUE);
    }

    public void testIsFalseOperator()
    {
        setFor(SqlStdOperatorTable.isFalseOperator);
        checkBoolean("false is false", Boolean.TRUE);
        checkBoolean("true is false", Boolean.FALSE);
        checkBoolean(
            "cast(null as boolean) is false",
            Boolean.FALSE);
    }

    public void testIsNotUnknownOperator()
    {
        setFor(SqlStdOperatorTable.isNotUnknownOperator);
        checkBoolean("false is not unknown", Boolean.TRUE);
        checkBoolean("true is not unknown", Boolean.TRUE);
        checkBoolean(
            "cast(null as boolean) is not unknown",
            Boolean.FALSE);
        checkBoolean("unknown is not unknown", Boolean.FALSE);
        checkFails(
            "^'abc' IS NOT UNKNOWN^",
            "(?s).*Cannot apply 'IS NOT UNKNOWN'.*",
            false);
    }

    public void testIsUnknownOperator()
    {
        setFor(SqlStdOperatorTable.isUnknownOperator);
        checkBoolean("false is unknown", Boolean.FALSE);
        checkBoolean("true is unknown", Boolean.FALSE);
        checkBoolean(
            "cast(null as boolean) is unknown",
            Boolean.TRUE);
        checkBoolean("unknown is unknown", Boolean.TRUE);
        checkFails(
            "0 = 1 AND ^2 IS UNKNOWN^ AND 3 > 4",
            "(?s).*Cannot apply 'IS UNKNOWN'.*",
            false);
    }

    public void testIsASetOperator()
    {
        setFor(SqlStdOperatorTable.isASetOperator);
    }

    public void testExistsOperator()
    {
        setFor(SqlStdOperatorTable.existsOperator);
    }

    public void testNotOperator()
    {
        setFor(SqlStdOperatorTable.notOperator);
        checkBoolean("not true", Boolean.FALSE);
        checkBoolean("not false", Boolean.TRUE);
        checkBoolean("not unknown", null);
        checkNull("not cast(null as boolean)");
    }

    public void testPrefixMinusOperator()
    {
        setFor(SqlStdOperatorTable.prefixMinusOperator);
        checkFails(
            "'a' + ^- 'b'^ + 'c'",
            "(?s)Cannot apply '-' to arguments of type '-<CHAR\\(1\\)>'.*",
            false);
        checkScalarExact("-1", "-1");
        checkScalarExact(
            "-1.23",
            "DECIMAL(3, 2) NOT NULL",
            "-1.23");
        checkScalarApprox("-1.0e0", "DOUBLE NOT NULL", -1, 0);
        checkNull("-cast(null as integer)");
        checkNull("-cast(null as tinyint)");

        // Intervals
        checkScalar(
            "-interval '-6:2:8' hour to second",
            "+6:02:08",
            "INTERVAL HOUR TO SECOND NOT NULL");
        checkScalar(
            "- -interval '-6:2:8' hour to second",
            "-6:02:08",
            "INTERVAL HOUR TO SECOND NOT NULL");
        checkScalar(
            "-interval '5' month",
            "-5",
            "INTERVAL MONTH NOT NULL");
        checkNull(
            "-cast(null as interval day to minute)");
    }

    public void testPrefixPlusOperator()
    {
        setFor(SqlStdOperatorTable.prefixPlusOperator);
        checkScalarExact("+1", "1");
        checkScalarExact("+1.23", "DECIMAL(3, 2) NOT NULL", "1.23");
        checkScalarApprox("+1.0e0", "DOUBLE NOT NULL", 1, 0);
        checkNull("+cast(null as integer)");
        checkNull("+cast(null as tinyint)");

        // Intervals
        checkScalar(
            "+interval '-6:2:8' hour to second",
            "-6:02:08",
            "INTERVAL HOUR TO SECOND NOT NULL");
        checkScalar(
            "++interval '-6:2:8' hour to second",
            "-6:02:08",
            "INTERVAL HOUR TO SECOND NOT NULL");
        checkScalar(
            "+interval '6:2:8.234' hour to second",
            "+6:02:08.234",
            "INTERVAL HOUR TO SECOND NOT NULL");
        checkScalar(
            "+interval '5' month",
            "+5",
            "INTERVAL MONTH NOT NULL");
        checkNull(
            "+cast(null as interval day to minute)");
    }

    public void testExplicitTableOperator()
    {
        setFor(SqlStdOperatorTable.explicitTableOperator);
    }

    public void testValuesOperator()
    {
        setFor(SqlStdOperatorTable.valuesOperator);
        check(
            "select 'abc' from (values(true))",
            new AbstractSqlTester.StringTypeChecker("CHAR(3) NOT NULL"),
            "abc",
            0);
    }

    public void testNotLikeOperator()
    {
        setFor(SqlStdOperatorTable.notLikeOperator);
        checkBoolean("'abc' not like '_b_'", Boolean.FALSE);
    }

    public void testLikeOperator()
    {
        setFor(SqlStdOperatorTable.likeOperator);
        checkBoolean("''  like ''", Boolean.TRUE);
        checkBoolean("'a' like 'a'", Boolean.TRUE);
        checkBoolean("'a' like 'b'", Boolean.FALSE);
        checkBoolean("'a' like 'A'", Boolean.FALSE);
        checkBoolean("'a' like 'a_'", Boolean.FALSE);
        checkBoolean("'a' like '_a'", Boolean.FALSE);
        checkBoolean("'a' like '%a'", Boolean.TRUE);
        checkBoolean("'a' like '%a%'", Boolean.TRUE);
        checkBoolean("'a' like 'a%'", Boolean.TRUE);
        checkBoolean("'ab'   like 'a_'", Boolean.TRUE);
        checkBoolean("'abc'  like 'a_'", Boolean.FALSE);
        checkBoolean("'abcd' like 'a%'", Boolean.TRUE);
        checkBoolean("'ab'   like '_b'", Boolean.TRUE);
        checkBoolean("'abcd' like '_d'", Boolean.FALSE);
        checkBoolean("'abcd' like '%d'", Boolean.TRUE);
    }

    public void testNotSimilarToOperator()
    {
        setFor(SqlStdOperatorTable.notSimilarOperator);
        checkBoolean("'ab' not similar to 'a_'", Boolean.FALSE);
    }

    public void testSimilarToOperator()
    {
        setFor(SqlStdOperatorTable.similarOperator);

        // like LIKE
        checkBoolean("''  similar to ''", Boolean.TRUE);
        checkBoolean("'a' similar to 'a'", Boolean.TRUE);
        checkBoolean("'a' similar to 'b'", Boolean.FALSE);
        checkBoolean("'a' similar to 'A'", Boolean.FALSE);
        checkBoolean("'a' similar to 'a_'", Boolean.FALSE);
        checkBoolean("'a' similar to '_a'", Boolean.FALSE);
        checkBoolean("'a' similar to '%a'", Boolean.TRUE);
        checkBoolean("'a' similar to '%a%'", Boolean.TRUE);
        checkBoolean("'a' similar to 'a%'", Boolean.TRUE);
        checkBoolean("'ab'   similar to 'a_'", Boolean.TRUE);
        checkBoolean("'abc'  similar to 'a_'", Boolean.FALSE);
        checkBoolean("'abcd' similar to 'a%'", Boolean.TRUE);
        checkBoolean("'ab'   similar to '_b'", Boolean.TRUE);
        checkBoolean("'abcd' similar to '_d'", Boolean.FALSE);
        checkBoolean("'abcd' similar to '%d'", Boolean.TRUE);

        // simple regular expressions
        // ab*c+d matches acd, abcd, acccd, abcccd but not abd, aabc
        checkBoolean("'acd'    similar to 'ab*c+d'", Boolean.TRUE);
        checkBoolean("'abcd'   similar to 'ab*c+d'", Boolean.TRUE);
        checkBoolean("'acccd'  similar to 'ab*c+d'", Boolean.TRUE);
        checkBoolean("'abcccd' similar to 'ab*c+d'", Boolean.TRUE);
        checkBoolean("'abd'    similar to 'ab*c+d'", Boolean.FALSE);
        checkBoolean("'aabc'   similar to 'ab*c+d'", Boolean.FALSE);

        // compound regular expressions
        // x(ab|c)*y matches xy, xccy, xababcy but not xbcy
        checkBoolean(
            "'xy'      similar to 'x(ab|c)*y'",
            Boolean.TRUE);
        checkBoolean(
            "'xccy'    similar to 'x(ab|c)*y'",
            Boolean.TRUE);
        checkBoolean(
            "'xababcy' similar to 'x(ab|c)*y'",
            Boolean.TRUE);
        checkBoolean(
            "'xbcy'    similar to 'x(ab|c)*y'",
            Boolean.FALSE);

        // x(ab|c)+y matches xccy, xababcy but not xy, xbcy
        checkBoolean(
            "'xy'      similar to 'x(ab|c)+y'",
            Boolean.FALSE);
        checkBoolean(
            "'xccy'    similar to 'x(ab|c)+y'",
            Boolean.TRUE);
        checkBoolean(
            "'xababcy' similar to 'x(ab|c)+y'",
            Boolean.TRUE);
        checkBoolean(
            "'xbcy'    similar to 'x(ab|c)+y'",
            Boolean.FALSE);
    }

    public void testEscapeOperator()
    {
        setFor(SqlStdOperatorTable.escapeOperator);
    }

    public void testConvertFunc()
    {
        setFor(SqlStdOperatorTable.convertFunc);
    }

    public void testTranslateFunc()
    {
        setFor(SqlStdOperatorTable.translateFunc);
    }

    public void testOverlayFunc()
    {
        setFor(SqlStdOperatorTable.overlayFunc);
        checkString(
            "overlay('ABCdef' placing 'abc' from 1)",
            "abcdef",
            "VARCHAR(9) NOT NULL");
        checkString(
            "overlay('ABCdef' placing 'abc' from 1 for 2)",
            "abcCdef",
            "VARCHAR(9) NOT NULL");
        checkString(
            "overlay(cast('ABCdef' as varchar(10)) placing "
            + "cast('abc' as char(5)) from 1 for 2)",
            "abc  Cdef",
            "VARCHAR(15) NOT NULL");
        checkString(
            "overlay(cast('ABCdef' as char(10)) placing "
            + "cast('abc' as char(5)) from 1 for 2)",
            "abc  Cdef    ",
            "VARCHAR(15) NOT NULL");
        checkNull(
            "overlay('ABCdef' placing 'abc' from 1 for cast(null as integer))");
        checkNull(
            "overlay(cast(null as varchar(1)) placing 'abc' from 1)");

        if (todo) {
            // todo: hex strings not yet implemented in calc
            checkNull(
                "overlay(x'abc' placing x'abc' from cast(null as integer))");
        }
    }

    public void testPositionFunc()
    {
        setFor(SqlStdOperatorTable.positionFunc);
        checkScalarExact("position('b' in 'abc')", "2");
        checkScalarExact("position('' in 'abc')", "1");

        // FRG-211
        checkScalarExact("position('tra' in 'fdgjklewrtra')", "10");

        checkNull("position(cast(null as varchar(1)) in '0010')");
        checkNull("position('a' in cast(null as varchar(1)))");

        checkScalar(
            "position(cast('a' as char) in cast('bca' as varchar))",
            0,
            "INTEGER NOT NULL");
    }

    public void testCharLengthFunc()
    {
        setFor(SqlStdOperatorTable.charLengthFunc);
        checkScalarExact("char_length('abc')", "3");
        checkNull("char_length(cast(null as varchar(1)))");
    }

    public void testCharacterLengthFunc()
    {
        setFor(SqlStdOperatorTable.characterLengthFunc);
        checkScalarExact("CHARACTER_LENGTH('abc')", "3");
        checkNull("CHARACTER_LENGTH(cast(null as varchar(1)))");
    }

    public void testUpperFunc()
    {
        setFor(SqlStdOperatorTable.upperFunc);
        checkString("upper('a')", "A", "CHAR(1) NOT NULL");
        checkString("upper('A')", "A", "CHAR(1) NOT NULL");
        checkString("upper('1')", "1", "CHAR(1) NOT NULL");
        checkString("upper('aa')", "AA", "CHAR(2) NOT NULL");
        checkNull("upper(cast(null as varchar(1)))");
    }

    public void testLowerFunc()
    {
        setFor(SqlStdOperatorTable.lowerFunc);

        // SQL:2003 6.29.8 The type of lower is the type of its argument
        checkString("lower('A')", "a", "CHAR(1) NOT NULL");
        checkString("lower('a')", "a", "CHAR(1) NOT NULL");
        checkString("lower('1')", "1", "CHAR(1) NOT NULL");
        checkString("lower('AA')", "aa", "CHAR(2) NOT NULL");
        checkNull("lower(cast(null as varchar(1)))");
    }

    public void testInitcapFunc()
    {
        // Note: the initcap function is an Oracle defined function and is not
        // defined in the '03 standard
        setFor(SqlStdOperatorTable.initcapFunc);
        checkString("initcap('aA')", "Aa", "CHAR(2) NOT NULL");
        checkString("initcap('Aa')", "Aa", "CHAR(2) NOT NULL");
        checkString("initcap('1a')", "1a", "CHAR(2) NOT NULL");
        checkString(
            "initcap('ab cd Ef 12')",
            "Ab Cd Ef 12",
            "CHAR(11) NOT NULL");
        checkNull("initcap(cast(null as varchar(1)))");

        // dtbug 232
        checkFails(
            "^initcap(cast(null as date))^",
            "Cannot apply 'INITCAP' to arguments of type 'INITCAP\\(<DATE>\\)'\\. Supported form\\(s\\): 'INITCAP\\(<CHARACTER>\\)'",
            false);
    }

    public void testPowFunc()
    {
        setFor(SqlStdOperatorTable.powFunc);
        checkScalarApprox("pow(2,-2)", "DOUBLE NOT NULL", 0.25, 0);
        checkNull("pow(cast(null as integer),2)");
        checkNull("pow(2,cast(null as double))");
    }

    public void testExpFunc()
    {
        setFor(SqlStdOperatorTable.expFunc);
        checkScalarApprox(
            "exp(2)",
            "DOUBLE NOT NULL",
            7.389056,
            0.000001);
        checkScalarApprox(
            "exp(-2)",
            "DOUBLE NOT NULL",
            0.1353,
            0.0001);
        checkNull("exp(cast(null as integer))");
        checkNull("exp(cast(null as double))");
    }

    public void testModFunc()
    {
        setFor(SqlStdOperatorTable.modFunc);
        checkScalarExact("mod(4,2)", "0");
        checkScalarExact("mod(8,5)", "3");
        checkScalarExact("mod(-12,7)", "-5");
        checkScalarExact("mod(-12,-7)", "-5");
        checkScalarExact("mod(12,-7)", "5");
        checkScalarExact(
            "mod(cast(12 as tinyint), cast(-7 as tinyint))",
            "TINYINT NOT NULL",
            "5");

        checkScalarExact(
            "mod(cast(9 as decimal(2, 0)), 7)",
            "INTEGER NOT NULL",
            "2");
        checkScalarExact(
            "mod(7, cast(9 as decimal(2, 0)))",
            "DECIMAL(2, 0) NOT NULL",
            "7");
        checkScalarExact(
            "mod(cast(-9 as decimal(2, 0)), cast(7 as decimal(1, 0)))",
            "DECIMAL(1, 0) NOT NULL",
            "-2");
        checkNull("mod(cast(null as integer),2)");
        checkNull("mod(4,cast(null as tinyint))");
        checkNull("mod(4,cast(null as decimal(12,0)))");
    }

    public void testModFuncDivByZero()
    {
        // The extra CASE expression is to fool Janino.  It does constant
        // reduction and will throw the divide by zero exception while
        // compiling the expression.  The test frame work would then issue
        // unexpected exception occured during "validation".  You cannot
        // submit as non-runtime because the janino exception does not have
        // error position information and the framework is unhappy with that.
        checkFails("mod(3,case 'a' when 'a' then 0 end)", divisionByZeroMessage, true);
    }

    public void testLnFunc()
    {
        setFor(SqlStdOperatorTable.lnFunc);
        checkScalarApprox(
            "ln(2.71828)",
            "DOUBLE NOT NULL",
            1.0,
            0.000001);
        checkScalarApprox(
            "ln(2.71828)",
            "DOUBLE NOT NULL",
            0.999999327,
            0.0000001);
        checkNull("ln(cast(null as tinyint))");
    }

    public void testLogFunc()
    {
        setFor(SqlStdOperatorTable.log10Func);
        checkScalarApprox(
            "log10(10)",
            "DOUBLE NOT NULL",
            1.0,
            0.000001);
        checkScalarApprox(
            "log10(100.0)",
            "DOUBLE NOT NULL",
            2.0,
            0.000001);
        checkScalarApprox(
            "log10(cast(10e8 as double))",
            "DOUBLE NOT NULL",
            9.0,
            0.000001);
        checkScalarApprox(
            "log10(cast(10e2 as float))",
            "DOUBLE NOT NULL",
            3.0,
            0.000001);
        checkScalarApprox(
            "log10(cast(10e-3 as real))",
            "DOUBLE NOT NULL",
            -2.0,
            0.000001);
        checkNull("log10(cast(null as real))");
    }

    public void testAbsFunc()
    {
        setFor(SqlStdOperatorTable.absFunc);

        checkScalarExact("abs(-1)", "1");
        checkScalarExact(
            "abs(cast(10 as TINYINT))",
            "TINYINT NOT NULL",
            "10");
        checkScalarExact(
            "abs(cast(-20 as SMALLINT))",
            "SMALLINT NOT NULL",
            "20");
        checkScalarExact(
            "abs(cast(-100 as INT))",
            "INTEGER NOT NULL",
            "100");
        checkScalarExact(
            "abs(cast(1000 as BIGINT))",
            "BIGINT NOT NULL",
            "1000");
        checkScalarExact(
            "abs(54.4)",
            "DECIMAL(3, 1) NOT NULL",
            "54.4");
        checkScalarExact(
            "abs(-54.4)",
            "DECIMAL(3, 1) NOT NULL",
            "54.4");
        checkScalarApprox(
            "abs(-9.32E-2)",
            "DOUBLE NOT NULL",
            0.0932,
            0);
        checkScalarApprox(
            "abs(cast(-3.5 as double))",
            "DOUBLE NOT NULL",
            3.5,
            0);
        checkScalarApprox(
            "abs(cast(-3.5 as float))",
            "FLOAT NOT NULL",
            3.5,
            0);
        checkScalarApprox(
            "abs(cast(3.5 as real))",
            "REAL NOT NULL",
            3.5,
            0);

        checkNull("abs(cast(null as double))");

        // Intervals
        checkScalar(
            "abs(interval '-2' day)",
            "+2",
            "INTERVAL DAY NOT NULL");
        checkScalar(
            "abs(interval '-5-03' year to month)",
            "+5-03",
            "INTERVAL YEAR TO MONTH NOT NULL");
        checkNull("abs(cast(null as interval hour))");
    }

    public void testNullifFunc()
    {
        setFor(SqlStdOperatorTable.nullIfFunc);
        checkNull("nullif(1,1)");
        checkScalarExact(
            "nullif(1.5, 13.56)",
            "DECIMAL(2, 1)",
            "1.5");
        checkScalarExact(
            "nullif(13.56, 1.5)",
            "DECIMAL(4, 2)",
            "13.56");
        checkScalarExact("nullif(1.5, 3)", "DECIMAL(2, 1)", "1.5");
        checkScalarExact("nullif(3, 1.5)", "INTEGER", "3");
        checkScalarApprox("nullif(1.5e0, 3e0)", "DOUBLE", 1.5, 0);
        checkScalarApprox(
            "nullif(1.5, cast(3e0 as REAL))",
            "DECIMAL(2, 1)",
            1.5,
            0);
        checkScalarExact("nullif(3, 1.5e0)", "INTEGER", "3");
        checkScalarExact(
            "nullif(3, cast(1.5e0 as REAL))",
            "INTEGER",
            "3");
        checkScalarApprox("nullif(1.5e0, 3.4)", "DOUBLE", 1.5, 0);
        checkScalarExact(
            "nullif(3.4, 1.5e0)",
            "DECIMAL(2, 1)",
            "3.4");
        checkString("nullif('a','bc')",
            "a",
            "CHAR(1)");
        checkString(
            "nullif('a',cast(null as varchar(1)))",
            "a",
            "CHAR(1)");
        checkNull("nullif(cast(null as varchar(1)),'a')");
        checkNull("nullif(cast(null as numeric(4,3)), 4.3)");

        // Error message reflects the fact that Nullif is expanded before it is
        // validated (like a C macro). Not perfect, but good enough.
        checkFails(
            "1 + ^nullif(1, date '2005-8-4')^ + 2",
            "(?s)Cannot apply '=' to arguments of type '<INTEGER> = <DATE>'\\..*",
            false);

        // TODO: fix frg 65 (dtbug 324).
        if (Bug.Frg65Fixed) {
            checkFails(
                "1 + ^nullif(1, 2, 3)^ + 2",
                "invalid number of arguments to NULLIF",
                false);
        }

        // Intervals
        checkScalar(
            "nullif(interval '2' month, interval '3' year)",
            "+2",
            "INTERVAL MONTH");
        checkScalar(
            "nullif(interval '2 5' day to hour, interval '5' second)",
            "+2 05",
            "INTERVAL DAY TO HOUR");
        checkNull(
            "nullif(interval '3' day, interval '3' day)");
    }

    public void testCoalesceFunc()
    {
        setFor(SqlStdOperatorTable.coalesceFunc);
        checkString("coalesce('a','b')", "a", "CHAR(1) NOT NULL");
        checkScalarExact("coalesce(null,null,3)", "3");
        checkFails(
            "1 + ^coalesce('a', 'b', 1, null)^ + 2",
            "Illegal mixing of types in CASE or COALESCE statement",
            false);
    }

    public void testUserFunc()
    {
        setFor(SqlStdOperatorTable.userFunc);
        checkString("USER", "sa", "VARCHAR(2000) NOT NULL");
    }

    public void testCurrentUserFunc()
    {
        setFor(SqlStdOperatorTable.currentUserFunc);
        checkString("CURRENT_USER", "sa", "VARCHAR(2000) NOT NULL");
    }

    public void testSessionUserFunc()
    {
        setFor(SqlStdOperatorTable.sessionUserFunc);
        checkString("SESSION_USER", "sa", "VARCHAR(2000) NOT NULL");
    }

    public void testSystemUserFunc()
    {
        setFor(SqlStdOperatorTable.systemUserFunc);
        String user = System.getProperty("user.name"); // e.g. "jhyde"
        checkString("SYSTEM_USER", user, "VARCHAR(2000) NOT NULL");
    }

    public void testCurrentPathFunc()
    {
        setFor(SqlStdOperatorTable.currentPathFunc);
        checkString("CURRENT_PATH", "", "VARCHAR(2000) NOT NULL");
    }

    public void testCurrentRoleFunc()
    {
        setFor(SqlStdOperatorTable.currentRoleFunc);

        // By default, the CURRENT_ROLE function returns
        // the empty string because a role has to be set explicitly.
        checkString("CURRENT_ROLE", "", "VARCHAR(2000) NOT NULL");
    }

    public void testLocalTimeFunc()
    {
        setFor(SqlStdOperatorTable.localTimeFunc);
        checkScalar("LOCALTIME", timePattern, "TIME(0) NOT NULL");
        checkFails(
            "^LOCALTIME()^",
            "No match found for function signature LOCALTIME\\(\\)",
            false);
        checkScalar(
            "LOCALTIME(1)",
            timePattern,
            "TIME(1) NOT NULL");
    }

    public void testLocalTimestampFunc()
    {
        setFor(SqlStdOperatorTable.localTimestampFunc);
        checkScalar(
            "LOCALTIMESTAMP",
            timestampPattern,
            "TIMESTAMP(0) NOT NULL");
        checkFails(
            "^LOCALTIMESTAMP()^",
            "No match found for function signature LOCALTIMESTAMP\\(\\)",
            false);
        checkFails(
            "LOCALTIMESTAMP(^4000000000^)",
            literalOutOfRangeMessage,
            false);
        checkScalar(
            "LOCALTIMESTAMP(1)",
            timestampPattern,
            "TIMESTAMP(1) NOT NULL");
    }

    public void testCurrentTimeFunc()
    {
        setFor(SqlStdOperatorTable.currentTimeFunc);
        checkScalar(
            "CURRENT_TIME",
            timePattern,
            "TIME(0) NOT NULL");
        checkFails(
            "^CURRENT_TIME()^",
            "No match found for function signature CURRENT_TIME\\(\\)",
            false);
        checkScalar(
            "CURRENT_TIME(1)",
            timePattern,
            "TIME(1) NOT NULL");
    }

    public void testCurrentTimestampFunc()
    {
        setFor(SqlStdOperatorTable.currentTimestampFunc);
        checkScalar(
            "CURRENT_TIMESTAMP",
            timestampPattern,
            "TIMESTAMP(0) NOT NULL");
        checkFails(
            "^CURRENT_TIMESTAMP()^",
            "No match found for function signature CURRENT_TIMESTAMP\\(\\)",
            false);
        checkFails(
            "CURRENT_TIMESTAMP(^4000000000^)",
            literalOutOfRangeMessage,
            false);
        checkScalar(
            "CURRENT_TIMESTAMP(1)",
            timestampPattern,
            "TIMESTAMP(1) NOT NULL");
    }

    public void testCurrentDateFunc()
    {
        setFor(SqlStdOperatorTable.currentDateFunc);
        checkScalar("CURRENT_DATE", datePattern, "DATE NOT NULL");
        checkFails(
            "^CURRENT_DATE()^",
            "No match found for function signature CURRENT_DATE\\(\\)",
            false);
    }

    public void testSubstringFunction()
    {
        setFor(SqlStdOperatorTable.substringFunc);
        checkString(
            "substring('abc' from 1 for 2)",
            "ab",
            "VARCHAR(3) NOT NULL");
        checkString(
            "substring('abc' from 2)",
            "bc",
            "VARCHAR(3) NOT NULL");

        //substring reg exp not yet supported
        //checkString("substring('foobar' from '%#\"o_b#\"%' for
        //'#')", "oob");
        checkNull("substring(cast(null as varchar(1)),1,2)");
    }

    public void testTrimFunc()
    {
        setFor(SqlStdOperatorTable.trimFunc);

        // SQL:2003 6.29.11 Trimming a CHAR yields a VARCHAR
        checkString(
            "trim('a' from 'aAa')",
            "A",
            "VARCHAR(3) NOT NULL");
        checkString(
            "trim(both 'a' from 'aAa')",
            "A",
            "VARCHAR(3) NOT NULL");
        checkString(
            "trim(leading 'a' from 'aAa')",
            "Aa",
            "VARCHAR(3) NOT NULL");
        checkString(
            "trim(trailing 'a' from 'aAa')",
            "aA",
            "VARCHAR(3) NOT NULL");
        checkNull("trim(cast(null as varchar(1)) from 'a')");
        checkNull("trim('a' from cast(null as varchar(1)))");

        if (Bug.Fnl3Fixed) {
            // SQL:2003 6.29.9: trim string must have length=1. Failure occurs
            // at runtime.
            //
            // TODO: Change message to "Invalid argument\(s\) for 'TRIM' function",
            // The message should come from a resource file, and should still
            // have the SQL error code 22027.
            checkFails(
                "trim('xy' from 'abcde')",
                "could not calculate results for the following row:" + NL
                + "\\[ 0 \\]" + NL
                + "Messages:" + NL
                + "\\[0\\]:PC=0 Code=22027 ",
                true);
            checkFails(
                "trim('' from 'abcde')",
                "could not calculate results for the following row:" + NL
                + "\\[ 0 \\]" + NL
                + "Messages:" + NL
                + "\\[0\\]:PC=0 Code=22027 ",
                true);
        }
    }

    public void testWindow()
    {
        setFor(SqlStdOperatorTable.windowOperator);
        if (Bug.Frg188Fixed) {
            check(
                "select sum(1) over (order by x) from (select 1 as x, 2 as y from (values (true)))",
                new AbstractSqlTester.StringTypeChecker("INTEGER"),
                "1",
                0);
        }
    }

    public void testElementFunc()
    {
        setFor(SqlStdOperatorTable.elementFunc);
        if (todo) {
            checkString(
                "element(multiset['abc']))",
                "abc",
                "char(3) not null");
            checkNull("element(multiset[cast(null as integer)]))");
        }
    }

    public void testCardinalityFunc()
    {
        setFor(SqlStdOperatorTable.cardinalityFunc);
        if (todo) {
            checkScalarExact(
                "cardinality(multiset[cast(null as integer),2]))",
                "2");
        }
    }

    public void testMemberOfOperator()
    {
        setFor(SqlStdOperatorTable.memberOfOperator);
        checkBoolean("1 member of multiset[1]", Boolean.TRUE);
        checkBoolean(
            "'2' member of multiset['1']",
            Boolean.FALSE);
        checkBoolean(
            "cast(null as double) member of multiset[cast(null as double)]",
            Boolean.TRUE);
        checkBoolean(
            "cast(null as double) member of multiset[1.1]",
            Boolean.FALSE);
        checkBoolean(
            "1.1 member of multiset[cast(null as double)]",
            Boolean.FALSE);
    }

    public void testCollectFunc()
    {
        setFor(SqlStdOperatorTable.collectFunc);
    }

    public void testFusionFunc()
    {
        setFor(SqlStdOperatorTable.fusionFunc);
    }

    public void testExtractFunc()
    {
        setFor(SqlStdOperatorTable.extractFunc);

        // Intervals
        checkScalar(
            "extract(day from interval '2 3:4:5.678' day to second)",
            "2",
            "BIGINT NOT NULL");
        checkScalar(
            "extract(hour from interval '2 3:4:5.678' day to second)",
            "3",
            "BIGINT NOT NULL");
        checkScalar(
            "extract(minute from interval '2 3:4:5.678' day to second)",
            "4",
            "BIGINT NOT NULL");

        // TODO: Seconds should include precision
        checkScalar(
            "extract(second from interval '2 3:4:5.678' day to second)",
            "5",
            "BIGINT NOT NULL");
        checkScalar(
            "extract(minute from interval '-1:2' hour to minute)",
            "2",
            "BIGINT NOT NULL");
        checkScalar(
            "extract(hour from interval '-1:2:3' hour to second)",
            "-1",
            "BIGINT NOT NULL");
        checkScalar(
            "extract(year from interval '4-2' year to month)",
            "4",
            "BIGINT NOT NULL");
        checkScalar(
            "extract(month from interval '4-2' year to month)",
            "2",
            "BIGINT NOT NULL");
        checkNull(
            "extract(month from cast(null as interval year))");
    }

    public void testCeilFunc()
    {
        setFor(SqlStdOperatorTable.ceilFunc);
        checkScalarApprox("ceil(10.1e0)", "DOUBLE NOT NULL", 11, 0);
        checkScalarApprox(
            "ceil(cast(-11.2e0 as real))",
            "REAL NOT NULL",
            -11,
            0);
        checkScalarExact("ceil(100)", "INTEGER NOT NULL", "100");
        checkScalarExact(
            "ceil(1.3)",
            "DECIMAL(2, 0) NOT NULL",
            "2");
        checkScalarExact(
            "ceil(-1.7)",
            "DECIMAL(2, 0) NOT NULL",
            "-1");
        checkNull("ceiling(cast(null as decimal(2,0)))");
        checkNull("ceiling(cast(null as double))");

        // Intervals
        checkScalar(
            "ceil(interval '3:4:5' hour to second)",
            "+4:00:00",
            "INTERVAL HOUR TO SECOND NOT NULL");
        checkScalar(
            "ceil(interval '-6.3' second)",
            "-6",
            "INTERVAL SECOND NOT NULL");
        checkScalar(
            "ceil(interval '5-1' year to month)",
            "+6-00",
            "INTERVAL YEAR TO MONTH NOT NULL");
        checkScalar(
            "ceil(interval '-5-1' year to month)",
            "-5-00",
            "INTERVAL YEAR TO MONTH NOT NULL");
        checkNull(
            "ceil(cast(null as interval year))");
    }

    public void testFloorFunc()
    {
        setFor(SqlStdOperatorTable.floorFunc);
        checkScalarApprox("floor(2.5e0)", "DOUBLE NOT NULL", 2, 0);
        checkScalarApprox(
            "floor(cast(-1.2e0 as real))",
            "REAL NOT NULL",
            -2,
            0);
        checkScalarExact("floor(100)", "INTEGER NOT NULL", "100");
        checkScalarExact(
            "floor(1.7)",
            "DECIMAL(2, 0) NOT NULL",
            "1");
        checkScalarExact(
            "floor(-1.7)",
            "DECIMAL(2, 0) NOT NULL",
            "-2");
        checkNull("floor(cast(null as decimal(2,0)))");
        checkNull("floor(cast(null as real))");

        // Intervals
        checkScalar(
            "floor(interval '3:4:5' hour to second)",
            "+3:00:00",
            "INTERVAL HOUR TO SECOND NOT NULL");
        checkScalar(
            "floor(interval '-6.3' second)",
            "-7",
            "INTERVAL SECOND NOT NULL");
        checkScalar(
            "floor(interval '5-1' year to month)",
            "+5-00",
            "INTERVAL YEAR TO MONTH NOT NULL");
        checkScalar(
            "floor(interval '-5-1' year to month)",
            "-6-00",
            "INTERVAL YEAR TO MONTH NOT NULL");
        checkNull(
            "floor(cast(null as interval year))");
    }

    public void testDenseRankFunc()
    {
        setFor(SqlStdOperatorTable.denseRankFunc);
    }

    public void testPercentRankFunc()
    {
        setFor(SqlStdOperatorTable.percentRankFunc);
    }

    public void testRankFunc()
    {
        setFor(SqlStdOperatorTable.rankFunc);
    }

    public void testCumeDistFunc()
    {
        setFor(SqlStdOperatorTable.cumeDistFunc);
    }

    public void testRowNumberFunc()
    {
        setFor(SqlStdOperatorTable.rowNumberFunc);
    }

    public void testCountFunc()
    {
        if (Bug.Frg188Fixed) {
            setFor(SqlStdOperatorTable.countOperator);
            checkType("count(*)", "BIGINT NOT NULL");
            checkType("count('name')", "BIGINT NOT NULL");
            checkType("count(1)", "BIGINT NOT NULL");
            checkType("count(1.2)", "BIGINT NOT NULL");
            checkType("COUNT(DISTINCT 'x')", "BIGINT NOT NULL");
            checkFails(
                "^COUNT()^",
                "Invalid number of arguments to function 'COUNT'. Was expecting 1 arguments",
                false);
            checkFails(
                "^COUNT(1, 2)^",
                "Invalid number of arguments to function 'COUNT'. Was expecting 1 arguments",
                false);
            final String [] values = { "0", "CAST(null AS INTEGER)", "1", "0" };
            checkAgg(
                "COUNT(x)",
                values,
                3,
                0);
            checkAgg(
                "COUNT(CASE x WHEN 0 THEN NULL ELSE -1 END)",
                values,
                2,
                0);
            checkAgg(
                "COUNT(DISTINCT x)",
                values,
                2,
                0);

            // string values -- note that empty string is not null
            final String [] stringValues =
            { "'a'", "CAST(NULL AS VARCHAR(1))", "''" };
            checkAgg(
                "COUNT(*)",
                stringValues,
                3,
                0);
            checkAgg(
                "COUNT(x)",
                stringValues,
                2,
                0);
            checkAgg(
                "COUNT(DISTINCT x)",
                stringValues,
                2,
                0);
            checkAgg(
                "COUNT(DISTINCT 123)",
                stringValues,
                1,
                0);
        }
    }

    public void testSumFunc()
    {
        if (Bug.Frg188Fixed) {
            setFor(SqlStdOperatorTable.sumOperator);
            checkFails(
                "sum(^*^)",
                "Unknown identifier '\\*'",
                false);
            checkFails(
                "^sum('name')^",
                "(?s)Cannot apply 'SUM' to arguments of type 'SUM\\(<CHAR\\(4\\)>\\)'\\. Supported form\\(s\\): 'SUM\\(<NUMERIC>\\)'.*",
                false);
            checkType("sum(1)", "INTEGER");
            checkType("sum(1.2)", "DECIMAL(2, 1)");
            checkType("sum(DISTINCT 1.5)", "DECIMAL(2, 1)");
            checkFails(
                "^sum()^",
                "Invalid number of arguments to function 'SUM'. Was expecting 1 arguments",
                false);
            checkFails(
                "^sum(1, 2)^",
                "Invalid number of arguments to function 'SUM'. Was expecting 1 arguments",
                false);
            checkFails(
                "^sum(cast(null as varchar(2)))^",
                "(?s)Cannot apply 'SUM' to arguments of type 'SUM\\(<VARCHAR\\(2\\)>\\)'\\. Supported form\\(s\\): 'SUM\\(<NUMERIC>\\)'.*",
                false);
            final String [] values = { "0", "CAST(null AS INTEGER)", "2", "2" };
            checkAgg(
                "sum(x)",
                values,
                4,
                0);
            checkAgg(
                "sum(CASE x WHEN 0 THEN NULL ELSE -1 END)",
                values,
                -3,
                0);
            checkAgg(
                "sum(DISTINCT CASE x WHEN 0 THEN NULL ELSE -1 END)",
                values,
                -1,
                0);
            checkAgg(
                "sum(DISTINCT x)",
                values,
                2,
                0);
        }
    }

    public void testAvgFunc()
    {
        if (Bug.Frg188Fixed) {
            setFor(SqlStdOperatorTable.avgOperator);
            checkFails(
                "avg(^*^)",
                "Unknown identifier '\\*'",
                false);
            checkFails(
                "^avg(cast(null as varchar(2)))^",
                "(?s)Cannot apply 'AVG' to arguments of type 'AVG\\(<VARCHAR\\(2\\)>\\)'\\. Supported form\\(s\\): 'AVG\\(<NUMERIC>\\)'.*",
                false);
            checkType("AVG(CAST(NULL AS INTEGER))", "INTEGER");
            checkType("AVG(DISTINCT 1.5)", "DECIMAL(2, 1)");
            final String [] values = { "0", "CAST(null AS INTEGER)", "3", "3" };
            checkAgg(
                "AVG(x)",
                values,
                1.0,
                0);
            checkAgg(
                "AVG(DISTINCT x)",
                values,
                1.5,
                0);
            checkAgg(
                "avg(DISTINCT CASE x WHEN 0 THEN NULL ELSE -1 END)",
                values,
                -1,
                0);
        }
    }

    public void testLastValueFunc()
    {
        if (Bug.Frg188Fixed) {
            setFor(SqlStdOperatorTable.lastValueOperator);
            checkScalarExact("last_value(1)", "1");
            checkScalarExact(
                "last_value(1.2)",
                "DECIMAL(2, 1) NOT NULL",
                "1.2");
            checkType("last_value('name')", "CHAR(4) NOT NULL");
            checkString(
                "last_value('name')",
                "name",
                "CHAR(4) NOT NULL");
        }
    }

    public void testFirstValueFunc()
    {
        if (Bug.Frg188Fixed) {
            setFor(SqlStdOperatorTable.firstValueOperator);
            checkScalarExact("first_value(1)", "1");
            checkScalarExact(
                "first_value(1.2)",
                "DECIMAL(2, 1) NOT NULL",
                "1.2");
            checkType("first_value('name')", "CHAR(4) NOT NULL");
            checkString(
                "first_value('name')",
                "name",
                "CHAR(4) NOT NULL");
        }
    }

    /**
     * Tests that CAST fails when given a value just outside the valid range for
     * that type. For example,
     *
     * <ul>
     * <li>CAST(-200 AS TINYINT) fails because the value is less than -128;
     * <li>CAST(1E-999 AS FLOAT) fails because the value underflows;
     * <li>CAST(123.4567891234567 AS FLOAT) fails because the value loses
     * precision.
     * </ul>
     */
    public void testLiteralAtLimit()
    {
        final SqlTester tester = getTester();
        tester.setFor(SqlStdOperatorTable.castFunc);
        for (BasicSqlType type : SqlLimitsTest.getTypes()) {
            for (Object o : getValues(type, true)) {
                SqlLiteral literal =
                    type.getSqlTypeName().createLiteral(o, SqlParserPos.ZERO);
                String literalString =
                    literal.toSqlString(SqlUtil.dummyDialect);
                final String expr =
                    "CAST(" + literalString
                    + " AS " + type + ")";
                if (type.getSqlTypeName() == SqlTypeName.VARBINARY &&
                    !Bug.Frg283Fixed) {
                    continue;
                }
                try {
                    tester.checkType(
                        expr,
                        type.getFullTypeString());

                    if (type.getSqlTypeName() == SqlTypeName.BINARY) {
                        // Casting a string/binary values may change the value.
                        // For example, CAST(X'AB' AS BINARY(2)) yields
                        // X'AB00'.
                    } else {
                        tester.checkScalar(
                            expr + " = " + literalString,
                            true,
                            "BOOLEAN NOT NULL");
                    }
                } catch (Error e) {
                    System.out.println("Failed for expr=[" + expr + "]");
                    throw e;
                } catch (RuntimeException e) {
                    System.out.println("Failed for expr=[" + expr + "]");
                    throw e;
                }
            }
        }
    }

    /**
     * Tests that CAST fails when given a value just outside the valid range for
     * that type. For example,
     *
     * <ul>
     * <li>CAST(-200 AS TINYINT) fails because the value is less than -128;
     * <li>CAST(1E-999 AS FLOAT) fails because the value underflows;
     * <li>CAST(123.4567891234567 AS FLOAT) fails because the value loses
     * precision.
     * </ul>
     */
    public void testLiteralBeyondLimit()
    {
        final SqlTester tester = getTester();
        tester.setFor(SqlStdOperatorTable.castFunc);
        for (BasicSqlType type : SqlLimitsTest.getTypes()) {
            for (Object o : getValues(type, false)) {
                SqlLiteral literal =
                    type.getSqlTypeName().createLiteral(o, SqlParserPos.ZERO);
                String literalString =
                    literal.toSqlString(SqlUtil.dummyDialect);

                if ((type.getSqlTypeName() == SqlTypeName.BIGINT)
                    || ((type.getSqlTypeName() == SqlTypeName.DECIMAL)
                        && (type.getPrecision() == 19)))
                {
                    // Values which are too large to be literals fail at
                    // validate time.
                    tester.checkFails(
                        "CAST(^" + literalString + "^ AS " + type + ")",
                        "Numeric literal '.*' out of range",
                        false);
                } else if (
                    (type.getSqlTypeName() == SqlTypeName.CHAR)
                    || (type.getSqlTypeName() == SqlTypeName.VARCHAR)
                    || (type.getSqlTypeName() == SqlTypeName.BINARY)
                    || (type.getSqlTypeName() == SqlTypeName.VARBINARY))
                {
                    // Casting overlarge string/binary values do not fail -
                    // they are truncated. See testCastTruncates().
                } else {
                    // Value outside legal bound should fail at runtime (not
                    // validate time).
                    //
                    // NOTE: Because Java and Fennel calcs give
                    // different errors, the pattern hedges its bets.
                    tester.checkFails(
                        "CAST(" + literalString + " AS " + type + ")",
                        "(?s).*(Overflow during calculation or cast\\.|Code=22003).*",
                        true);
                }
            }
        }
    }

    public void testCastTruncates()
    {
        final SqlTester tester = getTester();
        tester.setFor(SqlStdOperatorTable.castFunc);
        tester.checkScalar(
            "CAST('ABCD' AS CHAR(2))",
            "AB",
            "CHAR(2) NOT NULL");
        tester.checkScalar(
            "CAST('ABCD' AS VARCHAR(2))",
            "AB",
            "VARCHAR(2) NOT NULL");
        tester.checkScalar(
            "CAST(x'ABCDEF12' AS BINARY(2))",
            "ABCD",
            "BINARY(2) NOT NULL");

        if (Bug.Frg283Fixed)
        tester.checkScalar(
            "CAST(x'ABCDEF12' AS VARBINARY(2))",
            "ABCD",
            "VARBINARY(2) NOT NULL");

        tester.checkBoolean(
            "CAST(X'' AS BINARY(3)) = X'000000'",
            true);
        tester.checkBoolean(
            "CAST(X'' AS BINARY(3)) = X''",
            false);
    }

    private List<Object> getValues(BasicSqlType type, boolean inBound)
    {
        List<Object> values = new ArrayList<Object>();
        for (boolean sign : FalseTrue) {
            for (SqlTypeName.Limit limit : SqlTypeName.Limit.values()) {
                Object o = type.getLimit(sign, limit, !inBound);
                if (o == null) {
                    continue;
                }
                if (!values.contains(o)) {
                    values.add(o);
                }
            }
        }
        return values;
    }

    // TODO: Test other stuff
}

// End SqlOperatorTests.java
