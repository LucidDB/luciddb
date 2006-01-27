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
import org.eigenbase.sql.SqlJdbcFunctionCall;
import org.eigenbase.sql.SqlOperator;
import org.eigenbase.sql.fun.SqlStdOperatorTable;

import java.util.regex.Pattern;
import java.math.BigDecimal;

import static org.eigenbase.util.TestUtil.NL;

/**
 * Contains unit tests for all operators. Each of the methods is named after an
 * operator.
 *
 * <p>The class is abstract. It contains a test for every operator, but does
 * not provide a mechanism to execute the tests: parse, validate, and execute
 * expressions on the operators. This is left to a {@link SqlTester} object
 * which the derived class must provide.
 *
 * <p>Different implementations of {@link SqlTester} are possible, such as:<ul>
 * <li>Execute against a real farrago database
 * <li>Execute in pure java (parsing and validation can be done, but expression
 *     evaluation is not possible)
 * <li>Generate a SQL script.
 * <li>Analyze which operators are adequately tested.
 * </ul>
 *
 * <p>A typical method will be named after the operator it is testing
 * (say <code>testSubstringFunc</code>).
 * It first calls {@link SqlTester#setFor(SqlOperator)} to declare which
 * operator it is testing.
 *
 * <blockqoute><pre><code>
 * public void testSubstringFunc() {
 *     getTester().isFor(SqlStdOperatorTable.substringFunc);
 *     getTester().checkScalar("sin(0)", "0");
 *     getTester().checkScalar("sin(1.5707)", "1");
 * }</code></pre></blockqoute>
 *
 * The rest of the method contains calls to the various
 * <code>checkXxx</code> methods in the {@link SqlTester} interface.
 * For an operator to be adequately tested, there need to be tests for:<ul>
 *
 * <li>Parsing all of its the syntactic variants.
 *
 * <li>Deriving the type of in all combinations of arguments.<ul>
 *
 *     <li>Pay particular attention to nullability.
 *     For example, the result of the "+" operator
 *     is NOT NULL if and only if both of its arguments are NOT NULL.</li>
 *
 *     <li>Also pay attention to precsion/scale/length.
 *     For example, the maximum length of the "||" operator is the sum of the
 *     maximum lengths of its arguments.</li></ul></li>
 *
 * <li>Executing the function. Pay particular attention to corner cases such
 *     as null arguments or null results.</li>
 *
 * </ul>
 *
 * @author Julian Hyde
 * @since October 1, 2004
 * @version $Id$
 */
public abstract class SqlOperatorTests extends TestCase
{
    /**
     * Remove this constant when dtbug 315 has been fixed.
     */
    public static final boolean bug315Fixed = false;


    /**
     * Whether <a href="http://jirahost.eigenbase.org:8080/browse/FNL-3">issue
     * Fnl-3</a> is fixed.
     */
    public static final boolean issueFnl3Fixed = false;
    
    /**
     * Whether <a href="http://jirahost.eigenbase.org:8080/browse/FRG-26">
     * issue FRG-26</a> is fixed.
     */
    public static final boolean issueFrg26Fixed = false;

    // TODO: Change message when issueFnl3Fixed to something like
    // "Invalid character for cast: PC=0 Code=22018"
    public static final String invalidCharMessage = "(?s).*";

    // TODO: Change message when issueFnl3Fixed to something like
    // "Overflow during calculation or cast: PC=0 Code=22003"
    public static final String outOfRangeMessage = "(?s).*";

    // TODO: Change message when issueFnl3Fixed to something like
    // "Division by zero: PC=0 Code=22012"
    public static final String divisionByZeroMessage = "(?s).*";

    // TODO: Change message when issueFnl3Fixed to something like
    // "String right truncation: PC=0 Code=22001"
    public static final String stringTruncMessage = "(?s).*";

    public static final String literalOutOfRangeMessage =
            "(?s).*Numeric literal.*out of range.*";

    /**
     * Regular expression for a SQL TIME(0) value.
     */
    public static final Pattern timePattern = Pattern.compile(
        "[0-9][0-9]:[0-9][0-9]:[0-9][0-9]");
    /**
     * Regular expression for a SQL TIMESTAMP(3) value.
     */
    public static final Pattern timestampPattern = Pattern.compile(
        "[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9] " +
        "[0-9][0-9]:[0-9][0-9]:[0-9][0-9]\\.[0-9]+");
    /**
     * Regular expression for a SQL DATE value.
     */
    public static final Pattern datePattern = Pattern.compile(
        "[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]");
    public static final boolean todo = false;

    
    public static final String[] numericTypeNames =
        new String[] {
            "TINYINT", "SMALLINT", "INTEGER", "BIGINT",
            "DECIMAL(5, 2)", "REAL", "FLOAT", "DOUBLE"
        };

    public static final String[] minNumericStrings =
        new String[] {
            Long.toString(Byte.MIN_VALUE),   
            Long.toString(Short.MIN_VALUE),
            Long.toString(Integer.MIN_VALUE),
            Long.toString(Long.MIN_VALUE),
            "-999.99",
            Float.toString(Float.MIN_VALUE),
            Double.toString(Double.MIN_VALUE),
            Double.toString(Double.MIN_VALUE),
        };

    public static final String[] minOverflowNumericStrings =
        new String[] {
            Long.toString(Byte.MIN_VALUE - 1),
            Long.toString(Short.MIN_VALUE - 1),
            Long.toString((long) Integer.MIN_VALUE - 1),
            (new BigDecimal(Long.MIN_VALUE)).subtract(BigDecimal.ONE).toString(),
            "-1000.00"
        };

    public static final String[] maxNumericStrings =
        new String[] {
            Long.toString(Byte.MAX_VALUE),
            Long.toString(Short.MAX_VALUE),
            Long.toString(Integer.MAX_VALUE),
            Long.toString(Long.MAX_VALUE),
            "999.99",
            Float.toString(Float.MAX_VALUE),
            Double.toString(Double.MAX_VALUE),
            Double.toString(Double.MAX_VALUE),
        };

    public static final String[] maxOverflowNumericStrings =
        new String[] {
            Long.toString(Byte.MAX_VALUE + 1),
            Long.toString(Short.MAX_VALUE + 1),
            Long.toString((long) Integer.MAX_VALUE + 1),
            (new BigDecimal(Long.MAX_VALUE)).add(BigDecimal.ONE).toString(),
            "1000.00"
        };

    public SqlOperatorTests(String testName)
    {
        super(testName);
    }

    /**
     * Derived class must implement this method to provide a means to validate,
     * execute various statements.
     */
    protected abstract SqlTester getTester();

    protected void setUp() throws Exception
    {
        getTester().setFor(null);
    }

    public void testBetween()
    {
        getTester().setFor(SqlStdOperatorTable.betweenOperator);
        getTester().checkBoolean("2 between 1 and 3", Boolean.TRUE);
        getTester().checkBoolean("2 between 3 and 2", Boolean.FALSE);
        getTester().checkBoolean("2 between symmetric 3 and 2", Boolean.TRUE);
        getTester().checkBoolean("3 between 1 and 3", Boolean.TRUE);
        getTester().checkBoolean("4 between 1 and 3", Boolean.FALSE);
        getTester().checkBoolean("1 between 4 and -3", Boolean.FALSE);
        getTester().checkBoolean("1 between -1 and -3", Boolean.FALSE);
        getTester().checkBoolean("1 between -1 and 3", Boolean.TRUE);
        getTester().checkBoolean("1 between 1 and 1", Boolean.TRUE);
        getTester().checkBoolean("1.5 between 1 and 3", Boolean.TRUE);
        getTester().checkBoolean("1.2 between 1.1 and 1.3", Boolean.TRUE);
        getTester().checkBoolean("1.5 between 2 and 3", Boolean.FALSE);
        getTester().checkBoolean("1.5 between 1.6 and 1.7", Boolean.FALSE);
        getTester().checkBoolean("x'' between x'' and x''", Boolean.TRUE);
        getTester().checkNull("cast(null as integer) between -1 and 2");
        getTester().checkNull("1 between -1 and cast(null as integer)");
        getTester().checkNull(
            "1 between cast(null as integer) and cast(null as integer)");
        getTester().checkNull("1 between cast(null as integer) and 1");
    }

    public void testNotBetween()
    {
        getTester().setFor(SqlStdOperatorTable.notBetweenOperator);
        getTester().checkBoolean("2 not between 1 and 3", Boolean.FALSE);
        getTester().checkBoolean("3 not between 1 and 3", Boolean.FALSE);
        getTester().checkBoolean("4 not between 1 and 3", Boolean.TRUE);
        getTester().checkBoolean("1.2e0 between 1.1 and 1.3", Boolean.FALSE);
        getTester().checkBoolean("1.5e0 between 2 and 3", Boolean.TRUE);
    }

    private String getCastString(String value, String targetType)
    {
        return "cast(" + value + " as " + targetType + ")";
    }

    private void checkCastToScalarOkay(String value, String targetType)
    {
        getTester().checkScalarExact(
                getCastString(value, targetType), targetType + " NOT NULL", value);
    }

    private void checkCastFails(String value, String targetType, String expectedError)
    {
        getTester().checkFails(
                getCastString(value, targetType), expectedError);
    }

    public void testCast()
    {
        getTester().setFor(SqlStdOperatorTable.castFunc);

        // Test casting for min,max, out of range for numeric types
        // TODO: Fix test for approx types
        for (int i = 0; i < 5/*numericTypeNames.length*/; i++) {
            String type = numericTypeNames[i];
            if (type.equalsIgnoreCase("BIGINT")) {
                checkCastToScalarOkay(maxNumericStrings[i], type);
                if (todo) {
                    // Literal is out or range because negative numbers are
                    // treated as expression with the minus as a prefix operator
                    checkCastToScalarOkay(minNumericStrings[i], type);
                }
                // Literal of range
                checkCastFails(maxOverflowNumericStrings[i], type, literalOutOfRangeMessage);
                checkCastFails(minOverflowNumericStrings[i], type, literalOutOfRangeMessage);

            } else {
                checkCastToScalarOkay(maxNumericStrings[i], type);
                checkCastToScalarOkay(minNumericStrings[i], type);
                checkCastFails(maxOverflowNumericStrings[i], type, outOfRangeMessage);
                checkCastFails(minOverflowNumericStrings[i], type, outOfRangeMessage);
            }
        }

        getTester().checkScalarExact("cast(1.0 as bigint)", "BIGINT NOT NULL", "1");
        getTester().checkScalarExact("cast(1.0 as int)", "1");
    }

    public void testCastDecimalToInteger() {
        getTester().setFor(SqlStdOperatorTable.castFunc);
        // decimal to integer
        getTester().checkScalarExact("cast(1.25 as integer)", "1");
        getTester().checkScalarExact("cast(-1.25 as integer)", "-1");
        getTester().checkScalarExact("cast(1.75 as integer)", "2");
        getTester().checkScalarExact("cast(-1.75 as integer)", "-2");
        getTester().checkScalarExact("cast(1.5 as integer)", "2");
        getTester().checkScalarExact("cast(-1.5 as integer)", "-2");

    }

    public void testCastDecimalToDecimal() {
        getTester().setFor(SqlStdOperatorTable.castFunc);
        // decimal to decimal
        getTester().checkScalarExact(
            "cast(1.29 as decimal(2,1))", "DECIMAL(2, 1) NOT NULL", "1.3");
        getTester().checkScalarExact(
            "cast(1.25 as decimal(2,1))", "DECIMAL(2, 1) NOT NULL", "1.3");
        getTester().checkScalarExact(
            "cast(1.21 as decimal(2,1))", "DECIMAL(2, 1) NOT NULL", "1.2");
        getTester().checkScalarExact(
            "cast(-1.29 as decimal(2,1))", "DECIMAL(2, 1) NOT NULL", "-1.3");
        getTester().checkScalarExact(
            "cast(-1.25 as decimal(2,1))", "DECIMAL(2, 1) NOT NULL", "-1.3");
        getTester().checkScalarExact(
            "cast(-1.21 as decimal(2,1))", "DECIMAL(2, 1) NOT NULL", "-1.2");

        // 9.99 round to 10.0, should give out of range error
        getTester().checkFails("cast(9.99 as decimal(2,1))", outOfRangeMessage);
    }

    public void testCastDecimalToDoubleToInteger() {
        getTester().setFor(SqlStdOperatorTable.castFunc);

        getTester().checkScalarExact("cast( cast(1.25 as double) as integer)", "1");
        getTester().checkScalarExact("cast( cast(-1.25 as double) as integer)", "-1");
        getTester().checkScalarExact("cast( cast(1.75 as double) as integer)", "2");
        getTester().checkScalarExact("cast( cast(-1.75 as double) as integer)", "-2");
        getTester().checkScalarExact("cast( cast(1.5 as double) as integer)", "2");

        getTester().checkScalarExact("cast( cast(-1.5 as double) as integer)", "-2");
    }

    public void testCastToDouble() {
        getTester().setFor(SqlStdOperatorTable.castFunc);
        getTester().checkScalarApprox("cast(1 as double)", "DOUBLE NOT NULL", 1, 0);
        getTester().checkScalarApprox("cast(1.0 as double)", "DOUBLE NOT NULL", 1, 0);
        getTester().checkScalarApprox("cast(-5.9 as double)", "DOUBLE NOT NULL", -5.9, 0);
    }

    public void testCastNull() {
        getTester().setFor(SqlStdOperatorTable.castFunc);
        // null
        getTester().checkNull("cast(null as decimal(4,3))");
        getTester().checkNull("cast(null as double)");
    }

    public void testCastDateTime()
    {
        // Test cast for date/time/timestamp
        getTester().setFor(SqlStdOperatorTable.castFunc);

        if (todo) {
        // TODO: Should the final result have the precision?
        getTester().checkScalar(
                "cast(TIMESTAMP '1945-02-24 12:42:25.34' as TIMESTAMP)",
                "1945-02-24 12:42:25.34",
                "TIMESTAMP NOT NULL");
        getTester().checkScalar(
                "cast(TIME '12:42:25.34' as TIME)",
                "12:42:25.34",
                "TIME NOT NULL");

        getTester().checkScalar(
                "cast(DATE '1945-02-24' as DATE)",
                "1945-02-24",
                "DATE NOT NULL");

        // timestamp <-> time
        getTester().checkScalar(
                "cast(TIMESTAMP '1945-02-24 12:42:25.34' as TIME)",
                "12:42:25",
                "TIME NOT NULL");
        getTester().checkScalar(
                "cast(TIME '12:42:25.34' as TIMESTAMP)",
                datePattern + " 12:42:25",
                "TIMESTAMP NOT NULL");

        // timestamp <-> date
        getTester().checkScalar(
                "cast(TIMESTAMP '1945-02-24 12:42:25.34' as DATE)",
                "1945-02-24",
                "DATE NOT NULL");
        getTester().checkScalar(
                "cast(DATE '1945-02-24' as TIMESTAMP)",
                "1945-02-24 00:00:00",
                "TIMESTAMP NOT NULL");

        // TODO: time <-> string

        // TODO: timestamp <-> string

        // TODO: date <-> string

        getTester().checkNull("cast(null as date)");
        getTester().checkNull("cast(null as timestamp)");
        getTester().checkNull("cast(null as time)");
        }
    }

    public void testCastString()
    {
        getTester().setFor(SqlStdOperatorTable.castFunc);

        // string to decimal
        getTester().checkScalarExact(
                "cast('1.29' as decimal(2,1))", "DECIMAL(2, 1) NOT NULL", "1.3");
        getTester().checkScalarExact(
                "cast(' 1.25 ' as decimal(2,1))", "DECIMAL(2, 1) NOT NULL", "1.3");
        getTester().checkScalarExact(
                "cast('1.21' as decimal(2,1))", "DECIMAL(2, 1) NOT NULL", "1.2");
        getTester().checkScalarExact(
                "cast(' -1.29 ' as decimal(2,1))", "DECIMAL(2, 1) NOT NULL", "-1.3");
        getTester().checkScalarExact(
                "cast('-1.25' as decimal(2,1))", "DECIMAL(2, 1) NOT NULL", "-1.3");
        getTester().checkScalarExact(
                "cast(' -1.21 ' as decimal(2,1))", "DECIMAL(2, 1) NOT NULL", "-1.2");
        getTester().checkFails(
                "cast(' -1.21e' as decimal(2,1))", invalidCharMessage);

        // decimal to string
        getTester().checkString(
                "cast(1.29 as varchar(10))", "1.29", "VARCHAR(10) NOT NULL");
        getTester().checkString(
                "cast(.48 as varchar(10))", ".48", "VARCHAR(10) NOT NULL");
        getTester().checkString(
                "cast(-0.29 as varchar(10))", "-.29", "VARCHAR(10) NOT NULL");
        getTester().checkString(
                "cast(-1.29 as varchar(10))", "-1.29", "VARCHAR(10) NOT NULL");

        // string to integer
        getTester().checkScalarExact("cast('6543' as integer)", "6543");
        if (issueFrg26Fixed) {
            getTester().checkScalarExact("cast(' -123 ' as int)", "-123");
        }
        getTester().checkScalarExact(
                "cast('654342432412312' as bigint)",
                "BIGINT NOT NULL", "654342432412312");

        // integer to string
        getTester().checkString(
                "cast(9354 as varchar(10))", "9354", "VARCHAR(10) NOT NULL");

        // string to double/float/real
        getTester().checkScalarApprox("cast('1' as double)", "DOUBLE NOT NULL", 1, 0);
        getTester().checkScalarApprox("cast('2.3' as float)", "FLOAT NOT NULL", 2.3, 0);
        getTester().checkScalarApprox("cast('-10.2' as real)", "REAL NOT NULL", -10.2, 0);

        // double/float/real to string
        // NOTE: Both Java and Fennel implementaions no do conform to
        // SQL.2003 standard, part 2, section 6.12, general rules 10/11, b
        // since it does not use the shortest character string
        if (todo) {
            // Is 4.5000000000000000E+002 in Fennel calc on Windows;
            // see http://issues.eigenbase.org/browse/FNL-7
            // Really should be 4.5E2
            getTester().checkString(
                "cast(45e1 as varchar(32))",
                "4.5000000000000000E+02", "VARCHAR(32) NOT NULL");

            // Is -3.5999999999999996E-03 in java calc
            getTester().checkString(
                    "cast(cast(-0.0036 as float) as varchar(32))",
                    "-3.5999999999999999E-03", "VARCHAR(32) NOT NULL");

            // Does not fit in fennel calc (because it is doing double)
            getTester().checkString(
                    "cast(cast(3.23 as real) as varchar(20))",
                    "3.23000000E+00", "VARCHAR(20) NOT NULL");
        }

        // boolean to string (char)
        getTester().checkString(
            "cast(true as char(4))", "TRUE", "CHAR(4) NOT NULL");
        getTester().checkString(
            "cast(false as char(5))", "FALSE", "CHAR(5) NOT NULL");
        getTester().checkString(
            "cast(true as char(8))", "TRUE    ", "CHAR(8) NOT NULL");
        getTester().checkString(
            "cast(false as char(8))", "FALSE   ", "CHAR(8) NOT NULL");
        getTester().checkFails("cast(true as char(3))", invalidCharMessage);
        getTester().checkFails("cast(false as char(4))", invalidCharMessage);

        // boolean to string (varchar)
        getTester().checkString(
            "cast(true as varchar(4))", "TRUE", "VARCHAR(4) NOT NULL");
        getTester().checkString(
            "cast(false as varchar(5))", "FALSE", "VARCHAR(5) NOT NULL");
        getTester().checkString(
            "cast(true as varchar(8))", "TRUE", "VARCHAR(8) NOT NULL");
        getTester().checkString(
            "cast(false as varchar(8))", "FALSE", "VARCHAR(8) NOT NULL");
        getTester().checkFails("cast(true as varchar(3))", invalidCharMessage);
        getTester().checkFails("cast(false as varchar(4))", invalidCharMessage);

        // string to boolean
        getTester().checkBoolean("cast('true' as boolean)", Boolean.TRUE);
        getTester().checkBoolean("cast('false' as boolean)", Boolean.FALSE);
        getTester().checkBoolean("cast('  trUe' as boolean)", Boolean.TRUE);
        getTester().checkBoolean("cast('  fALse' as boolean)", Boolean.FALSE);
        getTester().checkFails("cast('unknown' as boolean)", invalidCharMessage);
        getTester().checkFails("cast('blah' as boolean)", invalidCharMessage);

        getTester().checkBoolean(
                "cast(cast('true' as varchar(10))  as boolean)", Boolean.TRUE);
        getTester().checkBoolean(
                "cast(cast('false' as varchar(10)) as boolean)", Boolean.FALSE);
        getTester().checkFails(
                "cast(cast('blah' as varchar(10)) as boolean)", invalidCharMessage);

        // null
        getTester().checkNull("cast(null as varchar(10))");
        getTester().checkNull("cast(null as char(10))");
    }

    public void testCase()
    {
        getTester().setFor(SqlStdOperatorTable.caseOperator);
        getTester().checkScalarExact("case when 'a'='a' then 1 end", "1");

        getTester().checkString("case 2 when 1 then 'a' when 2 then 'b' end", "b", "todo: CHAR(1)");
        getTester().checkScalarExact("case 'a' when 'a' then 1 end", "1");
        getTester().checkNull("case 'a' when 'b' then 1 end");
        getTester().checkScalarExact(
            "case when 'a'=cast(null as varchar(1)) then 1 else 2 end", "2");
    }

    public void testJdbcFn()
    {
        getTester().setFor(new SqlJdbcFunctionCall("dummy"));
    }

    public void testSelect()
    {
        getTester().setFor(SqlStdOperatorTable.selectOperator);
        getTester().check("select * from (values(1))",
            AbstractSqlTester.IntegerTypeChecker,
            "1",
            0);
    }

    public void testLiteralChain()
    {
        getTester().setFor(SqlStdOperatorTable.literalChainOperator);
        getTester().checkString("'buttered'\n' toast'", "buttered toast", "CHAR(14) NOT NULL");
        getTester().checkString("'corned'\n' beef'\n' on'\n' rye'",
            "corned beef on rye", "CHAR(18) NOT NULL");
        getTester().checkString("_latin1'Spaghetti'\n' all''Amatriciana'",
            "Spaghetti all'Amatriciana", "CHAR(25) NOT NULL");
        getTester().checkBoolean("x'1234'\n'abcd' = x'1234abcd'", Boolean.TRUE);
        getTester().checkBoolean("x'1234'\n'' = x'1234'", Boolean.TRUE);
        getTester().checkBoolean("x''\n'ab' = x'ab'", Boolean.TRUE);
    }

    public void testRow()
    {
        getTester().setFor(SqlStdOperatorTable.rowConstructor);
    }

    public void testAndOperator()
    {
        getTester().setFor(SqlStdOperatorTable.andOperator);
        getTester().checkBoolean("true and false", Boolean.FALSE);
        getTester().checkBoolean("true and true", Boolean.TRUE);
        getTester().checkBoolean("cast(null as boolean) and false", Boolean.FALSE);
        getTester().checkBoolean("false and cast(null as boolean)", Boolean.FALSE);
        getTester().checkNull("cast(null as boolean) and true");
        getTester().checkBoolean("true and (not false)", Boolean.TRUE);
    }

    public void testConcatOperator()
    {
        getTester().setFor(SqlStdOperatorTable.concatOperator);
        getTester().checkString(" 'a'||'b' ", "ab", "todo: CHAR(2) NOT NULL");

        if (false) {
            // not yet implemented
            getTester().checkString(" x'f'||x'f' ", "X'FF", "BINARY(1) NOT NULL");
            getTester().checkNull("x'ff' || cast(null as varbinary)");
        }
    }

    public void testDivideOperator()
    {
        getTester().setFor(SqlStdOperatorTable.divideOperator);
        getTester().checkScalarExact("10 / 5", "2");
        getTester().checkScalarExact("-10 / 5", "-2");
        getTester().checkScalarExact("1 / 3", "0");
        getTester().checkScalarApprox(
                " cast(10.0 as double) / 5", "DOUBLE NOT NULL", 2.0, 0);
        getTester().checkScalarExact(
            "10.0 / 5.0", "DECIMAL(9, 6) NOT NULL", "2.000000");
        getTester().checkScalarExact(
            "1.0 / 3.0", "DECIMAL(8, 6) NOT NULL", "0.333333");
        getTester().checkScalarExact(
            "100.1 / 0.0001", "DECIMAL(14, 7) NOT NULL", "1001000.0000000");
        getTester().checkScalarExact(
            "100.1 / 0.00000001", "DECIMAL(19, 8) NOT NULL", "10010000000.00000000");
        getTester().checkNull("1e1 / cast(null as float)");

        getTester().checkFails(
            "100.1 / 0.00000000000000001", outOfRangeMessage);
    }

    public void testEqualsOperator()
    {
        getTester().setFor(SqlStdOperatorTable.equalsOperator);
        getTester().checkBoolean("1=1", Boolean.TRUE);
        getTester().checkBoolean("1=1.0", Boolean.TRUE);
        getTester().checkBoolean("1.34=1.34", Boolean.TRUE);
        getTester().checkBoolean("1=1.34", Boolean.FALSE);
        getTester().checkBoolean("'a'='b'", Boolean.FALSE);
        getTester().checkNull("cast(null as boolean)=cast(null as boolean)");
        getTester().checkNull("cast(null as integer)=1");
    }

    public void testGreaterThanOperator()
    {
        getTester().setFor(SqlStdOperatorTable.greaterThanOperator);
        getTester().checkBoolean("1>2", Boolean.FALSE);
        getTester().checkBoolean("cast(-1 as TINYINT)>cast(1 as TINYINT)", Boolean.FALSE);
        getTester().checkBoolean("cast(1 as SMALLINT)>cast(1 as SMALLINT)", Boolean.FALSE);
        getTester().checkBoolean("2>1", Boolean.TRUE);
        getTester().checkBoolean("1.1>1.2", Boolean.FALSE);
        getTester().checkBoolean("-1.1>-1.2", Boolean.TRUE);
        getTester().checkBoolean("1.1>1.1", Boolean.FALSE);
        getTester().checkBoolean("1.2>1", Boolean.TRUE);
        getTester().checkBoolean("true>false", Boolean.TRUE);
        getTester().checkBoolean("true>true", Boolean.FALSE);
        getTester().checkBoolean("false>false", Boolean.FALSE);
        getTester().checkBoolean("false>true", Boolean.FALSE);
        getTester().checkNull("3.0>cast(null as double)");
    }

    public void testIsDistinctFromOperator()
    {
        getTester().setFor(SqlStdOperatorTable.isDistinctFromOperator);
        getTester().checkBoolean("1 is distinct from 1", Boolean.FALSE);
        getTester().checkBoolean("1 is distinct from 1.0", Boolean.FALSE);
        getTester().checkBoolean("1 is distinct from 2", Boolean.TRUE);
        getTester().checkBoolean("cast(null as integer) is distinct from 2", Boolean.TRUE);
        getTester().checkBoolean("cast(null as integer) is distinct from cast(null as integer)", Boolean.FALSE);
        getTester().checkBoolean("1.23 is distinct from 1.23", Boolean.FALSE);
        getTester().checkBoolean("1.23 is distinct from 5.23", Boolean.TRUE);
        //getTester().checkBoolean("row(1,1) is distinct from row(1,1)", Boolean.TRUE);
        //getTester().checkBoolean("row(1,1) is distinct from row(1,2)", Boolean.FALSE);
    }

    public void testIsNotDistinctFromOperator()
    {
        getTester().setFor(SqlStdOperatorTable.isNotDistinctFromOperator);
        getTester().checkBoolean("1 is not distinct from 1", Boolean.TRUE);
        getTester().checkBoolean("1 is not distinct from 1.0", Boolean.TRUE);
        getTester().checkBoolean("1 is not distinct from 2", Boolean.FALSE);
        getTester().checkBoolean("cast(null as integer) is not distinct from 2", Boolean.FALSE);
        getTester().checkBoolean("cast(null as integer) is not distinct from cast(null as integer)", Boolean.TRUE);
        getTester().checkBoolean("1.23 is not distinct from 1.23", Boolean.TRUE);
        getTester().checkBoolean("1.23 is not distinct from 5.23", Boolean.FALSE);
        //getTester().checkBoolean("row(1,1) is not distinct from row(1,1)", Boolean.FALSE);
        //getTester().checkBoolean("row(1,1) is not distinct from row(1,2)", Boolean.TRUE);
    }

    public void testGreaterThanOrEqualOperator()
    {
        getTester().setFor(SqlStdOperatorTable.greaterThanOrEqualOperator);
        getTester().checkBoolean("1>=2", Boolean.FALSE);
        getTester().checkBoolean("-1>=1", Boolean.FALSE);
        getTester().checkBoolean("1>=1", Boolean.TRUE);
        getTester().checkBoolean("2>=1", Boolean.TRUE);
        getTester().checkBoolean("1.1>=1.2", Boolean.FALSE);
        getTester().checkBoolean("-1.1>=-1.2", Boolean.TRUE);
        getTester().checkBoolean("1.1>=1.1", Boolean.TRUE);
        getTester().checkBoolean("1.2>=1", Boolean.TRUE);
        getTester().checkBoolean("true>=false", Boolean.TRUE);
        getTester().checkBoolean("true>=true", Boolean.TRUE);
        getTester().checkBoolean("false>=false", Boolean.TRUE);
        getTester().checkBoolean("false>=true", Boolean.FALSE);
        getTester().checkNull("cast(null as real)>=999");
    }

    public void testInOperator()
    {
        getTester().setFor(SqlStdOperatorTable.inOperator);
    }

    public void testOverlapsOperator()
    {
        getTester().setFor(SqlStdOperatorTable.overlapsOperator);
    }

    public void testLessThanOperator()
    {
        getTester().setFor(SqlStdOperatorTable.lessThanOperator);
        getTester().checkBoolean("1<2", Boolean.TRUE);
        getTester().checkBoolean("-1<1", Boolean.TRUE);
        getTester().checkBoolean("1<1", Boolean.FALSE);
        getTester().checkBoolean("2<1", Boolean.FALSE);
        getTester().checkBoolean("1.1<1.2", Boolean.TRUE);
        getTester().checkBoolean("-1.1<-1.2", Boolean.FALSE);
        getTester().checkBoolean("1.1<1.1", Boolean.FALSE);
        getTester().checkBoolean("1.2<1", Boolean.FALSE);
        getTester().checkBoolean("true<false", Boolean.FALSE);
        getTester().checkBoolean("true<true", Boolean.FALSE);
        getTester().checkBoolean("false<false", Boolean.FALSE);
        getTester().checkBoolean("false<true", Boolean.TRUE);
        getTester().checkNull("123<cast(null as bigint)");
        getTester().checkNull("cast(null as tinyint)<123");
        getTester().checkNull("cast(null as integer)<1.32");
    }

    public void testLessThanOrEqualOperator()
    {
        getTester().setFor(SqlStdOperatorTable.lessThanOrEqualOperator);
        getTester().checkBoolean("1<=2", Boolean.TRUE);
        getTester().checkBoolean("1<=1", Boolean.TRUE);
        getTester().checkBoolean("-1<=1", Boolean.TRUE);
        getTester().checkBoolean("2<=1", Boolean.FALSE);
        getTester().checkBoolean("1.1<=1.2", Boolean.TRUE);
        getTester().checkBoolean("-1.1<=-1.2", Boolean.FALSE);
        getTester().checkBoolean("1.1<=1.1", Boolean.TRUE);
        getTester().checkBoolean("1.2<=1", Boolean.FALSE);
        getTester().checkBoolean("true<=false", Boolean.FALSE);
        getTester().checkBoolean("true<=true", Boolean.TRUE);
        getTester().checkBoolean("false<=false", Boolean.TRUE);
        getTester().checkBoolean("false<=true", Boolean.TRUE);
        getTester().checkNull("cast(null as integer)<=3");
        getTester().checkNull("3<=cast(null as smallint)");
        getTester().checkNull("cast(null as integer)<=1.32");
    }

    public void testMinusOperator()
    {
        getTester().setFor(SqlStdOperatorTable.minusOperator);
        getTester().checkScalarExact("-2-1", "-3");
        getTester().checkScalarExact("2-1", "1");
        getTester().checkScalarApprox(
                "cast(2.0 as double) -1", "DOUBLE NOT NULL", 1, 0);
        getTester().checkScalarExact("1-2", "-1");
        getTester().checkScalarExact(
            "10.0 - 5.0", "DECIMAL(4, 1) NOT NULL", "5.0");
        getTester().checkScalarExact(
            "19.68 - 4.2", "DECIMAL(5, 2) NOT NULL", "15.48");
        getTester().checkNull("1e1-cast(null as double)");
        getTester().checkNull("cast(null as tinyint) - cast(null as smallint)");

        if (todo) {
            // Should throw out of range error
            getTester().checkFails(
                    "cast(100 as tinyint) - cast(-100 as tinyint)", outOfRangeMessage);
            getTester().checkFails(
                    "cast(-20000 as smallint) - cast(20000 as smallint)", outOfRangeMessage);
            getTester().checkFails(
                    "cast(1.5e9 as integer) - cast(-1.5e9 as integer)", outOfRangeMessage);
            getTester().checkFails(
                    "cast(-5e18 as bigint) - cast(5e18 as bigint)", outOfRangeMessage);
            getTester().checkFails(
                    "cast(5e18 as decimal(19,0)) - cast(-5e18 as decimal(19,0))", outOfRangeMessage);
            getTester().checkFails(
                    "cast(-5e8 as decimal(19,10)) - cast(5e8 as decimal(19,10))", outOfRangeMessage);
        }
    }

    public void testMinusDateOperator()
    {
        getTester().setFor(SqlStdOperatorTable.minusDateOperator);
        //todo
    }

    public void testMultiplyOperator()
    {
        getTester().setFor(SqlStdOperatorTable.multiplyOperator);
        getTester().checkScalarExact("2*3", "6");
        getTester().checkScalarExact("2*-3", "-6");
        getTester().checkScalarExact("+2*3", "6");
        getTester().checkScalarExact("2*0", "0");
        getTester().checkScalarApprox(
            "cast(2.0 as double)*3", "DOUBLE NOT NULL", 6, 0);
        getTester().checkScalarExact(
                "10.0 * 5.0", "DECIMAL(5, 2) NOT NULL", "50.00");
        getTester().checkScalarExact(
                "19.68 * 4.2", "DECIMAL(6, 3) NOT NULL", "82.656");
        getTester().checkNull("2e-3*cast(null as integer)");
        getTester().checkNull("cast(null as tinyint) * cast(4 as smallint)");

        if (todo) {
            // Should throw out of range error
            getTester().checkFails(
                    "cast(100 as tinyint) * cast(-2 as tinyint)", outOfRangeMessage);
            getTester().checkFails(
                    "cast(200 as smallint) * cast(200 as smallint)", outOfRangeMessage);
            getTester().checkFails(
                    "cast(1.5e9 as integer) * cast(-2 as integer)", outOfRangeMessage);
            getTester().checkFails(
                    "cast(5e9 as bigint) * cast(2e9 as bigint)", outOfRangeMessage);
            getTester().checkFails(
                    "cast(2e9 as decimal(19,0)) * cast(-5e9 as decimal(19,0))", outOfRangeMessage);
            getTester().checkFails(
                    "cast(5e4 as decimal(19,10)) * cast(2e4 as decimal(19,10))", outOfRangeMessage);
        }
    }

    public void testNotEqualsOperator()
    {
        getTester().setFor(SqlStdOperatorTable.notEqualsOperator);
        getTester().checkBoolean("1<>1", Boolean.FALSE);
        getTester().checkBoolean("'a'<>'A'", Boolean.TRUE);
        getTester().checkNull("'a'<>cast(null as varchar(1))");
    }

    public void testOrOperator()
    {
        getTester().setFor(SqlStdOperatorTable.orOperator);
        getTester().checkBoolean("true or false", Boolean.TRUE);
        getTester().checkBoolean("false or false", Boolean.FALSE);
        getTester().checkBoolean("true or cast(null as boolean)", Boolean.TRUE);
        getTester().checkNull("false or cast(null as boolean)");
    }

    public void testPlusOperator()
    {
        getTester().setFor(SqlStdOperatorTable.plusOperator);
        getTester().checkScalarExact("1+2", "3");
        getTester().checkScalarExact("-1+2", "1");
        getTester().checkScalarApprox(
                "1+cast(2.0 as double)", "DOUBLE NOT NULL", 3, 0);
        getTester().checkScalarExact(
            "10.0 + 5.0", "DECIMAL(4, 1) NOT NULL", "15.0");
        getTester().checkScalarExact(
            "19.68 + 4.2", "DECIMAL(5, 2) NOT NULL", "23.88");
        getTester().checkScalarApprox(
            "19.68 + cast(4.2 as float)", "DOUBLE NOT NULL", 23.88, 0);
        getTester().checkNull("cast(null as tinyint)+1");
        getTester().checkNull("1e-2+cast(null as double)");

        if (todo) {
            // Should throw out of range error
            getTester().checkFails(
                    "cast(100 as tinyint) + cast(100 as tinyint)", outOfRangeMessage);
            getTester().checkFails(
                    "cast(-20000 as smallint) + cast(-20000 as smallint)", outOfRangeMessage);
            getTester().checkFails(
                    "cast(1.5e9 as integer) + cast(1.5e9 as integer)", outOfRangeMessage);
            getTester().checkFails(
                    "cast(5e18 as bigint) + cast(5e18 as bigint)", outOfRangeMessage);
            getTester().checkFails(
                    "cast(-5e18 as decimal(19,0)) + cast(-5e18 as decimal(19,0))", outOfRangeMessage);
            getTester().checkFails(
                    "cast(5e8 as decimal(19,10)) + cast(5e8 as decimal(19,10))", outOfRangeMessage);
        }
    }

    public void testDescendingOperator()
    {
        getTester().setFor(SqlStdOperatorTable.descendingOperator);
    }

    public void testIsNotNullOperator()
    {
        getTester().setFor(SqlStdOperatorTable.isNotNullOperator);
        getTester().checkBoolean("true is not null", Boolean.TRUE);
        getTester().checkBoolean("cast(null as boolean) is not null",
            Boolean.FALSE);
    }

    public void testIsNullOperator()
    {
        getTester().setFor(SqlStdOperatorTable.isNullOperator);
        getTester().checkBoolean("true is null", Boolean.FALSE);
        getTester().checkBoolean("cast(null as boolean) is null",
            Boolean.TRUE);
    }

    public void testIsNotTrueOperator()
    {
        getTester().setFor(SqlStdOperatorTable.isNotTrueOperator);
        getTester().checkBoolean("true is not true", Boolean.FALSE);
        getTester().checkBoolean("false is not true", Boolean.TRUE);
        getTester().checkBoolean("cast(null as boolean) is not true",
            Boolean.TRUE);
        getTester().checkInvalid(
            "select ^'a string' is not true^ from (values (1))",
            "(?s)Cannot apply 'IS NOT TRUE' to arguments of type '<CHAR\\(8\\)> IS NOT TRUE'. Supported form\\(s\\): '<BOOLEAN> IS NOT TRUE'.*");
    }

    public void testIsTrueOperator()
    {
        getTester().setFor(SqlStdOperatorTable.isTrueOperator);
        getTester().checkBoolean("true is true", Boolean.TRUE);
        getTester().checkBoolean("false is true", Boolean.FALSE);
        getTester().checkBoolean("cast(null as boolean) is true",
            Boolean.FALSE);
    }

    public void testIsNotFalseOperator()
    {
        getTester().setFor(SqlStdOperatorTable.isNotFalseOperator);
        getTester().checkBoolean("false is not false", Boolean.FALSE);
        getTester().checkBoolean("true is not false", Boolean.TRUE);
        getTester().checkBoolean("cast(null as boolean) is not false",
            Boolean.TRUE);
    }

    public void testIsFalseOperator()
    {
        getTester().setFor(SqlStdOperatorTable.isFalseOperator);
        getTester().checkBoolean("false is false", Boolean.TRUE);
        getTester().checkBoolean("true is false", Boolean.FALSE);
        getTester().checkBoolean("cast(null as boolean) is false",
            Boolean.FALSE);
    }

    public void testIsNotUnknownOperator()
    {
        getTester().setFor(SqlStdOperatorTable.isNotUnknownOperator);
        getTester().checkBoolean("false is not unknown", Boolean.TRUE);
        getTester().checkBoolean("true is not unknown", Boolean.TRUE);
        getTester().checkBoolean("cast(null as boolean) is not unknown",
            Boolean.FALSE);
        getTester().checkBoolean("unknown is not unknown", Boolean.FALSE);
        getTester().checkInvalid("^'abc' IS NOT UNKNOWN^", "(?s).*Cannot apply 'IS NOT UNKNOWN'.*");
    }

    public void testIsUnknownOperator()
    {
        getTester().setFor(SqlStdOperatorTable.isUnknownOperator);
        getTester().checkBoolean("false is unknown", Boolean.FALSE);
        getTester().checkBoolean("true is unknown", Boolean.FALSE);
        getTester().checkBoolean("cast(null as boolean) is unknown",
            Boolean.TRUE);
        getTester().checkBoolean("unknown is unknown", Boolean.TRUE);
        getTester().checkInvalid("0 = 1 AND ^2 IS UNKNOWN^ AND 3 > 4", "(?s).*Cannot apply 'IS UNKNOWN'.*");
    }

    public void testIsASetOperator()
    {
        getTester().setFor(SqlStdOperatorTable.isASetOperator);
    }

    public void testExistsOperator()
    {
        getTester().setFor(SqlStdOperatorTable.existsOperator);
    }

    public void testNotOperator()
    {
        getTester().setFor(SqlStdOperatorTable.notOperator);
        getTester().checkBoolean("not true", Boolean.FALSE);
        getTester().checkBoolean("not false", Boolean.TRUE);
        getTester().checkBoolean("not unknown", null);
        getTester().checkNull("not cast(null as boolean)");
    }

    public void testPrefixMinusOperator()
    {
        getTester().setFor(SqlStdOperatorTable.prefixMinusOperator);
        getTester().checkInvalid(
            "'a' + ^- 'b'^ + 'c'",
            "(?s)Cannot apply '-' to arguments of type '-<CHAR\\(1\\)>'.*");
        getTester().checkScalarExact("-1", "-1");
        getTester().checkScalarExact("-1.23", "DECIMAL(3, 2) NOT NULL", "-1.23");
        getTester().checkScalarApprox("-1.0e0", "DOUBLE NOT NULL", -1, 0);
        getTester().checkNull("-cast(null as integer)");
        getTester().checkNull("-cast(null as tinyint)");
    }

    public void testPrefixPlusOperator()
    {
        getTester().setFor(SqlStdOperatorTable.prefixPlusOperator);
        getTester().checkScalarExact("+1", "1");
        getTester().checkScalarExact("+1.23", "DECIMAL(3, 2) NOT NULL", "1.23");
        getTester().checkScalarApprox("+1.0e0", "DOUBLE NOT NULL", 1, 0);
        getTester().checkNull("+cast(null as integer)");
        getTester().checkNull("+cast(null as tinyint)");
    }

    public void testExplicitTableOperator()
    {
        getTester().setFor(SqlStdOperatorTable.explicitTableOperator);
    }

    public void testValuesOperator()
    {
        getTester().setFor(SqlStdOperatorTable.valuesOperator);
        getTester().check(
            "select 'abc' from (values(true))",
            new AbstractSqlTester.StringTypeChecker("CHAR(3) NOT NULL"),
            "abc",
            0);
    }

    public void testNotLikeOperator()
    {
        getTester().setFor(SqlStdOperatorTable.notLikeOperator);
        getTester().checkBoolean("'abc' not like '_b_'", Boolean.FALSE);
    }

    public void testLikeOperator()
    {
        getTester().setFor(SqlStdOperatorTable.likeOperator);
        getTester().checkBoolean("''  like ''", Boolean.TRUE);
        getTester().checkBoolean("'a' like 'a'", Boolean.TRUE);
        getTester().checkBoolean("'a' like 'b'", Boolean.FALSE);
        getTester().checkBoolean("'a' like 'A'", Boolean.FALSE);
        getTester().checkBoolean("'a' like 'a_'", Boolean.FALSE);
        getTester().checkBoolean("'a' like '_a'", Boolean.FALSE);
        getTester().checkBoolean("'a' like '%a'", Boolean.TRUE);
        getTester().checkBoolean("'a' like '%a%'", Boolean.TRUE);
        getTester().checkBoolean("'a' like 'a%'", Boolean.TRUE);
        getTester().checkBoolean("'ab'   like 'a_'", Boolean.TRUE);
        getTester().checkBoolean("'abc'  like 'a_'", Boolean.FALSE);
        getTester().checkBoolean("'abcd' like 'a%'", Boolean.TRUE);
        getTester().checkBoolean("'ab'   like '_b'", Boolean.TRUE);
        getTester().checkBoolean("'abcd' like '_d'", Boolean.FALSE);
        getTester().checkBoolean("'abcd' like '%d'", Boolean.TRUE);
    }

    public void testNotSimilarToOperator()
    {
        getTester().setFor(SqlStdOperatorTable.notSimilarOperator);
        getTester().checkBoolean("'ab' not similar to 'a_'", Boolean.FALSE);
    }

    public void testSimilarToOperator()
    {
        getTester().setFor(SqlStdOperatorTable.similarOperator);
        // like LIKE
        getTester().checkBoolean("''  similar to ''", Boolean.TRUE);
        getTester().checkBoolean("'a' similar to 'a'", Boolean.TRUE);
        getTester().checkBoolean("'a' similar to 'b'", Boolean.FALSE);
        getTester().checkBoolean("'a' similar to 'A'", Boolean.FALSE);
        getTester().checkBoolean("'a' similar to 'a_'", Boolean.FALSE);
        getTester().checkBoolean("'a' similar to '_a'", Boolean.FALSE);
        getTester().checkBoolean("'a' similar to '%a'", Boolean.TRUE);
        getTester().checkBoolean("'a' similar to '%a%'", Boolean.TRUE);
        getTester().checkBoolean("'a' similar to 'a%'", Boolean.TRUE);
        getTester().checkBoolean("'ab'   similar to 'a_'", Boolean.TRUE);
        getTester().checkBoolean("'abc'  similar to 'a_'", Boolean.FALSE);
        getTester().checkBoolean("'abcd' similar to 'a%'", Boolean.TRUE);
        getTester().checkBoolean("'ab'   similar to '_b'", Boolean.TRUE);
        getTester().checkBoolean("'abcd' similar to '_d'", Boolean.FALSE);
        getTester().checkBoolean("'abcd' similar to '%d'", Boolean.TRUE);

        // simple regular expressions
        // ab*c+d matches acd, abcd, acccd, abcccd but not abd, aabc
        getTester().checkBoolean("'acd'    similar to 'ab*c+d'", Boolean.TRUE);
        getTester().checkBoolean("'abcd'   similar to 'ab*c+d'", Boolean.TRUE);
        getTester().checkBoolean("'acccd'  similar to 'ab*c+d'", Boolean.TRUE);
        getTester().checkBoolean("'abcccd' similar to 'ab*c+d'", Boolean.TRUE);
        getTester().checkBoolean("'abd'    similar to 'ab*c+d'", Boolean.FALSE);
        getTester().checkBoolean("'aabc'   similar to 'ab*c+d'", Boolean.FALSE);

        // compound regular expressions
        // x(ab|c)*y matches xy, xccy, xababcy but not xbcy
        getTester().checkBoolean("'xy'      similar to 'x(ab|c)*y'", Boolean.TRUE);
        getTester().checkBoolean("'xccy'    similar to 'x(ab|c)*y'", Boolean.TRUE);
        getTester().checkBoolean("'xababcy' similar to 'x(ab|c)*y'", Boolean.TRUE);
        getTester().checkBoolean("'xbcy'    similar to 'x(ab|c)*y'", Boolean.FALSE);

        // x(ab|c)+y matches xccy, xababcy but not xy, xbcy
        getTester().checkBoolean("'xy'      similar to 'x(ab|c)+y'", Boolean.FALSE);
        getTester().checkBoolean("'xccy'    similar to 'x(ab|c)+y'", Boolean.TRUE);
        getTester().checkBoolean("'xababcy' similar to 'x(ab|c)+y'", Boolean.TRUE);
        getTester().checkBoolean("'xbcy'    similar to 'x(ab|c)+y'", Boolean.FALSE);
    }

    public void testEscapeOperator()
    {
        getTester().setFor(SqlStdOperatorTable.escapeOperator);
    }

    public void testConvertFunc()
    {
        getTester().setFor(SqlStdOperatorTable.convertFunc);
    }

    public void testTranslateFunc()
    {
        getTester().setFor(SqlStdOperatorTable.translateFunc);
    }

    public void testOverlayFunc()
    {
        getTester().setFor(SqlStdOperatorTable.overlayFunc);
        getTester().checkString("overlay('ABCdef' placing 'abc' from 1)",
            "abcdef", "todo: CHAR(9) NOT NULL");
        getTester().checkString("overlay('ABCdef' placing 'abc' from 1 for 2)",
            "abcCdef", "todo: CHAR(9) NOT NULL");
        getTester().checkNull(
            "overlay('ABCdef' placing 'abc' from 1 for cast(null as integer))");
        getTester().checkNull(
            "overlay(cast(null as varchar(1)) placing 'abc' from 1)");

        if (false) {
            // hex strings not yet implemented in calc
            getTester().checkNull("overlay(x'abc' placing x'abc' from cast(null as integer))");
        }
    }

    public void testPositionFunc()
    {
        getTester().setFor(SqlStdOperatorTable.positionFunc);
        getTester().checkScalarExact("position('b' in 'abc')", "2");
        getTester().checkScalarExact("position('' in 'abc')", "1");

        getTester().checkNull("position(cast(null as varchar(1)) in '0010')");
        getTester().checkNull("position('a' in cast(null as varchar(1)))");
    }

    public void testCharLengthFunc()
    {
        getTester().setFor(SqlStdOperatorTable.charLengthFunc);
        getTester().checkScalarExact("char_length('abc')", "3");
        getTester().checkNull("char_length(cast(null as varchar(1)))");
    }

    public void testCharacterLengthFunc()
    {
        getTester().setFor(SqlStdOperatorTable.characterLengthFunc);
        getTester().checkScalarExact("CHARACTER_LENGTH('abc')", "3");
        getTester().checkNull("CHARACTER_LENGTH(cast(null as varchar(1)))");
    }

    public void testUpperFunc()
    {
        getTester().setFor(SqlStdOperatorTable.upperFunc);
        getTester().checkString("upper('a')", "A", "todo: CHAR(1) NOT NULL");
        getTester().checkString("upper('A')", "A", "todo: CHAR(1) NOT NULL");
        getTester().checkString("upper('1')", "1", "todo: CHAR(1) NOT NULL");
        getTester().checkNull("upper(cast(null as varchar(1)))");
    }

    public void testLowerFunc()
    {
        getTester().setFor(SqlStdOperatorTable.lowerFunc);
        // SQL:2003 6.29.8 The type of lower is the type of its argument
        getTester().checkString("lower('A')", "a", "todo: CHAR(1) NOT NULL");
        getTester().checkString("lower('a')", "a", "todo: CHAR(1) NOT NULL");
        getTester().checkString("lower('1')", "1", "todo: CHAR(1) NOT NULL");
        getTester().checkNull("lower(cast(null as varchar(1)))");
    }

    public void testInitcapFunc()
    {
        getTester().setFor(SqlStdOperatorTable.initcapFunc);
        getTester().checkString("initcap('aA')", "Aa", "todo:");
        getTester().checkString("initcap('Aa')", "Aa", "todo:");
        getTester().checkString("initcap('1a')", "1a", "todo:");
        getTester().checkString("initcap('ab cd Ef 12')", "Ab Cd Ef 12", "todo:");
        getTester().checkNull("initcap(cast(null as varchar(1)))");
    }

    public void testPowFunc()
    {
        getTester().setFor(SqlStdOperatorTable.powFunc);
        getTester().checkScalarApprox("pow(2,-2)", "todo:", 0.25, 0);
        getTester().checkNull("pow(cast(null as integer),2)");
        getTester().checkNull("pow(2,cast(null as double))");
    }

    public void testExpFunc()
    {
        getTester().setFor(SqlStdOperatorTable.expFunc);
        getTester().checkScalarApprox("exp(2)", "DOUBLE NOT NULL", 7.389056, 0.000001);
        getTester().checkScalarApprox("exp(-2)", "DOUBLE NOT NULL", 0.1353, 0.0001);
        getTester().checkNull("exp(cast(null as integer))");
        getTester().checkNull("exp(cast(null as double))");
    }

    public void testModFunc()
    {
        getTester().setFor(SqlStdOperatorTable.modFunc);
        getTester().checkScalarExact("mod(4,2)", "0");
        getTester().checkScalarExact("mod(8,5)", "3");
        getTester().checkScalarExact("mod(-12,7)", "-5");
        getTester().checkScalarExact("mod(-12,-7)", "-5");
        getTester().checkScalarExact("mod(12,-7)", "5");
        getTester().checkScalarExact(
                "mod(cast(12 as tinyint), cast(-7 as tinyint))",
                "TINYINT NOT NULL", "5");

        getTester().checkScalarExact(
                "mod(cast(9 as decimal(2, 0)), 7)", "INTEGER NOT NULL", "2");
        getTester().checkScalarExact(
                "mod(7, cast(9 as decimal(2, 0)))", "DECIMAL(2, 0) NOT NULL", "7");
        getTester().checkScalarExact(
                "mod(cast(-9 as decimal(2, 0)), cast(7 as decimal(1, 0)))",
                "DECIMAL(1, 0) NOT NULL", "-2");
        getTester().checkNull("mod(cast(null as integer),2)");
        getTester().checkNull("mod(4,cast(null as tinyint))");
        getTester().checkNull("mod(4,cast(null as decimal(12,0)))");
        getTester().checkFails("mod(3,0)", divisionByZeroMessage);
    }

    public void testLnFunc()
    {
        getTester().setFor(SqlStdOperatorTable.lnFunc);
        getTester().checkScalarApprox("ln(2.71828)", "DOUBLE NOT NULL", 1.0, 0.000001);
        getTester().checkScalarApprox("ln(2.71828)", "DOUBLE NOT NULL", 0.999999327, 0.0000001);
        getTester().checkNull("ln(cast(null as tinyint))");
    }

    public void testLogFunc()
    {
        getTester().setFor(SqlStdOperatorTable.log10Func);
        getTester().checkScalarApprox("log10(10)", "DOUBLE NOT NULL", 1.0, 0.000001);
        getTester().checkScalarApprox("log10(100.0)", "DOUBLE NOT NULL", 2.0, 0.000001);
        getTester().checkScalarApprox("log10(cast(10e8 as double))", "DOUBLE NOT NULL", 9.0, 0.000001);
        getTester().checkScalarApprox("log10(cast(10e2 as float))", "DOUBLE NOT NULL", 3.0, 0.000001);
        getTester().checkScalarApprox("log10(cast(10e-3 as real))", "DOUBLE NOT NULL", -2.0, 0.000001);
        getTester().checkNull("log10(cast(null as real))");
    }

    public void testAbsFunc()
    {
        getTester().setFor(SqlStdOperatorTable.absFunc);

        getTester().checkScalarExact("abs(-1)", "1");
        getTester().checkScalarExact("abs(cast(10 as TINYINT))", "TINYINT NOT NULL", "10");
        getTester().checkScalarExact("abs(cast(-20 as SMALLINT))", "SMALLINT NOT NULL", "20");
        getTester().checkScalarExact("abs(cast(-100 as INT))", "INTEGER NOT NULL", "100");
        getTester().checkScalarExact("abs(cast(1000 as BIGINT))", "BIGINT NOT NULL", "1000");
        getTester().checkScalarExact("abs(54.4)", "DECIMAL(3, 1) NOT NULL", "54.4");
        getTester().checkScalarExact("abs(-54.4)", "DECIMAL(3, 1) NOT NULL", "54.4");
        getTester().checkScalarApprox("abs(-9.32E-2)", "DOUBLE NOT NULL", 0.0932, 0);
        getTester().checkScalarApprox("abs(cast(-3.5 as double))", "DOUBLE NOT NULL", 3.5, 0);
        getTester().checkScalarApprox("abs(cast(-3.5 as float))", "FLOAT NOT NULL", 3.5, 0);
        getTester().checkScalarApprox("abs(cast(3.5 as real))", "REAL NOT NULL", 3.5, 0);

        getTester().checkNull("abs(cast(null as double))");
    }

    public void testNullifFunc()
    {
        getTester().setFor(SqlStdOperatorTable.nullIfFunc);
        getTester().checkNull("nullif(1,1)");
        getTester().checkString("nullif('a','bc')", "a", "todo: VARCHAR(2) NOT NULL");
        getTester().checkString("nullif('a',cast(null as varchar(1)))", "a", "todo: VARCHAR(1) NOT NULL");
        getTester().checkNull("nullif(cast(null as varchar(1)),'a')");
        getTester().checkNull("nullif(cast(null as numeric(4,3)), 4.3)");
        // Error message reflects the fact that Nullif is expanded before it is
        // validated (like a C macro). Not perfect, but good enough.
        getTester().checkInvalid("1 + ^nullif(1, date '2005-8-4')^ + 2",
            "(?s)Cannot apply '=' to arguments of type '<INTEGER> = <DATE>'\\..*");
        // TODO: fix bug 324.
        if (todo) {
        getTester().checkInvalid("1 + ^nullif(1, 2, 3)^ + 2",
            "invalid number of arguments to NULLIF");
        }
    }

    public void testCoalesceFunc()
    {
        getTester().setFor(SqlStdOperatorTable.coalesceFunc);
        getTester().checkString("coalesce('a','b')", "a", "CHAR(1) NOT NULL");
        getTester().checkScalarExact("coalesce(null,null,3)", "3");
        getTester().checkInvalid("1 + ^coalesce('a', 'b', 1, null)^ + 2",
            "Illegal mixing of types in CASE or COALESCE statement");
    }

    public void testUserFunc()
    {
        getTester().setFor(SqlStdOperatorTable.userFunc);
        getTester().checkString("USER", "sa", "VARCHAR(2000) NOT NULL");
    }

    public void testCurrentUserFunc()
    {
        getTester().setFor(SqlStdOperatorTable.currentUserFunc);
        getTester().checkString("CURRENT_USER", "sa", "VARCHAR(2000) NOT NULL");
    }

    public void testSessionUserFunc()
    {
        getTester().setFor(SqlStdOperatorTable.sessionUserFunc);
        getTester().checkString("SESSION_USER", "sa", "VARCHAR(2000) NOT NULL");
    }

    public void testSystemUserFunc()
    {
        getTester().setFor(SqlStdOperatorTable.systemUserFunc);
        String user = System.getProperty("user.name"); // e.g. "jhyde"
        getTester().checkString("SYSTEM_USER", user, "VARCHAR(2000) NOT NULL");
    }

    public void testCurrentPathFunc()
    {
        getTester().setFor(SqlStdOperatorTable.currentPathFunc);
        getTester().checkString("CURRENT_PATH", "", "VARCHAR(2000) NOT NULL");
    }

    public void testCurrentRoleFunc()
    {
        getTester().setFor(SqlStdOperatorTable.currentRoleFunc);
        // By default, the CURRENT_ROLE function returns
        // the empty string because a role has to be set explicitly.
        getTester().checkString("CURRENT_ROLE", "", "VARCHAR(2000) NOT NULL");
    }

    public void testLocalTimeFunc()
    {
        getTester().setFor(SqlStdOperatorTable.localTimeFunc);
        getTester().checkScalar("LOCALTIME", timePattern, "TIME(0) NOT NULL");
        getTester().checkInvalid(
            "^LOCALTIME()^",
            "No match found for function signature LOCALTIME\\(\\)");
        getTester().checkScalar("LOCALTIME(1)", timePattern,
            "TIME(1) NOT NULL");
    }

    public void testLocalTimestampFunc()
    {
        getTester().setFor(SqlStdOperatorTable.localTimestampFunc);
        getTester().checkScalar("LOCALTIMESTAMP", timestampPattern,
            "TIMESTAMP(0) NOT NULL");
        getTester().checkInvalid(
            "^LOCALTIMESTAMP()^",
            "No match found for function signature LOCALTIMESTAMP\\(\\)");
        getTester().checkInvalid(
            "LOCALTIMESTAMP(^4000000000^)",
             literalOutOfRangeMessage);
        getTester().checkScalar(
            "LOCALTIMESTAMP(1)",
            timestampPattern,
            "TIMESTAMP(1) NOT NULL");
    }

    public void testCurrentTimeFunc()
    {
        getTester().setFor(SqlStdOperatorTable.currentTimeFunc);
        getTester().checkScalar("CURRENT_TIME", timePattern,
            "TIME(0) NOT NULL");
        getTester().checkInvalid(
            "^CURRENT_TIME()^",
            "No match found for function signature CURRENT_TIME\\(\\)");
        getTester().checkScalar("CURRENT_TIME(1)", timePattern,
            "TIME(1) NOT NULL");
    }

    public void testCurrentTimestampFunc()
    {
        getTester().setFor(SqlStdOperatorTable.currentTimestampFunc);
        getTester().checkScalar("CURRENT_TIMESTAMP", timestampPattern,
            "TIMESTAMP(0) NOT NULL");
        getTester().checkInvalid(
            "^CURRENT_TIMESTAMP()^",
            "No match found for function signature CURRENT_TIMESTAMP\\(\\)");
        getTester().checkInvalid(
            "CURRENT_TIMESTAMP(^4000000000^)",
             literalOutOfRangeMessage);
        getTester().checkScalar("CURRENT_TIMESTAMP(1)", timestampPattern,
            "TIMESTAMP(1) NOT NULL");
    }

    public void testCurrentDateFunc()
    {
        getTester().setFor(SqlStdOperatorTable.currentDateFunc);
        getTester().checkScalar("CURRENT_DATE", datePattern, "DATE NOT NULL");
        getTester().checkInvalid(
            "^CURRENT_DATE()^",
            "No match found for function signature CURRENT_DATE\\(\\)");
    }

    public void testSubstringFunction()
    {
        getTester().setFor(SqlStdOperatorTable.substringFunc);
        getTester().checkString("substring('abc' from 1 for 2)", "ab", "VARCHAR(3) NOT NULL");
        getTester().checkString("substring('abc' from 2)", "bc", "todo: VARCHAR(3) NOT NULL");

        //substring reg exp not yet supported
        //                    getTester().checkString("substring('foobar' from '%#\"o_b#\"%' for '#')", "oob");
        getTester().checkNull("substring(cast(null as varchar(1)),1,2)");
    }

    public void testTrimFunc()
    {
        getTester().setFor(SqlStdOperatorTable.trimFunc);
        // SQL:2003 6.29.11 Trimming a CHAR yields a VARCHAR
        getTester().checkString("trim('a' from 'aAa')", "A", "todo: VARCHAR(3) NOT NULL");
        getTester().checkString("trim(both 'a' from 'aAa')", "A", "todo: VARCHAR(3) NOT NULL");
        getTester().checkString("trim(leading 'a' from 'aAa')", "Aa", "todo: VARCHAR(3) NOT NULL");
        getTester().checkString("trim(trailing 'a' from 'aAa')", "aA", "todo: VARCHAR(3) NOT NULL");
        getTester().checkNull("trim(cast(null as varchar(1)) from 'a')");
        getTester().checkNull("trim('a' from cast(null as varchar(1)))");

        if (issueFnl3Fixed) {
            // SQL:2003 6.29.9: trim string must have length=1.
            // Failure occurs at runtime.
            //
            // TODO: Change message to "Invalid argument\(s\) for 'TRIM' function",
            // The message should come from a resource file, and should still
            // have the SQL error code 22027.
            getTester().checkFails("trim('xy' from 'abcde')",
                "could not calculate results for the following row:" + NL +
                "\\[ 0 \\]" + NL +
                "Messages:" + NL +
                "\\[0\\]:PC=0 Code=22027 ");
            getTester().checkFails("trim('' from 'abcde')",
                "could not calculate results for the following row:" + NL +
                "\\[ 0 \\]" + NL +
                "Messages:" + NL +
                "\\[0\\]:PC=0 Code=22027 ");
        }
    }

    public void testWindow() {
        getTester().setFor(SqlStdOperatorTable.windowOperator);
        getTester().check(
            "select sum(1) over (order by x) from (select 1 as x, 2 as y from (values (true)))",
            new AbstractSqlTester.StringTypeChecker("INTEGER"),
            "1",
            0);
    }

    public void testElementFunc()
    {
        getTester().setFor(SqlStdOperatorTable.elementFunc);
        if (todo) {
            getTester().checkString("element(multiset['abc']))","abc", "todo:");
            getTester().checkNull("element(multiset[cast(null as integer)]))");
        }
    }

    public void testCardinalityFunc() {
        getTester().setFor(SqlStdOperatorTable.cardinalityFunc);
        if (todo) {
            getTester().checkScalarExact("cardinality(multiset[cast(null as integer),2]))","2");
        }
    }

    public void testMemberOfOperator() {
        getTester().setFor(SqlStdOperatorTable.memberOfOperator);
        if (todo) {
            getTester().checkBoolean("1 member of multiset[1]",Boolean.TRUE);
            getTester().checkBoolean("'2' member of multiset['1']",Boolean.FALSE);
            getTester().checkBoolean("cast(null as double) member of multiset[cast(null as double)]",Boolean.TRUE);
            getTester().checkBoolean("cast(null as double) member of multiset[1.1]",Boolean.FALSE);
            getTester().checkBoolean("1.1 member of multiset[cast(null as double)]",Boolean.FALSE);
        }
    }

    public void testCollectFunc()
    {
        getTester().setFor(SqlStdOperatorTable.collectFunc);
    }

    public void testFusionFunc()
    {
        getTester().setFor(SqlStdOperatorTable.fusionFunc);
    }

    public void testExtractFunc()
    {
        getTester().setFor(SqlStdOperatorTable.extractFunc);
    }

    public void testCeilFunc()
    {
        getTester().setFor(SqlStdOperatorTable.ceilFunc);
    }

    public void testFloorFunc()
    {
        getTester().setFor(SqlStdOperatorTable.floorFunc);
    }

    public void testDenseRankFunc()
    {
        getTester().setFor(SqlStdOperatorTable.denseRankFunc);
    }

    public void testPercentRankFunc()
    {
        getTester().setFor(SqlStdOperatorTable.percentRankFunc);
    }

    public void testRankFunc()
    {
        getTester().setFor(SqlStdOperatorTable.rankFunc);
    }

    public void testCumeDistFunc()
    {
        getTester().setFor(SqlStdOperatorTable.cumeDistFunc);
    }

    public void testRowNumberFunc()
    {
        getTester().setFor(SqlStdOperatorTable.rowNumberFunc);
    }

    public void testCountFunc()
    {
        getTester().setFor(SqlStdOperatorTable.countOperator);
        getTester().checkType("count(*)","BIGINT NOT NULL");
        getTester().checkType("count('name')","BIGINT NOT NULL");
        getTester().checkType("count(1)","BIGINT NOT NULL");
        getTester().checkType("count(1.2)","BIGINT NOT NULL");
        getTester().checkType("COUNT(DISTINCT 'x')","BIGINT NOT NULL");
        getTester().checkInvalid("^COUNT()^",
            "Invalid number of arguments to function 'COUNT'. Was expecting 1 arguments");
        getTester().checkInvalid("^COUNT(1, 2)^",
            "Invalid number of arguments to function 'COUNT'. Was expecting 1 arguments");
        final String[] values = {"0", "CAST(null AS INTEGER)", "1", "0"};
        getTester().checkAgg("COUNT(x)", values, new Integer(3), 0);
        getTester().checkAgg("COUNT(CASE x WHEN 0 THEN NULL ELSE -1 END)",
            values, new Integer(2), 0);
        getTester().checkAgg("COUNT(DISTINCT x)", values, new Integer(2), 0);
        // string values -- note that empty string is not null
        final String[] stringValues = {"'a'", "CAST(NULL AS VARCHAR(1))", "''"};
        getTester().checkAgg("COUNT(*)", stringValues, new Integer(3), 0);
        getTester().checkAgg("COUNT(x)", stringValues, new Integer(2), 0);
        getTester().checkAgg("COUNT(DISTINCT x)", stringValues, new Integer(2), 0);
        getTester().checkAgg("COUNT(DISTINCT 123)", stringValues, new Integer(1), 0);
   }

    public void testSumFunc()
    {
        getTester().setFor(SqlStdOperatorTable.sumOperator);
        getTester().checkInvalid("sum(^*^)",
            "Unknown identifier '\\*'");
        getTester().checkInvalid("^sum('name')^",
            "(?s)Cannot apply 'SUM' to arguments of type 'SUM\\(<CHAR\\(4\\)>\\)'\\. Supported form\\(s\\): 'SUM\\(<NUMERIC>\\)'.*");
        getTester().checkType("sum(1)","INTEGER");
        getTester().checkType("sum(1.2)","DECIMAL(2, 1)");
        getTester().checkType("sum(DISTINCT 1.5)","DECIMAL(2, 1)");
        getTester().checkInvalid("^sum()^",
            "Invalid number of arguments to function 'SUM'. Was expecting 1 arguments");
        getTester().checkInvalid("^sum(1, 2)^",
            "Invalid number of arguments to function 'SUM'. Was expecting 1 arguments");
        getTester().checkInvalid("^sum(cast(null as varchar(2)))^",
            "(?s)Cannot apply 'SUM' to arguments of type 'SUM\\(<VARCHAR\\(2\\)>\\)'\\. Supported form\\(s\\): 'SUM\\(<NUMERIC>\\)'.*");
        final String[] values = {"0", "CAST(null AS INTEGER)", "2", "2"};
        getTester().checkAgg("sum(x)", values, new Integer(4), 0);
        getTester().checkAgg("sum(CASE x WHEN 0 THEN NULL ELSE -1 END)",
            values, new Integer(-3), 0);
        getTester().checkAgg(
            "sum(DISTINCT CASE x WHEN 0 THEN NULL ELSE -1 END)",
            values, new Integer(-1), 0);
        getTester().checkAgg("sum(DISTINCT x)", values, new Integer(2), 0);
    }

    public void testAvgFunc()
    {
        getTester().setFor(SqlStdOperatorTable.avgOperator);
        getTester().checkInvalid("avg(^*^)",
            "Unknown identifier '\\*'");
        getTester().checkInvalid("^avg(cast(null as varchar(2)))^",
            "(?s)Cannot apply 'AVG' to arguments of type 'AVG\\(<VARCHAR\\(2\\)>\\)'\\. Supported form\\(s\\): 'AVG\\(<NUMERIC>\\)'.*");
        getTester().checkType("AVG(CAST(NULL AS INTEGER))", "INTEGER");
        getTester().checkType("AVG(DISTINCT 1.5)", "DECIMAL(2, 1) NOT NULL");
        final String[] values = {"0", "CAST(null AS INTEGER)", "3", "3"};
        getTester().checkAgg("AVG(x)", values, new Double(1), 0);
        getTester().checkAgg("AVG(DISTINCT x)", values, new Double(1.5), 0);
        getTester().checkAgg(
            "avg(DISTINCT CASE x WHEN 0 THEN NULL ELSE -1 END)",
            values, new Integer(-1), 0);
    }

    public void testLastValueFunc()
    {
        getTester().setFor(SqlStdOperatorTable.lastValueOperator);
        getTester().checkScalarExact("last_value(1)","1");
        getTester().checkScalarExact("last_value(1.2)","DECIMAL(2, 1) NOT NULL", "1.2");
        getTester().checkType("last_value('name')","CHAR(4) NOT NULL");
        getTester().checkString("last_value('name')","name","todo: CHAR(4) NOT NULL");
    }

    public void testFirstValueFunc()
    {
        getTester().setFor(SqlStdOperatorTable.firstValueOperator);
        getTester().checkScalarExact("first_value(1)","1");
        getTester().checkScalarExact("first_value(1.2)","DECIMAL(2, 1) NOT NULL", "1.2");
        getTester().checkType("first_value('name')","CHAR(4) NOT NULL");
        getTester().checkString("first_value('name')","name","todo: CHAR(4) NOT NULL");
    }
}

// End SqlOperatorTests.java

