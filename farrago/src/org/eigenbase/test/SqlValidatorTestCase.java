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

package org.eigenbase.test;

import junit.framework.AssertionFailedError;
import junit.framework.TestCase;
import org.eigenbase.reltype.RelDataType;
import org.eigenbase.reltype.RelDataTypeFactory;
import org.eigenbase.sql.*;
import org.eigenbase.sql.test.SqlOperatorTests;
import org.eigenbase.sql.test.SqlTester;
import org.eigenbase.sql.test.AbstractSqlTester;
import org.eigenbase.sql.fun.SqlStdOperatorTable;
import org.eigenbase.sql.parser.SqlParseException;
import org.eigenbase.sql.parser.SqlParser;
import org.eigenbase.sql.parser.SqlParserUtil;
import org.eigenbase.sql.type.SqlTypeFactoryImpl;
import org.eigenbase.sql.type.SqlTypeName;
import org.eigenbase.sql.validate.SqlValidator;
import org.eigenbase.sql.validate.SqlValidatorUtil;
import org.eigenbase.util.*;

import java.nio.charset.Charset;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * An abstract base class for implementing tests against
 * {@link SqlValidator}.
 *
 * <p>A derived class can refine this test in two ways. First, it can add
 * <code>testXxx()</code> methods, to test more functionality.
 *
 * <p>Second, it can override the {@link #getTester} method to return
 * a different implementation of the {@link Tester} object. This encapsulates
 * the differences between test environments, for example, which SQL parser or
 * validator to use.</p>
 *
 * @author Wael Chatila
 * @since Jan 12, 2004
 * @version $Id$
 **/
public class SqlValidatorTestCase extends TestCase
{
    protected final Tester tester = getTester();

    //~ Static fields/initializers --------------------------------------------

    protected static final String NL = System.getProperty("line.separator");

    private static final Pattern lineColPattern =
        Pattern.compile("At line ([0-9]+), column ([0-9]+)");

    private static final Pattern lineColTwicePattern =
        Pattern.compile("(?s)From line ([0-9]+), column ([0-9]+) to line ([0-9]+), column ([0-9]+): (.*)");

    /**
     * Whether to assert if a test doesn't specify the location of the error.
     * <p/>
     * todo: Set this to true, make all the tests succeed, then remove it.
     */
    private static final boolean FailIfNoPosition =
        SqlOperatorTests.bug315Fixed;

    private String buildQuery(String expression)
    {
        return "values (" + expression + ")";
    }

    /**
     * Encapsulates differences between test environments, for example, which
     * SQL parser or validator to use.
     *
     * <p>It contains a mock schema with <code>EMP</code> and <code>DEPT</code>
     * tables, which can run without having to start up Farrago.
     */
    public interface Tester
    {
        SqlNode parseQuery(String sql) throws SqlParseException;

        SqlValidator getValidator();

        /**
         * Checks that a query is valid, or, if invalid, throws the right
         * message at the right location.
         *
         * <p>If <code>expectedMsgPattern</code> is null, the query must succeed.
         *
         * <p>If <code>expectedMsgPattern</code> is not null, the query must
         * fail, and give an error location of (expectedLine, expectedColumn)
         * through (expectedEndLine, expectedEndColumn).
         *
         * @param sql
         * @param expectedMsgPattern If this parameter is null the query must
         *   be valid for the test to pass;
         *   If this parameter is not null the query must be malformed and the
         *   message given must match the pattern
         */
        void assertExceptionIsThrown(
            String sql,
            String expectedMsgPattern);

        /**
         * Returns the data type of the first column of a SQL query.
         * For example, <code>getResultType("VALUES (1, 'foo')")</code>
         * returns <code>INTEGER</code>.
         */
        RelDataType getResultType(String sql);

        void checkCollation(
            String sql,
            String expectedCollationName,
            SqlCollation.Coercibility expectedCoercibility);

        void checkCharset(
            String sql,
            Charset expectedCharset);

        /**
         * Checks that the first column of a query has the expected type.
         * For example,
         * <code>checkType(VALUES (1 + 2), "INTEGER NOT NULL")</code>.
         */
        void checkQueryType(
            String sql,
            String expected);

        /**
         * Checks if the interval value conversion to milliseconds is valid.
         * For example,
         * <code>checkIntervalConv(VALUES (INTERVAL '1' Minute), "60000")</code>.
         */
        void checkIntervalConv(
            String sql,
            String expected);
    }

    //~ Methods ---------------------------------------------------------------

    /**
     * Returns a tester. Derived classes should override this method to run
     * the same set of tests in a different testing environment.
     */
    public Tester getTester()
    {
        return new TesterImpl();
    }

    public void check(String sql)
    {
        tester.assertExceptionIsThrown(sql, null);
    }

    public void checkExp(String sql)
    {
        tester.assertExceptionIsThrown(buildQuery(sql), null);
    }

    /**
     * Checks that a SQL query gives a particular error, but without specifying
     * the location of that error.
     */
    public final void checkFails(
        String sql,
        String expected)
    {
        tester.assertExceptionIsThrown(sql, expected);
    }

    /**
     * Asserts that a query throws an exception matching a given pattern.
     *
     * @deprecated Switch to {@link #checkFails(String, String)}
     */
    public void checkFails(
        String sql,
        String expected,
        int line,
        int column)
    {
        tester.assertExceptionIsThrown(sql, expected);
    }

    /**
     * Checks that a SQL expression gives a particular error.
     */
    public final void checkExpFails(
        String sql,
        String expected)
    {
        tester.assertExceptionIsThrown(buildQuery(sql), expected);
    }

    /**
     * Checks that a SQL expression gives a particular error, and that the
     * location of the error is the whole expression.
     */
    public final void checkWholeExpFails(
        String sql,
        String expected)
    {
        assert sql.indexOf('^') < 0;
        checkExpFails("^" + sql + "^", expected);
    }

    public void checkExpType(
        String sql,
        String expected)
    {
        checkQueryType(buildQuery(sql), expected);
    }

    /**
     * Checks that the first column returned by a query has the expected type.
     * For example,
     * <blockquote><code>
     * checkQueryType("SELECT empno FROM Emp", "INTEGER NOT NULL");
     * </code></blockquote>
     *
     * @param sql Query
     * @param expected Expected type, including nullability
     */
    public void checkQueryType(
        String sql,
        String expected)
    {
        tester.checkQueryType(sql, expected);
    }

    /**
     * Checks that the first column returned by a query has the expected type.
     * For example,
     * <blockquote><code>
     * checkQueryType("SELECT empno FROM Emp", "INTEGER NOT NULL");
     * </code></blockquote>
     *
     * @param sql Query
     * @param expected Expected type, including nullability
     */
    public void checkIntervalConv(
        String sql,
        String expected)
    {
        tester.checkIntervalConv(buildQuery(sql), expected);
    }

    protected final void assertExceptionIsThrown(
        String sql,
        String expectedMsgPattern)
    {
        tester.assertExceptionIsThrown(sql, expectedMsgPattern);
    }

    public void checkCharset(
        String sql,
        Charset expectedCharset)
    {
        tester.checkCharset(sql, expectedCharset);
    }

    public void checkCollation(
        String sql,
        String expectedCollationName,
        SqlCollation.Coercibility expectedCoercibility)
    {
        tester.checkCollation(sql, expectedCollationName, expectedCoercibility);
    }

    protected Compatible getCompatible() {
        return Compatible.Default;
    }

    /**
     * Checks whether an exception matches the pattern and expected position
     * expected.
     *
     * @param ex Exception thrown
     * @param expectedMsgPattern Expected pattern
     * @param sap Query and position in query
     */
    public static void checkEx(
        Throwable ex,
        String expectedMsgPattern,
        SqlParserUtil.StringAndPos sap)
    {
        if (null == ex) {
            if (expectedMsgPattern == null) {
                // No error expected, and no error happened.
                return;
            } else {
                throw new AssertionFailedError(
                    "Expected query to throw exception, but it did not; " +
                    "query [" + sap.sql +
                    "]; expected [" + expectedMsgPattern + "]");
            }
        }
        Throwable actualException = ex;
        String actualMessage = actualException.getMessage();
        int actualLine = -1;
        int actualColumn = -1;
        int actualEndLine = 100;
        int actualEndColumn = 99;

        // Search for an EigenbaseContextException somewhere in the stack.
        EigenbaseContextException ece = null;
        for (Throwable x = ex; x != null; x = x.getCause()) {
            if (x instanceof EigenbaseContextException) {
                ece = (EigenbaseContextException) x;
                break;
            }
            if (x.getCause() == x) {
                break;
            }
        }

        // Search for a SqlParseException -- with its position set -- somewhere
        // in the stack.
        SqlParseException spe = null;
        for (Throwable x = ex; x != null; x = x.getCause()) {
            if (x instanceof SqlParseException &&
                ((SqlParseException) x).getPos() != null) {
                spe = (SqlParseException) x;
                break;
            }
            if (x.getCause() == x) {
                break;
            }
        }

        if (ece != null) {
            actualLine = ece.getPosLine();
            actualColumn = ece.getPosColumn();
            actualEndLine = ece.getEndPosLine();
            actualEndColumn = ece.getEndPosColumn();
            if (ece.getCause() != null) {
                actualException = ece.getCause();
                actualMessage = actualException.getMessage();
            }

        } else if (spe != null) {
            actualLine = spe.getPos().getLineNum();
            actualColumn = spe.getPos().getColumnNum();
            actualEndLine = spe.getPos().getEndLineNum();
            actualEndColumn = spe.getPos().getEndColumnNum();
            if (spe.getCause() != null) {
                actualException = spe.getCause();
                actualMessage = actualException.getMessage();
            }
        } else {
            final String message = ex.getMessage();
            if (message != null) {
                Matcher matcher = lineColTwicePattern.matcher(message);
                if (matcher.matches()) {
                    actualLine = Integer.parseInt(matcher.group(1));
                    actualColumn = Integer.parseInt(matcher.group(2));
                    actualEndLine = Integer.parseInt(matcher.group(3));
                    actualEndColumn = Integer.parseInt(matcher.group(4));
                    actualMessage = matcher.group(5);
                } else {
                    matcher = lineColPattern.matcher(message);
                    if (matcher.matches()) {
                        actualLine = Integer.parseInt(matcher.group(1));
                        actualColumn = Integer.parseInt(matcher.group(2));
                    }
                }
            }
        }

        if (null == expectedMsgPattern) {
            if (null != actualException) {
                actualException.printStackTrace();
                fail("SqlValidationTest: Validator threw unexpected exception" +
                    "; query [" + sap.sql +
                    "]; exception [" + actualMessage +
                    "]; pos [line " + actualLine +
                    " col " + actualColumn +
                    " thru line " + actualLine +
                    " col " + actualColumn + "]");
            }
        } else if (null != expectedMsgPattern) {
            if (null == actualException) {
                fail("SqlValidationTest: Expected validator to throw " +
                    "exception, but it did not; query [" + sap.sql +
                    "]; expected [" + expectedMsgPattern + "]");
            } else {
                String sqlWithCarets;
                if (actualColumn <= 0 ||
                    actualLine <= 0 ||
                    actualEndColumn <= 0 ||
                    actualEndLine <= 0) {
                    if (FailIfNoPosition) {
                        throw new AssertionFailedError(
                            "Error did not have position: " +
                            " actual pos [line " + actualLine +
                            " col " + actualColumn +
                            " thru line " + actualEndLine +
                            " col " + actualEndColumn + "]");
                    }
                    sqlWithCarets = sap.sql;
                } else {
                    sqlWithCarets =
                        SqlParserUtil.addCarets(
                            sap.sql, actualLine, actualColumn,
                            actualEndLine, actualEndColumn);
                }
                if (FailIfNoPosition && sap.pos == null) {
                    throw new AssertionFailedError(
                        "todo: add carets to sql: " + sqlWithCarets);
                }
                if (actualMessage == null ||
                    !actualMessage.matches(expectedMsgPattern)) {
                    actualException.printStackTrace();
                    final String actualJavaRegexp =
                        TestUtil.quoteForJava(
                            TestUtil.quotePattern(actualMessage));
                    fail("SqlValidationTest: Validator threw different " +
                        "exception than expected; query [" + sap.sql +
                        "];" + NL +
                        " expected pattern [" + expectedMsgPattern +
                        "];" + NL +
                        " actual [" + actualMessage +
                        "];" + NL +
                        " actual as java regexp [" + actualJavaRegexp +
                        "]; pos [" + actualLine +
                        " col " + actualColumn +
                        " thru line " + actualEndLine +
                        " col " + actualEndColumn +
                        "]; sql [" + sqlWithCarets + "]");
                } else if (sap.pos != null &&
                    (actualLine != sap.pos.getLineNum() ||
                    actualColumn != sap.pos.getColumnNum() ||
                    actualEndLine != sap.pos.getEndLineNum() ||
                    actualEndColumn != sap.pos.getEndColumnNum())) {
                    fail("SqlValidationTest: Validator threw expected " +
                        "exception [" + actualMessage +
                        "]; but at pos [line " + actualLine +
                        " col " + actualColumn +
                        " thru line " + actualEndLine +
                        " col " + actualEndColumn +
                        "]; sql [" + sqlWithCarets + "]");
                }
            }
        }
    }

    /**
     * Implementation of {@link org.eigenbase.test.SqlValidatorTestCase.Tester}
     * which talks to a mock catalog.
     *
     * <p>It is also a pure-Java implementation of the {@link SqlTester}
     * used by {@link SqlOperatorTests}. It can parse and validate queries,
     * but it does not invoke Farrago, so it is very fast but cannot execute
     * functions.
     */
    public class TesterImpl implements Tester, SqlTester
    {

        public SqlValidator getValidator()
        {
            final RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl();
            return SqlValidatorUtil.newValidator(SqlStdOperatorTable.instance(),
                new MockCatalogReader(typeFactory),
                typeFactory);
        }

        public void assertExceptionIsThrown(
            String sql,
            String expectedMsgPattern)
        {
            SqlValidator validator;
            SqlNode sqlNode;
            SqlParserUtil.StringAndPos sap = SqlParserUtil.findPos(sql);
            try {
                sqlNode = parseQuery(sap.sql);
                validator = getValidator();
            } catch (SqlParseException ex) {
                String errMessage = ex.getMessage();
                if (null == errMessage ||
                    expectedMsgPattern == null ||
                    !errMessage.matches(expectedMsgPattern)) {
                    ex.printStackTrace();
                    fail("SqlValidationTest: Parse Error while parsing query="
                        + sap.sql + "\n" + errMessage);
                }
                return;
            } catch (Throwable e) {
                e.printStackTrace(System.err);
                fail("SqlValidationTest: Failed while trying to connect or " +
                    "get statement");
                return;
            }

            Throwable thrown = null;
            try {
                validator.validate(sqlNode);
            } catch (Throwable ex) {
                thrown = ex;
            }

            checkEx(thrown, expectedMsgPattern, sap);
        }

        public RelDataType getResultType(String sql)
        {
            SqlValidator validator = getValidator();
            SqlNode n = parseAndValidate(validator, sql);

            RelDataType rowType = validator.getValidatedNodeType(n);
            RelDataType actualType = rowType.getFields()[0].getType();
            return actualType;
        }

        protected SqlNode parseAndValidate(SqlValidator validator, String sql)
        {
            SqlNode sqlNode;
            try {
                sqlNode = parseQuery(sql);
            } catch (SqlParseException ex) {
                ex.printStackTrace();
                throw new AssertionFailedError("SqlValidationTest: " +
                    "Parse Error while parsing query=" + sql
                    + "\n" + ex.getMessage());
            } catch (Throwable e) {
                e.printStackTrace(System.err);
                throw new AssertionFailedError("SqlValidationTest: " +
                    "Failed while trying to connect or get statement");
            }
            SqlNode n = validator.validate(sqlNode);
            return n;
        }

        public SqlNode parseQuery(String sql) throws SqlParseException
        {
            SqlParser parser = new SqlParser(sql);
            SqlNode sqlNode = parser.parseQuery();
            return sqlNode;
        }

        public void checkQueryType(String sql, String expected)
        {
            RelDataType actualType = getResultType(sql);
            if (expected.startsWith("todo:")) {
                Util.permAssert(
                    !SqlOperatorTests.bug315Fixed,
                    "After bug 315 is fixed, no type should start 'todo:'");
                return; // don't check the type for now
            }
            String actual = getTypeString(actualType);
            assertEquals(expected, actual);
        }

        public void checkIntervalConv(String sql, String expected)
        {
            SqlValidator validator = getValidator();
            SqlNode n = parseAndValidate(validator, sql);

            SqlNode node = null;
            for (int i = 0; i < ((SqlSelect) n).getOperands().length; i++) {
                node = ((SqlSelect) n).getOperands()[i];
                if (node instanceof SqlCall) {
                    if (node.isA(SqlKind.As)) {
                        node = ((SqlCall) node).operands[0];
                    }
                    node = ((SqlCall) ((SqlCall) node).getOperands()[0]).getOperands()[0];
                    break;
                }
            }

            long l = SqlParserUtil.intervalToMillis((SqlIntervalLiteral.IntervalValue)
                    ((SqlIntervalLiteral) node).getValue());
            String actual = l + "";
            assertEquals(expected, actual);
        }

        public void checkType(String expression, String type)
        {
            checkQueryType(buildQuery(expression), type);
        }

        private String getTypeString(RelDataType sqlType)
        {
            switch (sqlType.getSqlTypeName().getOrdinal()) {
            case SqlTypeName.Varchar_ordinal:
                String actual = "VARCHAR(" + sqlType.getPrecision() + ")";
                return sqlType.isNullable() ?
                    actual :
                    actual + " NOT NULL";
            case SqlTypeName.Char_ordinal:
                actual = "CHAR(" + sqlType.getPrecision() + ")";
                return sqlType.isNullable() ?
                    actual :
                    actual + " NOT NULL";
            default:
                return sqlType.getFullTypeString();
            }
        }

        public void checkCollation(String sql,
            String expectedCollationName,
            SqlCollation.Coercibility expectedCoercibility)
        {
            RelDataType actualType = getResultType(buildQuery(sql));
            SqlCollation collation = actualType.getCollation();

            String actualName = collation.getCollationName();
            int actualCoercibility = collation.getCoercibility().getOrdinal();
            int expectedCoercibilityOrd = expectedCoercibility.getOrdinal();
            assertEquals(expectedCollationName, actualName);
            assertEquals(expectedCoercibilityOrd, actualCoercibility);
        }

        public void checkCharset(String sql,
            Charset expectedCharset)
        {
            RelDataType actualType = tester.getResultType(buildQuery(sql));
            Charset actualCharset = actualType.getCharset();

            if (!expectedCharset.equals(actualCharset)) {
                fail(NL + "Expected=" + expectedCharset.name() + NL +
                    "  actual=" + actualCharset.name());
            }
        }

        // SqlTester methods

        public void setFor(SqlOperator operator)
        {
            // do nothing
        }

        public void checkAgg(
            String expr,
            String[] inputValues,
            Object result,
            int delta)
        {
            String query = AbstractSqlTester.generateAggQuery(expr, inputValues);
            check(query, AbstractSqlTester.AnyTypeChecker, result, delta);
        }

        public void checkScalar(
            String expression,
            Object result,
            String resultType)
        {
            checkType(expression, resultType);
            check(buildQuery(expression), AbstractSqlTester.AnyTypeChecker, result, 0);
        }

        public void checkScalarExact(
            String expression,
            String result)
        {
            String sql = buildQuery(expression);
            check(sql, AbstractSqlTester.IntegerTypeChecker, result, 0);
        }

        public void checkScalarExact(
            String expression,
            String expectedType,
            String result)
        {
            String sql = buildQuery(expression);
            TypeChecker typeChecker = new AbstractSqlTester.StringTypeChecker(expectedType);
            check(sql, typeChecker, result, 0);
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
                AbstractSqlTester.AnyTypeChecker :
                new AbstractSqlTester.StringTypeChecker(expectedType);
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
                    AbstractSqlTester.BooleanTypeChecker, result.toString(),
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
                AbstractSqlTester.AnyTypeChecker :
                new AbstractSqlTester.StringTypeChecker(expectedType);
            check(sql, typeChecker, result, 0);
        }

        public void checkNull(String expression)
        {
            String sql = buildQuery(expression);
            check(sql, AbstractSqlTester.AnyTypeChecker, null, 0);
        }

        public void check(
            String query,
            TypeChecker typeChecker,
            Object result,
            double delta)
        {
            // This implementation does NOT check the result!
            // (It can't because we're pure Java.)
            // All it does is check the return type.

            // Parse and validate. There should be no errors.
            RelDataType actualType = getResultType(query);

            // Check result type.
            typeChecker.checkType(actualType);
        }

        public void checkInvalid(String expression, String expectedError)
        {
            // After bug 315 is fixed, take this assert out: the other assert
            // will be sufficient.
            if (!SqlOperatorTests.bug315Fixed) {
                assertTrue(
                    "All negative tests must contain an error location",
                    expression.indexOf('^') >= 0);
            }
            SqlValidatorTestCase.this.checkFails(
                buildQuery(expression),
                expectedError);
        }

        public void checkFails(String expression, String expectedError)
        {
            // We need to test that the expression fails at runtime.
            // Ironically, that means that is must succeed at prepare time.
            SqlValidator validator = getValidator();
            final String sql = buildQuery(expression);
            SqlNode n = parseAndValidate(validator, sql);
            assertNotNull(n);
        }
    }

    /**
     * Describes the valid SQL compatiblity modes.
     */
    public static class Compatible extends EnumeratedValues.BasicValue {
        private Compatible(String name, int ordinal) {
            super(name, ordinal, null);
        }

        public static final int Default_ordinal = 0;
        public static final int Strict92_ordinal = 1;
        public static final int Strict99_ordinal = 2;
        public static final int Pragmatic99_ordinal = 3;
        public static final int Oracle10g_ordinal = 4;
        public static final int Sql2003_ordinal = 5;

        public static final SqlValidatorTest.Compatible Strict92 =
            new SqlValidatorTest.Compatible("Strict92", Strict92_ordinal);
        public static final SqlValidatorTest.Compatible Strict99 =
            new SqlValidatorTest.Compatible("Strict99", Strict99_ordinal);
        public static final SqlValidatorTest.Compatible Pragmatic99 =
            new SqlValidatorTest.Compatible("Pragmatic99", Pragmatic99_ordinal);
        public static final SqlValidatorTest.Compatible Oracle10g =
            new SqlValidatorTest.Compatible("Oracle10g", Oracle10g_ordinal);
        public static final SqlValidatorTest.Compatible Sql2003 =
            new SqlValidatorTest.Compatible("Sql2003", Sql2003_ordinal);
        public static final SqlValidatorTest.Compatible Default =
            new SqlValidatorTest.Compatible("Default", Default_ordinal);
    }
}



// End SqlValidatorTestCase.java
