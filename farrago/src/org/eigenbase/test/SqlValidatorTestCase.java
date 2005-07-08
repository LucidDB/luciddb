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
import org.eigenbase.sql.SqlCollation;
import org.eigenbase.sql.SqlNode;
import org.eigenbase.sql.test.SqlOperatorTests;
import org.eigenbase.sql.fun.SqlStdOperatorTable;
import org.eigenbase.sql.parser.SqlParseException;
import org.eigenbase.sql.parser.SqlParser;
import org.eigenbase.sql.parser.SqlParserUtil;
import org.eigenbase.sql.type.SqlTypeFactoryImpl;
import org.eigenbase.sql.type.SqlTypeName;
import org.eigenbase.sql.validate.SqlValidator;
import org.eigenbase.sql.validate.SqlValidatorUtil;
import org.eigenbase.util.EigenbaseContextException;
import org.eigenbase.util.EnumeratedValues;
import org.eigenbase.util.Util;

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

    /**
     * Encapsulates differences between test environments, for example, which
     * SQL parser or validator to use.
     *
     * <p>It contains a mock schema with <code>EMP</code> and <code>DEPT</code>
     * tables, which can run without having to start up Farrago.
     */
    public interface Tester {
        SqlNode parseQuery(String sql) throws SqlParseException;
        SqlValidator getValidator();
        /**
         * Asserts either if a sql query is valid or not.
         * @param sql
         * @param expectedMsgPattern If this parameter is null the query must be
         *   valid for the test to pass;
         *   If this parameter is not null the query must be malformed and the msg
         * @param expectedEndLine
         * @param expectedEndColumn
         */
        void assertExceptionIsThrown(String sql,
            String expectedMsgPattern,
            int expectedLine,
            int expectedColumn, int expectedEndLine, int expectedEndColumn);

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
         * For example, <code>checkType(VALUUES (1 + 2), "INTEGER")</code>.
         */
        void checkType(
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
        tester.assertExceptionIsThrown(sql, null, -1, -1, -1, -1);
    }

    public void checkExp(String sql)
    {
        sql = "select " + sql + " from (values(true))";
        tester.assertExceptionIsThrown(sql, null, -1, -1, -1, -1);
    }

    /**
     * Checks that a SQL query gives a particular error, but without specifying
     * the location of that error.
     */
    public final void checkFails(
        String sql,
        String expected)
    {
        SqlParserUtil.StringAndPos sap = SqlParserUtil.findPos(sql);
        if (sap.pos == null) {
            tester.assertExceptionIsThrown(
                sap.sql,
                expected,
                -1, -1, -1, -1);
        } else {
            tester.assertExceptionIsThrown(
                sap.sql,
                expected,
                sap.pos.getLineNum(), sap.pos.getColumnNum(),
                sap.pos.getEndLineNum(), sap.pos.getEndColumnNum());
        }
    }

    /**
     * Asserts that a query throws an exception matching a given pattern.
     */
    public void checkFails(
        String sql,
        String expected,
        int line,
        int column)
    {
        tester.assertExceptionIsThrown(sql, expected, line, column, -1, -1);
    }

    /**
     * Checks that a SQL expression gives a particular error, but without
     * specifying the location of that error.
     */
    public final void checkExpFails(
        String sql,
        String expected)
    {
        SqlParserUtil.StringAndPos sap = SqlParserUtil.findPos(sql);
        if (sap.pos == null) {
            tester.assertExceptionIsThrown(
                "select " + sap.sql + " from (values(true))",
                expected,
                -1, -1, -1, -1);
        } else {
            tester.assertExceptionIsThrown(
                "select " + sap.sql + " from (values(true))",
                expected,
                sap.pos.getLineNum(), sap.pos.getColumnNum() + 7,
                sap.pos.getEndLineNum(), sap.pos.getEndColumnNum() + 7);
        }
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

    /**
     * @deprecated Use {@link #checkExpFails(String, String)}
     */
    public void checkExpFails(
        String sql,
        String expected,
        int line,
        int column)
    {
        sql = "select " + sql + " from (values(true))";
        tester.assertExceptionIsThrown(sql, expected, line,
            column == -1 ? -1 : column + 7, -1, -1);
    }

    public void checkExpType(
        String sql,
        String expected)
    {
        sql = "select " + sql + " from (values(true))";
        checkType(sql, expected);
    }

    public void checkType(
        String sql,
        String expected)
    {
        tester.checkType(sql, expected);
    }

    protected final void assertExceptionIsThrown(
        String sql,
        String expectedMsgPattern)
    {
        tester.assertExceptionIsThrown(sql, expectedMsgPattern, -1, -1, -1, -1);
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
     * Implementation of {@link org.eigenbase.test.SqlValidatorTestCase.Tester}
     * which talks to a mock catalog.
     */
    public class TesterImpl implements Tester
    {
        private final Pattern lineColPattern =
            Pattern.compile("At line (.*), column (.*)");

        /**
         * Whether to assert if a test doesn't specify the location of the
         * error.
         *
         * todo: Set this to true, make all the tests succeed, then remove it.
         */
        private static final boolean FailIfNoPosition = false;

        public SqlValidator getValidator()
        {
            final RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl();
            return SqlValidatorUtil.newValidator(SqlStdOperatorTable.instance(),
                new MockCatalogReader(typeFactory),
                typeFactory);
        }

        /**
         * Asserts either if a sql query is valid or not.
         *
         * @param sql
         * @param expectedMsgPattern If this parameter is null the query must be
         * valid for the test to pass;
         * If this parameter is not null the query must be malformed and the msg
         * @param expectedEndLine
         * @param expectedEndColumn
         */
        public void assertExceptionIsThrown(
            String sql,
            String expectedMsgPattern,
            int expectedLine,
            int expectedColumn,
            int expectedEndLine,
            int expectedEndColumn)
        {
            SqlValidator validator;
            SqlNode sqlNode;
            try {
                sqlNode = parseQuery(sql);
                validator = getValidator();
            } catch (SqlParseException ex) {
                String errMessage = ex.getMessage();
                if (null == errMessage ||
                    expectedMsgPattern == null ||
                    !errMessage.matches(expectedMsgPattern)) {
                    ex.printStackTrace();
                    fail("SqlValidationTest: Parse Error while parsing query="
                        + sql + "\n" + errMessage);
                }
                return;
            } catch (Throwable e) {
                e.printStackTrace(System.err);
                fail("SqlValidationTest: Failed while trying to connect or " +
                    "get statement");
                return;
            }

            Throwable actualException = null;
            int actualLine = -1;
            int actualColumn = -1;
            int actualEndLine = 100;
            int actualEndColumn = 99;
            try {
                validator.validate(sqlNode);
            } catch (EigenbaseContextException ex) {
                actualLine = ex.getPosLine();
                actualColumn = ex.getPosColumn();
                actualEndLine = ex.getEndPosLine();
                actualEndColumn = ex.getEndPosColumn();
                actualException = ex.getCause() == null ?
                    ex :
                    ex.getCause();
            } catch (Throwable ex) {
                final String message = ex.getMessage();
                final Matcher matcher = lineColPattern.matcher(message);
                if (message != null && matcher.matches()) {
                    actualException = ex.getCause();
                    actualLine = Integer.parseInt(matcher.group(1));
                    actualColumn = Integer.parseInt(matcher.group(2));
                } else {
                    actualException = ex;
                }
            }

            if (null == expectedMsgPattern) {
                if ((null != actualException)) {
                    actualException.printStackTrace();
                    String actualMessage = actualException.getMessage();
                    fail("SqlValidationTest: Validator threw unexpected exception" +
                        "; query [" + sql +
                        "]; exception [" + actualMessage +
                        "]; line [" + actualLine +
                        "]; column [" + actualColumn + "]");
                }
            } else if (null != expectedMsgPattern) {
                if (null == actualException) {
                    fail("SqlValidationTest: Expected validator to throw " +
                        "exception, but it did not; query [" + sql +
                        "]; expected [" + expectedMsgPattern + "]");
                } else {
                    String sqlWithCarets;
                    if (actualColumn <= 0 ||
                        actualLine <= 0 ||
                        actualEndColumn <= 0 ||
                        actualEndLine <= 0) {
                        if (FailIfNoPosition) {
                            assert false :
                                "Error did not have position: " +
                                " actualLine=" + actualLine +
                                " actualColumn=" + actualColumn +
                                " actualEndLine=" + actualEndLine +
                                " actualEndColumn=" + actualEndColumn;
                        }
                        sqlWithCarets = sql;
                    } else {
                        sqlWithCarets =
                            SqlParserUtil.addCarets(
                                sql, actualLine, actualColumn, actualEndLine,
                                actualEndColumn);
                    }
                    if (FailIfNoPosition &&
                        (expectedLine == -1 ||
                        expectedColumn == -1 ||
                        expectedEndLine == -1 ||
                        expectedEndColumn == -1)) {
                        assert false :
                            "todo: add carets to sql: " + sqlWithCarets;
                    }
                    String actualMessage = actualException.getMessage();
                    if (actualMessage == null ||
                        !actualMessage.matches(expectedMsgPattern)) {
                        actualException.printStackTrace();
                        fail("SqlValidationTest: Validator threw different " +
                            "exception than expected; query [" + sql +
                            "]; expected [" + expectedMsgPattern +
                            "]; actual [" + actualMessage +
                            "]; line [" + actualLine +
                            "]; column [" + actualColumn + "]");
                    } else if ((expectedLine != -1 &&
                        actualLine != expectedLine) ||
                        (expectedColumn != -1 &&
                        actualColumn != expectedColumn)) {
                        fail("SqlValidationTest: Validator threw expected " +
                            "exception [" + actualMessage +
                            "]; but at line [" + actualLine +
                            "]; column [" + actualColumn +
                            "]; sql [" + sqlWithCarets + "]");
                    }
                }
            }
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

        public void checkType(
            String sql,
            String expected)
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
            sql = "select " + sql + " from (values(true))";
            RelDataType actualType = getResultType(sql);
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
            sql = "select " + sql + " from (values(true))";
            RelDataType actualType = tester.getResultType(sql);
            Charset actualCharset = actualType.getCharset();

            if (!expectedCharset.equals(actualCharset)) {
                fail(NL + "Expected=" + expectedCharset.name() + NL +
                    "  actual=" + actualCharset.name());
            }
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
