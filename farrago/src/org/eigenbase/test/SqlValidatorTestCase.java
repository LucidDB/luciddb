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

package org.eigenbase.test;

import junit.framework.TestCase;
import org.eigenbase.reltype.RelDataType;
import org.eigenbase.reltype.RelDataTypeFactory;
import org.eigenbase.reltype.RelDataTypeFactoryImpl;
import org.eigenbase.sql.*;
import org.eigenbase.sql.type.SqlTypeName;
import org.eigenbase.sql.parser.SqlParser;
import org.eigenbase.sql.parser.ParseException;
import org.eigenbase.util.EnumeratedValues;

import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.ArrayList;
import java.util.regex.Pattern;
import java.util.regex.Matcher;

/**
 * An abstract base class for implementing tests against
 * {@link org.eigenbase.sql.SqlValidator} and derived classes.
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
        SqlValidator getValidator();
        SqlParser getParser(String sql);
        /**
         * Asserts either if a sql query is valid or not.
         * @param sql
         * @param expectedMsgPattern If this parameter is null the query must be
         *   valid for the test to pass;
         *   If this parameter is not null the query must be malformed and the msg
         *   pattern must match the the error raised for the test to pass.
         */
        void assertExceptionIsThrown(
            String sql,
            String expectedMsgPattern,
            int expectedLine,
            int expectedColumn);

        /**
         * Returns the data type of a scalar SQL expression.
         */
        RelDataType getResultType(String sql);

        void checkCollation(
            String sql,
            String expectedCollationName,
            SqlCollation.Coercibility expectedCoercibility);

        void checkCharset(
            String sql,
            Charset expectedCharset);

        void checkType(
            String sql,
            String expected);
    }

    //~ Methods ---------------------------------------------------------------

    /**
     * Returns a tester. Derived classes should override this method to run
     * the same set of tests in a different testing environment.
     */
    public Tester getTester() {
        return new TesterImpl();
    }

    public void check(String sql)
    {
        tester.assertExceptionIsThrown(sql, null, -1, -1);
    }

    public void checkExp(String sql)
    {
        sql = "select " + sql + " from values(true)";
        tester.assertExceptionIsThrown(sql, null, -1, -1);
    }

    public final void checkFails(
        String sql,
        String expected)
    {
        tester.assertExceptionIsThrown(sql, expected, -1, -1);
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
        tester.assertExceptionIsThrown(sql, expected, line, column);
    }

    public final void checkExpFails(
        String sql,
        String expected)
    {
        checkExpFails(sql, expected, -1, -1);
    }

    public final void checkExpFails(String sql)
    {
        checkExpFails(sql, "(?s).*");
    }

    public void checkExpFails(
        String sql,
        String expected,
        int line,
        int column)
    {
        sql = "select " + sql + " from values(true)";
        tester.assertExceptionIsThrown(sql, expected, line, column);
    }

    public void checkExpType(
        String sql,
        String expected)
    {
        sql = "select " + sql + " from values(true)";
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
        tester.assertExceptionIsThrown(sql, expectedMsgPattern, -1, -1);
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
     * Mock implmenentation of {@link org.eigenbase.sql.SqlValidator.CatalogReader} which returns
     * tables "EMP", "DEPT", "BONUS", "SALGRADE" (same as Oracle's SCOTT
     * schema).
     */
    public class MockCatalogReader implements SqlValidator.CatalogReader
    {
        private final RelDataTypeFactory typeFactory;
        private final HashMap tables = new HashMap();

        public MockCatalogReader(RelDataTypeFactory typeFactory) {
            this.typeFactory = typeFactory;
            // Register "EMP" table.
            SqlValidatorTest.MockTable empTable = new SqlValidatorTest.MockTable("EMP");
            empTable.addColumn("EMPNO",
                typeFactory.createSqlType(SqlTypeName.Integer));
            empTable.addColumn("ENAME",
                typeFactory.createSqlType(SqlTypeName.Varchar, 20));
            empTable.addColumn("JOB",
                typeFactory.createSqlType(SqlTypeName.Varchar, 10));
            empTable.addColumn("MGR",
                typeFactory.createSqlType(SqlTypeName.Integer));
            empTable.addColumn("HIREDATE",
                typeFactory.createSqlType(SqlTypeName.Date));
            empTable.addColumn("SAL",
                typeFactory.createSqlType(SqlTypeName.Integer));
            empTable.addColumn("COMM",
                typeFactory.createSqlType(SqlTypeName.Integer));
            empTable.addColumn("DEPTNO",
                typeFactory.createSqlType(SqlTypeName.Integer));
            registerTable(empTable);
            // Register "DEPT" table.
            SqlValidatorTest.MockTable deptTable = new SqlValidatorTest.MockTable("DEPT");
            deptTable.addColumn("DEPTNO",
                typeFactory.createSqlType(SqlTypeName.Integer));
            deptTable.addColumn("NAME",
                typeFactory.createSqlType(SqlTypeName.Varchar, 10));
            registerTable(deptTable);
            // Register "BONUS" table.
            SqlValidatorTest.MockTable bonusTable = new SqlValidatorTest.MockTable("BONUS");
            bonusTable.addColumn("ENAME",
                typeFactory.createSqlType(SqlTypeName.Varchar, 20));
            bonusTable.addColumn("JOB",
                typeFactory.createSqlType(SqlTypeName.Varchar, 10));
            bonusTable.addColumn("SAL",
                typeFactory.createSqlType(SqlTypeName.Integer));
            bonusTable.addColumn("COMM",
                typeFactory.createSqlType(SqlTypeName.Integer));
            registerTable(bonusTable);
            // Register "SALGRADE" table.
            SqlValidatorTest.MockTable salgradeTable = new SqlValidatorTest.MockTable("SALGRADE");
            salgradeTable.addColumn("GRADE",
                typeFactory.createSqlType(SqlTypeName.Integer));
            salgradeTable.addColumn("LOSAL",
                typeFactory.createSqlType(SqlTypeName.Integer));
            salgradeTable.addColumn("HISAL",
                typeFactory.createSqlType(SqlTypeName.Integer));
            registerTable(salgradeTable);
        }

        private void registerTable(SqlValidatorTest.MockTable table) {
            table.onRegister(typeFactory);
            tables.put(table.names[0], table);
        }

        public SqlValidator.Table getTable(final String [] names)
        {
            if (names.length == 1) {
                return (SqlValidator.Table) tables.get(names[0]);
            }
            return null;
        }
    }

    /**
     * Mock implementation of {@link org.eigenbase.sql.SqlValidator.Table}.
     */
    public static class MockTable implements SqlValidator.Table
    {
        private final ArrayList columnNames = new ArrayList();
        private final ArrayList columnTypes = new ArrayList();
        private RelDataType rowType;
        private final String[] names;

        public MockTable(String name) {
            this.names = new String[] {name};
        }

        public RelDataType getRowType() {
            return rowType;
        }

        public void onRegister(RelDataTypeFactory typeFactory) {
            rowType = typeFactory.createProjectType(
                (RelDataType []) columnTypes.toArray(new RelDataType[0]),
                (String []) columnNames.toArray(new String[0]));
        }

        public String[] getQualifiedName() {
            return names;
        }

        public void addColumn(String name, RelDataType type) {
            columnNames.add(name);
            columnTypes.add(type);
        }
    }

    /**
     * Implementation of {@link org.eigenbase.test.SqlValidatorTestCase.Tester}
     * which talks to a mock catalog.
     */
    public class TesterImpl implements Tester {
        private final Pattern lineColPattern =
            Pattern.compile("At line (.*), column (.*)");

        public SqlValidator getValidator() {
            final RelDataTypeFactory typeFactory = new RelDataTypeFactoryImpl();
            return new SqlValidator(
                SqlOperatorTable.instance(),
                new MockCatalogReader(typeFactory),
                typeFactory);
        }

        public SqlParser getParser(String sql) {
            return new SqlParser(sql);
        }

        /**
         * Asserts either if a sql query is valid or not.
         * @param sql
         * @param expectedMsgPattern If this parameter is null the query must be
         *   valid for the test to pass;
         *   If this parameter is not null the query must be malformed and the msg
         *   pattern must match the the error raised for the test to pass.
         */
        public void assertExceptionIsThrown(
            String sql,
            String expectedMsgPattern,
            int expectedLine,
            int expectedColumn)
        {
            SqlParser parser;
            SqlValidator validator;
            SqlNode sqlNode;
            try {
                parser = getParser(sql);
                sqlNode = parser.parseQuery();
                validator = getValidator();
            } catch (ParseException ex) {
                ex.printStackTrace();
                fail("SqlValidationTest: Parse Error while parsing query=" + sql
                    + "\n" + ex.getMessage());
                return;
            } catch (Throwable e) {
                e.printStackTrace(System.err);
                fail(
                    "SqlValidationTest: Failed while trying to connect or get statement");
                return;
            }

            Throwable actualException = null;
            int actualLine = -1;
            int actualColumn = -1;
            try {
                validator.validate(sqlNode);
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
                            "]; column [" + actualColumn + "]");
                    }
                }
            }
        }

        public RelDataType getResultType(String sql) {
            SqlParser parser;
            SqlValidator validator;
            SqlNode sqlNode;
            try {
                parser = getParser(sql);
                sqlNode = parser.parseQuery();
                validator = getValidator();
            } catch (ParseException ex) {
                ex.printStackTrace();
                fail("SqlValidationTest: Parse Error while parsing query=" + sql
                    + "\n" + ex.getMessage());
                return null;
            } catch (Throwable e) {
                e.printStackTrace(System.err);
                fail(
                    "SqlValidationTest: Failed while trying to connect or get statement");
                return null;
            }
            SqlNode n = validator.validate(sqlNode);

            RelDataType actualType;
            if (false) {
                actualType = validator.getValidatedNodeType(
                    ((SqlNodeList) ((SqlCall) n).getOperands()[1]).get(0));
            } else {
                final RelDataType rowType = validator.getValidatedNodeType(n);
                actualType = rowType.getFields()[0].getType();
            }
            return actualType;
        }

        public void checkType(
            String sql,
            String expected)
        {
            RelDataType actualType = getResultType(sql);
            String actual = actualType.toString();
            // REVIEW (jhyde, 2004/8/4): Why not use assertEquals
            if (!expected.equals(actual)) {
                String msg =
                    NL + "Expected=" + expected + NL + "   actual=" + actual;
                fail(msg);
            }
        }

        public void checkCollation(
            String sql,
            String expectedCollationName,
            SqlCollation.Coercibility expectedCoercibility)
        {
            sql = "select " + sql + " from values(true)";
            RelDataType actualType = getResultType(sql);
            SqlCollation collation = actualType.getCollation();

            String actualName = collation.getCollationName();
            int actualCoercibility = collation.getCoercibility().getOrdinal();
            int expectedCoercibilityOrd = expectedCoercibility.getOrdinal();
            assertEquals(expectedCollationName, actualName);
            assertEquals(expectedCoercibilityOrd, actualCoercibility);
        }

        public void checkCharset(
            String sql,
            Charset expectedCharset)
        {
            sql = "select " + sql + " from values(true)";
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
