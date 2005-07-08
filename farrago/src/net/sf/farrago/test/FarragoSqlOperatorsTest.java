/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2003-2005 Disruptive Tech
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
package net.sf.farrago.test;

import junit.framework.Assert;
import junit.framework.Test;
import junit.framework.TestSuite;
import net.sf.farrago.test.regression.FarragoCalcSystemTest;
import org.eigenbase.sql.SqlOperator;
import org.eigenbase.sql.test.AbstractSqlTester;
import org.eigenbase.sql.test.SqlOperatorTests;
import org.eigenbase.sql.test.SqlTester;
import org.eigenbase.sql.type.BasicSqlType;
import org.eigenbase.sql.type.SqlTypeName;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Pattern;


/**
 * FarragoSqlOperatorsTest contains an implementation of
 * {@link AbstractSqlTester}. It uses the visitor pattern to visit every
 * {@link SqlOperator} for unit test purposes.
 *
 * @author Wael Chatila
 * @since May 25, 2004
 * @version $Id$
 */
public class FarragoSqlOperatorsTest
{

    //~ Instance fields -------------------------------------------------------


    //~ Constructors ----------------------------------------------------------

    private FarragoSqlOperatorsTest() {}

    //~ Methods ---------------------------------------------------------------


    //~ Inner Classes ---------------------------------------------------------

    /**
     * Implementation of {@link AbstractSqlTester}, leveraging connection setup
     * and result set comparison from the class {@link FarragoTestCase}.
     */
    private static class FarragoSqlTester extends AbstractSqlTester
    {
        private final MyFarragoTestCase farragoTest;

        /**
         * The virtual machine this test is targeted at.
         */
        private final FarragoCalcSystemTest.VirtualMachine vm;

        FarragoSqlTester(FarragoCalcSystemTest.VirtualMachine vm) throws Exception
        {
            this.vm = vm;
            this.farragoTest = new MyFarragoTestCase();
        }

        public void checkFails(
            String expression,
            String expectedError)
        {
            try {
                farragoTest.setUp();
                farragoTest.checkFails(vm, expression, expectedError);
            } catch (Exception e) {
                throw wrap(e);
            } finally {
                try {
                    farragoTest.tearDown();
                } catch (Exception e) {
                    throw wrap(e);
                }
            }
        }

        public void checkType(
            String expression,
            String type)
        {
            try {
                farragoTest.setUp();
                farragoTest.checkType(vm, getFor(), expression, type);
            } catch (Exception e) {
                throw wrap(e);
            } finally {
                try {
                    farragoTest.tearDown();
                } catch (Exception e) {
                    throw wrap(e);
                }
            }
        }

        public void check(
            String query,
            TypeChecker typeChecker,
            Object result,
            double delta)
        {
            try {
                farragoTest.setUp();
                farragoTest.check(vm, getFor(), query, typeChecker, result, delta);
            } catch (Exception e) {
                throw wrap(e);
            } finally {
                try {
                    farragoTest.tearDown();
                } catch (Exception e) {
                    throw wrap(e);
                }
            }
        }

        private static RuntimeException wrap(Exception e)
        {
            final RuntimeException rte = new RuntimeException(e);
            rte.setStackTrace(e.getStackTrace());
            return rte;
        }
    }

    public static class FarragoVmOperatorTest extends SqlOperatorTests
    {
        private final FarragoSqlTester tester;

        public FarragoVmOperatorTest(
            String testName,
            FarragoCalcSystemTest.VirtualMachine vm) throws Exception
        {
            super(testName);
            tester = new FarragoSqlTester(vm);
        }

        protected SqlTester getTester()
        {
            return tester;
        }
    }

    public static class FarragoJavaVmOperatorTest extends FarragoVmOperatorTest
    {
        public FarragoJavaVmOperatorTest(String testName) throws Exception
        {
            super(testName, FarragoCalcSystemTest.VirtualMachine.Java);
        }
    }

    public static class FarragoAutoVmOperatorTest extends FarragoVmOperatorTest
    {
        public FarragoAutoVmOperatorTest(String testName) throws Exception
        {
            super(testName, FarragoCalcSystemTest.VirtualMachine.Auto);
        }
    }

    public static class FarragoFennelVmOperatorTest extends FarragoVmOperatorTest
    {
        public FarragoFennelVmOperatorTest(String testName) throws Exception
        {
            super(testName, FarragoCalcSystemTest.VirtualMachine.Fennel);
        }
    }

    // implement TestCase
    public static Test suite()
    {
        final TestSuite suite = new TestSuite();
        suite.addTestSuite(FarragoJavaVmOperatorTest.class);
        suite.addTestSuite(FarragoAutoVmOperatorTest.class);
        suite.addTestSuite(FarragoFennelVmOperatorTest.class);
        return FarragoTestCase.wrappedSuite(suite);
    }

    private static class MyFarragoTestCase extends FarragoTestCase
    {

        public MyFarragoTestCase()
            throws Exception
        {
            super("dummy");
        }

        void checkFails(
            FarragoCalcSystemTest.VirtualMachine vm,
            String expression,
            String expectedError) throws SQLException
        {
            stmt.execute(vm.getAlterSystemCommand());
            String query = buildQuery(expression);
            Throwable actualException = null;

            try {
                resultSet = stmt.executeQuery(query);
            } catch (Throwable ex) {
                actualException = ex;
            }

            if (null == actualException) {
                Assert.fail("Expected query to throw " +
                    "exception, but it did not; query [" + expression +
                    "]; expected [" + expectedError + "]");
            } else {
                String actualMessage = actualException.getMessage();
                if (actualMessage == null ||
                    !actualMessage.matches(expectedError)) {
                    actualException.printStackTrace();
                    Assert.fail("Query threw different " +
                        "exception than expected; query [" + expression +
                        "]; expected [" + expectedError +
                        "]; actual [" + actualMessage +
                        "]");
                }
            }
        }

        void check(
            FarragoCalcSystemTest.VirtualMachine vm,
            SqlOperator operator,
            String query,
            SqlTester.TypeChecker typeChecker,
            Object result,
            double delta) throws Exception
        {
            assertNotNull("Test must call isFor() first", operator);
            if (!vm.canImplement(operator)) {
                return;
            }
            stmt.execute(vm.getAlterSystemCommand());
            resultSet = stmt.executeQuery(query);
            if (result instanceof Pattern) {
                compareResultSetWithPattern((Pattern) result);
            } else if (delta != 0) {
                Assert.assertTrue(result instanceof Number);
                compareResultSetWithDelta(
                    ((Number) result).doubleValue(), delta);
            } else {
                Set refSet = new HashSet();
                refSet.add(result == null ? null : result.toString());
                compareResultSet(refSet);
            }

            // Check result type
            ResultSetMetaData md = resultSet.getMetaData();
            int count = md.getColumnCount();
            Assert.assertEquals("query must return one column", count, 1);
            BasicSqlType type = getColumnType(md, 1);

            typeChecker.checkType(type);

            stmt.close();
            stmt = connection.createStatement();
        }

        private BasicSqlType getColumnType(
            ResultSetMetaData md,
            int column)
            throws SQLException
        {
            String actualTypeName = md.getColumnTypeName(column);
            int actualTypeOrdinal = md.getColumnType(column);
            SqlTypeName actualSqlTypeName =
                SqlTypeName.getNameForJdbcType(actualTypeOrdinal);
            assertNotNull(actualSqlTypeName);
            assertEquals(actualSqlTypeName.getName(), actualTypeName);
            final int actualNullable = md.isNullable(column);
            if (actualSqlTypeName.allowsNoPrecNoScale()) {
                return new BasicSqlType(actualSqlTypeName);
            } else if (actualSqlTypeName.allowsPrecNoScale()) {
                return new BasicSqlType(
                    actualSqlTypeName,
                    md.getPrecision(column));
            } else {
                return new BasicSqlType(
                    actualSqlTypeName,
                    md.getPrecision(column),
                    md.getScale(column));
            }
        }

        void checkType(
            FarragoCalcSystemTest.VirtualMachine vm,
            SqlOperator operator,
            String expression,
            String type) throws SQLException
        {
            assertNotNull("Test must call isFor() first", operator);
            if (!vm.canImplement(operator)) {
                return;
            }
            stmt.execute(vm.getAlterSystemCommand());

            String query = buildQuery(expression);
            resultSet = stmt.executeQuery(query);

            // Check type
            ResultSetMetaData md = resultSet.getMetaData();
            int count = md.getColumnCount();
            Assert.assertEquals(count, 1);
            String columnType = md.getColumnTypeName(1);
            if (type.indexOf('(') > 0) {
                columnType += "(" + md.getPrecision(1) + ")";
            }
            if (md.isNullable(1) == ResultSetMetaData.columnNoNulls) {
                columnType += " NOT NULL";
            }
            Assert.assertEquals(type, columnType);
        }

        protected String buildQuery(String expression)
        {
            return "values (" + expression + ")";
        }
    }
}

// End FarragoSqlOperatorsTest.java
