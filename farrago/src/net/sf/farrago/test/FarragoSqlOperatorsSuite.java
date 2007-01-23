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

import java.sql.*;

import java.util.*;
import java.util.regex.*;

import junit.framework.*;

import net.sf.farrago.jdbc.*;
import net.sf.farrago.query.*;
import net.sf.farrago.test.regression.*;

import org.eigenbase.sql.*;
import org.eigenbase.sql.parser.*;
import org.eigenbase.sql.test.*;
import org.eigenbase.sql.type.*;
import org.eigenbase.test.*;
import org.eigenbase.util.*;


/**
 * FarragoSqlOperatorsSuite runs operator tests defined in {@link
 * SqlOperatorTests} against a Farrago database.
 *
 * <p>The entry point is the {@link #suite} method.
 *
 * @author Wael Chatila
 * @version $Id$
 * @since May 25, 2004
 */
public class FarragoSqlOperatorsSuite
{

    //~ Static fields/initializers ---------------------------------------------

    private static final SqlTypeFactoryImpl sqlTypeFactory =
        new SqlTypeFactoryImpl();

    //~ Constructors -----------------------------------------------------------

    protected FarragoSqlOperatorsSuite()
    {
    }

    //~ Methods ----------------------------------------------------------------

    /**
     * Entry point for JUnit.
     */
    public static TestSuite suite()
    {
        final TestSuite suite = new TestSuite();
        suite.addTest(FarragoFennelVmOperatorTest.suite());
        suite.addTest(FarragoJavaVmOperatorTest.suite());
        suite.addTest(FarragoAutoVmOperatorTest.suite());
        return suite;
    }

    //~ Inner Classes ----------------------------------------------------------

    /**
     * Implementation of {@link AbstractSqlTester}, leveraging connection setup
     * and result set comparison from the class {@link FarragoTestCase}.
     */
    protected static class FarragoSqlTester
        extends AbstractSqlTester
    {
        /**
         * Helper.
         */
        private final FarragoTestCase farragoTest;

        /**
         * The virtual machine this test is targeted at.
         */
        private final FarragoCalcSystemTest.VirtualMachine vm;

        private FarragoSqlTester(FarragoCalcSystemTest.VirtualMachine vm)
            throws Exception
        {
            this.vm = vm;
            this.farragoTest = new MyFarragoTestCase();
        }

        protected FarragoSqlTester(FarragoCalcSystemTest.VirtualMachine vm,
            FarragoTestCase farragoTest)
            throws Exception
        {
            this.vm = vm;
            this.farragoTest = farragoTest;
        }

        public void checkFails(
            String expression,
            String expectedError,
            boolean runtime)
        {
            try {
                farragoTest.setUp();
                checkFails(vm, expression, expectedError, runtime);
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
                checkType(
                    vm,
                    getFor(),
                    expression,
                    type);
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
                check(
                    vm,
                    getFor(),
                    query,
                    typeChecker,
                    result,
                    delta);
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

        /**
         * Checks that a scalar expression fails at validate time or runtime on
         * a given virtual machine.
         */
        void checkFails(
            FarragoCalcSystemTest.VirtualMachine vm,
            String expression,
            String expectedError,
            boolean runtime)
            throws SQLException
        {
            farragoTest.stmt.execute(vm.getAlterSystemCommand());
            String query = buildQuery(expression);
            SqlParserUtil.StringAndPos sap = SqlParserUtil.findPos(query);
            if (!runtime) {
                Assert.assertNotNull(
                    "negative validation tests must contain an error location",
                    sap.pos);
            }

            Throwable thrown = null;
            try {
                farragoTest.resultSet = farragoTest.stmt.executeQuery(sap.sql);
                if (runtime) {
                    // If we're expecting a runtime error, we may need to ask
                    // for the row before the error occurs.
                    boolean hasNext = farragoTest.resultSet.next();
                    Util.discard(hasNext);
                }
            } catch (FarragoJdbcUtil.FarragoSqlException ex) {
                // The exception returned by the JDBC driver is dumbed down,
                // and doesn't contain the full position information.
                // Use the undiluted error instead.
                thrown = ex.getOriginalThrowable();
            } catch (Throwable ex) {
                thrown = ex;
            }

            SqlValidatorTestCase.checkEx(thrown, expectedError, sap);
        }

        void check(
            FarragoCalcSystemTest.VirtualMachine vm,
            SqlOperator operator,
            String query,
            SqlTester.TypeChecker typeChecker,
            Object result,
            double delta)
            throws Exception
        {
            farragoTest.assertNotNull("Test must call isFor() first", operator);
            if (!vm.canImplement(operator)) {
                return;
            }
            farragoTest.stmt.execute(vm.getAlterSystemCommand());
            farragoTest.resultSet = farragoTest.stmt.executeQuery(query);
            if (result instanceof Pattern) {
                farragoTest.compareResultSetWithPattern((Pattern) result);
            } else if (delta != 0) {
                Assert.assertTrue(result instanceof Number);
                farragoTest.compareResultSetWithDelta(
                    ((Number) result).doubleValue(),
                    delta);
            } else {
                Set refSet = new HashSet();
                refSet.add((result == null) ? null : result.toString());
                farragoTest.compareResultSet(refSet);
            }

            // Check result type
            ResultSetMetaData md = farragoTest.resultSet.getMetaData();
            int count = md.getColumnCount();
            Assert.assertEquals("query must return one column", count, 1);
            BasicSqlType type = getColumnType(md, 1);

            typeChecker.checkType(type);

            farragoTest.stmt.close();
            farragoTest.stmt = farragoTest.connection.createStatement();
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
            if (actualTypeOrdinal == Types.OTHER) {
                return null;
            }
            farragoTest.assertNotNull(actualSqlTypeName);
            farragoTest.assertEquals(
                actualSqlTypeName.getName(),
                actualTypeName);
            BasicSqlType sqlType;
            final int actualNullable = md.isNullable(column);
            if (actualSqlTypeName.allowsScale()) {
                sqlType =
                    new BasicSqlType(
                        actualSqlTypeName,
                        md.getPrecision(column),
                        md.getScale(column));
            } else if (actualSqlTypeName.allowsPrecNoScale()) {
                sqlType =
                    new BasicSqlType(
                        actualSqlTypeName,
                        md.getPrecision(column));
            } else {
                sqlType = new BasicSqlType(actualSqlTypeName);
            }
            if (actualNullable == ResultSetMetaData.columnNullable) {
                sqlType =
                    (BasicSqlType) sqlTypeFactory.createTypeWithNullability(
                        sqlType,
                        true);
            }
            return sqlType;
        }

        void checkType(
            FarragoCalcSystemTest.VirtualMachine vm,
            SqlOperator operator,
            String expression,
            String type)
            throws SQLException
        {
            farragoTest.assertNotNull("Test must call isFor() first", operator);
            if (!vm.canImplement(operator)) {
                return;
            }
            farragoTest.stmt.execute(vm.getAlterSystemCommand());

            String query = buildQuery(expression);
            farragoTest.resultSet = farragoTest.stmt.executeQuery(query);

            // Check type
            ResultSetMetaData md = farragoTest.resultSet.getMetaData();
            int count = md.getColumnCount();
            Assert.assertEquals(count, 1);
            String columnType = md.getColumnTypeName(1);
            if (type.indexOf('(') > 0) {
                columnType += "(" + md.getPrecision(1);
                if (type.indexOf(',') >= 0) {
                    columnType += ", " + md.getScale(1);
                }
                columnType += ")";
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

        private static RuntimeException wrap(Exception e)
        {
            final RuntimeException rte = new RuntimeException(e);
            rte.setStackTrace(e.getStackTrace());
            return rte;
        }
    }

    /**
     * Base class for all tests which test operators against a particular
     * virtual machine. Abstract so that Junit doesn't try to run it.
     */
    public static abstract class FarragoVmOperatorTestBase
        extends SqlOperatorTests
    {
        private final FarragoSqlTester tester;

        public FarragoVmOperatorTestBase(
            String testName,
            FarragoCalcSystemTest.VirtualMachine vm)
            throws Exception
        {
            super(testName);
            tester = new FarragoSqlTester(vm);
        }

        protected SqlTester getTester()
        {
            return tester;
        }
    }

    /**
     * Implementation of {@link SqlOperatorTests} which runs all tests in
     * Farrago with a pure-Java calculator.
     */
    public static class FarragoJavaVmOperatorTest
        extends FarragoVmOperatorTestBase
    {
        public FarragoJavaVmOperatorTest(String testName)
            throws Exception
        {
            super(testName, FarragoCalcSystemTest.VirtualMachine.Java);
        }

        // implement TestCase
        public static Test suite()
        {
            return FarragoTestCase.wrappedSuite(
                new TestSuite(FarragoJavaVmOperatorTest.class));
        }
    }

    /**
     * Implementation of {@link SqlOperatorTests} which runs all tests in
     * Farrago with a hybrid calculator.
     */
    public static class FarragoAutoVmOperatorTest
        extends FarragoVmOperatorTestBase
    {
        public FarragoAutoVmOperatorTest(String testName)
            throws Exception
        {
            super(testName, FarragoCalcSystemTest.VirtualMachine.Auto);
        }

        // implement TestCase
        public static Test suite()
        {
            return FarragoTestCase.wrappedSuite(
                new TestSuite(FarragoAutoVmOperatorTest.class));
        }
    }

    /**
     * Implementation of {@link SqlOperatorTests} which runs all tests in
     * Farrago with a C++ calculator.
     */
    public static class FarragoFennelVmOperatorTest
        extends FarragoVmOperatorTestBase
    {
        public FarragoFennelVmOperatorTest(String testName)
            throws Exception
        {
            super(testName, FarragoCalcSystemTest.VirtualMachine.Fennel);
        }

        // implement TestCase
        public static Test suite()
        {
            return FarragoTestCase.wrappedSuite(
                new TestSuite(FarragoFennelVmOperatorTest.class));
        }
    }

    /**
     * Helper class. Extends {@link FarragoTestCase} for management of
     * connections and statements.
     *
     * <p>Per that class, you must ensure that {@link #staticSetUp()} and {@link
     * #staticTearDown()} are called, and {@link #wrappedSuite(Class)} is a good
     * way to do this.
     */
    private static class MyFarragoTestCase
        extends FarragoTestCase
    {
        public MyFarragoTestCase()
            throws Exception
        {
            super("dummy");
        }

        public void setUp()
            throws Exception
        {
            // Constant reduction slows things down without any benefit,
            // so disable it.
            super.setUp();
            getSession().setOptRuleDescExclusionFilter(
                FarragoReduceExpressionsRule.EXCLUSION_PATTERN);
        }
    }

}

// End FarragoSqlOperatorsSuite.java
