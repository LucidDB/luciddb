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

import junit.framework.Test;
import junit.framework.TestSuite;
import net.sf.farrago.test.regression.FarragoCalcSystemTest;
import org.eigenbase.sql.SqlOperator;
import org.eigenbase.sql.SqlOperatorTable;
import org.eigenbase.sql.SqlSyntax;
import org.eigenbase.sql.SqlNode;
import org.eigenbase.sql.validate.SqlValidator;
import org.eigenbase.sql.fun.SqlStdOperatorTable;
import org.eigenbase.sql.test.AbstractSqlTester;
import org.eigenbase.sql.test.SqlOperatorIterator;
import org.eigenbase.sql.type.SqlTypeName;
import org.eigenbase.util.Util;
import org.eigenbase.reltype.RelDataType;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.regex.Pattern;
import java.sql.ResultSetMetaData;


/**
 * FarragoSqlOperatorsTest contains an implementation of
 * {@link AbstractSqlTester}. It uses the visitor pattern to visit every
 * {@link SqlOperator} for unit test purposes.
 *
 * @author Wael Chatila
 * @since May 25, 2004
 * @version $Id$
 */
public class FarragoSqlOperatorsTest extends FarragoTestCase
{
    private static final SqlStdOperatorTable opTab =
        SqlStdOperatorTable.instance();

    //~ Instance fields -------------------------------------------------------

    /**
     * The virtual machine this test is targeted at.
     */
    private final FarragoCalcSystemTest.VirtualMachine vm;
    /**
     * The operator this test is testing.
     */
    private final SqlOperator operator;

    //~ Constructors ----------------------------------------------------------

    public FarragoSqlOperatorsTest(
        FarragoCalcSystemTest.VirtualMachine vm,
        SqlOperator operator,
        String testName)
        throws Exception
    {
        super(testName);
        this.vm = vm;
        this.operator = operator;
    }

    //~ Methods ---------------------------------------------------------------

    public static Test suite()
        throws Exception
    {
        TestSuite suite = new TestSuite();
        addTests(suite, FarragoCalcSystemTest.VirtualMachine.Auto);
        addTests(suite, FarragoCalcSystemTest.VirtualMachine.Fennel);
        addTests(suite, FarragoCalcSystemTest.VirtualMachine.Java);

        return wrappedSuite(suite);
    }

    private static void addTests(TestSuite suite,
        FarragoCalcSystemTest.VirtualMachine vm)
        throws Exception
    {
        Iterator operatorsIt = new SqlOperatorIterator();
        while (operatorsIt.hasNext()) {
            SqlOperator op = (SqlOperator) operatorsIt.next();
            String testName = "SQL-TESTER-" + op.getName() + "-";
            if (!vm.canImplement(op)) {
                continue;
            }
            suite.addTest(
                new FarragoSqlOperatorsTest(vm,
                    op, testName + vm.getName()));
        }
    }

    protected void setUp()
        throws Exception
    {
        super.setUp();
        stmt.execute(vm.getAlterSystemCommand());
    }

    protected void runTest()
        throws Throwable
    {
        operator.test(new FarragoSqlTester());
    }

    //~ Inner Classes ---------------------------------------------------------

    /**
     * Implementation of {@link AbstractSqlTester}, leveraging connection setup
     * and result set comparison from the class {@link FarragoTestCase}.
     */
    private class FarragoSqlTester extends AbstractSqlTester
    {
        /**
         * Checks that query really contains a call to the operator we
         * are looking at
         */
        private void checkQuery(String query)
        {
            if (operator.getSyntax() != SqlSyntax.Internal) {
                String queryCmp = query.toUpperCase();
                String opNameCmp = operator.getName().toUpperCase();
                if (queryCmp.indexOf(opNameCmp) < 0) {
                    fail("Not exercising operator <" + operator + "> "
                        + "with the query <" + query + ">");
                }
            }
        }

        public void checkFails(
            String expression,
            String expectedError)
        {
            String query = buildQuery(expression);
            Throwable actualException = null;

            checkQuery(query);

            try {
                resultSet = stmt.executeQuery(query);
            } catch (Throwable ex) {
                actualException = ex;
            }

            if (null == actualException) {
                fail("FarragoSqlTester: Expected query to throw " +
                    "exception, but it did not; query [" + expression +
                    "]; expected [" + expectedError + "]");
            } else {
                String actualMessage = actualException.getMessage();
                if (actualMessage == null ||
                    !actualMessage.matches(expectedError)) {
                    actualException.printStackTrace();
                    fail("FarragoSqlTester: Query threw different " +
                        "exception than expected; query [" + expression +
                        "]; expected [" + expectedError +
                        "]; actual [" + actualMessage +
                        "]");
                }
            }
        }

        public void checkType(
            String expression,
            String type)
        {
            String query = buildQuery(expression);

            try {
                checkQuery(query);
                resultSet = stmt.executeQuery(query);

                /* Check type */
                ResultSetMetaData md = resultSet.getMetaData();
                int count = md.getColumnCount();
                assertEquals(count, 1);
                String columnType = md.getColumnTypeName(1);
                if (type.indexOf('(') > 0) {
                    columnType += "(" + md.getPrecision(1) + ")";
                }
                if (md.isNullable(1) == ResultSetMetaData.columnNoNulls) {
                    columnType += " NOT NULL";
                }
                assertEquals(type, columnType);

            }  catch (Throwable e) {
                RuntimeException newException =
                    new RuntimeException("Exception occured while testing "
                        + operator + ". " + "Exception msg = "
                        + e.getMessage());
                newException.setStackTrace(e.getStackTrace());
                throw newException;
            }
        }

        public void check(
            String query,
            Object result,
            SqlTypeName resultType)
        {
            try {
                checkQuery(query);
                resultSet = stmt.executeQuery(query);
                if (result instanceof Pattern) {
                    compareResultSetWithPattern((Pattern) result);
                } else {
                    Set refSet = new HashSet();
                    refSet.add(result);
                    compareResultSet(refSet);
                }

                if (resultType != null) {
                    /* Check result type */
                    ResultSetMetaData md = resultSet.getMetaData();

                    /* Assumes there is only one column */
                    int count = md.getColumnCount();
                    assertEquals(count, 1);
                    String columnType = md.getColumnTypeName(1);
                    assertEquals(resultType.toString(), columnType);
                }

                stmt.close();
                stmt = connection.createStatement();
            } catch (Throwable e) {
                RuntimeException newException =
                    new RuntimeException("Exception occured while testing "
                        + operator + ". " + "Exception msg = "
                        + e.getMessage());
                newException.setStackTrace(e.getStackTrace());
                throw newException;
            }
        }
    }
}

// End FarragoSqlOperatorsTest.java
