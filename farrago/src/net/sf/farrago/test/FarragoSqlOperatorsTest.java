/*
// Farrago is a relational database management system.
// Copyright (C) 2003-2004 John V. Sichi.
// Copyright (C) 2003-2004 Disruptive Tech
//
// This program is free software; you can redistribute it and/or
// modify it under the terms of the GNU Lesser General Public License
// as published by the Free Software Foundation; either version 2.1
// of the License, or (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with this program; if not, write to the Free Software
// Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
*/
package net.sf.farrago.test;

import junit.framework.Test;
import junit.framework.TestSuite;
import net.sf.farrago.test.regression.FarragoCalcSystemTest;
import org.eigenbase.sql.SqlOperator;
import org.eigenbase.sql.SqlOperatorTable;
import org.eigenbase.sql.SqlSyntax;
import org.eigenbase.sql.fun.SqlStdOperatorTable;
import org.eigenbase.sql.test.AbstractSqlTester;
import org.eigenbase.sql.test.SqlOperatorIterator;
import org.eigenbase.sql.type.SqlTypeName;
import org.eigenbase.util.Util;

import java.util.HashSet;
import java.util.Iterator;
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
public class FarragoSqlOperatorsTest extends FarragoTestCase
{
    private static final SqlStdOperatorTable opTab = SqlOperatorTable.std();
    private static final boolean bug260fixed = false;

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
            String testName = "SQL-TESTER-" + op.name + "-";
            if (!vm.canImplement(op)) {
                continue;
            }
            if (!bug260fixed) {
                if (op == opTab.orOperator ||
                    op == opTab.andOperator ||
                    op == opTab.isFalseOperator ||
                    op == opTab.literalChainOperator ||
                    op == opTab.multiplyOperator ||
                    op == opTab.localTimeFunc ||
                    op == opTab.localTimestampFunc) {
                    continue;
                }
            }
            suite.addTest(
                new FarragoSqlOperatorsTest(vm,
                    op, testName + vm.name));
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
        public void checkFails(
            String expression,
            String expectedError)
        {
            if (bug260fixed) {
                // todo: implement this
                throw Util.needToImplement(this);
            }
        }

        public void checkType(
            String expression,
            String type)
        {
            if (bug260fixed) {
                // todo: implement this
                throw Util.needToImplement(this);
            }
        }

        public void check(
            String query,
            Object result,
            SqlTypeName resultType)
        {
            try {
                if (operator.getSyntax() != SqlSyntax.Internal) {
                    // check that query really contains a call to the operator we
                    // are looking at
                    String queryCmp = query.toUpperCase();
                    String opNameCmp = operator.name.toUpperCase();
                    if (queryCmp.indexOf(opNameCmp) < 0) {
                        fail("Not exercising operator <" + operator + "> "
                            + "with the query <" + query + ">");
                    }
                }

                resultSet = stmt.executeQuery(query);
                if (result instanceof Pattern) {
                    compareResultSetWithPattern((Pattern) result);
                } else {
                    Set refSet = new HashSet();
                    refSet.add(result);
                    compareResultSet(refSet);
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
