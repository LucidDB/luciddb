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
import org.eigenbase.sql.SqlSyntax;
import org.eigenbase.sql.test.AbstractSqlTester;
import org.eigenbase.sql.test.SqlOperatorIterator;
import org.eigenbase.sql.type.SqlTypeName;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

/**
 * FarragoSqlOperatorsTest contains an implementation of {@link AbstractSqlTester}.
 * It uses the visitor pattern to vist all SqlOperators for unit test purposes.
 *
 * @author Wael Chatila
 * @since May 25, 2004
 * @version $Id$
 */
public class FarragoSqlOperatorsTest extends FarragoTestCase {

    String vmFlag;
    SqlOperator operator;

    public FarragoSqlOperatorsTest(String vmFlag, SqlOperator operator,
                                   String testName) throws Exception
    {
        super(testName);
        this.vmFlag = vmFlag;
        this.operator = operator;
    }

    /**
     * Implementation of {@link AbstractSqlTester}, leveraging connection setup
     * and result set comparing from the class {@link FarragoTestCase}
     */
    private class FarragoSqlTester extends AbstractSqlTester
    {
        /** The name of the operator which should be the same as its syntax */
        SqlOperator operator;

        FarragoSqlTester(SqlOperator op) {
            operator = op;
        }

        public void check(String query, String result, SqlTypeName resultType) {
            try {
                if (operator.getSyntax() != SqlSyntax.Internal) {
                    // check that query really contains a call to the operator we
                    // are looking at
                    String queryCmp = query.toUpperCase();
                    String opNameCmp = operator.name.toUpperCase();
                    if (queryCmp.indexOf(opNameCmp) < 0) {
                        fail("Not exercising operator <"+operator+"> " +
                             "with the query <"+query+">");
                    }
                }

                resultSet = stmt.executeQuery(query);
                Set refSet = new HashSet();
                refSet.add(result);
                compareResultSet(refSet);
                stmt.close();
                stmt = connection.createStatement();
            } catch (Throwable e) {
                RuntimeException newException = new RuntimeException(
                        "Exception occured while testing "+operator+". "+
                        "Exception msg = "+e.getMessage());
                newException.setStackTrace(e.getStackTrace());
                throw newException;
            }
        }
    }

    public static Test suite() throws Exception
    {
        TestSuite suite = new TestSuite();
        Iterator operatorsIt = new SqlOperatorIterator();
        while (operatorsIt.hasNext()) {
            SqlOperator op = (SqlOperator) operatorsIt.next();
            String testName = "SQL-TESTER-" + op.name + "-";
            suite.addTest(new FarragoSqlOperatorsTest(
                    FarragoCalcSystemTest.vmFennel,
                    op,
                    testName+"FENNEL"));
            if (false) {
                suite.addTest(new FarragoSqlOperatorsTest(
                        FarragoCalcSystemTest.vmJava,
                        op,
                        testName + "JAVA"));
            }
        }

        return wrappedSuite(suite);
    }

    protected void setUp() throws Exception {
        super.setUp();
        stmt.execute(vmFlag);
    }

    protected void runTest() throws Throwable {
        operator.test(new FarragoSqlTester(operator));
    }
}
