/*
// Farrago is a relational database management system.
// (C) Copyright 2004-2004 Disruptive Technologies, Inc.
// You must accept the terms in LICENSE.html to use this software.
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

package net.sf.farrago.test.regression;

import net.sf.saffron.sql.*;
import net.sf.saffron.sql.fun.SqlStdOperatorTable;
import net.sf.saffron.sql.test.SqlOperatorIterator;
import net.sf.farrago.test.FarragoTestCase;
import junit.framework.Test;
import junit.framework.TestSuite;

import java.util.Iterator;
import java.util.HashSet;
import java.util.List;

/**
 * This class contains tests that do full vertical system testing downto the
 * calculator (java / fennel).
 *
 * @author Wael Chatila
 * @since April 19, 2004
 * @version $Id$
 **/
public class FarragoCalcSystemTest extends FarragoTestCase{

    public static final String vmFennel =
            "alter system set \"calcVirtualMachine\" = 'CALCVM_FENNEL'";
    public static final String vmJava =
            "alter system set \"calcVirtualMachine\" = 'CALCVM_JAVA'";
    String sqlToExceute;
    String vmFlag;

    public FarragoCalcSystemTest(String vmFlag,
                                 String sql,
                                 String testName) throws Exception {
        super(testName);
        this.vmFlag = vmFlag;
        this.sqlToExceute = sql;
    }

    public static Test suite() throws Exception
    {
        SqlStdOperatorTable opTab = SqlOperatorTable.std();
        TestSuite suite = new TestSuite();

        HashSet exclude = new HashSet();
        // do not test operators added to exclude list.
        // Functions to be excluded are typically those that return null if input is null or
        // otherwise has some special syntax or irregularites to them,
        // making it harder to test them automatically.
        // --- NOTE ---
        // Do not add a function to this exclude list unless you first add a test
        // for it else where.
        // ------------
        exclude.add(opTab.isTrueOperator);
        exclude.add(opTab.isFalseOperator);
        exclude.add(opTab.isNullOperator);
        exclude.add(opTab.isNotTrueOperator);
        exclude.add(opTab.isNotFalseOperator);
        exclude.add(opTab.isNotNullOperator);
        exclude.add(opTab.explainOperator);
        exclude.add(opTab.unionAllOperator);
        exclude.add(opTab.unionOperator);
        exclude.add(opTab.valuesOperator);
        exclude.add(opTab.deleteOperator);
        exclude.add(opTab.betweenOperator);
        exclude.add(opTab.notBetweenOperator);
        exclude.add(opTab.updateOperator);
        exclude.add(opTab.exceptOperator);
        exclude.add(opTab.exceptAllOperator);
        exclude.add(opTab.inOperator);
        exclude.add(opTab.insertOperator);
        exclude.add(opTab.intersectOperator);
        exclude.add(opTab.intersectAllOperator);
        exclude.add(opTab.caseOperator);
        exclude.add(opTab.explicitTableOperator);
        exclude.add(opTab.orderByOperator);
        exclude.add(opTab.selectOperator);
        exclude.add(opTab.dotOperator);
        exclude.add(opTab.joinOperator);
        exclude.add(opTab.rowConstructor);
        exclude.add(opTab.nullIfFunc);
        exclude.add(opTab.convertFunc);
        exclude.add(opTab.translateFunc);
        exclude.add(opTab.castFunc);
        exclude.add(opTab.coalesceFunc);
        exclude.add(opTab.currentDateFunc); //TODO need to enable when ready. i.e not exclude
        exclude.add(opTab.overlayFunc);
        exclude.add(opTab.substringFunc);
        exclude.add(opTab.trimFunc);

        // --- NOTE ---
        // Do not add a function to this exclude list unless you first add a test
        // for it else where.
        // ------------

        SqlOperatorIterator operatorIt = new SqlOperatorIterator();
        // iterating over all operators
        while(operatorIt.hasNext()) {
            SqlOperator op = (SqlOperator) operatorIt.next();
            assert(null != op.name) : "Opertor name must not be null";
            if (exclude.contains(op)) {
                continue;
            }


            List nbrOfArgsList = op.getPossibleNumOfOperands();
            assert (nbrOfArgsList.size() > 0);
            Iterator it = nbrOfArgsList.iterator();
            // iterating over possible call signatures
            while (it.hasNext()) {
                Integer n = (Integer) it.next();
                SqlNode[] operands = new SqlNode[n.intValue()];
                SqlOperator.AllowedArgInference allowedTypes =
                        op.getAllowedArgInference();
                for (int i=0; i < n.intValue(); i++) {
                    // TODO wael: this is bogus, just to get an idea
                    // do casting using allowedTypes.getTypes()[i][0]
                    operands[i] = opTab.castFunc.createCall(SqlLiteral.createNull());
                }
                SqlCall call = op.createCall(operands);

                String sql = "SELECT "+call.toString()+" FROM VALUES(1)";
                String testName = "NULL-TEST-"+op.name+"-";
//                suite.addTest(new FarragoCalcSystemTest(vmFennel, sql, testName+"FENNEL"));
//                suite.addTest(new FarragoCalcSystemTest(vmJava, sql, testName+"JAVA"));
            }
        }

        return wrappedSuite(suite);
    }

    // implement TestCase
    protected void setUp() throws Exception {
        super.setUp();
        stmt.execute(vmFlag);
    }

    // implement TestCase
    protected void tearDown() throws Exception {
        // reset to use java vm by default
        stmt.execute(vmJava);
        super.tearDown();
    }

    protected void runTest() throws Throwable {
        resultSet = stmt.executeQuery(sqlToExceute);
        assertEquals(0,getResultSetCount());
        stmt.close();
    }
}
