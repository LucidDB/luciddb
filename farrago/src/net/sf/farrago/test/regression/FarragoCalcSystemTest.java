/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2004-2005 Disruptive Tech
// Copyright (C) 2005-2005 Red Square, Inc.
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
package net.sf.farrago.test.regression;

import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import junit.framework.Test;
import junit.framework.TestSuite;

import net.sf.farrago.test.FarragoTestCase;
import net.sf.farrago.ojrex.FarragoOJRexImplementorTable;

import org.eigenbase.sql.*;
import org.eigenbase.sql.fun.SqlStdOperatorTable;
import org.eigenbase.sql.test.SqlOperatorIterator;
import org.eigenbase.sql.type.SqlTypeName;
import org.eigenbase.sql.type.OperandsTypeChecking;
import org.eigenbase.util.Util;
import com.disruptivetech.farrago.calc.CalcRexImplementorTable;
import com.disruptivetech.farrago.calc.CalcRexImplementorTableImpl;


/**
 * This class contains tests that do full vertical system testing downto the
 * calculator (java / fennel).
 *
 * @author Wael Chatila
 * @since April 19, 2004
 * @version $Id$
 **/
public class FarragoCalcSystemTest extends FarragoTestCase
{
    //~ Static fields/initializers --------------------------------------------

    private static final SqlStdOperatorTable opTab =
        SqlStdOperatorTable.instance();
    private static FarragoOJRexImplementorTable javaTab =
        new FarragoOJRexImplementorTable(opTab);
    private static CalcRexImplementorTable fennelTab =
        CalcRexImplementorTableImpl.std();


    //~ Instance fields -------------------------------------------------------

    String sqlToExecute;
    VirtualMachine vm;

    //~ Constructors ----------------------------------------------------------

    public FarragoCalcSystemTest(
        VirtualMachine vm,
        String sql,
        String testName)
        throws Exception
    {
        super(testName);
        this.vm = vm;
        this.sqlToExecute = sql;
    }

    //~ Methods ---------------------------------------------------------------

    public static Test suite()
        throws Exception
    {
        TestSuite suite = new TestSuite();

        HashSet exclude = new HashSet();

        // do not test operators added to exclude list.
        // Functions to be excluded are typically those that return not null if
        // input is null or
        // otherwise has some special syntax or irregularites to them,
        // making it harder to test them automatically.
        // --- NOTE ---
        // Do not add a function to this exclude list unless you first add a
        // null test for it elsewhere.
        // ------------
        exclude.add(opTab.asOperator);
        exclude.add(opTab.isTrueOperator);
        exclude.add(opTab.isFalseOperator);
        exclude.add(opTab.isNullOperator);
        exclude.add(opTab.isUnknownOperator);
        exclude.add(opTab.isNotTrueOperator);
        exclude.add(opTab.isNotFalseOperator);
        exclude.add(opTab.isNotNullOperator);
        exclude.add(opTab.isNotUnknownOperator);
        exclude.add(opTab.explainOperator);
        exclude.add(opTab.unionAllOperator);
        exclude.add(opTab.unionOperator);
        exclude.add(opTab.valuesOperator);
        exclude.add(opTab.deleteOperator);
        exclude.add(opTab.betweenOperator);
        exclude.add(opTab.notBetweenOperator);
        exclude.add(opTab.updateOperator);
        exclude.add(opTab.existsOperator);
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
        exclude.add(opTab.castFunc);
        exclude.add(opTab.coalesceFunc);
        exclude.add(opTab.overlayFunc);
        exclude.add(opTab.substringFunc);
        exclude.add(opTab.trimFunc);
        exclude.add(opTab.isDistinctFromOperator);
        exclude.add(opTab.descendingOperator);
        exclude.add(opTab.literalChainOperator);
        exclude.add(opTab.escapeOperator);
        exclude.add(opTab.localTimeFunc);
        exclude.add(opTab.localTimestampFunc);
        exclude.add(opTab.currentTimestampFunc);
        exclude.add(opTab.currentTimeFunc);
        exclude.add(opTab.minusDateOperator);

        // Eventually need to include these when cast is working
        exclude.add(opTab.overlapsOperator);
        exclude.add(opTab.initcapFunc);
        exclude.add(opTab.currentDateFunc);
        exclude.add(opTab.convertFunc);
        exclude.add(opTab.translateFunc);

        // --- NOTE ---
        // Do not add a function to this exclude list unless you first add a
        // test for it elsewhere.
        // ------------
        SqlOperatorIterator operatorIt = new SqlOperatorIterator();

        // iterating over all operators
        while (operatorIt.hasNext()) {
            SqlOperator op = (SqlOperator) operatorIt.next();
            if (exclude.contains(op)) {
                continue;
            }
            addTestsForOp(op, suite, VirtualMachine.Fennel);
            if (false) {
                // todo: Enable automated tests for java operators (Bug 260
                //  is logged for this)
                addTestsForOp(op, suite, VirtualMachine.Java);
            }
        }

        return wrappedSuite(suite);
    }

    private static void addTestsForOp(SqlOperator op,
        TestSuite suite,
        VirtualMachine vm)
        throws Exception
    {
        assert (null != op.name) : "Operator name must not be null";

        // Some operators cannot be implemented in all VMs.
        if (!vm.canImplement(op)) {
            return;
        }

        List argCountList =
            op.getOperandsCountDescriptor().getPossibleNumOfOperands();
        assert (argCountList.size() > 0);
        Iterator it = argCountList.iterator();

        // iterating over possible call signatures
        while (it.hasNext()) {
            Integer n = (Integer) it.next();
            SqlNode [] operands = new SqlNode[n.intValue()];
            OperandsTypeChecking allowedTypes =
                op.getOperandsCheckingRule();
            SqlTypeName[][] rules = findRules(allowedTypes);

            if (null == allowedTypes) {
                throw Util.needToImplement("Need to add to exclude list"
                    + " and manually add test");
            }

            for (int i = 0; i < n.intValue(); i++) {
                SqlTypeName typeName = rules[i][0];
                if (typeName.equals(SqlTypeName.Null)) {
                    typeName = rules[i][1];
                } else if (typeName.equals(SqlTypeName.Any)) {
                    typeName = SqlTypeName.Boolean;
                }

                int precision = 0;
                if (typeName.allowsPrecNoScale()) {
                    precision = 1;
                }
                SqlDataTypeSpec dt =
                    new SqlDataTypeSpec(new SqlIdentifier(typeName.name, null),
                        precision,
                        0,
                        null,
                        null);

                operands[i] =
                    opTab.castFunc.createCall(
                        SqlLiteral.createNull(null),
                        dt,
                        null);
            }

            if (operands.length == 0) {
                // What we're testing here is the rule 'if any of the operands
                // are null, the result should be null'. But if there are no
                // operands, that doesn't apply. So skip this form of the
                // operator.
                continue;
            }
            SqlCall call = op.createCall(operands, null);

            String sql = "SELECT " + call.toString() + " FROM (VALUES(1))";
            String testName = "NULL-TEST-" + op.name + "-";
            suite.addTest(
                new FarragoCalcSystemTest(vm, sql, testName + vm.name));
        }
    }

    // REVIEW jvs 17-Mar-2005:  This whole thing is really hokey.
    private static SqlTypeName [][] findRules(OperandsTypeChecking otc)
    {
        SqlTypeName[][] rules;
        if (otc instanceof
            OperandsTypeChecking.CompositeOperandsTypeChecking)
        {
            OperandsTypeChecking rule =
                ((OperandsTypeChecking.CompositeOperandsTypeChecking)
                    otc).getRules()[0];
            return findRules(rule);
        } else if (otc instanceof
            OperandsTypeChecking.SimpleOperandsTypeChecking)
        {
            return ((OperandsTypeChecking.SimpleOperandsTypeChecking)
                otc).getTypes();
        } else {
            throw Util.needToImplement(otc);
        }
    }

    // implement TestCase
    protected void setUp()
        throws Exception
    {
        super.setUp();
        stmt.execute(vm.getAlterSystemCommand());
    }

    protected void runTest()
        throws Throwable
    {
        resultSet = stmt.executeQuery(sqlToExecute);
        Set refSet = new HashSet();
        refSet.add(null);
        compareResultSet(refSet);
    }

    /**
     * Defines a virtual machine (FENNEL, JAVA, AUTO) and the operators it can
     * implement.
     */
    public static class VirtualMachine
    {
        public final String name;

        private VirtualMachine(String name)
        {
            this.name = name;
        }

        public String getAlterSystemCommand()
        {
            return "alter system set \"calcVirtualMachine\" = '" +
                "CALCVM_" + name + "'";
        }

        public boolean canImplement(SqlOperator op)
        {
            if ((this == Java || this == Auto) &&
                javaTab.get(op) != null) {
                    return true;
            }
            if ((this == Fennel || this == Auto) &&
                fennelTab.get(op) != null) {
                return true;
            }
            return false;
        }

        public static final VirtualMachine Fennel = new VirtualMachine("FENNEL");
        public static final VirtualMachine Java = new VirtualMachine("JAVA");
        public static final VirtualMachine Auto = new VirtualMachine("AUTO");
    }
}
