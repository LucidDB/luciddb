/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2007 The Eigenbase Project
// Copyright (C) 2004-2007 Disruptive Tech
// Copyright (C) 2005-2007 LucidEra, Inc.
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

import com.disruptivetech.farrago.calc.*;

import java.util.*;

import junit.framework.*;

import net.sf.farrago.ojrex.*;
import net.sf.farrago.test.*;

import org.eigenbase.sql.*;
import org.eigenbase.sql.fun.*;
import org.eigenbase.sql.parser.*;
import org.eigenbase.sql.type.*;
import org.eigenbase.util.*;


/**
 * This class contains tests that do full vertical system testing downto the
 * calculator (java / fennel).
 *
 * @author Wael Chatila
 * @version $Id$
 * @since April 19, 2004
 */
public class FarragoCalcSystemTest
    extends FarragoTestCase
{
    //~ Static fields/initializers ---------------------------------------------

    private static final SqlStdOperatorTable opTab =
        SqlStdOperatorTable.instance();
    private static FarragoOJRexImplementorTable javaTab =
        new FarragoOJRexImplementorTable(opTab);
    private static CalcRexImplementorTable fennelTab =
        CalcRexImplementorTableImpl.std();

    // Table of operators to be tested using auto VM that
    // may not have been explicitly registered in the java and fennel calcs
    private static Map<SqlOperator, Boolean> autoTab =
        new HashMap<SqlOperator, Boolean>();

    static {
        // TODO: Should also test these operators for java and fennel calcs
        // if they the rewrites only involve operators that are implemented
        // in the java and fennel calcs
        autoTab.put(SqlStdOperatorTable.betweenOperator, Boolean.TRUE);
        autoTab.put(SqlStdOperatorTable.notBetweenOperator, Boolean.TRUE);
        autoTab.put(SqlStdOperatorTable.selectOperator, Boolean.TRUE);
        autoTab.put(SqlStdOperatorTable.literalChainOperator, Boolean.TRUE);
        autoTab.put(SqlStdOperatorTable.isDistinctFromOperator, Boolean.TRUE);
        autoTab.put(
            SqlStdOperatorTable.isNotDistinctFromOperator,
            Boolean.TRUE);
        autoTab.put(SqlStdOperatorTable.overlapsOperator, Boolean.TRUE);
        autoTab.put(SqlStdOperatorTable.isUnknownOperator, Boolean.TRUE);
        autoTab.put(SqlStdOperatorTable.isNotUnknownOperator, Boolean.TRUE);
        autoTab.put(SqlStdOperatorTable.valuesOperator, Boolean.TRUE);
        autoTab.put(SqlStdOperatorTable.nullIfFunc, Boolean.TRUE);
        autoTab.put(SqlStdOperatorTable.coalesceFunc, Boolean.TRUE);
        autoTab.put(SqlStdOperatorTable.windowOperator, Boolean.TRUE);
        autoTab.put(SqlStdOperatorTable.countOperator, Boolean.TRUE);
        autoTab.put(SqlStdOperatorTable.sumOperator, Boolean.TRUE);
        autoTab.put(SqlStdOperatorTable.avgOperator, Boolean.TRUE);
        autoTab.put(SqlStdOperatorTable.firstValueOperator, Boolean.TRUE);
        autoTab.put(SqlStdOperatorTable.lastValueOperator, Boolean.TRUE);
        autoTab.put(SqlStdOperatorTable.extractFunc, Boolean.TRUE);
    }

    //~ Instance fields --------------------------------------------------------

    String sqlToExecute;
    VirtualMachine vm;

    //~ Constructors -----------------------------------------------------------

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

    //~ Methods ----------------------------------------------------------------

    public static Test suite()
        throws Exception
    {
        TestSuite suite = new TestSuite();

        Set<SqlOperator> exclude = new HashSet<SqlOperator>();

        // do not test operators added to exclude list. Functions to be excluded
        // are typically those that return not null if input is null or
        // otherwise has some special syntax or irregularites to them, making it
        // harder to test them automatically. --- NOTE --- Do not add a function
        // to this exclude list unless you first add a null test for it
        // elsewhere. ------------
        exclude.add(SqlStdOperatorTable.asOperator);
        exclude.add(SqlStdOperatorTable.isTrueOperator);
        exclude.add(SqlStdOperatorTable.isFalseOperator);
        exclude.add(SqlStdOperatorTable.isNullOperator);
        exclude.add(SqlStdOperatorTable.isUnknownOperator);
        exclude.add(SqlStdOperatorTable.isNotTrueOperator);
        exclude.add(SqlStdOperatorTable.isNotFalseOperator);
        exclude.add(SqlStdOperatorTable.isNotNullOperator);
        exclude.add(SqlStdOperatorTable.isNotUnknownOperator);
        exclude.add(SqlStdOperatorTable.explainOperator);
        exclude.add(SqlStdOperatorTable.unionAllOperator);
        exclude.add(SqlStdOperatorTable.unionOperator);
        exclude.add(SqlStdOperatorTable.valuesOperator);
        exclude.add(SqlStdOperatorTable.deleteOperator);
        exclude.add(SqlStdOperatorTable.betweenOperator);
        exclude.add(SqlStdOperatorTable.notBetweenOperator);
        exclude.add(SqlStdOperatorTable.updateOperator);
        exclude.add(SqlStdOperatorTable.existsOperator);
        exclude.add(SqlStdOperatorTable.exceptOperator);
        exclude.add(SqlStdOperatorTable.exceptAllOperator);
        exclude.add(SqlStdOperatorTable.inOperator);
        exclude.add(SqlStdOperatorTable.insertOperator);
        exclude.add(SqlStdOperatorTable.intersectOperator);
        exclude.add(SqlStdOperatorTable.intersectAllOperator);
        exclude.add(SqlStdOperatorTable.caseOperator);
        exclude.add(SqlStdOperatorTable.explicitTableOperator);
        exclude.add(SqlStdOperatorTable.orderByOperator);
        exclude.add(SqlStdOperatorTable.selectOperator);
        exclude.add(SqlStdOperatorTable.dotOperator);
        exclude.add(SqlStdOperatorTable.joinOperator);
        exclude.add(SqlStdOperatorTable.rowConstructor);
        exclude.add(SqlStdOperatorTable.newOperator);
        exclude.add(SqlStdOperatorTable.nullIfFunc);
        exclude.add(SqlStdOperatorTable.castFunc);
        exclude.add(SqlStdOperatorTable.coalesceFunc);
        exclude.add(SqlStdOperatorTable.overlayFunc);
        exclude.add(SqlStdOperatorTable.substringFunc);
        exclude.add(SqlStdOperatorTable.trimFunc);
        exclude.add(SqlStdOperatorTable.isDistinctFromOperator);
        exclude.add(SqlStdOperatorTable.descendingOperator);
        exclude.add(SqlStdOperatorTable.literalChainOperator);
        exclude.add(SqlStdOperatorTable.escapeOperator);
        exclude.add(SqlStdOperatorTable.localTimeFunc);
        exclude.add(SqlStdOperatorTable.localTimestampFunc);
        exclude.add(SqlStdOperatorTable.currentTimestampFunc);
        exclude.add(SqlStdOperatorTable.currentTimeFunc);
        exclude.add(SqlStdOperatorTable.minusDateOperator);
        exclude.add(SqlStdOperatorTable.throwOperator);
        exclude.add(SqlStdOperatorTable.reinterpretOperator);
        exclude.add(SqlStdOperatorTable.sliceOp);
        exclude.add(SqlStdOperatorTable.nextValueFunc);
        exclude.add(SqlStdOperatorTable.histogramMaxFunction);
        exclude.add(SqlStdOperatorTable.histogramMinFunction);
        exclude.add(SqlStdOperatorTable.histogramFirstValueFunction);
        exclude.add(SqlStdOperatorTable.histogramLastValueFunction);
        exclude.add(SqlStdOperatorTable.isDifferentFromOperator);
        exclude.add(SqlStdOperatorTable.divideIntegerOperator);

        // Eventually need to include these when cast is working
        exclude.add(SqlStdOperatorTable.overlapsOperator);
        exclude.add(SqlStdOperatorTable.initcapFunc);
        exclude.add(SqlStdOperatorTable.currentDateFunc);
        exclude.add(SqlStdOperatorTable.convertFunc);
        exclude.add(SqlStdOperatorTable.translateFunc);

        // --- NOTE ---
        // Do not add a function to this exclude list unless you first add a
        // test for it elsewhere.
        // ------------
        // iterating over all operators
        for (SqlOperator op : SqlStdOperatorTable.instance().getOperatorList()) {
            if (exclude.contains(op)) {
                continue;
            }

            addTestsForOp(op, suite, VirtualMachine.Fennel);
            addTestsForOp(op, suite, VirtualMachine.Java);
        }

        return wrappedSuite(suite);
    }

    private static void addTestsForOp(
        SqlOperator op,
        TestSuite suite,
        VirtualMachine vm)
        throws Exception
    {
        assert (null != op.getName()) : "Operator name must not be null";

        // Some operators cannot be implemented in all VMs.
        if (!vm.canImplement(op)) {
            return;
        }

        List<Integer> argCountList = op.getOperandCountRange().getAllowedList();
        assert (argCountList.size() > 0);

        // iterating over possible call signatures
        for (int n : argCountList) {
            SqlNode [] operands = new SqlNode[n];
            SqlOperandTypeChecker allowedTypes = op.getOperandTypeChecker();
            SqlTypeFamily [] families = findRules(allowedTypes);

            if (null == allowedTypes) {
                throw Util.needToImplement(
                    "Need to add to exclude list"
                    + " and manually add test");
            }

            for (int i = 0; i < n; i++) {
                SqlTypeName typeName =
                    (SqlTypeName) families[i].getTypeNames().iterator().next();
                if (typeName.equals(SqlTypeName.ANY)) {
                    typeName = SqlTypeName.BOOLEAN;
                }

                int precision = 0;
                if (typeName.allowsPrecNoScale()) {
                    precision = 1;
                }
                SqlDataTypeSpec dt =
                    new SqlDataTypeSpec(
                        new SqlIdentifier(
                            typeName.name(),
                            SqlParserPos.ZERO),
                        precision,
                        0,
                        null,
                        null,
                        SqlParserPos.ZERO);

                operands[i] =
                    SqlStdOperatorTable.castFunc.createCall(
                        SqlParserPos.ZERO,
                        SqlLiteral.createNull(SqlParserPos.ZERO),
                        dt);
            }

            if (operands.length == 0) {
                // What we're testing here is the rule 'if any of the operands
                // are null, the result should be null'. But if there are no
                // operands, that doesn't apply. So skip this form of the
                // operator.
                continue;
            }
            SqlCall call = op.createCall(SqlParserPos.ZERO, operands);

            String sql = "SELECT " + call.toString() + " FROM (VALUES(1))";
            String testName = "NULL-TEST-" + op.getName() + "-";
            suite.addTest(
                new FarragoCalcSystemTest(vm, sql, testName + vm.name));
        }
    }

    // REVIEW jvs 17-Mar-2005:  This whole thing is really hokey.
    private static SqlTypeFamily [] findRules(SqlOperandTypeChecker otc)
    {
        if (otc instanceof CompositeOperandTypeChecker) {
            SqlOperandTypeChecker rule =
                ((CompositeOperandTypeChecker) otc).getRules()[0];
            return findRules(rule);
        } else if (otc instanceof FamilyOperandTypeChecker) {
            return ((FamilyOperandTypeChecker) otc).getFamilies();
        } else {
            Integer nOperands =
                (Integer) otc.getOperandCountRange().getAllowedList().get(0);
            SqlTypeFamily [] families = new SqlTypeFamily[nOperands.intValue()];
            Arrays.fill(families, SqlTypeFamily.BOOLEAN);
            return families;
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
        Set<String> refSet = new HashSet<String>();
        refSet.add(null);
        compareResultSet(refSet);
    }

    //~ Inner Classes ----------------------------------------------------------

    /**
     * Defines a virtual machine (FENNEL, JAVA, AUTO) and the operators it can
     * implement.
     */
    public static class VirtualMachine
    {
        public static final VirtualMachine Fennel =
            new VirtualMachine("FENNEL");
        public static final VirtualMachine Java = new VirtualMachine("JAVA");
        public static final VirtualMachine Auto = new VirtualMachine("AUTO");
        private final String name;

        public VirtualMachine(String name)
        {
            this.name = name;
        }

        public String getName()
        {
            return name;
        }

        public String getAlterSystemCommand()
        {
            return "alter system set \"calcVirtualMachine\" = '"
                + "CALCVM_" + name + "'";
        }

        public boolean canImplement(SqlOperator op)
        {
            if (((this == Java) || (this == Auto))
                && (javaTab.get(op) != null))
            {
                return true;
            }
            if (((this == Fennel) || (this == Auto))
                && (fennelTab.get(op) != null))
            {
                return true;
            }
            if (this == Auto) {
                if (autoTab.get(op) != null) {
                    return true;
                }

                // This operator cannot be implemented at all!
                assert (false) : op + " cannot be implemented";
            }
            return false;
        }
    }
}

// End FarragoCalcSystemTest.java
