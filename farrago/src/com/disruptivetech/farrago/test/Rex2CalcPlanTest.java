/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2002-2007 Disruptive Tech
// Copyright (C) 2005-2007 The Eigenbase Project
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
package com.disruptivetech.farrago.test;

import com.disruptivetech.farrago.calc.*;
import com.disruptivetech.farrago.rel.*;

import java.util.*;

import junit.framework.*;

import org.eigenbase.rel.*;
import org.eigenbase.relopt.*;
import org.eigenbase.reltype.*;
import org.eigenbase.rex.*;
import org.eigenbase.sql.*;
import org.eigenbase.sql.fun.*;
import org.eigenbase.test.*;
import org.eigenbase.util.*;


/**
 * Validates that {@link RexNode} expressions get translated to the correct
 * calculator program.
 *
 * @author Wael Chatila
 * @version $Id$
 * @since Feb 3, 2004
 */
public class Rex2CalcPlanTest
    extends TestCase
{
    //~ Instance fields --------------------------------------------------------

    protected final Tester tester = createTester();

    //~ Constructors -----------------------------------------------------------

    public Rex2CalcPlanTest(String testName)
    {
        super(testName);
    }

    //~ Methods ----------------------------------------------------------------

    //--- Helper Functions ----------------------------------------------------

    protected Tester createTester()
    {
        return new TesterImpl();
    }

    protected DiffRepository getDiffRepos()
    {
        return DiffRepository.lookup(Rex2CalcPlanTest.class);
    }

    //--- Tests ------------------------------------------------
    public void testSimplePassThroughFilter()
    {
        String sql = "SELECT empno,empno FROM emp WHERE empno > 10";
        tester.check(sql, false, false);
    }

    public void testAggCount()
    {
        String sql = "SELECT COUNT(empno) FROM emp";
        tester.checkAgg(sql, false);
    }

    public void testAggSum()
    {
        tester.checkAgg("SELECT SUM(empno+5) FROM emp", false);
    }

    public void testAggSumExp()
    {
        tester.checkAgg(
            "SELECT empno, SUM(empno+deptno), COUNT(empno) "
            + "FROM emp "
            + "GROUP BY empno,deptno",
            false);
    }

    public void testWindowedMinMax()
    {
        tester.checkWinAgg(
            "SELECT\n"
            + " MIN(empno) OVER last3,\n"
            + " MAX(empno) OVER last3\n"
            + "FROM emp\n"
            + "WINDOW last3 AS (ORDER BY empno ROWS 3 PRECEDING)",
            true);
    }

    public void testWindowedFirstLastValue()
    {
        tester.checkWinAgg(
            "SELECT\n"
            + " FIRST_VALUE(empno) OVER last3,\n"
            + " LAST_VALUE(empno) OVER last3,\n"
            + " FIRST_VALUE(empno + 1) OVER last3 + 2,\n"
            + " 3 + MIN(empno + 1) OVER last3\n"
            + "FROM emp\n"
            + "WINDOW last3 AS (ORDER BY empno ROWS 3 PRECEDING)",
            true);
    }

    public void testWindowDisallowPartial()
    {
        tester.checkWinAgg(
            "SELECT\n"
            + " SUM(empno) OVER last3Full,\n"
            + " AVG(empno) OVER last3Full,\n"
            + " AVG(empno) OVER last3\n"
            + "FROM emp\n"
            + "WINDOW last3 AS (ORDER BY empno ROWS 3 PRECEDING),\n"
            + " last3Full AS (ORDER BY empno ROWS 3 PRECEDING DISALLOW PARTIAL)",
            true);
    }

    public void testSimplyEqualsFilter()
    {
        String sql = "select empno from emp where empno=123";
        tester.check(sql, true, false);
    }

    public void testSimplyEqualsFilterWithComments()
    {
        String sql = "select empno from emp where ename='Wael' and 123=0";
        tester.check(sql, false, false);
    }

    public void testSimplyEqualsFilterShortCircuit()
    {
        String sql = "select empno from emp where empno=123";
        tester.check(sql, true, true);
    }

    public void testBooleanExpressions()
    {
        //AND has higher precedence than OR
        String sql =
            "SELECT empno FROM emp WHERE true and not true or false and (not true and true)";
        tester.check(sql, false, false);
    }

    public void testScalarExpression()
    {
        String sql = "SELECT 2-2*2+2/2-2  FROM emp WHERE empno > 10";
        tester.check(sql, true, false);
    }

    public void testMixedExpression()
    {
        String sql =
            "SELECT ename, 2*2  FROM emp WHERE ename = 'Fred' AND  empno > 10";
        tester.check(sql, true, false);
    }

    public void testNumbers()
    {
        String sql =
            "SELECT "
            + "-(1+-2.*-3.e-1/-.4)>=+5, "
            + " 1e200 / 0.4"
            + "FROM emp WHERE empno > 10";
        tester.check(sql, false, false);
    }

    public void testHexBitBinaryString()
    {
        String sql = "SELECT X'0001'=x'FFeeDD' FROM emp WHERE empno > 10";
        tester.check(sql, false, false);
    }

    public void testStringLiterals()
    {
        String sql =
            "SELECT n'aBc',_iso-8859-1'', 'abc' FROM emp WHERE empno > 10";
        tester.check(sql, false, false);
    }

    public void testSimpleCompare()
    {
        String sql =
            "SELECT "
            + "1<>2"
            + ",1=2 is true is false is null is unknown"
            + ",true is not true "
            + ",true is not false"
            + ",true is not null "
            + ",true is not unknown"
            + " FROM emp WHERE empno > 10";
        tester.check(sql, false, false);
    }

    public void testArithmeticOperators()
    {
        String sql =
            "SELECT POWER(1.0,1.0), MOD(1,1), ABS(5000000000), ABS(1), "
            + "ABS(1.1), LN(1), LOG10(1) FROM emp WHERE empno > 10";
        tester.check(sql, false, false);
    }

    public void testFunctionInFunction()
    {
        String sql = "SELECT POWER(3.0, ABS(2)+1) FROM emp WHERE empno > 10";
        tester.check(sql, false, false);
    }

    public void testCaseExpressions()
    {
        String sql =
            "SELECT case 1+1 when 1 then 'wael' when 2 then 'waels clone' end"
            + ",case when 1=1 then 1+1+2 else 2+10 end"
            + " FROM emp WHERE empno > 10";
        tester.check(sql, false, false);
    }

    public void testNullifExpression()
    {
        String sql = "SELECT nullif(1,2) "
            + " FROM emp WHERE empno > 10";
        tester.check(sql, false, false);
    }

    public void testCoalesce()
    {
        String sql = "SELECT coalesce(1,2,3) FROM emp WHERE empno > 10";

        //CASE WHEN 1 IS NOT NULL THEN 1 ELSE (CASE WHEN 2 IS NOT NULL THEN 2
        //ELSE 3) END
        tester.check(sql, false, false);
    }

    public void testCase1()
    {
        String sql =
            "SELECT CASE WHEN TRUE THEN 0 ELSE 1 END "
            + "FROM emp WHERE empno > 10";
        tester.check(sql, false, false);
    }

    public void testCase2()
    {
        String sql1 =
            "SELECT CASE WHEN slacker then 2 when TRUE THEN 0 ELSE 1 END "
            + "FROM emp WHERE empno > 10";
        tester.check(sql1, false, false);
    }

    public void testCase3()
    {
        String sql2 =
            "select CASE 1 WHEN 1 THEN cast(null as integer) else 0 END "
            + "FROM emp WHERE empno > 10";
        tester.check(sql2, false, false);
    }

    public void testCharEq()
    {
        checkCharOp("=", "EQ");
    }

    public void testCharNe()
    {
        checkCharOp("<>", "NE");
    }

    public void testCharGt()
    {
        checkCharOp(">", "GT");
    }

    public void testCharLt()
    {
        checkCharOp("<", "LT");
    }

    public void testCharGe()
    {
        checkCharOp(">=", "GE");
    }

    public void testCharLe()
    {
        checkCharOp("<=", "LE");
    }

    private void checkCharOp(final String op, final String instr)
    {
        String sql = "SELECT 'a' " + op
            + "'b' FROM emp WHERE empno > 10";
        tester.check(sql, false, false);
    }

    public void testBinaryGt()
    {
        checkBinaryOp(">", "GT");
    }

    private void checkBinaryOp(final String op, final String instr)
    {
        String sql = "SELECT x'ff' " + op
            + "x'01' FROM emp WHERE empno > 10";
        tester.check(sql, false, false);
    }

    public void testStringFunctions()
    {
        String sql =
            "SELECT "
            + "char_length('a'),"
            + "upper('a'),"
            + "lower('a'),"
            + "position('a' in 'a'),"
            + "trim('a' from 'a'),"
            + "overlay('a' placing 'a' from 1),"
            + "substring('a' from 1),"
            + "substring(cast('a' as char(2)) from 1),"
            + "substring('a' from 1 for 10),"
            + (Bug.Frg296Fixed ? "substring('a' from 'a' for '\\' )," : "")
            + "'a'||'a'||'b'"
            + " FROM emp WHERE empno > 10";
        tester.check(sql, false, false);
    }

    public void testPosition()
    {
        String sql =
            "SELECT "
            + "position('a' in  cast('a' as char(1))),"
            + "position('a' in  cast('ba' as char(2))),"
            + "position(cast('abc' as char(3)) in 'bbabc')"
            + " FROM emp WHERE empno > 10";
        tester.check(sql, false, false);
    }

    public void testOverlay()
    {
        String sql =
            "SELECT "
            + "overlay('12' placing cast('abc' as char(3)) from 1),"
            + "overlay(cast('12' as char(3)) placing 'abc' from 1)"
            + " FROM emp WHERE empno > 10";
        tester.check(sql, false, false);
    }

    public void testLikeAndSimilar()
    {
        String sql =
            "SELECT "
            + "'a' like 'b'"
            + ",'a' like 'b' escape 'c'"
            + ",'a' similar to 'a'"
            + ",'a' similar to 'a' escape 'c'"
            + " FROM emp WHERE empno > 10";
        tester.check(sql, false, false);
    }

    public void testBetween1()
    {
        String sql1 = "select empno  from emp where empno between 40 and 60";
        tester.check(sql1, false, false);
    }

    public void testBetween2()
    {
        String sql2 =
            "select empno  from emp where empno between asymmetric 40 and 60";
        tester.check(sql2, false, false);
    }

    public void testBetween3()
    {
        String sql3 =
            "select empno  from emp where empno not between 40 and 60";
        tester.check(sql3, false, false);
    }

    public void testBetween4()
    {
        String sql4 =
            "select empno  from emp where empno between symmetric 40 and 60";
        tester.check(sql4, false, false);
    }

    public void testCastNull()
    {
        String sql =
            "SELECT "
            + "cast(null as varchar(1))"
            + ", cast(null as integer)"
            + " FROM emp WHERE empno > 10";
        tester.check(sql, false, false);
    }

    public void testJdbcFunctionSyntax()
    {
        String sql = "SELECT "
            + "{fn log(1.0)}"
            + " FROM emp WHERE empno > 10";
        tester.check(sql, false, false);
    }

    public void testMixingTypes()
    {
        String sql = "SELECT "
            + "1+1.0"
            + " FROM emp WHERE empno > 10";
        tester.check(sql, false, false);
    }

    public void testCastCharTypesToNumbersAndBack()
    {
        String sql =
            "SELECT "
            + "cast(123 as varchar(3))"
            + ",cast(123 as char(3))"
            + ",cast(12.3 as varchar(3))"
            + ",cast(12.3 as char(3))"
            + ",cast('123' as bigint)"
            + ",cast('123' as tinyint)"
            + ",cast('123' as double)"
            + " FROM emp WHERE empno > 10";
        tester.check(sql, false, false);
    }

    //~ Inner Interfaces -------------------------------------------------------

    /**
     * Test helper. The default implementation is {@link TesterImpl}, but
     * sub-tests can customize behavior by creating their own Tester.
     */
    interface Tester
        extends SqlToRelTestBase.Tester
    {
        /**
         * Compiles a SQL statement, and compares the generated calc program
         * with the contents of a reference file with the same name as the
         * current test.
         *
         * @param sql SQL statement. Must be of the form "<code>SELECT ... FROM
         * ... WHERE</code>".
         * @param nullSemantics If true, adds logic to ensure that a <code>
         * WHERE</code> clause which evalutes to <code>NULL</code> will filter
         * out rows (as if it had evaluated to <code>FALSE</code>).
         * @param shortCircuit Generate short-circuit logic to optimize logical
         * operations such as <code>AND</code> and <code>OR</OR> conditions.
         */
        void check(
            String sql,
            boolean nullSemantics,
            boolean shortCircuit);

        /**
         * Compiles a SQL statement containing windowed aggregation, and
         * compares the generated programs (for init, add, drop and output) with
         * the contents of a reference file.
         *
         * @param sql SQL statement. Must be of the form "<code>SELECT agg OVER
         * window, agg OVER window ... FROM ... WINDOW decl</code>".
         * @param shortCircuit Generate short-circuit logic to optimize logical
         * operations such as <code>AND</code> and <code>OR</OR> conditions.
         */
        void checkWinAgg(
            String sql,
            boolean shortCircuit);

        /**
         * Compiles a SQL statement containing aggregation, and compares the
         * generated programs (init, add, drop) with the contents of a reference
         * file.
         *
         * @param sql SQL statement. Must be of the form "<code>SELECT ... FROM
         * ... WHERE ... GROUP BY ...</code>".
         * @param shortCircuit Generate short-circuit logic to optimize logical
         * operations such as <code>AND</code> and <code>OR</OR> conditions.
         */
        void checkAgg(
            String sql,
            boolean shortCircuit);
    }

    //~ Inner Classes ----------------------------------------------------------

    /**
     * Implementation of {@link Tester}.
     */
    class TesterImpl
        extends SqlToRelTestBase.TesterImpl
        implements Tester
    {
        public void check(
            String sql,
            boolean nullSemantics,
            boolean shortCircuit)
        {
            boolean doComments = true;
            RelNode rootRel = convertSqlToRel(sql);

            RexBuilder rexBuilder = rootRel.getCluster().getRexBuilder();
            ProjectRel project = (ProjectRel) rootRel;
            RelNode input = project.getInput(0);

            RexNode condition;
            if (input instanceof FilterRel) {
                FilterRel filter = (FilterRel) project.getInput(0);
                condition = filter.getCondition();
                input = filter.getInput(0);
            } else {
                condition = null;
            }

            // Create a program builder, and add the project expressions.
            final RexProgramBuilder programBuilder =
                new RexProgramBuilder(
                    input.getRowType(),
                    rexBuilder);
            final RexNode [] projectExps = project.getProjectExps();
            for (int i = 0; i < projectExps.length; i++) {
                programBuilder.addProject(
                    projectExps[i],
                    project.getRowType().getFields()[i].getName());
            }

            // Add a condition, if any.
            if ((condition != null) && nullSemantics) {
                condition =
                    rexBuilder.makeCall(
                        SqlStdOperatorTable.isTrueOperator,
                        condition);
                condition =
                    new RexTransformer(condition, rexBuilder)
                    .transformNullSemantics();
            }
            if (condition != null) {
                programBuilder.addCondition(condition);
            }

            RexProgram program = programBuilder.getProgram();

            // rewrite decimals
            ReduceDecimalsRule rule = new ReduceDecimalsRule();
            RexShuttle shuttle = rule.new DecimalShuttle(rexBuilder);
            RexProgramBuilder updater =
                RexProgramBuilder.create(
                    rexBuilder,
                    program.getInputRowType(),
                    program.getExprList(),
                    program.getProjectList(),
                    program.getCondition(),
                    program.getOutputRowType(),
                    shuttle,
                    true);
            program = updater.getProgram();

            RexToCalcTranslator translator =
                new RexToCalcTranslator(rexBuilder, rootRel);
            translator.setGenerateShortCircuit(shortCircuit);
            translator.setGenerateComments(doComments);
            String actual = translator.generateProgram(null, program).trim();

            final DiffRepository diffRepos = getDiffRepos();
            diffRepos.assertEquals(
                "expectedProgram",
                "${expectedProgram}",
                TestUtil.NL + actual);
        }

        public void checkWinAgg(
            String sql,
            boolean shortCircuit)
        {
            RelNode rootRel = convertSqlToRel(sql);

            ProjectRel project = (ProjectRel) rootRel;
            RelNode input = project.getInput(0);

            RexNode condition;
            if (input instanceof FilterRel) {
                FilterRel filter = (FilterRel) project.getInput(0);
                condition = filter.getCondition();
                input = filter.getInput(0);
            } else {
                condition = null;
            }

            // Convert project to calc.
            CalcRel calcRel =
                (CalcRel) callRule(
                    ProjectToCalcRule.instance,
                    new RelNode[] { project });

            // Convert calc to calc/winagg/calc.
            CalcRel calcRel2 =
                (CalcRel) callRule(
                    WindowedAggSplitterRule.instance,
                    new RelNode[] { calcRel });
            final WindowedAggregateRel winAggRel =
                (WindowedAggregateRel) calcRel2.getInput(0);

            // Convert calc/winagg/calc to fennelwindow.
            final RelNode child = winAggRel.getInput(0);
            FennelWindowRel windowRel =
                (FennelWindowRel) callRule(
                    (child instanceof CalcRel)
                    ? FennelWindowRule.CalcOnWinOnCalc
                    : FennelWindowRule.CalcOnWin,
                    new RelNode[] { calcRel2, winAggRel, child });

            final String [] programs = windowRel.getSoleProgram();
            DiffRepository diffRepos = getDiffRepos();
            diffRepos.assertEqualsMulti(
                new String[] {
                    "expectedInit",
                    "expectedAdd",
                    "expectedDrop",
                    "expectedOutput",
                },
                new String[] {
                    "${expectedInit}",
                    "${expectedAdd}",
                    "${expectedDrop}",
                    "${expectedOutput}",
                },
                new String[] {
                    TestUtil.NL + programs[0],
                    TestUtil.NL + programs[1],
                    TestUtil.NL + programs[2],
                    TestUtil.NL + programs[3],
                },
                false);
        }

        private RelNode callRule(
            final RelOptRule rule,
            final RelNode [] rels)
        {
            final MyRelOptRuleCall ruleCall =
                new MyRelOptRuleCall(rule.getOperand(), rels);
            rule.onMatch(ruleCall);
            return ruleCall.rel;
        }

        public void checkAgg(
            String sql,
            boolean shortCircuit)
        {
            boolean doComments = true;
            RelNode rootRel = convertSqlToRel(sql);

            RexBuilder rexBuilder = rootRel.getCluster().getRexBuilder();
            AggregateRel aggregate;
            if (rootRel instanceof ProjectRel) {
                ProjectRel project = (ProjectRel) rootRel;
                aggregate = (AggregateRel) project.getInput(0);
            } else {
                aggregate = (AggregateRel) rootRel;
            }

            // Create a program builder, and add the project expressions.
            RelDataType inputRowType = aggregate.getInput(0).getRowType();
            final RexProgramBuilder programBuilder =
                new RexProgramBuilder(inputRowType, rexBuilder);
            int i = -1;
            for (AggregateCall aggCall : aggregate.getAggCallList()) {
                ++i;
                RexNode [] exprs = new RexNode[aggCall.getArgList().size()];
                for (int j = 0; j < aggCall.getArgList().size(); j++) {
                    int argOperand = aggCall.getArgList().get(j);
                    exprs[j] =
                        rexBuilder.makeInputRef(
                            inputRowType.getFields()[argOperand].getType(),
                            argOperand);
                }
                RexNode call =
                    rexBuilder.makeCall(
                        (SqlOperator) aggCall.getAggregation(),
                        exprs);
                programBuilder.addProject(
                    call,
                    aggregate.getRowType().getFields()[i].getName());
            }

            final RexProgram program = programBuilder.getProgram();

            RexToCalcTranslator translator =
                new RexToCalcTranslator(rexBuilder, aggregate);
            translator.setGenerateShortCircuit(shortCircuit);
            translator.setGenerateComments(doComments);

            DiffRepository diffRepos = getDiffRepos();
            diffRepos.assertEqualsMulti(
                new String[] {
                    "expectedInit",
                    "expectedAdd",
                    "expectedDrop",
                },
                new String[] {
                    "${expectedInit}",
                    "${expectedAdd}",
                    "${expectedDrop}",
                },
                new String[] {
                    TestUtil.NL + translator.getAggProgram(program, AggOp.Init),
                    TestUtil.NL + translator.getAggProgram(program, AggOp.Add),
                    TestUtil.NL + translator.getAggProgram(program, AggOp.Drop)
                },
                false);
        }

        private class MyRelOptRuleCall
            extends RelOptRuleCall
        {
            private RelNode rel;

            public MyRelOptRuleCall(
                RelOptRuleOperand operand,
                final RelNode [] rels)
            {
                super(
                    null,
                    operand,
                    rels,
                    Collections.<RelNode, List<RelNode>>emptyMap());
            }

            public void transformTo(RelNode rel)
            {
                this.rel = rel;
            }
        }
    }
}

// End Rex2CalcPlanTest.java
