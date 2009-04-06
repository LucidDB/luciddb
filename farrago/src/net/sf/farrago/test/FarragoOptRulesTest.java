/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2006-2009 The Eigenbase Project
// Copyright (C) 2006-2009 SQLstream, Inc.
// Copyright (C) 2006-2009 LucidEra, Inc.
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

import net.sf.farrago.fennel.rel.*;

import java.util.*;
import java.util.logging.*;

import junit.framework.*;

import net.sf.farrago.query.*;
import net.sf.farrago.session.*;
import net.sf.farrago.trace.*;

import org.eigenbase.oj.rel.*;
import org.eigenbase.rel.*;
import org.eigenbase.rel.rules.*;
import org.eigenbase.relopt.*;
import org.eigenbase.relopt.hep.*;
import org.eigenbase.test.*;


/**
 * FarragoOptRulesTest is like {@link RelOptRulesTest}, but for rules specific
 * to Farrago.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class FarragoOptRulesTest
    extends FarragoSqlToRelTestBase
{
    //~ Static fields/initializers ---------------------------------------------

    protected static final String NL = System.getProperty("line.separator");

    private static boolean doneStaticSetup;

    //~ Instance fields --------------------------------------------------------

    private HepProgram program;

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a new FarragoOptRulesTest object.
     *
     * @param testName JUnit test name
     *
     * @throws Exception .
     */
    public FarragoOptRulesTest(String testName)
        throws Exception
    {
        super(testName);
    }

    //~ Methods ----------------------------------------------------------------

    // implement TestCase
    public static Test suite()
    {
        return wrappedSuite(FarragoOptRulesTest.class);
    }

    // implement TestCase
    protected void setUp()
        throws Exception
    {
        super.setUp();
        if (doneStaticSetup) {
            return;
        }
        doneStaticSetup = true;

        Level vizLevel = FarragoTrace.getPlannerVizTracer().getLevel();
        if (vizLevel == null) {
            return;
        }

        stmt.executeUpdate(
            "create schema plannerviz");
        stmt.executeUpdate(
            "create jar plannerviz.plannerviz_plugin"
            + " library 'file:examples/plannerviz/plugin/FarragoPlannerviz.jar'"
            + " options(0)");
        stmt.executeUpdate(
            "alter session implementation set jar"
            + " plannerviz.plannerviz_plugin");
    }

    protected DiffRepository getDiffRepos()
    {
        return DiffRepository.lookup(FarragoOptRulesTest.class);
    }

    protected void checkAbstract(
        FarragoPreparingStmt stmt,
        RelNode relBefore)
        throws Exception
    {
        final DiffRepository diffRepos = getDiffRepos();

        String planBefore = NL + RelOptUtil.toString(relBefore);
        diffRepos.assertEquals("planBefore", "${planBefore}", planBefore);

        RelOptPlanner planner = stmt.getPlanner();
        planner.setRoot(relBefore);
        RelNode relAfter = planner.findBestExp();

        String planAfter = NL + RelOptUtil.toString(relAfter);
        diffRepos.assertEquals("planAfter", "${planAfter}", planAfter);
    }

    private void check(
        HepProgram program,
        String sql)
        throws Exception
    {
        check(program, sql, null);
    }

    private void check(
        HepProgram program,
        String sql,
        List<RelOptRule> rules)
        throws Exception
    {
        this.program = program;

        final DiffRepository diffRepos = getDiffRepos();
        String sql2 = diffRepos.expand("sql", sql);

        String explainQuery = "EXPLAIN PLAN WITHOUT IMPLEMENTATION FOR " + sql2;

        addRulesAndCheckQuery(explainQuery, rules);
    }

    private void check(
        RelOptRule rule,
        String sql)
        throws Exception
    {
        HepProgramBuilder programBuilder = new HepProgramBuilder();
        programBuilder.addRuleInstance(rule);

        check(
            programBuilder.createProgram(),
            sql);
    }

    protected void initPlanner(FarragoPreparingStmt stmt)
    {
        FarragoSessionPlanner planner =
            new FarragoTestPlanner(
                program,
                stmt);
        stmt.setPlanner(planner);
    }

    public void testMergeFilterWithJoinCondition()
        throws Exception
    {
        HepProgramBuilder programBuilder = new HepProgramBuilder();
        programBuilder.addRuleInstance(TableAccessRule.instance);
        programBuilder.addRuleInstance(ExtractJoinFilterRule.instance);
        programBuilder.addRuleInstance(FilterToCalcRule.instance);
        programBuilder.addRuleInstance(MergeCalcRule.instance);
        programBuilder.addRuleInstance(FennelCalcRule.instance);
        programBuilder.addRuleInstance(FennelCartesianJoinRule.instance);
        programBuilder.addRuleInstance(ProjectToCalcRule.instance);
        programBuilder.addRuleInstance(IterRules.IterCalcRule.instance);
        programBuilder.addRuleInstance(FennelToIteratorConverter.Rule);

        check(
            programBuilder.createProgram(),
            "select d.name as dname,e.name as ename"
            + " from sales.emps e inner join sales.depts d"
            + " on e.deptno=d.deptno"
            + " where d.name='Propane'");
    }

    public void testHeterogeneousConversion()
        throws Exception
    {
        // This one tests the planner's ability to correctly
        // apply different converters on top of a common
        // subexpression.  The common subexpression is the
        // reference to the table sales.emps.  On top of that
        // are two projections, unioned at the top.  For one
        // of the projections, we force a Fennel implementation.
        // For the other, we force a Java implementation.
        // Then, we request conversion from Fennel to Java,
        // and verify that it only applies to one usage of the
        // table, not both (which would be incorrect).

        HepProgramBuilder programBuilder = new HepProgramBuilder();
        programBuilder.addRuleInstance(TableAccessRule.instance);
        programBuilder.addRuleInstance(ProjectToCalcRule.instance);

        // Control the calc conversion.
        programBuilder.addMatchLimit(1);
        programBuilder.addRuleInstance(IterRules.IterCalcRule.instance);
        programBuilder.addRuleInstance(FennelCalcRule.instance);

        // Let the converter rule fire to its heart's content.
        programBuilder.addMatchLimit(HepProgram.MATCH_UNTIL_FIXPOINT);
        programBuilder.addRuleInstance(FennelToIteratorConverter.Rule);

        check(
            programBuilder.createProgram(),
            "select upper(name) from sales.emps union all"
            + " select lower(name) from sales.emps");
    }

    public void testFennelToIteratorConverterRule()
        throws Exception
    {
        HepProgramBuilder programBuilder = new HepProgramBuilder();
        programBuilder.addRuleInstance(TableAccessRule.instance);
        programBuilder.addRuleInstance(ProjectToCalcRule.instance);
        programBuilder.addRuleInstance(IterRules.IterCalcRule.instance);
        programBuilder.addRuleInstance(FennelToIteratorConverter.Rule);
        check(
            programBuilder.createProgram(),
            "select upper(name) from sales.emps");
    }

    public void testFtrsScanToSearchRule()
        throws Exception
    {
        HepProgramBuilder programBuilder = new HepProgramBuilder();
        programBuilder.addRuleInstance(TableAccessRule.instance);
        programBuilder.addRuleByDescription("FtrsScanToSearchRule");
        check(
            programBuilder.createProgram(),
            "select * from sales.depts where deptno=5");
    }

    public void testFennelSortRule()
        throws Exception
    {
        check(
            FennelSortRule.instance,
            "select * from sales.depts order by deptno");
    }

    public void testFennelCartesianJoinRule()
        throws Exception
    {
        check(
            FennelCartesianJoinRule.instance,
            "select 1 from sales.emps,sales.depts");
    }

    public void testFennelAggRule()
        throws Exception
    {
        check(
            FennelAggRule.instance,
            "select deptno from sales.depts group by deptno");
    }

    public void testFtrsTableAccessRule()
        throws Exception
    {
        check(
            TableAccessRule.instance,
            "select deptno from sales.depts");
    }

    public void testFtrsTableProjectionRule()
        throws Exception
    {
        // This one also tests the planner's ability to deal with
        // a rule which produces multiple alternatives.  That would
        // be better done in HepPlannerTest, but org.eigebase doesn't
        // contain any rules like that.  In this case, the planner
        // picks the unclustered index scan over the clustered index
        // scan because the unclustered index is narrower.

        HepProgramBuilder programBuilder = new HepProgramBuilder();
        programBuilder.addRuleInstance(TableAccessRule.instance);
        programBuilder.addRuleByDescription("FtrsTableProjectionRule");
        check(
            programBuilder.createProgram(),
            "select name from sales.emps");
    }

    public void testPushSemiJoinPastJoinRule_Left()
        throws Exception
    {
        // tests the case where the semijoin is pushed to the left
        HepProgramBuilder programBuilder = new HepProgramBuilder();
        programBuilder.addRuleInstance(PushFilterPastJoinRule.instance);
        programBuilder.addRuleInstance(AddRedundantSemiJoinRule.instance);
        programBuilder.addRuleInstance(PushSemiJoinPastJoinRule.instance);
        check(
            programBuilder.createProgram(),
            "select e1.name from sales.emps e1, sales.depts d, sales.emps e2 "
            + "where e1.deptno = d.deptno and e1.empno = e2.empno");
    }

    public void testPushSemiJoinPastJoinRule_Right()
        throws Exception
    {
        // tests the case where the semijoin is pushed to the right
        HepProgramBuilder programBuilder = new HepProgramBuilder();
        programBuilder.addRuleInstance(PushFilterPastJoinRule.instance);
        programBuilder.addRuleInstance(AddRedundantSemiJoinRule.instance);
        programBuilder.addRuleInstance(PushSemiJoinPastJoinRule.instance);
        check(
            programBuilder.createProgram(),
            "select e1.name from sales.emps e1, sales.depts d, sales.emps e2 "
            + "where e1.deptno = d.deptno and d.deptno = e2.deptno");
    }

    public void testPushSemiJoinPastFilter()
        throws Exception
    {
        HepProgramBuilder programBuilder = new HepProgramBuilder();
        programBuilder.addRuleInstance(PushFilterPastJoinRule.instance);
        programBuilder.addRuleInstance(AddRedundantSemiJoinRule.instance);
        programBuilder.addRuleInstance(PushSemiJoinPastFilterRule.instance);
        check(
            programBuilder.createProgram(),
            "select e.name from sales.emps e, sales.depts d "
            + "where e.deptno = d.deptno and e.name = 'foo'");
    }

    public void testConvertMultiJoinRule()
        throws Exception
    {
        HepProgramBuilder programBuilder = new HepProgramBuilder();
        programBuilder.addRuleInstance(PushFilterPastJoinRule.instance);
        programBuilder.addMatchOrder(HepMatchOrder.BOTTOM_UP);
        programBuilder.addRuleInstance(ConvertMultiJoinRule.instance);
        check(
            programBuilder.createProgram(),
            "select e1.name from sales.emps e1, sales.depts d, sales.emps e2 "
            + "where e1.deptno = d.deptno and d.deptno = e2.deptno");
    }

    public void testReduceConstants()
        throws Exception
    {
        HepProgramBuilder programBuilder = new HepProgramBuilder();
        programBuilder.addRuleInstance(
            FarragoReduceExpressionsRule.projectInstance);
        programBuilder.addRuleInstance(
            FarragoReduceExpressionsRule.filterInstance);
        programBuilder.addRuleInstance(
            FarragoReduceExpressionsRule.joinInstance);

        // NOTE jvs 27-May-2006: among other things, this verifies
        // intentionally different treatment for identical coalesce expression
        // in select and where.

        check(
            programBuilder.createProgram(),
            "select 1+2, d.deptno+(3+4), (5+6)+d.deptno, cast(null as integer),"
            + " coalesce(2,null), row(7+8)"
            + " from sales.depts d inner join sales.emps e"
            + " on d.deptno = e.deptno + (5-5)"
            + " where d.deptno=(7+8) and d.deptno=coalesce(2,null)");
    }

    public void testReduceConstantsEliminatesFilter()
        throws Exception
    {
        HepProgramBuilder programBuilder = new HepProgramBuilder();
        programBuilder.addRuleInstance(
            FarragoReduceExpressionsRule.filterInstance);

        // WHERE NULL is the same as WHERE FALSE, so get empty result
        check(
            programBuilder.createProgram(),
            "select * from (values (1,2)) where 1 + 2 > 3 + CAST(NULL AS INTEGER)");
    }

    public void testAlreadyFalseEliminatesFilter()
        throws Exception
    {
        HepProgramBuilder programBuilder = new HepProgramBuilder();
        programBuilder.addRuleInstance(
            FarragoReduceExpressionsRule.filterInstance);

        check(
            programBuilder.createProgram(),
            "select * from (values (1,2)) where false");
    }

    public void testReduceConstantsCalc()
        throws Exception
    {
        // This reduction does not work using
        // FarragoReduceExpressionsRule.projectInstance or filterInstance,
        // only calcInstance, because we need to pull the project expression
        //    upper('table')
        // into the condition
        //    upper('table') = 'TABLE'
        // and reduce it to TRUE. Only in the Calc are projects and conditions
        // combined.
        HepProgramBuilder programBuilder = new HepProgramBuilder();
        programBuilder.addRuleInstance(PushFilterPastProjectRule.instance);
        programBuilder.addRuleInstance(PushFilterPastSetOpRule.instance);
        programBuilder.addRuleInstance(FilterToCalcRule.instance);
        programBuilder.addRuleInstance(ProjectToCalcRule.instance);
        programBuilder.addRuleInstance(MergeCalcRule.instance);
        programBuilder.addRuleInstance(
            FarragoReduceExpressionsRule.calcInstance);

        // the hard part is done... a few more rule calls to clean up
        programBuilder.addRuleInstance(RemoveEmptyRule.unionInstance);
        programBuilder.addRuleInstance(ProjectToCalcRule.instance);
        programBuilder.addRuleInstance(MergeCalcRule.instance);
        programBuilder.addRuleInstance(
            FarragoReduceExpressionsRule.calcInstance);

        // Result should be same as typing
        //  SELECT * FROM (VALUES ('TABLE        ', 'T')) AS T(U, S)
        check(
            programBuilder.createProgram(),
            "select * from (\n"
            + "  select upper(substring(x FROM 1 FOR 2) || substring(x FROM 3)) as u,\n"
            + "      substring(x FROM 1 FOR 1) as s\n"
            + "  from (\n"
            + "    select 'table' as x from (values (true))\n"
            + "    union\n"
            + "    select 'view' from (values (true))\n"
            + "    union\n"
            + "    select 'foreign table' from (values (true))\n"
            + "  )\n"
            + ") where u = 'TABLE'");
    }

    public void testRemoveSemiJoin()
        throws Exception
    {
        HepProgramBuilder programBuilder = new HepProgramBuilder();
        programBuilder.addRuleInstance(PushFilterPastJoinRule.instance);
        programBuilder.addRuleInstance(AddRedundantSemiJoinRule.instance);
        programBuilder.addRuleInstance(RemoveSemiJoinRule.instance);
        check(
            programBuilder.createProgram(),
            "select e.name from sales.emps e, sales.depts d "
            + "where e.deptno = d.deptno");
    }

    public void testRemoveSemiJoinWithFilter()
        throws Exception
    {
        HepProgramBuilder programBuilder = new HepProgramBuilder();
        programBuilder.addRuleInstance(PushFilterPastJoinRule.instance);
        programBuilder.addRuleInstance(AddRedundantSemiJoinRule.instance);
        programBuilder.addRuleInstance(PushSemiJoinPastFilterRule.instance);
        programBuilder.addRuleInstance(RemoveSemiJoinRule.instance);
        check(
            programBuilder.createProgram(),
            "select e.name from sales.emps e, sales.depts d "
            + "where e.deptno = d.deptno and e.name = 'foo'");
    }

    public void testRemoveSemiJoinRight()
        throws Exception
    {
        HepProgramBuilder programBuilder = new HepProgramBuilder();
        programBuilder.addRuleInstance(PushFilterPastJoinRule.instance);
        programBuilder.addRuleInstance(AddRedundantSemiJoinRule.instance);
        programBuilder.addRuleInstance(PushSemiJoinPastJoinRule.instance);
        programBuilder.addRuleInstance(RemoveSemiJoinRule.instance);
        check(
            programBuilder.createProgram(),
            "select e1.name from sales.emps e1, sales.depts d, sales.emps e2 "
            + "where e1.deptno = d.deptno and d.deptno = e2.deptno");
    }

    public void testRemoveSemiJoinRightWithFilter()
        throws Exception
    {
        HepProgramBuilder programBuilder = new HepProgramBuilder();
        programBuilder.addRuleInstance(PushFilterPastJoinRule.instance);
        programBuilder.addRuleInstance(AddRedundantSemiJoinRule.instance);
        programBuilder.addRuleInstance(PushSemiJoinPastJoinRule.instance);
        programBuilder.addRuleInstance(PushSemiJoinPastFilterRule.instance);
        programBuilder.addRuleInstance(RemoveSemiJoinRule.instance);
        check(
            programBuilder.createProgram(),
            "select e1.name from sales.emps e1, sales.depts d, sales.emps e2 "
            + "where e1.deptno = d.deptno and d.deptno = e2.deptno "
            + "and d.name = 'foo'");
    }

    public void testConvertMultiJoinRuleOuterJoins()
        throws Exception
    {
        stmt.executeUpdate("create schema oj");
        stmt.executeUpdate("set schema 'oj'");
        stmt.executeUpdate(
            "create table A(a int primary key)");
        stmt.executeUpdate(
            "create table B(b int primary key)");
        stmt.executeUpdate(
            "create table C(c int primary key)");
        stmt.executeUpdate(
            "create table D(d int primary key)");
        stmt.executeUpdate(
            "create table E(e int primary key)");
        stmt.executeUpdate(
            "create table F(f int primary key)");
        stmt.executeUpdate(
            "create table G(g int primary key)");
        stmt.executeUpdate(
            "create table H(h int primary key)");
        stmt.executeUpdate(
            "create table I(i int primary key)");
        stmt.executeUpdate(
            "create table J(j int primary key)");

        HepProgramBuilder programBuilder = new HepProgramBuilder();
        programBuilder.addMatchOrder(HepMatchOrder.BOTTOM_UP);
        programBuilder.addRuleInstance(RemoveTrivialProjectRule.instance);
        programBuilder.addRuleInstance(ConvertMultiJoinRule.instance);
        check(
            programBuilder.createProgram(),
            "select * from "
            + "    (select * from "
            + "        (select * from "
            + "            (select * from A right outer join B on a = b) "
            + "            left outer join "
            + "            (select * from C full outer join D on c = d)"
            + "            on a = c and b = d) "
            + "        right outer join "
            + "        (select * from "
            + "            (select * from E full outer join F on e = f) "
            + "            right outer join "
            + "            (select * from G left outer join H on g = h) "
            + "            on e = g and f = h) "
            + "        on a = e and b = f and c = g and d = h) "
            + "    inner join "
            + "    (select * from I inner join J on i = j) "
            + "    on a = i and h = j");
    }

    public void testPushSemiJoinPastProject()
        throws Exception
    {
        HepProgramBuilder programBuilder = new HepProgramBuilder();
        programBuilder.addRuleInstance(PushFilterPastJoinRule.instance);
        programBuilder.addRuleInstance(AddRedundantSemiJoinRule.instance);
        programBuilder.addRuleInstance(PushSemiJoinPastProjectRule.instance);
        check(
            programBuilder.createProgram(),
            "select e.* from "
            + "(select name, trim(city), age * 2, deptno from sales.emps) e, "
            + "sales.depts d "
            + "where e.deptno = d.deptno");
    }

    public void testReduceValuesUnderFilter()
        throws Exception
    {
        HepProgramBuilder programBuilder = new HepProgramBuilder();
        programBuilder.addRuleInstance(PushFilterPastProjectRule.instance);
        programBuilder.addRuleInstance(FarragoReduceValuesRule.filterInstance);

        // Plan should be same as for
        // select a, b from (values (10,'x')) as t(a, b)");
        check(
            programBuilder.createProgram(),
            "select a, b from (values (10, 'x'), (20, 'y')) as t(a, b) where a < 15");
    }

    public void testReduceValuesUnderProject()
        throws Exception
    {
        HepProgramBuilder programBuilder = new HepProgramBuilder();
        programBuilder.addRuleInstance(MergeProjectRule.instance);
        programBuilder.addRuleInstance(
            FarragoReduceValuesRule.projectInstance);

        // Plan should be same as for
        // select a, b as x from (values (11), (23)) as t(x)");
        check(
            programBuilder.createProgram(),
            "select a + b from (values (10, 1), (20, 3)) as t(a, b)");
    }

    public void testReduceValuesUnderProjectFilter()
        throws Exception
    {
        HepProgramBuilder programBuilder = new HepProgramBuilder();
        programBuilder.addRuleInstance(PushFilterPastProjectRule.instance);
        programBuilder.addRuleInstance(MergeProjectRule.instance);
        programBuilder.addRuleInstance(
            FarragoReduceValuesRule.projectFilterInstance);

        // Plan should be same as for
        // select * from (values (11, 1, 10), (23, 3, 20)) as t(x, b, a)");
        check(
            programBuilder.createProgram(),
            "select a + b as x, b, a from (values (10, 1), (30, 7), (20, 3)) as t(a, b)"
            + " where a - b < 21");
    }

    public void testReduceValuesNull()
        throws Exception
    {
        // The NULL literal presents pitfalls for value-reduction. Only
        // an INSERT statement contains un-CASTed NULL values.
        HepProgramBuilder programBuilder = new HepProgramBuilder();
        programBuilder.addRuleInstance(
            FarragoReduceValuesRule.projectInstance);

        check(
            programBuilder.createProgram(),
            "insert into sales.depts(deptno,name) values (NULL, 'null')");
    }

    public void testReduceValuesToEmpty()
        throws Exception
    {
        HepProgramBuilder programBuilder = new HepProgramBuilder();
        programBuilder.addRuleInstance(PushFilterPastProjectRule.instance);
        programBuilder.addRuleInstance(MergeProjectRule.instance);
        programBuilder.addRuleInstance(
            FarragoReduceValuesRule.projectFilterInstance);

        // Plan should be same as for
        // select * from (values (11, 1, 10), (23, 3, 20)) as t(x, b, a)");
        check(
            programBuilder.createProgram(),
            "select a + b as x, b, a from (values (10, 1), (30, 7)) as t(a, b)"
            + " where a - b < 0");
    }

    public void testEmptyFilterProjectUnion()
        throws Exception
    {
        HepProgramBuilder programBuilder = new HepProgramBuilder();
        programBuilder.addRuleInstance(PushFilterPastSetOpRule.instance);
        programBuilder.addRuleInstance(PushFilterPastProjectRule.instance);
        programBuilder.addRuleInstance(MergeProjectRule.instance);
        programBuilder.addRuleInstance(
            FarragoReduceValuesRule.projectFilterInstance);
        programBuilder.addRuleInstance(RemoveEmptyRule.projectInstance);
        programBuilder.addRuleInstance(RemoveEmptyRule.unionInstance);

        // Plan should be same as for
        // select * from (values (30, 3)) as t(x, y)");
        check(
            programBuilder.createProgram(),
            "select * from (\n"
            + "select * from (values (10, 1), (30, 3)) as t (x, y)\n"
            + "union all\n"
            + "select * from (values (20, 2))\n"
            + ")\n"
            + "where x + y > 30");
    }

    public void testReduceCasts()
        throws Exception
    {
        HepProgramBuilder programBuilder = new HepProgramBuilder();
        programBuilder.addRuleInstance(
            FarragoReduceExpressionsRule.projectInstance);
        programBuilder.addRuleInstance(
            FarragoReduceExpressionsRule.filterInstance);
        programBuilder.addRuleInstance(
            FarragoReduceExpressionsRule.joinInstance);

        // The resulting plan should have no cast expressions
        check(
            programBuilder.createProgram(),
            "select cast(d.name as varchar(128)), cast(e.empno as integer) "
            + "from sales.depts d inner join sales.emps e "
            + "on cast(d.deptno as integer) = cast(e.deptno as integer) "
            + "where cast(e.gender as char(1)) = 'M'");
    }

    public void testReduceCastAndConsts()
        throws Exception
    {
        HepProgramBuilder programBuilder = new HepProgramBuilder();
        programBuilder.addRuleInstance(
            FarragoReduceExpressionsRule.filterInstance);

        // Make sure constant expressions inside the cast can be reduced
        // in addition to the casts.
        check(
            programBuilder.createProgram(),
            "select * from sales.emps "
            + "where cast((empno + (10/2)) as int) = 13");
    }

    public void testReduceCastsNullable()
        throws Exception
    {
        HepProgramBuilder programBuilder = new HepProgramBuilder();

        // Simulate the way INSERT will insert casts to the target types
        programBuilder.addRuleInstance(
            new CoerceInputsRule(TableModificationRel.class, false));

        // Convert projects to calcs, merge two calcs, and then
        // reduce redundant casts in merged calc.
        programBuilder.addRuleInstance(ProjectToCalcRule.instance);
        programBuilder.addRuleInstance(MergeCalcRule.instance);
        programBuilder.addRuleInstance(
            FarragoReduceExpressionsRule.calcInstance);
        check(
            programBuilder.createProgram(),
            "insert into sales.depts(name) "
            + "select cast(gender as varchar(128)) from sales.emps");
    }

    public void testFennelBufferCommonRelSubExprRule()
        throws Exception
    {
        HepProgramBuilder programBuilder = new HepProgramBuilder();

        // Explicitly add the instruction to apply common subexpression rules,
        // so FennelBufferCommonRelSubExprRule will only be applied on
        // subexpressions that appear more than once in the query.  Adding
        // FennelBufferCommonRelSubExprRule as a rule instance in the program
        // builder will incorrectly result in the rule being applied on all
        // RelNodes.
        programBuilder.addCommonRelSubExprInstruction();

        List<RelOptRule> rules = new ArrayList<RelOptRule>();
        rules.add(FennelBufferCommonRelSubExprRule.instance);

        // Make sure the common subexpression is complex enough so buffering
        // is beneficial.  Note that the 2nd sub-select is not the same as
        // the other two, so it won't get buffered.
        check(
            programBuilder.createProgram(),
            "select * from "
            + "(select * from sales.emps e, sales.depts d "
            + "   where e.deptno = d.deptno), "
            + "(select * from sales.depts d, sales.emps e "
            + "   where e.deptno = d.deptno), "
            + "(select * from sales.emps e, sales.depts d "
            + "   where e.deptno = d.deptno)",
            rules);
    }
}

// End FarragoOptRulesTest.java
