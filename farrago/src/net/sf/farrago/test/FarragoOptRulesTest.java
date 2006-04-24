/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2006-2006 The Eigenbase Project
// Copyright (C) 2006-2006 Disruptive Tech
// Copyright (C) 2006-2006 LucidEra, Inc.
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

import org.eigenbase.test.*;
import org.eigenbase.rel.*;
import org.eigenbase.rel.rules.*;
import org.eigenbase.relopt.*;
import org.eigenbase.relopt.hep.*;
import org.eigenbase.oj.rel.*;

import net.sf.farrago.query.*;
import net.sf.farrago.session.*;
import net.sf.farrago.trace.*;
import net.sf.farrago.namespace.ftrs.*;

import com.disruptivetech.farrago.rel.*;

import junit.framework.*;

import java.util.logging.*;

/**
 * FarragoOptRulesTest is like {@link RelOptRulesTest}, but for
 * rules specific to Farrago.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class FarragoOptRulesTest extends FarragoSqlToRelTestBase
{
    protected static final String NL = System.getProperty("line.separator");
    
    private HepProgram program;

    private static boolean doneStaticSetup;
    
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
        this.program = program;
        
        final DiffRepository diffRepos = getDiffRepos();
        String sql2 = diffRepos.expand("sql", sql);

        String explainQuery =
            "EXPLAIN PLAN WITHOUT IMPLEMENTATION FOR " + sql2;

        checkQuery(explainQuery);
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
        FarragoSessionPlanner planner = new FarragoTestPlanner(
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
        programBuilder.addRuleInstance(new FennelCartesianJoinRule());
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
            new FennelSortRule(),
            "select * from sales.depts order by deptno");
    }

    public void testFennelCartesianJoinRule()
        throws Exception
    {
        check(
            new FennelCartesianJoinRule(),
            "select 1 from sales.emps,sales.depts");
    }

    public void testFennelAggRule()
        throws Exception
    {
        check(
            new FennelAggRule(),
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
        programBuilder.addRuleInstance(new PushFilterRule());
        programBuilder.addRuleInstance(new AddRedundantSemiJoinRule());
        programBuilder.addRuleInstance(new PushSemiJoinPastJoinRule());
        check(
            programBuilder.createProgram(),
            "select e1.name from sales.emps e1, sales.depts d, sales.emps e2 " +
            "where e1.deptno = d.deptno and e1.empno = e2.empno");
    }
    
    public void testPushSemiJoinPastJoinRule_Right()
        throws Exception
    {
        // tests the case where the semijoin is pushed to the right
        HepProgramBuilder programBuilder = new HepProgramBuilder();
        programBuilder.addRuleInstance(new PushFilterRule());
        programBuilder.addRuleInstance(new AddRedundantSemiJoinRule());
        programBuilder.addRuleInstance(new PushSemiJoinPastJoinRule());
        check(
            programBuilder.createProgram(),
            "select e1.name from sales.emps e1, sales.depts d, sales.emps e2 " +
        "where e1.deptno = d.deptno and d.deptno = e2.deptno");
    }
}

// End FarragoOptRulesTest.java
