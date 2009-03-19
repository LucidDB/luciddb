/*
// $Id$
// Package org.eigenbase is a class library of data management components.
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
package org.eigenbase.test;

import org.eigenbase.rel.*;
import org.eigenbase.relopt.hep.*;


/**
 * HepPlannerTest is a unit test for {@link HepPlanner}. See {@link
 * RelOptRulesTest} for an explanation of how to add tests; the tests in this
 * class are targeted at exercising the planner, and use specific rules for
 * convenience only, whereas the tests in that class are targeted at exercising
 * specific rules, and use the planner for convenience only. Hence the split.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class HepPlannerTest
    extends RelOptTestBase
{
    //~ Static fields/initializers ---------------------------------------------

    private static final String unionTree =
        "(select name from dept union select ename from emp)"
        + " union (select ename from bonus)";

    //~ Methods ----------------------------------------------------------------

    protected DiffRepository getDiffRepos()
    {
        return DiffRepository.lookup(HepPlannerTest.class);
    }

    public void testRuleClass()
        throws Exception
    {
        // Verify that an entire class of rules can be applied.

        HepProgramBuilder programBuilder = new HepProgramBuilder();
        programBuilder.addRuleClass(CoerceInputsRule.class);

        HepPlanner planner =
            new HepPlanner(
                programBuilder.createProgram());

        planner.addRule(new CoerceInputsRule(UnionRel.class, false));
        planner.addRule(new CoerceInputsRule(IntersectRel.class, false));

        checkPlanning(
            planner,
            "(select name from dept union select ename from emp)"
            + " intersect (select fname from customer.contact)");
    }

    public void testRuleDescription()
        throws Exception
    {
        // Verify that a rule can be applied via its description.

        HepProgramBuilder programBuilder = new HepProgramBuilder();
        programBuilder.addRuleByDescription("FilterToCalcRule");

        HepPlanner planner =
            new HepPlanner(
                programBuilder.createProgram());

        planner.addRule(FilterToCalcRule.instance);

        checkPlanning(
            planner,
            "select name from sales.dept where deptno=12");
    }

    public void testMatchLimitOneTopDown()
        throws Exception
    {
        // Verify that only the top union gets rewritten.

        HepProgramBuilder programBuilder = new HepProgramBuilder();
        programBuilder.addMatchOrder(HepMatchOrder.TOP_DOWN);
        programBuilder.addMatchLimit(1);
        programBuilder.addRuleInstance(new UnionToDistinctRule());

        checkPlanning(
            programBuilder.createProgram(),
            unionTree);
    }

    public void testMatchLimitOneBottomUp()
        throws Exception
    {
        // Verify that only the bottom union gets rewritten.

        HepProgramBuilder programBuilder = new HepProgramBuilder();
        programBuilder.addMatchLimit(1);
        programBuilder.addMatchOrder(HepMatchOrder.BOTTOM_UP);
        programBuilder.addRuleInstance(new UnionToDistinctRule());

        checkPlanning(
            programBuilder.createProgram(),
            unionTree);
    }

    public void testMatchUntilFixpoint()
        throws Exception
    {
        // Verify that both unions get rewritten.

        HepProgramBuilder programBuilder = new HepProgramBuilder();
        programBuilder.addMatchLimit(HepProgram.MATCH_UNTIL_FIXPOINT);
        programBuilder.addRuleInstance(new UnionToDistinctRule());

        checkPlanning(
            programBuilder.createProgram(),
            unionTree);
    }

    public void testReplaceCommonSubexpression()
        throws Exception
    {
        // Note that here it may look like the rule is firing
        // twice, but actually it's only firing once on the
        // common subexpression.  The purpose of this test
        // is to make sure the planner can deal with
        // rewriting something used as a common subexpression
        // twice by the same parent (the join in this case).

        checkPlanning(
            RemoveTrivialProjectRule.instance,
            "select d1.deptno from (select * from dept) d1,"
            + " (select * from dept) d2");
    }

    public void testSubprogram()
        throws Exception
    {
        // Verify that subprogram gets re-executed until fixpoint.
        // In this case, the first time through we limit it to generate
        // only one calc; the second time through it will generate
        // a second calc, and then merge them.
        HepProgramBuilder subprogramBuilder = new HepProgramBuilder();
        subprogramBuilder.addMatchOrder(HepMatchOrder.TOP_DOWN);
        subprogramBuilder.addMatchLimit(1);
        subprogramBuilder.addRuleInstance(ProjectToCalcRule.instance);
        subprogramBuilder.addRuleInstance(MergeCalcRule.instance);

        HepProgramBuilder programBuilder = new HepProgramBuilder();
        programBuilder.addSubprogram(subprogramBuilder.createProgram());

        checkPlanning(
            programBuilder.createProgram(),
            "select upper(ename) from (select lower(ename) as ename from emp)");
    }

    public void testGroup()
        throws Exception
    {
        // Verify simultaneous application of a group of rules.
        // Intentionally add them in the wrong order to make sure
        // that order doesn't matter within the group.
        HepProgramBuilder programBuilder = new HepProgramBuilder();
        programBuilder.addGroupBegin();
        programBuilder.addRuleInstance(MergeCalcRule.instance);
        programBuilder.addRuleInstance(ProjectToCalcRule.instance);
        programBuilder.addRuleInstance(FilterToCalcRule.instance);
        programBuilder.addGroupEnd();

        checkPlanning(
            programBuilder.createProgram(),
            "select upper(name) from dept where deptno=20");
    }
}

// End HepPlannerTest.java
