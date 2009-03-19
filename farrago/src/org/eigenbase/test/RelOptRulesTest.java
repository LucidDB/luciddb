/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2006-2007 The Eigenbase Project
// Copyright (C) 2006-2007 SQLstream, Inc.
// Copyright (C) 2006-2007 LucidEra, Inc.
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
import org.eigenbase.rel.rules.*;


/**
 * Unit test for rules in {@link org.eigenbase.rel} and subpackages.
 *
 * <p>As input, the test supplies a SQL statement and a single rule; the SQL is
 * translated into relational algebra and then fed into a {@link
 * org.eigenbase.relopt.hep.HepPlanner}. The planner fires the rule on every
 * pattern match in a depth-first left-to-right preorder traversal of the tree
 * for as long as the rule continues to succeed in applying its transform. (For
 * rules which call transformTo more than once, only the last result is used.)
 * The plan before and after "optimization" is diffed against a .ref file using
 * {@link DiffRepository}.
 *
 * <p>Procedure for adding a new test case:
 *
 * <ol>
 * <li>Add a new public test method for your rule, following the existing
 * examples. You'll have to come up with an SQL statement to which your rule
 * will apply in a meaningful way. See {@link SqlToRelTestBase} class comments
 * for details on the schema.
 * <li>Run the test. It should fail. Inspect the output in
 * RelOptRulesTest.log.xml; verify that the "planBefore" is the correct
 * translation of your SQL, and that it contains the pattern on which your rule
 * is supposed to fire. If all is well, check out RelOptRulesTest.ref.xml and
 * replace it with the new .log.xml.
 * <li>Run the test again. It should fail again, but this time it should contain
 * a "planAfter" entry for your rule. Verify that your rule applied its
 * transformation correctly, and then update the .ref.xml file again.
 * <li>Run the test one last time; this time it should pass.
 * </ol>
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class RelOptRulesTest
    extends RelOptTestBase
{
    //~ Methods ----------------------------------------------------------------

    protected DiffRepository getDiffRepos()
    {
        return DiffRepository.lookup(RelOptRulesTest.class);
    }

    public void testUnionToDistinctRule()
    {
        checkPlanning(
            new UnionToDistinctRule(),
            "select * from dept union select * from dept");
    }

    public void testExtractJoinFilterRule()
    {
        checkPlanning(
            ExtractJoinFilterRule.instance,
            "select 1 from emp inner join dept on emp.deptno=dept.deptno");
    }

    public void testAddRedundantSemiJoinRule()
    {
        checkPlanning(
            new AddRedundantSemiJoinRule(),
            "select 1 from emp inner join dept on emp.deptno = dept.deptno");
    }

    public void testPushFilterThroughOuterJoin()
    {
        checkPlanning(
            new PushFilterPastJoinRule(),
            "select 1 from sales.dept d left outer join sales.emp e"
            + " on d.deptno = e.deptno"
            + " where d.name = 'Charlie'");
    }

    public void testReduceAverage()
    {
        checkPlanning(
            ReduceAggregatesRule.instance,
            "select name, max(name), avg(deptno), min(name)"
            + " from sales.dept group by name");
    }

    public void testPushProjectPastFilter()
    {
        checkPlanning(
            new PushProjectPastFilterRule(),
            "select empno + deptno from emp where sal = 10 * comm "
            + "and upper(ename) = 'FOO'");
    }

    public void testPushProjectPastJoin()
    {
        checkPlanning(
            new PushProjectPastJoinRule(),
            "select e.sal + b.comm from emp e inner join bonus b "
            + "on e.ename = b.ename and e.deptno = 10");
    }

    public void testPushProjectPastSetOp()
    {
        checkPlanning(
            new PushProjectPastSetOpRule(),
            "select sal from "
            + "(select * from emp e1 union all select * from emp e2)");
    }
}

// End RelOptRulesTest.java
