/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005-2006 The Eigenbase Project
// Copyright (C) 2002-2006 Disruptive Tech
// Copyright (C) 2005-2006 LucidEra, Inc.
// Portions Copyright (C) 2003-2006 John V. Sichi
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

import org.eigenbase.rel.RelNode;
import org.eigenbase.relopt.RelOptUtil;
import org.eigenbase.relopt.RelOptXmlPlanWriter;
import org.eigenbase.sql.SqlExplainLevel;
import org.eigenbase.sql.fun.SqlCaseOperator;
import org.eigenbase.sql.fun.SqlStdOperatorTable;
import org.eigenbase.sql2rel.SqlToRelConverter;
import org.eigenbase.util.TestUtil;

import java.io.PrintWriter;
import java.io.StringWriter;

/**
 * Unit test for {@link SqlToRelConverter}.
 *
 * @author jhyde
 * @version $Id$
 */
public class SqlToRelConverterTest extends SqlToRelTestBase
{
    protected DiffRepository getDiffRepos()
    {
        return DiffRepository.lookup(SqlToRelConverterTest.class);
    }

    protected void check(
        String sql,
        String plan)
    {
        final DiffRepository diffRepos = getDiffRepos();
        String sql2 = diffRepos.expand("sql", sql);
        final RelNode rel = tester.convertSqlToRel(sql2);

        assertTrue(rel != null);
        // NOTE jvs 28-Mar-2006:  insert leading newline so
        // that plans come out nicely stacked instead of first
        // line immediately after CDATA start
        String actual = NL + RelOptUtil.toString(rel);
        diffRepos.assertEquals("plan", plan, actual);
    }

    //~ TESTS --------------------------------

    public void testIntegerLiteral()
    {
        check("select 1 from emp",
            "${plan}");
    }

    public void testGroup()
    {
        check("select deptno from emp group by deptno",
            "${plan}");
    }

    public void testGroupJustOneAgg()
    {
        // just one agg
        check("select deptno, sum(sal) from emp group by deptno",
            "${plan}");
    }

    public void testGroupExpressionsInsideAndOut()
    {
        // Expressions inside and outside aggs.
        // Common sub-expressions should be eliminated: 'sal' always translates
        // to expression #2.
        check("select deptno + 4, sum(sal), sum(3 + sal), 2 * count(sal) from emp group by deptno",
            "${plan}");
    }

    public void testHaving()
    {
        // empty group-by clause, having
        check("select sum(sal + sal) from emp having sum(sal) > 10",
            "${plan}");
    }

    public void testGroupBug281() {
        // Dtbug 281 gives:
        //   Internal error:
        //   Type 'RecordType(VARCHAR(128) $f0)' has no field 'NAME'
        check("select name from (select name from dept group by name)",
            "${plan}");
    }

    public void testGroupBug281b() {

        // Try to confuse it with spurious columns.
        check("select name, foo from (" +
            "select deptno, name, count(deptno) as foo " +
            "from dept " +
            "group by name, deptno, name)",
            "${plan}");
    }

    public void testAggDistinct()
    {
        check(
            "select deptno, sum(sal), sum(distinct sal), count(*) " +
            "from emp " +
            "group by deptno",
            "${plan}");
    }

    public void testSelectDistinct()
    {
        check("select distinct sal + 5 from emp",
            "${plan}");
    }

    public void testSelectDistinctGroup()
    {
        check("select distinct sum(sal) from emp group by deptno",
            "${plan}");
    }

    public void testOrder()
    {
        check("select empno from emp order by empno",
            "${plan}");

    }

    public void testOrderByOrdinalDesc()
    {
        // FRG-98
        if (!tester.getCompatible().isSortByOrdinal()) {
            return;
        }
        check("select empno + 1, deptno, empno from emp order by 2 desc",
            "${plan}");

        // ordinals rounded down, so 2.5 should have same effect as 2, and
        // generate identical plan
        check("select empno + 1, deptno, empno from emp order by 2.5 desc",
            "${plan}");
    }

    public void testOrderDistinct()
    {
        // The relexp aggregates by 3 expressions - the 2 select expressions
        // plus the one to sort on. A little inefficient, but acceptable.
        check("select distinct empno, deptno + 1 from emp order by deptno + 1 + empno",
            "${plan}");
    }

    public void testOrderByNegativeOrdinal()
    {
        // Regardless of whether sort-by-ordinals is enabled, negative ordinals
        // are treated like ordinary numbers.
        check("select empno + 1, deptno, empno from emp order by -1 desc",
            "${plan}");
    }

    public void testOrderByOrdinalInExpr()
    {
        // Regardless of whether sort-by-ordinals is enabled, ordinals
        // inside expressions are treated like integers.
        check("select empno + 1, deptno, empno from emp order by 1 + 2 desc",
            "${plan}");
    }

    public void testOrderByIdenticalExpr()
    {
        // Expression in ORDER BY clause is identical to expression in SELECT
        // clause, so plan should not need an extra project.
        check("select empno + 1 from emp order by deptno asc, empno + 1 desc",
            "${plan}");
    }

    public void testOrderByAlias()
    {
        check("select empno + 1 as x, empno - 2 as y from emp order by y",
            "${plan}");
    }

    public void testOrderByAliasInExpr()
    {
        check("select empno + 1 as x, empno - 2 as y from emp order by y + 3",
            "${plan}");
    }

    public void testOrderByAliasOverrides()
    {
        if (!tester.getCompatible().isSortByAlias()) {
            return;
        }
        // plan should contain '(empno + 1) + 3'
        check("select empno + 1 as empno, empno - 2 as y from emp order by empno + 3",
            "${plan}");
    }

    public void testOrderByAliasDoesNotOverride()
    {
        if (tester.getCompatible().isSortByAlias()) {
            return;
        }
        // plan should contain 'empno + 3', not '(empno + 1) + 3'
        check("select empno + 1 as empno, empno - 2 as y from emp order by empno + 3",
            "${plan}");
    }

    public void testOrderBySameExpr()
    {
        check("select empno from emp, dept order by sal + empno desc, sal * empno, sal + empno",
            "${plan}");
    }

    public void testOrderUnion()
    {
        check("select empno, sal from emp " +
            "union all " +
            "select deptno, deptno from dept " +
            "order by sal desc, empno asc",
            "${plan}");
    }

    public void testOrderUnionOrdinal()
    {
        if (!tester.getCompatible().isSortByOrdinal()) {
            return;
        }
        check("select empno, sal from emp " +
            "union all " +
            "select deptno, deptno from dept " +
            "order by 2",
            "${plan}");
    }

    public void testOrderUnionExprs()
    {
        check("select empno, sal from emp " +
            "union all " +
            "select deptno, deptno from dept " +
            "order by empno * sal + 2",
            "${plan}");
    }

    public void testOrderGroup()
    {
        check("select deptno, count(*) " +
            "from emp " +
            "group by deptno " +
            "order by deptno * sum(sal) desc, min(empno)",
            "${plan}");
    }

    public void testExplicitTable()
    {
        check("table emp",
            "${plan}");
    }

    public void testCollectionTable()
    {
        check("select * from table(ramp(3))",
            "${plan}");
    }

    public void testSample()
    {
        check("select * from emp tablesample substitute('DATASET1') where empno > 5",
            "${plan}");
    }

    public void testSampleQuery()
    {
        check("select * from (\n" +
            " select * from emp tablesample substitute('DATASET1') as e\n" +
            " join dept on e.deptno = dept.deptno\n" +
            ") tablesample substitute('DATASET2')\n" +
            "where empno > 5",
            "${plan}");
    }

    public void testCollectionTableWithCursorParam()
    {
        check(
            "select * from table(dedup("
            + "cursor(select ename from emp),"
            + " cursor(select name from dept), 'NAME'))",
            "${plan}");
    }

    public void testUnnest()
    {
        check("select*from unnest(multiset[1,2])",
            "${plan}");
    }

    public void testUnnestSubquery()
    {
        check("select*from unnest(multiset(select*from dept))",
            "${plan}");
    }

    public void testMultisetSubquery()
    {
        check("select multiset(select deptno from dept) from (values(true))",
            "${plan}");
    }

    public void testMultiset()
    {
        check("select 'a',multiset[10] from dept",
            "${plan}");
    }

    public void testMultisetOfColumns() {
        check("select 'abc',multiset[deptno,sal] from emp",
            "${plan}");
    }

    public void testCorrelationJoin()
    {
        check("select *," +
            "         multiset(select * from emp where deptno=dept.deptno) " +
            "               as empset" +
            "      from dept",
            "${plan}");
    }

    public void testExists()
    {
        check("select*from emp where exists (select 1 from dept where deptno=55)",
            "${plan}");
    }

    public void testExistsCorrelated()
    {
        check("select*from emp where exists (select 1 from dept where emp.deptno=dept.deptno)",
            "${plan}");
    }

    public void testInValueListShort()
    {
        check("select empno from emp where deptno in (10, 20)", "${plan}");
    }

    public void testInValueListLong()
    {
        // Go over the default threshold of 20 to force a subquery.
        check(
            "select empno from emp where deptno in"
            + " (10, 20, 30, 40, 50, 60, 70, 80, 90, 100"
            + ", 110, 120, 130, 140, 150, 160, 170, 180, 190"
            + ", 200, 210, 220, 230)", "${plan}");
    }

    public void testInUncorrelatedSubquery()
    {
        check(
            "select empno from emp where deptno in"
            + " (select deptno from dept)", "${plan}");
    }

    public void testUnnestSelect() {
        check("select*from unnest(select multiset[deptno] from dept)",
            "${plan}");
    }

    public void testLateral() {
        check("select * from emp, LATERAL (select * from dept where emp.deptno=dept.deptno)",
            "${plan}");
    }

    public void testElement()
    {
        check("select element(multiset[5]) from emp",
            "${plan}");

    }

    public void testElementInValues()
    {
        check("values element(multiset[5])",
            "${plan}");
    }

    public void testUnionAll()
    {
        // union all
        check( "select empno from emp union all select deptno from dept",
            "${plan}");
    }

    public void testUnion()
    {
        // union without all
        check("select empno from emp union select deptno from dept",
            "${plan}");
    }

    public void testUnionValues()
    {
        // union with values
        check("values (10), (20)" + NL +
            "union all" + NL +
            "select 34 from emp" + NL +
            "union all values (30), (45 + 10)",
            "${plan}");
    }

    public void testUnionSubquery()
    {
        // union of subquery, inside from list, also values
        check("select deptno from emp as emp0 cross join" + NL +
            " (select empno from emp union all " + NL +
            "  select deptno from dept where deptno > 20 union all" + NL +
            "  values (45), (67))",
            "${plan}");
    }

    public void testIsDistinctFrom()
    {
        check("select 1 is distinct from 2 from (values(true))",
            "${plan}");
    }

    public void testIsNotDistinctFrom()
    {
        check("select 1 is not distinct from 2 from (values(true))",
            "${plan}");
    }

    public void testNotLike()
    {
        // note that 'x not like y' becomes 'not(x like y)'
        check("values ('a' not like 'b' escape 'c')",
            "${plan}");
    }

    public void testOverMultiple() {
        check(
            "select sum(sal) over w1," + NL +
            "  sum(deptno) over w1," + NL +
            "  sum(deptno) over w2" + NL +
            "from emp" + NL +
            "where sum(deptno - sal) over w1 > 999" + NL +
            "window w1 as (partition by job order by hiredate rows 2 preceding)," + NL +
            "  w2 as (partition by job order by hiredate rows 3 preceding)," + NL +
            "  w3 as (partition by job order by hiredate range interval '1' second preceding)",
            "${plan}");
    }

    /**
     * Test one of the custom conversions which is recognized by the class
     * of the operator (in this case, {@link SqlCaseOperator}).
     */
    public void testCase()
    {
        check("values (case 'a' when 'a' then 1 end)",
            "${plan}");
    }

    /**
     * Tests one of the custom conversions which is recognized by the identity
     * of the operator (in this case,
     * {@link SqlStdOperatorTable#characterLengthFunc}).
     */
    public void testCharLength()
    {
        // Note that CHARACTER_LENGTH becomes CHAR_LENGTH.
        check("values (character_length('foo'))",
            "${plan}");
    }

    public void testOverAvg()
    {
        check(
            "select sum(sal) over w1," + NL +
            "  avg(sal) over w1" + NL +
            "from emp" + NL +
            "window w1 as (partition by job order by hiredate rows 2 preceding)",

            "${plan}");
    }

    public void testOverCountStar()
    {
        check(
            "select count(sal) over w1," + NL +
            "  count(*) over w1" + NL +
            "from emp" + NL +
            "window w1 as (partition by job order by hiredate rows 2 preceding)",

            "${plan}");
    }

    public void testExplainAsXml()
    {
        String sql = "select 1 + 2, 3 from (values (true))";
        final RelNode rel = tester.convertSqlToRel(sql);
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        RelOptXmlPlanWriter planWriter =
            new RelOptXmlPlanWriter(pw, SqlExplainLevel.DIGEST_ATTRIBUTES);
        rel.explain(planWriter);
        pw.flush();
        TestUtil.assertEqualsVerbose(
            TestUtil.fold(new String[]{
                "<RelNode type=\"ProjectRel\">",
                "\t<Property name=\"EXPR$0\">",
                "\t\t+(1, 2)\t</Property>",
                "\t<Property name=\"EXPR$1\">",
                "\t\t3\t</Property>",
                "\t<Inputs>",
                "\t\t<RelNode type=\"ProjectRel\">",
                "\t\t\t<Property name=\"EXPR$0\">",
                "\t\t\t\t$0\t\t\t</Property>",
                "\t\t\t<Inputs>",
                "\t\t\t\t<RelNode type=\"ProjectRel\">",
                "\t\t\t\t\t<Property name=\"EXPR$0\">",
                "\t\t\t\t\t\ttrue\t\t\t\t\t</Property>",
                "\t\t\t\t\t<Inputs>",
                "\t\t\t\t\t\t<RelNode type=\"OneRowRel\">",
                "\t\t\t\t\t\t\t<Inputs/>",
                "\t\t\t\t\t\t</RelNode>",
                "\t\t\t\t\t</Inputs>",
                "\t\t\t\t</RelNode>",
                "\t\t\t</Inputs>",
                "\t\t</RelNode>",
                "\t</Inputs>",
                "</RelNode>",
                ""}),
            sw.toString());
    }
}

// End SqlToRelConverterTest.java
