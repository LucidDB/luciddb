/*
// $Id$
// Package org.eigenbase is a class library of database components.
// Copyright (C) 2002-2004 Disruptive Tech
// Copyright (C) 2003-2004 John V. Sichi
//
// This program is free software; you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation; either version 2 of the License, or
// (at your option) any later version.
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

import org.eigenbase.reltype.RelDataTypeFactory;
import org.eigenbase.sql.SqlValidator;
import org.eigenbase.sql.advise.SqlAdvisor;
import org.eigenbase.sql.advise.SqlAdvisorValidator;
import org.eigenbase.sql.fun.SqlStdOperatorTable;
import org.eigenbase.sql.parser.SqlParserPos;
import org.eigenbase.sql.type.SqlTypeFactoryImpl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.logging.Logger;

/**
 * Concrete child class of {@link SqlValidatorTestCase}, containing
 * unit tests for SqlAdvisor.
 *
 * @author Tim Leung
 * @since Jan 16, 2005
 * @version $Id$
 **/
public class SqlAdvisorTest extends SqlValidatorTestCase
{
    public final Logger logger = Logger.getLogger(getClass().getName());

    private final String hintToken = "$suggest$";

    //~ Methods ---------------------------------------------------------------
    public void testFrom() throws Exception {
        String sql;
        sql = "select a.empno, b.deptno from dummy a, sales.dummy b";
        ArrayList expected = new ArrayList();
        expected.add("SALES");
        expected.add("EMP");
        expected.add("DEPT");
        expected.add("BONUS");
        expected.add("SALGRADE");
        expected.add("CUSTOMER");
        expected.add("CONTACT");
        expected.add("ACCOUNT");
        assertHint(sql, new SqlParserPos(1,31), expected); // join

        sql = "select a.empno, b.deptno from dummy a, sales.dummy b";
        expected.clear();
        expected.add("EMP");
        expected.add("DEPT");
        expected.add("BONUS");
        expected.add("SALGRADE");
        assertHint(sql, new SqlParserPos(1,40), expected); // join
    }

    public void testJoin() throws Exception {
        String sql;
        sql = "select a.empno, b.deptno from dummy a join sales.dummy b "
            + "on a.deptno=b.deptno where empno=1";
        ArrayList expected = new ArrayList();
        expected.add("SALES");
        expected.add("EMP");
        expected.add("DEPT");
        expected.add("BONUS");
        expected.add("SALGRADE");
        expected.add("CUSTOMER");
        expected.add("CONTACT");
        expected.add("ACCOUNT");
        assertHint(sql, new SqlParserPos(1,31), expected); // from

        expected.clear();
        expected.add("EMP");
        expected.add("DEPT");
        expected.add("BONUS");
        expected.add("SALGRADE");
        assertHint(sql, new SqlParserPos(1,44), expected); // join
    }

    public void testOnCondition() throws Exception {
        String sql;
        sql = "select a.empno, b.deptno from sales.emp a join sales.dept b "
            + "on a.deptno=b.dummy where empno=1";
        ArrayList expected = new ArrayList();
        expected.add("EMPNO");
        expected.add("ENAME");
        expected.add("JOB");
        expected.add("MGR");
        expected.add("HIREDATE");
        expected.add("SAL");
        expected.add("COMM");
        expected.add("DEPTNO");
        assertHint(sql, new SqlParserPos(1,64), expected); // on left

        expected.clear();
        expected.add("DEPTNO");
        expected.add("NAME");
        assertHint(sql, new SqlParserPos(1,73), expected); // on right
    }

    public void testFromWhere() throws Exception {
        String sql;
        sql = "select a.empno, b.deptno from sales.emp a, sales.dept b "
            + "where b.deptno=a.dummy";
        ArrayList expected = new ArrayList();
        expected.add("EMPNO");
        expected.add("ENAME");
        expected.add("JOB");
        expected.add("MGR");
        expected.add("HIREDATE");
        expected.add("SAL");
        expected.add("COMM");
        expected.add("DEPTNO");
        assertHint(sql, new SqlParserPos(1,72), expected); // where list

        sql = "select a.empno, b.deptno from sales.emp a, sales.dept b "
            + "where dummy=1";
        expected.clear();
        expected.add("SALES.EMP");
        expected.add("SALES.DEPT");
        expected.add("EMPNO");
        expected.add("ENAME");
        expected.add("JOB");
        expected.add("MGR");
        expected.add("HIREDATE");
        expected.add("SAL");
        expected.add("COMM");
        expected.add("DEPTNO");
        expected.add("NAME");
        assertHint(sql, new SqlParserPos(1,63), expected); // where list
    }

    public void testWhereList() throws Exception {
        String sql;
        sql = "select a.empno, b.deptno from sales.emp a join sales.dept b "
            + "on a.deptno=b.deptno where dummy=1";
        ArrayList expected = new ArrayList();
        expected.add("SALES.EMP");
        expected.add("SALES.DEPT");
        expected.add("EMPNO");
        expected.add("ENAME");
        expected.add("JOB");
        expected.add("MGR");
        expected.add("HIREDATE");
        expected.add("SAL");
        expected.add("COMM");
        expected.add("DEPTNO");
        expected.add("NAME");
        assertHint(sql, new SqlParserPos(1,88), expected); // where list

        sql = "select a.empno, b.deptno from sales.emp a join sales.dept b "
            + "on a.deptno=b.deptno where a.dummy=1";
        expected.clear();
        expected.add("EMPNO");
        expected.add("ENAME");
        expected.add("JOB");
        expected.add("MGR");
        expected.add("HIREDATE");
        expected.add("SAL");
        expected.add("COMM");
        expected.add("DEPTNO");
        assertHint(sql, new SqlParserPos(1,88), expected); // where list
    }

    public void testSelectList() throws Exception {
        String sql;
        sql = "select dummy, b.dummy from sales.emp a join sales.dept b "
            + "on a.deptno=b.deptno where empno=1";
        ArrayList expected = new ArrayList();
        expected.add("SALES.EMP");
        expected.add("SALES.DEPT");
        expected.add("EMPNO");
        expected.add("ENAME");
        expected.add("JOB");
        expected.add("MGR");
        expected.add("HIREDATE");
        expected.add("SAL");
        expected.add("COMM");
        expected.add("DEPTNO");
        expected.add("NAME");
        assertHint(sql, new SqlParserPos(1,8), expected); // select list
        expected.clear();
        expected.add("DEPTNO");
        expected.add("NAME");
        assertHint(sql, new SqlParserPos(1,15), expected); // select list

        sql = "select emp.dummy from sales.emp";
        expected.clear();
        expected.add("EMPNO");
        expected.add("ENAME");
        expected.add("JOB");
        expected.add("MGR");
        expected.add("HIREDATE");
        expected.add("SAL");
        expected.add("COMM");
        expected.add("DEPTNO");
        assertHint(sql, new SqlParserPos(1,8), expected); // select list
    }

    public void testOrderByList() throws Exception {
        String sql;
        sql = "select emp.empno from sales.emp where empno=1 order by dummy";
        ArrayList expected = new ArrayList();
        expected.add("SALES.EMP");
        expected.add("EMPNO");
        assertHint(sql, new SqlParserPos(1,56), expected); // where list
    }


    public void testSubQuery() throws Exception {
        String sql;
        sql = "select t.dummy from (select 1 as x, 2 as y from sales.emp) as t where t.dummy=1";
        ArrayList expected = new ArrayList();
        expected.add("X");
        expected.add("Y");
        assertHint(sql, new SqlParserPos(1,8), expected); // select list

        sql = "select t.dummy from (select 1 as x, 2 as y from sales.emp) as t where t.dummy=1";
        expected.clear();
        expected.add("X");
        expected.add("Y");
        assertHint(sql, new SqlParserPos(1,71), expected); // select list

    }

    public void testSimpleParser() {
        String sql;
        String expected;

        // from
        sql = "select * from ^where";
        expected = "select * from $suggest$";
        assertSimplify(sql, expected);

        // from
        sql = "select a.empno, b.deptno from ^";
        expected = "select a.empno, b.deptno from $suggest$";
        assertSimplify(sql, expected);

        // select list
        sql = "select emp.^ from sales.emp";
        expected = "select emp.$suggest$ from sales.emp";
        assertSimplify(sql, expected);

        sql = "select ^from sales.emp";
        expected = "select $suggest$ from sales.emp";
        assertSimplify(sql, expected);

        sql = "select a.empno ,^  from sales.emp a , sales.dept b";
        expected = "select a.empno ,$suggest$ from sales.emp a , sales.dept b";
        assertSimplify(sql, expected);

        // join
        sql = "select a.empno, b.deptno from dummy a join ^on where empno=1";
        expected="select a.empno, b.deptno from dummy a join $suggest$ "
            + "where empno=1";
        assertSimplify(sql, expected);

        // on
        sql = "select a.empno, b.deptno from sales.emp a join sales.dept b "
            + "on a.deptno=^";
        expected="select a.empno, b.deptno from sales.emp a join sales.dept b "
            + "on a.deptno=$suggest$";
        assertSimplify(sql, expected);

        // where
        sql = "select a.empno, b.deptno from sales.emp a, sales.dept b "
            + "where ^";
        expected = "select a.empno, b.deptno from sales.emp a, sales.dept b "
            + "where $suggest$";
        assertSimplify(sql, expected);

        // order by
        sql = "select emp.empno from sales.emp where empno=1 order by ^";
        expected = "select emp.empno from sales.emp where empno=1 order by $suggest$";
        assertSimplify(sql, expected);

        // subquery
        sql = "select t.^ from (select 1 as x, 2 as y from sales.emp) as t where t.dummy=1";
        expected = "select t.$suggest$ from (select 1 as x, 2 as y from sales.emp) as t where t.dummy=1";
        assertSimplify(sql, expected);

        sql = "select t. from (select 1 as x, 2 as y from (select x from sales.emp)) as t where ^";
        expected = "select t. from (select 1 as x, 2 as y from (select x from sales.emp)) as t where $suggest$";
        assertSimplify(sql, expected);

        sql = "select ^from (select 1 as x, 2 as y from sales.emp), (select 2 as y from (select m from n where)) as t where t.dummy=1";
        expected = "select $suggest$ from (select 1 as x, 2 as y from sales.emp), (select 2 as y from (select m from n)) as t where t.dummy=1";
        assertSimplify(sql, expected);
    }

    /**
     * Checks that a given SQL statement yields the expected set of completion
     * hints.
     *
     * <p>REVIEW: use caret to indicate position in statement -- just for
     * testing purposes -- and obsolete pos parameter.
     */
    protected void assertHint(
        String sql,
        SqlParserPos pos,
        List expectedResults)
        throws Exception
    {
        SqlValidator validator = tester.getValidator();
        SqlAdvisor advisor = new SqlAdvisor(validator);
        HashMap uniqueResults = new HashMap();
        String [] results = advisor.getCompletionHints(sql, pos);
        for (int i = 0; i < results.length; i++) {
            uniqueResults.put(results[i], results[i]);
        }
        if (!(expectedResults.containsAll(uniqueResults.values()) &&
            (expectedResults.size() == uniqueResults.values().size()))) {
            fail("SqlAdvisorTest: completion hints results not as expected:\n"
            + uniqueResults.values() + "\nExpected:\n" + expectedResults);
        }
        return;
    }

    protected String simplify(String sql, int cursor)
    {
        SqlValidator validator = tester.getValidator();
        SqlAdvisor advisor = new SqlAdvisor(validator);
        String goodSql = advisor.simplifySql(sql, cursor);
        return goodSql;
    }

    /**
     * Tests that a given SQL statement simplifies to the expected result.
     *
     * @param sql SQL statement to simplify. The SQL statement must contain
     *   precisely one caret '^', which marks the location where completion is
     *   to occur.
     * @param expected Expected result after simplification.
     */
    protected void assertSimplify(String sql, String expected)
    {
        int cursor = sql.indexOf('^');
        if (cursor < 0) {
            fail("Invalid test SQL: should contain '^'");
        }
        if (sql.indexOf('^', cursor + 1) >= 0) {
            fail("Invalid test SQL: contains more than one '^'");
        }
        String sqlSansCaret = sql.substring(0, cursor) +
            sql.substring(cursor + 1);
        String actual = simplify(sqlSansCaret, cursor);
        assertEquals(actual, expected);
    }

    // REVIEW not used?
    private void assertHint(
        String sql,
        int cursor,
        List expectedResults)
        throws Exception
    {
        String simpleSql = simplify(sql, cursor);

        int idx = simpleSql.indexOf(hintToken);
        SqlParserPos pp = new SqlParserPos(1, idx+1);
        assertHint(simpleSql, pp, expectedResults);
    }

    public Tester getTester() {
        return new AdvisorTestImpl();
    }

    public class AdvisorTestImpl extends TesterImpl {
        // REVIEW this is the same as the base method
        public SqlValidator getValidator() {
            final RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl();
            return new SqlAdvisorValidator(
                SqlStdOperatorTable.instance(),
                new MockCatalogReader(typeFactory),
                typeFactory);
        }
    }
}


// End SqlValidatorTest.java

