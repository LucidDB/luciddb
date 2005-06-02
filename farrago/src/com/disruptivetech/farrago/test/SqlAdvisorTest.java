/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2002-2005 Disruptive Tech
// Copyright (C) 2005-2005 The Eigenbase Project
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

import org.eigenbase.reltype.RelDataTypeFactory;
import org.eigenbase.sql.fun.SqlStdOperatorTable;
import org.eigenbase.sql.parser.SqlParserPos;
import org.eigenbase.sql.parser.SqlParserUtil;
import org.eigenbase.sql.type.SqlTypeFactoryImpl;
import org.eigenbase.sql.validate.SqlValidator;
import org.eigenbase.test.SqlValidatorTestCase;
import org.eigenbase.test.MockCatalogReader;
import org.eigenbase.util.TestUtil;

import com.disruptivetech.farrago.sql.advise.SqlAdvisor;
import com.disruptivetech.farrago.sql.advise.SqlAdvisorValidator;

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

    //~ Methods ---------------------------------------------------------------
    public void testFrom() throws Exception {
        String sql;
        ArrayList expected = new ArrayList();
        expected.add("SALES");
        expected.add("EMP");
        expected.add("DEPT");
        expected.add("BONUS");
        expected.add("SALGRADE");
        expected.add("CUSTOMER");
        expected.add("CONTACT");
        expected.add("ACCOUNT");
        expected.add("EMP_ADDRESS");

        sql = "select a.empno, b.deptno from ^dummy a, sales.dummy b";
        assertHint(sql, expected); // join

        sql = "select a.empno, b.deptno from ^";
        assertComplete(sql, expected);
        sql = "select a.empno, b.deptno from ^, sales.dummy b";
        assertComplete(sql, expected);
        sql = "select a.empno, b.deptno from ^a";
        assertComplete(sql, expected);

        expected.clear();
        expected.add("EMP");
        expected.add("DEPT");
        expected.add("BONUS");
        expected.add("SALGRADE");
        expected.add("EMP_ADDRESS");

        sql = "select a.empno, b.deptno from dummy a, ^sales.dummy b";
        assertHint(sql, expected); // join

        sql = "select a.empno, b.deptno from dummy a, sales.^";
        assertComplete(sql, expected);
    }

    public void testJoin() throws Exception {
        String sql;
        ArrayList expected = new ArrayList();
        expected.add("SALES");
        expected.add("EMP");
        expected.add("DEPT");
        expected.add("BONUS");
        expected.add("SALGRADE");
        expected.add("CUSTOMER");
        expected.add("CONTACT");
        expected.add("ACCOUNT");
        expected.add("EMP_ADDRESS");

        sql = "select a.empno, b.deptno from ^dummy a join sales.dummy b "
            + "on a.deptno=b.deptno where empno=1";
        assertHint(sql, expected); // from

        sql = "select a.empno, b.deptno from ^ a join sales.dummy b";
        assertComplete(sql, expected); // from

        expected.clear();
        expected.add("EMP");
        expected.add("DEPT");
        expected.add("BONUS");
        expected.add("SALGRADE");
        expected.add("EMP_ADDRESS");

        sql = "select a.empno, b.deptno from dummy a join ^sales.dummy b "
            + "on a.deptno=b.deptno where empno=1";
        assertHint(sql, expected); // join

        sql = "select a.empno, b.deptno from dummy a join sales.^";
        assertComplete(sql, expected); // join
        sql = "select a.empno, b.deptno from dummy a join sales.^ on";
        assertComplete(sql, expected); // join
        sql = "select a.empno, b.deptno from dummy a join sales.^ on a.deptno=";
        assertComplete(sql, expected); // join
    }

    public void testOnCondition() throws Exception {
        String sql;
        ArrayList expected = new ArrayList();
        expected.add("EMPNO");
        expected.add("ENAME");
        expected.add("JOB");
        expected.add("MGR");
        expected.add("HIREDATE");
        expected.add("SAL");
        expected.add("COMM");
        expected.add("DEPTNO");

        sql = "select a.empno, b.deptno from sales.emp a join sales.dept b "
            + "on ^a.deptno=b.dummy where empno=1";
        assertHint(sql, expected); // on left

        sql = "select a.empno, b.deptno from sales.emp a join sales.dept b "
            + "on a.^";
        assertComplete(sql, expected); // on left

        expected.clear();
        expected.add("DEPTNO");
        expected.add("NAME");

        sql = "select a.empno, b.deptno from sales.emp a join sales.dept b "
            + "on a.deptno=^b.dummy where empno=1";
        assertHint(sql, expected); // on right

        sql = "select a.empno, b.deptno from sales.emp a join sales.dept b "
            + "on a.deptno=b.^ where empno=1";
        assertComplete(sql, expected); // on right

        sql = "select a.empno, b.deptno from sales.emp a join sales.dept b "
            + "on a.deptno=b.^";
        assertComplete(sql, expected); // on right
    }

    public void testFromWhere() throws Exception {
        String sql;
        ArrayList expected = new ArrayList();
        expected.add("EMPNO");
        expected.add("ENAME");
        expected.add("JOB");
        expected.add("MGR");
        expected.add("HIREDATE");
        expected.add("SAL");
        expected.add("COMM");
        expected.add("DEPTNO");

        sql = "select a.empno, b.deptno from sales.emp a, sales.dept b "
            + "where b.deptno=^a.dummy";
        assertHint(sql, expected); // where list

        sql = "select a.empno, b.deptno from sales.emp a, sales.dept b "
            + "where b.deptno=a.^";
        assertComplete(sql, expected); // where list

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

        sql = "select a.empno, b.deptno from sales.emp a, sales.dept b "
            + "where ^dummy=1";
        assertHint(sql, expected); // where list

        sql = "select a.empno, b.deptno from sales.emp a, sales.dept b "
            + "where ^";
        assertComplete(sql, expected); // where list
    }

    public void testWhereList() throws Exception {
        String sql;
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

        sql = "select a.empno, b.deptno from sales.emp a join sales.dept b "
            + "on a.deptno=b.deptno where ^dummy=1";
        assertHint(sql, expected); // where list

        sql = "select a.empno, b.deptno from sales.emp a join sales.dept b "
            + "on a.deptno=b.deptno where ^";
        assertComplete(sql, expected); // where list

        expected.clear();
        expected.add("EMPNO");
        expected.add("ENAME");
        expected.add("JOB");
        expected.add("MGR");
        expected.add("HIREDATE");
        expected.add("SAL");
        expected.add("COMM");
        expected.add("DEPTNO");

        sql = "select a.empno, b.deptno from sales.emp a join sales.dept b "
            + "on a.deptno=b.deptno where ^a.dummy=1";
        assertHint(sql, expected); // where list

        sql = "select a.empno, b.deptno from sales.emp a join sales.dept b "
            + "on a.deptno=b.deptno where a.^";
        assertComplete(sql, expected); // where list
    }

    public void testSelectList() throws Exception {
        String sql;
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

        sql = "select ^dummy, b.dummy from sales.emp a join sales.dept b "
            + "on a.deptno=b.deptno where empno=1";
        assertHint(sql, expected); // select list

        sql = "select ^, b.dummy from sales.emp a join sales.dept b ";
        assertComplete(sql, expected); // select list

        expected.clear();
        expected.add("DEPTNO");
        expected.add("NAME");

        sql = "select dummy, ^b.dummy from sales.emp a join sales.dept b "
            + "on a.deptno=b.deptno where empno=1";
        assertHint(sql, expected); // select list

        sql = "select dummy, b.^ from sales.emp a join sales.dept b";
        assertComplete(sql, expected); // select list

        expected.clear();
        sql = "select dummy, b.^ from sales.emp a";
        assertComplete(sql, expected); // select list

        expected.clear();
        expected.add("EMPNO");
        expected.add("ENAME");
        expected.add("JOB");
        expected.add("MGR");
        expected.add("HIREDATE");
        expected.add("SAL");
        expected.add("COMM");
        expected.add("DEPTNO");

        sql = "select ^emp.dummy from sales.emp";
        assertHint(sql, expected); // select list

        sql = "select emp.^ from sales.emp";
        assertComplete(sql, expected); // select list
    }

    public void testOrderByList() throws Exception {
        String sql;
        ArrayList expected = new ArrayList();
        expected.add("SALES.EMP");
        expected.add("EMPNO");

        sql = "select emp.empno from sales.emp where empno=1 order by ^dummy";
        assertHint(sql, expected); // where list

        sql = "select emp.empno from sales.emp where empno=1 order by ^";
        assertComplete(sql, expected); // where list
    }


    public void testSubQuery() throws Exception {
        String sql;
        ArrayList expected = new ArrayList();
        expected.add("X");
        expected.add("Y");

        sql = "select ^t.dummy from (select 1 as x, 2 as y from sales.emp) as t where t.dummy=1";
        assertHint(sql, expected); // select list

        sql = "select t.^ from (select 1 as x, 2 as y from sales.emp) as t";
        assertComplete(sql, expected); // select list

        expected.clear();
        expected.add("X");
        expected.add("Y");

        sql = "select t.x from (select 1 as x, 2 as y from sales.emp) as t where ^t.dummy=1";
        assertHint(sql, expected); // select list

        sql = "select t.x from (select 1 as x, 2 as y from sales.emp) as t where t.^";
        assertComplete(sql, expected); // select list

        sql = "select t. from (select 1 as x, 2 as y from (select x from sales.emp)) as t where ^";
        assertComplete(sql, expected);

        expected.clear();
        expected.add("EMP");
        expected.add("DEPT");
        expected.add("BONUS");
        expected.add("SALGRADE");
        expected.add("EMP_ADDRESS");

        sql = "select t.x from (select 1 as x, 2 as y from sales.^) as t";
        assertComplete(sql, expected); // select list
    }

    public void testSimpleParser() {
        String sql;
        String expected;

        // from
        sql = "select * from ^where";
        expected = "select * from _suggest_";
        assertSimplify(sql, expected);

        // from
        sql = "select a.empno, b.deptno from ^";
        expected = "select a.empno , b.deptno from _suggest_";
        assertSimplify(sql, expected);

        // select list
        sql = "select emp.^ from sales.emp";
        expected = "select emp._suggest_ from sales.emp";
        assertSimplify(sql, expected);

        sql = "select ^from sales.emp";
        expected = "select _suggest_ from sales.emp";
        assertSimplify(sql, expected);

        sql = "select a.empno ,^  from sales.emp a , sales.dept b";
        expected = "select a.empno , _suggest_ from sales.emp a , sales.dept b";
        assertSimplify(sql, expected);

        // join
        sql = "select a.empno, b.deptno from dummy a join ^on where empno=1";
        expected="select a.empno , b.deptno from dummy a join _suggest_ "
            + "where empno=1";
        assertSimplify(sql, expected);

        // on
        sql = "select a.empno, b.deptno from sales.emp a join sales.dept b "
            + "on a.deptno=^";
        expected="select a.empno , b.deptno from sales.emp a join sales.dept b "
            + "on a.deptno = _suggest_";
        assertSimplify(sql, expected);

        // where
        sql = "select a.empno, b.deptno from sales.emp a, sales.dept b "
            + "where ^";
        expected = "select a.empno , b.deptno from sales.emp a , sales.dept b "
            + "where _suggest_";
        assertSimplify(sql, expected);

        // order by
        sql = "select emp.empno from sales.emp where empno=1 order by ^";
        expected = "select emp.empno from sales.emp where empno=1 order by _suggest_";
        assertSimplify(sql, expected);

        // subquery
        sql = "select t.^ from (select 1 as x, 2 as y from sales.emp) as t where t.dummy=1";
        expected = "select t._suggest_ from (select 1 as x , 2 as y from sales.emp) as t where t.dummy=1";
        assertSimplify(sql, expected);

        sql = "select t. from (select 1 as x, 2 as y from (select x from sales.emp)) as t where ^";
        expected = "select t from (select 1 as x , 2 as y from (select x from sales.emp)) as t where _suggest_";
        assertSimplify(sql, expected);

        sql = "select ^from (select 1 as x, 2 as y from sales.emp), (select 2 as y from (select m from n where)) as t where t.dummy=1";
        expected = "select _suggest_ from (select 1 as x , 2 as y from sales.emp) , (select 2 as y from (select m from n)) as t where t.dummy=1";
        assertSimplify(sql, expected);

        sql = "select t.x from (select 1 as x, 2 as y from sales.^";
        expected = "select 1 as x , 2 as y from sales._suggest_";
        assertSimplify(sql, expected);


        sql = "select a.empno, b.deptno from dummy a, sales.^";
        expected = "select a.empno , b.deptno from dummy a , sales._suggest_";
        assertSimplify(sql, expected);

        // function
        sql = "select count(1) from sales.emp a where ^";
        expected = "select count(1) from sales.emp a where _suggest_";
        assertSimplify(sql, expected);
    }

    /**
     * Checks that a given SQL statement yields the expected set of completion
     * hints.
     */
    protected void assertHint(
        String sql,
        List expectedResults)
        throws Exception
    {
        SqlValidator validator = tester.getValidator();
        SqlAdvisor advisor = new SqlAdvisor(validator);

        SqlParserUtil.StringAndPos sap = SqlParserUtil.findPos(sql);

        String [] results = advisor.getCompletionHints(
            sap.sql, sap.pos);
        assertEquals(results, expectedResults);
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
        SqlValidator validator = tester.getValidator();
        SqlAdvisor advisor = new SqlAdvisor(validator);

        SqlParserUtil.StringAndPos sap = SqlParserUtil.findPos(sql);
        String actual = advisor.simplifySql(sap.sql, sap.cursor);
        assertEquals(actual, expected);
    }

    /**
     * Tests that a given SQL which may be invalid or incomplete simplifies
     * itself and yields the expected set of completion hints.
     * This is an integration test of assertHint and assertSimplify.
     */
    protected void assertComplete(
        String sql,
        List expectedResults)
        throws Exception
    {
        SqlValidator validator = tester.getValidator();
        SqlAdvisor advisor = new SqlAdvisor(validator);

        SqlParserUtil.StringAndPos sap = SqlParserUtil.findPos(sql);
        String [] results = advisor.getCompletionHints(sap.sql, sap.cursor);
        assertEquals(results, expectedResults);
    }

    protected void assertEquals(
        String [] actualResults,
        List expectedResults)
        throws Exception
    {
        HashMap uniqueResults = new HashMap();
        for (int i = 0; i < actualResults.length; i++) {
            uniqueResults.put(actualResults[i], actualResults[i]);
        }
        if (!(expectedResults.containsAll(uniqueResults.values()) &&
            (expectedResults.size() == uniqueResults.values().size()))) {
            fail("SqlAdvisorTest: completion hints results not as expected:\n"
            + uniqueResults.values() + "\nExpected:\n" + expectedResults);
        }
        return;
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

