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

import junit.framework.TestCase;
import org.eigenbase.reltype.RelDataTypeFactory;
import org.eigenbase.sql.SqlValidator;
import org.eigenbase.sql.parser.SqlParserPos;
import org.eigenbase.sql.parser.SqlParseException;
import org.eigenbase.sql.advise.*;
import org.eigenbase.sql.type.*;
import org.eigenbase.sql.fun.*;

import java.util.logging.Logger;
import java.util.ArrayList;
import java.util.List;
import java.util.HashMap;

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
    public void testFrom() {
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
        hint(sql, new SqlParserPos(1,31), expected); // join
        
        sql = "select a.empno, b.deptno from dummy a, sales.dummy b";
        expected.clear();
        expected.add("EMP");
        expected.add("DEPT");
        expected.add("BONUS");
        expected.add("SALGRADE");
        hint(sql, new SqlParserPos(1,40), expected); // join
    }

    public void testJoin() {
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
        hint(sql, new SqlParserPos(1,31), expected); // from

        expected.clear();
        expected.add("EMP");
        expected.add("DEPT");
        expected.add("BONUS");
        expected.add("SALGRADE");
        hint(sql, new SqlParserPos(1,44), expected); // join
    }

    public void testOnCondition() {
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
        hint(sql, new SqlParserPos(1,64), expected); // on left

        expected.clear();
        expected.add("DEPTNO");
        expected.add("NAME");
        hint(sql, new SqlParserPos(1,73), expected); // on right
    }

    public void testFromWhere() {
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
        hint(sql, new SqlParserPos(1,72), expected); // where list
        
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
        hint(sql, new SqlParserPos(1,63), expected); // where list
    }
    
    public void testWhereList() {
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
        hint(sql, new SqlParserPos(1,88), expected); // where list
        
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
        hint(sql, new SqlParserPos(1,88), expected); // where list
    }
        
    public void testSelectList() {
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
        hint(sql, new SqlParserPos(1,8), expected); // select list
        expected.clear();
        expected.add("DEPTNO");
        expected.add("NAME");
        hint(sql, new SqlParserPos(1,15), expected); // select list
        
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
        hint(sql, new SqlParserPos(1,8), expected); // select list
    }

    public void testOrderByList() {
        String sql;
        sql = "select emp.empno from sales.emp where empno=1 order by dummy";
        ArrayList expected = new ArrayList();
        expected.add("SALES.EMP");
        expected.add("EMPNO");
        hint(sql, new SqlParserPos(1,56), expected); // where list
    }


    public void testSubQuery() {
        String sql;
        sql = "select t.dummy from (select 1 as x, 2 as y from sales.emp) as t where t.dummy=1";
        ArrayList expected = new ArrayList();
        expected.add("X");
        expected.add("Y");
        hint(sql, new SqlParserPos(1,8), expected); // select list
        
        sql = "select t.dummy from (select 1 as x, 2 as y from sales.emp) as t where t.dummy=1";
        expected.clear();
        expected.add("X");
        expected.add("Y");
        hint(sql, new SqlParserPos(1,71), expected); // select list

    }

    public void hint(String sql, SqlParserPos pp, List expectedResults) 
    {
        SqlValidator validator;
        validator = tester.getValidator();
        SqlAdvisor advisor = new SqlAdvisor(validator);
        HashMap uniqueResults = new HashMap();
        try {
            String [] results = advisor.getCompletionHints(sql, pp);
            for (int i = 0; i < results.length; i++) {
                uniqueResults.put(results[i], results[i]);
            }
        } catch (SqlParseException ex) {
            ex.printStackTrace();
            fail("SqlAdvisorTest: Parse Error while parsing query=" + sql
               + "\n" + ex.getMessage());
        } catch (Exception ex) {
            ex.printStackTrace();
            fail("SqlAdvisorTest: Exception caught: \n" + ex.getMessage());
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

