/*
// Saffron preprocessor and data engine.
// Copyright (C) 2002-2004 Disruptive Tech
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

package sales;

import java.io.PrintWriter;
import java.sql.SQLException;
import java.util.*;

import junit.framework.TestCase;

import net.sf.saffron.oj.stmt.OJStatement;

import openjava.tools.DebugOut;

import org.eigenbase.relopt.RelOptConnection;
import org.eigenbase.util.Util;


/**
 * <code>Test</code> is a test harness for the <code>saffron</code> package.
 *
 * @author jhyde
 * @version $Id$
 *
 * @since 26 July, 2001
 */
public class Test extends TestCase
{
    static final String nl = System.getProperty("line.separator");
    private static final OJStatement.Argument [] noArguments =
        new OJStatement.Argument[0];
    private static String [] statements =
    {
        
        // 0
        "select i from new int[] {1,2,3} as i",
        

        // 1
        "select {} from sales.emps as emp "
        + "join sales.depts as dept on emp.deptno == dept.deptno",
        

        // 2
        "select {} from sales.emps as emp "
        + "left join sales.depts as dept on emp.deptno == dept.deptno",
        

        // 3; empty select list means 'zero columns'
        "select {} from sales.emps as emp",
        

        // 4; empty from list means 'zero relations' -- hence, one row with no
        // columns
        "select 5 where 2 < 3",
        

        // 5
        "select false where 2 > 3",
        

        // 6
        "select \"hello\"",
        

        // 7
        "select 1 from sales.emps as emp where emp.deptno > 30",
        

        // 8
        "select 1 as tt from sales.emps",
        

        // 9
        "select {emp, dept} from sales.emps as emp "
        + "join sales.depts as dept " + "on emp.deptno == dept.deptno "
        + "where emp.deptno > 15",
        

        // 10
        "select {emp, dept} from ("
        + " select emps from sales.emps where emps.deptno == 10) as emp "
        + "join sales.depts as dept " + " on emp.deptno == dept.deptno",
        

        // 11
        "select {emp, dept} from ("
        + " select {emp.name.substring(0,1) as initial,"
        + "  emp.deptno * 2 as twiceDeptno} " + " from sales.emps as emp "
        + " where emp.deptno == 10) as emp " + "join sales.depts as dept "
        + " on emp.twiceDeptno == dept.deptno",
        

        // todo: expression with order by
        // 12; 'in'
        "select emp from sales.emps as emp where emp.deptno in ("
        + " select dept.deptno from sales.depts as dept where dept.deptno > 15)",
        

        // 13; 'in' with non-query
        "\"fred\" in new String[] {\"bill\",\"fred\"}",
        

        // 14; 'exists' as boolean expression
        "System.out.println(\"Exists: \" + (exists ("
        + " select {} from sales.emps where emps.deptno == 10"
        + ") ? \"yes\" : \"no\"))",
        

        // 15; 'exists' within query
        "select dept from sales.depts as dept where exists ("
        + " select {} from sales.emps as emp "
        + " where emp.deptno == dept.deptno && "
        + "  emp.gender.equals(\"F\"))",
        

        // 16; array expression
        "\"There are \" + ("
        + "select {} from sales.emps as emp where emp.deptno > 15"
        + ").length + \" female employees.\"",
        

        // 17; 'union'
        "(select emp from sales.emps as emp where emp.deptno == 10) "
        + "union "
        + "(select emp from sales.emps as emp where emp.deptno == 20)",
        

        // 18; 'union' with non-query
        "new int[] {1,3,5} union new int[] {3,2,1}",
        

        // 19; empty select, one table -- yields several rows of Emp
        "select from sales.emps",
        

        // 20; empty select, no tables -- yields one row, no columns
        "select",
        

        // 21; empty select, 2 tables -- yields several rows
        "select from sales.emps join sales.depts "
        + " on emps.deptno == depts.deptno",
        

        // 22; empty select, 1 table, results in unboxed select list, so this
        // FAILS
        "select t.emps from (select from sales.emps) as t /*FAILS*/",
        

        // 23; empty select, 2 tables, results in boxed select list, so this
        // SUCCEEDS
        "select t.emps from (" + " select from sales.emps"
        + " join sales.depts on emps.deptno == depts.deptno) as t",
        

        // 24; empty select, 0 tables, results in boxed select list
        "select t.getClass() from (select) as t",
        

        // 25; expression in boxed list must be aliasable, so this FAILS
        "select {1} from sales.emps /*FAILS*/",
        

        // 26; expression in unboxed list does not need to be aliasable, so
        // this SUCCEEDS
        "select 1 from sales.emps",
        

        // 27; empty from, singleton select -- yields one row {1}
        "select 1 as one",
        

        // 28; empty from, pair select -- yields one row {1, 2}
        "select {1 as x, 2 as y}",
        

        // 29; empty from, with subquery
        "select 1 as one where exists (select)",
        

        // 30; empty from, with subquery
        "select 1 as one where exists (select where false)",
        

        // 31; empty from, with correlated subquery -- yields one row {1}
        "select i from new int[] {0,1,2} as i where exists (select where i > 1)",
        

        // 32; illegal reference of select item from subquery
        "select 1 as one where exists (select where one > 0) /*FAILS*/",
        

        // 33; refer to correlating variables 2 levels out
        "select from sales.emps as emp where emp.deptno in ("
        + " select dept.deptno from sales.depts as dept where exists ("
        + "  select \"xyz\" from sales.emps as emp2"
        + "  where emp2.deptno == dept.deptno &&"
        + "  !emp2.gender.equals(emp.gender)))",
        

        // todo: test compatibility of types in set operations
        // todo: Emp[][] empss; select * from empss as emps where (select 1
        // from emps as emp)
        // todo: Emp[][] empss; select * from empss as emps where exists
        // (select {} from emps as emp)
        //  -- should return all arrays which have at least one element
        // xx; only, one row -- yields Emp {100, "Fred", 10}
        "only (select from sales.emps as emp where emp.empno == 100)",
        

        // xx; only, no rows -- yields null
        "only (select from sales.emps as emp where false)",
        

        // xx; only, several rows -- throws exception
        "only (select from sales.emps)",
        

        // xx; only, in subquery -- yields Emp {110, "Eric", 20}
        "select from sales.emps as emp where emp.empno == 10 + only ("
        + " select emps.empno from sales.emps where emps.name.equals(\"Fred\"))",
        

        // xx; column is relation
        "select dept.deptno as deptno, "
        + " (select from sales.emps as emp where emp.deptno == dept.deptno) "
        + " as emps " + "from sales.depts",
        

        // iterators
        "select twos " + "from (int[]) new Iterator() { " + "		 int i; "
        + "		 boolean hasNext() { return true; } "
        + "		 Object nextElement() { return new Integer(i += 2); } "
        + "	 } as twos " + "join (int[]) new Iterator() { " + "		 int i; "
        + "		 boolean hasNext() { return true; } "
        + "		 Object nextElement() { return new Integer(i += 3); } "
        + "	 } as threes on twos == threes)) { ",
    };
    private static String [] groupStatements =
    {
        
        // #0
        "select {sum(emp.deptno) as sumDeptno, emp.gender} "
        + "group by {emp.gender} " + "from sales.emps as emp",
        

        // #1 empty group by
        "select {" + nl + " sum(emp.deptno) as sumDeptno," + nl
        + " min(emp.deptno) as minDeptno}" + nl + "group by {}"
        + "from sales.emps as emp",
        

        // #2 empty select
        "select {} group by {emp.deptno} from sales.emps as emp",
        

        // #2.5 todo: "select group by {emp.deptno} from sales.emps as emp",
        // #3 group not in select; unboxed select
        "select max(emp.gender + \"X\") as maxGenderX" + nl
        + "group by {emp.deptno}" + nl + "from sales.emps as emp",
        

        // #4 where clause
        "select {sum(emp.deptno) as sumDeptno}" + nl + "group by {emp.gender}"
        + nl + "from sales.emps as emp" + nl
        + "where emp.name != null && count() > 2",
        

        // #5 count
        // #6 subquery in where
        // #7 group by contains sub-expressions
        "select {t.x + t.y as a, t.x + t.y + t.z + 1 as b} "
        + "group by {t.x, t.x + t.y + t.z, t.y} "
        + "from (select {1 as x, 2 as y, 4 as z}) as t",
        

        // #8 3 forms of min()
        "select {" + nl + " emp.deptno," + nl + " min(emp.empno) as minEmpno,"
        + nl + " min(emp.gender) as minGender," + nl
        + " min(java.text.Collator.getInstance(), emp.gender) as minGender2} "
        + nl + "group by {emp.deptno} " + nl + "from sales.emps as emp",
        

        // #9 select distinct
        "select distinct {emp.deptno, emp.gender} from sales.emps as emp",
        

        // #10 hand-coded median
        "select {nth(countEmp / 2, salary) as medianSalary}" + nl
        + "group by {emp.deptno}" + nl
        + "from (select from emp order by emp.deptno) empSorted" + nl
        + "join (select {emp.deptno, count() as empCount}" + nl
        + "	   group by {emp.deptno} from emp) empCounted" + nl
        + "on empSorted.deptno == empCounted.deptno",
        

        // #11 hand-coded count distinct
        "select {" + nl + "	 empGrouped.deptno," + nl
        + "	 count(empGrouped.gender) as countDistinctGender}" + nl
        + "group by {empGrouped.deptno}" + nl + "from (" + nl
        + "	 select distinct {emp.deptno, emp.gender}" + nl
        + "	 from emps as emp) as empGrouped",
        

        // #12 locale
        "select {" + nl
        + "  new saffron.ext.LocaleMin(Locale.FRANCE).aggregate(emp.name)"
        + "    as minName," + nl
        + "  new Nth(3).aggregate(emp.gender) as gender}" + nl
        + "group by {emp.deptno}" + nl + "from sales.emps as emp",
    };
    private static String [] statementsWhichFail =
    { 
        
        // 1; this should fail ("emp not found")
        "select emp from emp",};
    static final String [] plusStatements =
    {
        
        // 0; read from array
        "select from sales.emps", 
        // 1; read from Vector
        "select from sales.vectorEmps", 
        // 2; read from Collection
        "select from sales.collEmps",
        

        // 3; read from Enumeration expression
        "select from sales.vectorEmps.elements() as x",
        

        // 4; read from Iterator expression
        "select from sales.collEmps.iterator() as x",
        

        // 5; read from Enumeration function
        "select from sales.getEmpsEnum() as x",
        

        // 6; read from Iterator function
        "select from sales.getEmpsIter() as x",
        

        // 7; read from parameterized Enumeration function
        "select from sales.getEmpsFilterEnum(\"F\") as x",
        

        // 8; array cast to iterator
        "(java.util.Iterator) (select from sales.emps)",
        

        // 9; enumeration cast to iterator
        "(java.util.Iterator) (select from sales.getEmpsFilterEnum(\"M\") as x)",
        

        // 10; scan hashtable
        "select {(String) map.key, (sales.Sales.Emp) map.value} "
        + "from sales.mapName2emp as map",
        

        // 11; scan HashMap
        "select from sales.htName2deptno as map join sales.depts as dept "
        + "on ((Integer) map.value).intValue() == dept.deptno",
        

        // 12; 'union' (copied from 17)
        "(select emp from sales.emps as emp where emp.deptno == 10) "
        + "union "
        + "(select emp from sales.emps as emp where emp.deptno == 20) ",
    };
    PrintWriter pw;

    Test(String [] args)
        throws SQLException
    {
        super("sales test");
        RelOptConnection connection = new SalesInMemoryConnection();
        this.pw = init(3);
        try {
            switch (2) {
            case 0:
                run0(args);
                break;
            case 1:
                run1();
                break;
            case 2:
                run2();
                break;
            case 3:
                run3(connection);
                break;
            default:
                throw new Error();
            }
        } finally {
            pw.flush();
        }
    }

    public static PrintWriter init(int debug)
    {
        DebugOut.setDebugLevel(debug);
        DebugOut.setDebugOut(System.out);
        boolean autoFlush = true;
        return new PrintWriter(System.out, autoFlush);
    }

    public static void main(String [] args)
        throws SQLException
    {
        new Test(args);
    }

    public void testIntArray()
    {
        Object o = run("select i from new int[] {1,2,3} as i");
        assertTrue(o instanceof int []);
        int [] result = (int []) o;
        assertTrue(result.length == 3);
        assertTrue((result[0] == 1) && (result[1] == 2) && (result[2] == 3));
    }

    void initJdbc()
    {
        try {
            //			Class.forName("com.ms.jdbc.odbc.JdbcOdbcDriver");
            Class.forName("sun.jdbc.odbc.JdbcOdbcDriver");
        } catch (ClassNotFoundException e) {
            throw new Error(e.toString());
        }
    }

    void run0(String [] args)
    {
        SalesInMemoryConnection connection = new SalesInMemoryConnection();
        runOne(connection, pw, "select i from new int[] {1,2,3} as i", "",
            noArguments);
    }

    void run1()
        throws java.sql.SQLException
    {
        initJdbc();
        java.sql.Connection sqlConnection =
            java.sql.DriverManager.getConnection("jdbc:odbc:empdept");
        Sales sales = new Sales(sqlConnection);
        runList(
            sales,
            new OJStatement.Argument [] {
                new OJStatement.Argument("sales", sales)
            },
            groupStatements);
    }

    void run2()
        throws java.sql.SQLException
    {
        SalesInMemoryConnection connection = new SalesInMemoryConnection();
        runList(
            connection,
            new OJStatement.Argument [] {
                new OJStatement.Argument("sales", connection)
            },
            statements);
    }

    void run3(RelOptConnection connection)
        throws java.sql.SQLException
    {
        FakeSalesPlus sales = new FakeSalesPlus();
        runList(
            connection,
            new OJStatement.Argument [] {
                new OJStatement.Argument("sales", sales)
            },
            plusStatements);
    }

    void runList(
        RelOptConnection connection,
        OJStatement.Argument [] args,
        String [] statements)
        throws SQLException
    {
        boolean autoFlush = true;
        PrintWriter pw = new PrintWriter(System.out, autoFlush);
        try {
            initJdbc();
            for (int i = 0; i < statements.length; i++) {
                String statement = statements[i];
                if (i < 4) {
                    continue;
                }
                if (statement.indexOf("/*FAILS*/") >= 0) {
                    continue;
                }
                runOne(connection, pw, statement, "Statement #" + i, args);
            }
        } finally {
            pw.flush();
        }
    }

    void runOne(
        RelOptConnection connection,
        PrintWriter pw,
        String s,
        String desc,
        OJStatement.Argument [] args)
    {
        pw.println();
        pw.println(desc + " [" + s + "]");
        OJStatement statement = new OJStatement(connection);
        try {
            Object o = statement.execute(s, args);
            pw.println("Result is " + o);
            Util.print(pw, o);
            pw.println();
        } catch (Throwable e) {
            pw.println("Received exception " + e.getClass() + ": " + e
                + " while executing " + desc + " [" + s + "]");
        }
    }

    private Object run(
        String s,
        OJStatement.Argument [] args)
    {
        SalesInMemoryConnection connection = new SalesInMemoryConnection();
        OJStatement statement = new OJStatement(connection);
        return statement.execute(s, args);
    }

    private Object run(String s)
    {
        return run(s, noArguments);
    }

    public class FakeSalesPlus extends SalesInMemory
    {
        public ArrayList collEmps = new ArrayList();
        public HashMap mapName2emp = new HashMap();
        public Hashtable htName2deptno = new Hashtable();
        public Vector vectorEmps = new Vector();

        public FakeSalesPlus()
        {
            for (int i = 0; i < emps.length; i++) {
                htName2deptno.put(
                    emps[i].name,
                    new Integer(emps[i].deptno));
                mapName2emp.put(emps[i].name, emps[i]);
                vectorEmps.addElement(emps[i]);
                collEmps.add(emps[i]);
            }
        }

        public Enumeration getEmpsEnum()
        {
            return vectorEmps.elements();
        }

        public Enumeration getEmpsFilterEnum(String gender)
        {
            Vector empsOfGender = new Vector();
            for (int i = 0; i < emps.length; i++) {
                if (emps[i].gender.equals(gender)) {
                    empsOfGender.addElement(emps[i]);
                }
            }
            return empsOfGender.elements();
        }

        public Iterator getEmpsIter()
        {
            return collEmps.iterator();
        }
    }
}


// End Test.java
