/*
// $Id$
// Saffron preprocessor and data engine
// (C) Copyright 2002-2003 Disruptive Technologies, Inc.
// You must accept the terms in LICENSE.html to use this software.
//
// This program is free software; you can redistribute it and/or
// modify it under the terms of the GNU Lesser General Public License
// as published by the Free Software Foundation; either version 2.1
// of the License, or (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with this program; if not, write to the Free Software
// Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
*/

package sales;

import net.sf.saffron.oj.stmt.OJStatement;
import org.eigenbase.runtime.Iterable;
import org.eigenbase.runtime.SyntheticObject;
import net.sf.saffron.test.SaffronTestCase;
import org.eigenbase.util.Util;

import java.util.*;


/**
 * <code>SalesTestCase</code> creates a suite of tests which use the sales
 * schema.
 *
 * @author jhyde
 * @version $Id$
 *
 * @since 26 April, 2002
 */
public abstract class SalesTestCase extends SaffronTestCase
{
    //~ Static fields/initializers --------------------------------------------

    private static final int empCount = 4;

    //~ Constructors ----------------------------------------------------------

    public SalesTestCase(String s) throws Exception
    {
        super(s);
    }

    //~ Methods ---------------------------------------------------------------

    /**
     * Disabled, because openjava can't seem to handle this anonymous class
     * syntax.
     */
    public void _testAnonymousIterator()
    {
        Iterator j =
            new Iterator() {
                int i;

                public boolean hasNext()
                {
                    return true;
                }

                public void remove()
                {
                }

                public Object next()
                {
                    return new Integer(i += 2);
                }
            };
        Util.discard(j);
        Object o =
            runQuery(
                "select twos " + "from (int[]) new java.util.Iterator() { "
                + "		 int i; "
                + "		 public boolean hasNext() { return true; } "
                + "		 public void remove() { } "
                + "		 public Object next() { return new Integer(i += 2); } "
                + "	 } as twos " + "join (int[]) new java.util.Iterator() { "
                + "		 int i; "
                + "		 public boolean hasNext() { return i < 100; } "
                + "		 public void remove() { } "
                + "		 public Object next() { return new Integer(i += 3); } "
                + "	 } as threes " + "on twos == threes");
        assertTrue(o instanceof int []);
        int [] a = (int []) o;
        assertEquals(a.length,16);
        assertEquals(a[0],6);
    }

    public void _testCorrelatedFromTwice()
    {
        Object o =
            runQuery(
                "select " + "from salesDb.emps as emp "
                + "join (select from salesDb.depts as d "
                + "      where d.deptno == emp.empno) as dept "
                + "join (select from salesDb.depts as d "
                + "      where d.deptno == emp.empno) as dept2");
        assertSynthetic(o,12,new String [] { "emp","dept","dept2" });
    }

    public void _testExists()
    {
        Object o =
            runQuery(
                "select dept from salesDb.depts as dept where exists ("
                + " select {} from salesDb.emps as emp "
                + " where emp.deptno == dept.deptno && "
                + "  emp.gender.equals(\"F\"))");
        assertTrue(o instanceof Dept []);
        Dept [] depts = (Dept []) o;
        assertEquals(depts.length,1);
        assertTrue(depts[0].deptno == 20);
    }

    public void _testExpressionWithOrderBy()
    {
        // todo:
    }

    public void _testInAgainstNullable()
    {
        // if we implement 'in' using an outer join and a 'null' test, we'll
        // get problems if one of the rows is actually null
        Object o =
            runQuery(
                "select "
                + "from new String[] {\"john\", \"paul\", null, \"jones\"} as s "
                + "where s in (" + " select " + " from new String[] {"
                + "  \"john\", \"paul\", null, \"george\", \"ringo\"})");
        assertEqualsDeep(new String [] { "john","paul",null },o);
    }

    public void _testIteratorJoin()
    {
        Iterator twos =
            new Iterator() {
                int i;

                public boolean hasNext()
                {
                    return i < 100;
                }

                public void remove()
                {
                }

                public Object next()
                {
                    return new Integer(i += 2);
                }
            };
        Iterator threes =
            new Iterator() {
                int i;

                public boolean hasNext()
                {
                    return i < 100;
                }

                public void remove()
                {
                }

                public Object next()
                {
                    return new Integer(i += 3);
                }
            };

        // if we put twos before threes in the from list, this wouldn't finish,
        // because twos is infinite, and it would end up as the outer loop
        Object o =
            runQuery(
                "select tt " + "from (int[]) threes as ttt "
                + "join (int[]) twos as tt " + "on tt == ttt",
                new OJStatement.Argument [] {
                    new OJStatement.Argument("twos",Iterator.class,twos),
                    new OJStatement.Argument("threes",Iterator.class,threes)
                });
        assertTrue(o instanceof int []);
        int [] a = (int []) o;
        assertEquals(a.length,16);
        assertEquals(a[0],6);
    }

    /**
     * Runs a query where one of the 'scalar expressions' is a relational
     * expression which yields an array.
     * 
     * <p>
     * Disabled for now. Should we translate the inner expression before or
     * after the outer? If before, the Rel tree can get huge. If afterwards,
     * the correlating variables the inner refers to ('dept' in this case)
     * will no longer exist. How about <i>during</i> (in other words,
     * <code>QueryInfo.convertExpToInternal()</code> does it)?
     * </p>
     */
    public void _testNested()
    {
        // column is relation
        Object o =
            runQuery(
                "select {" + " dept.deptno as deptno, "
                + " (select from salesDb.emps as emp "
                + "  where emp.deptno == dept.deptno) as emps} "
                + "from salesDb.depts as dept");
        assertTrue(o instanceof SyntheticObject []);
        SyntheticObject [] a = (SyntheticObject []) o;
        Object v0 = a[1].getFieldValue(0);
        assertTrue(v0 instanceof Integer);
        assertTrue(((Integer) v0).intValue() == 10);
        Object v1 = a[1].getFieldValue(1);
        assertTrue(v1 instanceof Emp []);
        assertTrue(((Emp []) v1).length == 2);
    }

    // todo: test compatibility of types in set operations
    // todo: Emp[][] empss; select * from empss as emps where (select 1
    // from emps as emp)
    // todo: Emp[][] empss; select * from empss as emps where exists
    // (select {} from emps as emp)
    //  -- should return all arrays which have at least one element
    public void _testOnly()
    {
        // only, one row -- yields Emp {100, "Fred", 10}
        Object o =
            runQuery(
                "only (select from salesDb.emps as emp where emp.empno == 100)");
        assertTrue(o instanceof Emp);
    }

    public void _testOnlyInSubquery()
    {
        // only, in subquery -- yields Emp {110, "Eric", 20}
        Object o =
            runQuery(
                "select from salesDb.emps as emp "
                + "where emp.empno == 10 + only ("
                + " select emps.empno from salesDb.emps "
                + " where emps.name.equals(\"Fred\"))");
        assertTrue(o instanceof Emp);
        Emp emp = (Emp) o;
        assertEquals(emp.name,"Eric");
    }

    public void _testOnlySeveralRowsFails()
    {
        // only, several rows -- throws exception
        Throwable throwable = runQueryCatch("only (select from salesDb.emps)");
        assertThrowableContains(throwable,"foo");
    }

    public void _testOnlyZeroRows()
    {
        // only, no rows -- yields null
        Object o =
            runQuery("only (select from salesDb.emps as emp where false)");
        assertNull(o);
    }

    public void _testQueryInFromWithSelectList()
    {
        Object o =
            runQuery(
                "select {emp, dept} from ("
                + " select {emp.name.substring(0,1) as initial,"
                + "  emp.deptno * 2 as twiceDeptno} "
                + " from salesDb.emps as emp "
                + " where emp.deptno == 10) as emp "
                + "join salesDb.depts as dept "
                + " on emp.twiceDeptno == dept.deptno");
        assertSynthetic(o,1,new String [] { "emp","dept" });
    }

    /**
     * Runs several test cases. Edit it for debugging purposes.
     */
    public void _testSeveral()
    {
        testCorrelatedFrom();
        testObjectPair();
        testIn();
    }

    public void _testUnaliasedBoxedSelectListFails()
    {
        // expression in boxed list must be aliasable, so this FAILS
        Throwable throwable = runQueryCatch("select {1} from salesDb.emps");
        assertThrowableContains(throwable,"foo");
    }

    public void _testUnionNoParentheses()
    {
        Util.discard(
            runQuery(
                "select emp.empno from salesDb.emps as emp " + "union "
                + "select dept.deptno from salesDb.depts as dept"));
    }

    public void testArrayInExpr()
    {
        Object o =
            runQuery(
                "\"There are \" + (" + "select {} from salesDb.emps as emp "
                + " where emp.gender.equals(\"F\")"
                + ").length + \" female employees.\"");
        assertEquals(o,"There are 1 female employees.");
    }

    public void testBoxedInt()
    {
        Object o = runQuery("select {1 as tt} from salesDb.emps");
        assertSynthetic(o,empCount,new String [] { "tt" });
    }

    public void testBoxedIntSyntaxError()
    {
        Throwable throwable =
            runQueryCatch("select 1 as tt from salesDb.emps");
        assertTrue(throwable != null);
    }

    public void testCartesianProduct()
    {
        Object o =
            runQuery(
                "select " + "from salesDb.depts as dept "
                + "join salesDb.emps as emp");
        assertSynthetic(o,12,new String [] { "dept","emp" });
    }

    public void testCorrelatedExistsMissingFrom()
    {
        // 31; empty from, with correlated subquery -- yields one row {1}
        Object o =
            runQuery(
                "select i from new int[] {0,3,1,2} as i "
                + "where exists (select where i > 1)");
        assertEqualsDeep(o,new int [] { 3,2 });
    }

    public void testCorrelatedFrom()
    {
        Object o =
            runQuery(
                "select " + "from (select from salesDb.depts as d "
                + "      where d.deptno == emp.deptno) as dept "
                + "join salesDb.emps as emp");
        assertSynthetic(o,3,new String [] { "dept","emp" });
    }

    public void testCorrelatedReferToVariables2LevelsOut()
    {
        // 33; refer to correlating variables 2 levels out
        Object o =
            runQuery(
                "select from salesDb.emps as emp where emp.deptno in ("
                + " select dept.deptno from salesDb.depts as dept where exists ("
                + "  select \"xyz\" from salesDb.emps as emp2"
                + "  where emp2.deptno == dept.deptno &&"
                + "  !emp2.gender.equals(emp.gender)))");
        assertTrue(o instanceof Emp []);
        Emp [] emps = (Emp []) o;
        assertTrue(emps.length == 2);
        assertTrue(emps[0].deptno == 20);
        assertTrue(emps[1].deptno == 20);
    }

    public void testEmptyFrom()
    {
        // empty from list means 'zero relations' -- hence, one row with no
        // columns -- but the type of the output is determined by the
        // select list, hence 'int' (not '{int}')
        Object o = runQuery("select 5 where 2 < 3");
        assertEqualsDeep(o,new int [] { 5 });
    }

    public void testEmptyFrom2()
    {
        Object o = runQuery("select false where 2 > 3");

        // 0 rows of 0 columns
        assertTrue(o instanceof boolean []);
        assertEqualsDeep(o,new boolean [] {  });
    }

    public void testEmptyFrom3()
    {
        Object o = runQuery("select \"hello\"");
        assertEqualsDeep(o,new String [] { "hello" });
    }

    public void testEmptyFromPairSelect()
    {
        // 28; empty from, pair select -- yields one row {1, 2}
        Object o = runQuery("select {1 as x, null as y}");
        assertSynthetic(
            o,
            1,
            new String [] { "x","y" },
            new Class [] { int.class,Object.class });
        SyntheticObject [] a = (SyntheticObject []) o;
        assertEquals(a.length,1);
        assertEquals(a[0].getFieldValue(0),new Integer(1));
        assertNull(a[0].getFieldValue(1));
    }

    public void testEmptyFromSingletonSelect()
    {
        // empty from, singleton select -- yields one row {1}
        Object o = runQuery("select 5");
        assertEqualsDeep(o,new int [] { 5 });
    }

    public void testEmptySelectInFromQueryFails()
    {
        // empty select, 1 table, results in unboxed select list, so this
        // FAILS
        Throwable throwable =
            runQueryCatch(
                "select t.emps from (select from salesDb.emps) as t");
        assertThrowableContains(throwable,"no field emps is accessible");
    }

    public void testEmptySelectList()
    {
        // empty select list means select the whole object
        Object o = runQuery("select from salesDb.emps as emp");
        assertTrue(o instanceof Emp []);
        Emp [] emps = (Emp []) o;
        assertEquals(emps.length,empCount);
    }

    public void testEmptySelectNoAlias()
    {
        // if there's only from item, it doesn't need an alias
        Object o = runQuery("select from new String[] {\"foo\",null}");
        assertEqualsDeep(new String [] { "foo",null },o);
    }

    public void testExistsAsExpression()
    {
        Object o =
            runQuery(
                "\"Exists: \" + (exists ("
                + "select {} from salesDb.emps where emps.deptno == 10"
                + ") ? \"yes\" : \"no\")");
        assertEquals(o,"Exists: yes");
    }

    public void testExistsEmptyFrom()
    {
        // 29; empty from, with subquery
        Object o = runQuery("select 1 where exists (select)");
        assertEqualsDeep(o,new int [] { 1 });
    }

    public void testExistsEmptyFromFalse()
    {
        // 30; empty from, with subquery
        Object o = runQuery("select 1 where exists (select where false)");
        assertEqualsDeep(o,new int[0]);
    }

    public void testIn()
    {
        Object o =
            runQuery(
                "select emp from salesDb.emps as emp "
                + "where emp.deptno in ("
                + " select dept.deptno from salesDb.depts as dept "
                + " where dept.deptno > 15)");
        assertTrue(o instanceof Emp []);
        assertEquals(((Emp []) o).length,2);
    }

    public void testInAsExpression()
    {
        Object o = runQuery("\"fred\" in new String[] {\"bill\",\"fred\"}");
        assertEquals(o,Boolean.TRUE);
    }

    public void testInIntoJoin()
    {
        Object o =
            runQuery(
                "select from new String[] {\"apple\", \"orange\", \"grape\"} as s "
                + "where s in new String[] {\"blue\", \"green\", \"orange\", \"yellow\"}");
        assertEquals(new String [] { "orange" },o);
    }

    public void testInnerJoin()
    {
        Object o =
            runQuery(
                "select {} from salesDb.emps as emp "
                + "join salesDb.depts as dept on emp.deptno == dept.deptno");
        assertSynthetic(o,3,emptyStringArray);
    }

    // -----------------------------------------------------------------------
    // the tests
    public void testIntArray()
    {
        Object o = runQuery("select i from new int[] {1,2,3} as i");
        assertTrue(o instanceof int []);
        int [] a = (int []) o;
        assertTrue(a.length == 3);
        assertTrue((a[0] == 1) && (a[1] == 2) && (a[2] == 3));
    }

    public void testIntUnion()
    {
        Object o = runQuery("new int[] {1,3,5} union new int[] {3,2,1}");
        assertTrue(o instanceof int []);
        int [] a = (int []) o;
        Arrays.sort(a);
        assertEqualsDeep(a,new int [] { 1,2,3,5 });
    }

    public void testIterable()
    {
        Iterable iterable =
            new Iterable() {
                public Iterator iterator()
                {
                    return makeIterator(new String [] { "foo","bar" });
                }
            };
        Object o =
            runQuery(
                "select string " + "from (String[]) iterable as string "
                + "where string.startsWith(\"b\")",
                new OJStatement.Argument [] {
                    new OJStatement.Argument(
                        "iterable",
                        Iterable.class,
                        iterable)
                });
        assertTrue(o instanceof String []);
        String [] a = (String []) o;
        assertEquals(a.length,1);
        assertEquals(a[0],"bar");
    }

    public void testIterator()
    {
        Iterator evens =
            new Iterator() {
                int i;

                public boolean hasNext()
                {
                    return i < 20;
                }

                public void remove()
                {
                }

                public Object next()
                {
                    return new Integer(i += 2);
                }
            };
        Object o =
            runQuery(
                "select tt " + "from (int[]) evens as tt " + "where tt < 10",
                new OJStatement.Argument [] {
                    new OJStatement.Argument("evens",Iterator.class,evens)
                });
        assertTrue(o instanceof int []);
        int [] a = (int []) o;
        assertEquals(a.length,4);
        assertEquals(a[0],2);
        assertEquals(a[3],8);
    }

    public void testIteratorJoinedToArray()
    {
        Iterator fours =
            new Iterator() {
                int i;

                public boolean hasNext()
                {
                    return i < 100;
                }

                public void remove()
                {
                }

                public Object next()
                {
                    return new Integer(i += 4);
                }
            };
        Object o =
            runQuery(
                "select {four, emp}" + "from (int[]) fours as four "
                + "join salesDb.emps as emp " + "on four == emp.deptno",
                new OJStatement.Argument [] {
                    new OJStatement.Argument("fours",Iterator.class,fours),
                    this.arguments[0]
                });
        assertSynthetic(
            o,
            3,
            new String [] { "four","emp" },
            new Class [] { int.class,Emp.class });
    }

    public void testJoinEmptySelect()
    {
        // 21; empty select, 2 tables -- yields several rows
        Object o =
            runQuery(
                "select from salesDb.emps join salesDb.depts "
                + " on emps.deptno == depts.deptno");
        assertSynthetic(o,3,new String [] { "emps","depts" });
    }

    public void testJoinWhere()
    {
        Object o =
            runQuery(
                "select from new int[] {1,2} as i" + nl
                + "join new int[] {1,3} as j on i == j" + nl
                + "where j in new int[] {1,3,5}");
        assertSynthetic(o,1,new String [] { "i","j" });
    }

    public void testLeftJoin()
    {
        Object o =
            runQuery(
                "select {} from salesDb.emps as emp "
                + "left join salesDb.depts as dept on emp.deptno == dept.deptno");
        assertSynthetic(o,3,emptyStringArray);
    }

    public void testNoSelectListOrFrom()
    {
        Object o = runQuery("select");
        assertSynthetic(o,1,emptyStringArray);
    }

    public void testNullSelect()
    {
        Object o = runQuery("select null from salesDb.depts");
        assertEqualsDeep(o,new Object [] { null,null,null });
    }

    public void testObjectPair()
    {
        Object o =
            runQuery(
                "select {emp, dept} from salesDb.emps as emp "
                + "join salesDb.depts as dept "
                + "on emp.deptno == dept.deptno " + "where emp.deptno > 15");
        assertSynthetic(o,2,new String [] { "emp","dept" });
    }

    public void testOutputAsArray()
    {
        Object o = runQuery("(sales.Emp[]) (select from salesDb.emps)");
        assertTrue(o instanceof Emp []);
        assertEquals(4,((Emp []) o).length);
    }

    public void testOutputAsCollection()
    {
        Object o =
            runQuery("(java.util.Collection) (select from salesDb.emps)");
        assertTrue(o instanceof Collection);
        assertEquals(4,((Collection) o).size());
    }

    public void testOutputAsEnumeration()
    {
        Object o =
            runQuery("(java.util.Enumeration) (select from salesDb.emps)");
        assertTrue(o instanceof Enumeration);
        assertEquals(4,toList((Enumeration) o).size());
    }

    public void testOutputAsIterable()
    {
        Object o =
            runQuery("(saffron.runtime.Iterable) (select from salesDb.emps)");
        assertTrue(o instanceof Iterable);
        assertEquals(4,toList(((Iterable) o).iterator()).size());
    }

    public void testOutputAsIterator()
    {
        Object o = runQuery("(java.util.Iterator) (select from salesDb.emps)");
        assertTrue(o instanceof Iterator);
        assertEquals(4,toList((Iterator) o).size());
    }

    public void testOutputAsResultSet()
    {
        Object o = runQuery("(java.sql.ResultSet) (select from salesDb.emps)");
        assertTrue(o instanceof java.sql.ResultSet);
    }

    public void testOutputAsVector()
    {
        Object o = runQuery("(java.util.Vector) (select from salesDb.emps)");
        assertTrue(o instanceof Vector);
        assertEquals(4,((Vector) o).size());
    }

    public void testQueryInFrom()
    {
        Object o =
            runQuery(
                "select {emp, dept} from ("
                + " select emps from salesDb.emps "
                + " where emps.deptno == 10) as emp "
                + "join salesDb.depts as dept "
                + " on emp.deptno == dept.deptno");
        assertSynthetic(o,1,new String [] { "emp","dept" });
    }

    public void testReferenceEnclosingSelectItemFails()
    {
        // illegal reference of select item from subquery
        Throwable throwable =
            runQueryCatch(
                "select {1 as one, 2 as two} where exists ("
                + " select where one > 0)");
        assertThrowableContains(throwable,"unknown field or variable: one");
    }

    public void testSingletonBoxed()
    {
        // RemoveTrivialProjectRule would erroneously remove the project,
        // causing the type to change from {int} to int.
        Object o = runQuery("select {x} from new int[] {1,2,3} as x");
        assertSynthetic(o,3,new String [] { "x" });
    }

    public void testUnaliasableUnboxed()
    {
        // expression in unboxed list does not need to be aliasable, so
        // this SUCCEEDS
        Object o = runQuery("select 1 from salesDb.emps");
        assertTrue(o instanceof int []);
        assertTrue(((int []) o).length == empCount);
    }

    /**
     * The danger is that the from expression 'fruit' will be resolved in the
     * wrong context, and thus resolve to itself.
     */
    public void testUnaliasedVariableInFrom()
    {
        String [] fruit = new String [] { "apple","pear" };
        Object o =
            runQuery(
                "select from fruit",
                new OJStatement.Argument [] {
                    new OJStatement.Argument("fruit",fruit)
                });
        assertEqualsDeep(o,fruit);
    }

    public void testUnboxedInt()
    {
        Object o =
            runQuery(
                "select 1 from salesDb.emps as emp where emp.deptno > 30");
        assertTrue(o instanceof int []);
        int [] a = (int []) o;
        assertTrue(a.length == 1);
        assertTrue(a[0] == 1);
    }

    public void testUnboxedSelectOnJoinFromQuery()
    {
        // empty select, 2 tables, results in boxed select list, so this
        // SUCCEEDS
        Object o =
            runQuery(
                "select t.emps from (" + " select from salesDb.emps"
                + " join salesDb.depts on emps.deptno == depts.deptno) as t");
        assertTrue(o instanceof Emp []);
        assertTrue(((Emp []) o).length == 3);
    }

    public void testUnboxedSelectOnZeroTableFromQuery()
    {
        // empty select, 0 tables, results in boxed select list
        Object o = runQuery("select t.getClass() from (select) as t");
        assertTrue(o instanceof Class []);
        assertTrue(((Class []) o).length == 1);
    }

    public void testUnion()
    {
        Object o =
            runQuery(
                "(select emp from salesDb.emps as emp where emp.deptno == 10) "
                + "union "
                + "(select emp from salesDb.emps as emp where emp.deptno == 20)");
        assertTrue(o instanceof Emp []);
        Emp [] emps = (Emp []) o;
        assertTrue(emps.length == 3);
    }

    public void testZeroColumns()
    {
        // 4 rows of zero columns
        Object o = runQuery("select {} from salesDb.emps as emp");
        assertSynthetic(o,empCount,emptyStringArray);
    }

    // todo: connection which comes from rel exp, e.g. "select from (select
    // from new SalesSchema[] {sales}).emps"
    // todo: "Emp[] emps = salesDb.emps" (check that optimizer kicks in and
    // removes contentsAsArray)
    // todo: "for (i in salesDb.emps) ..." (table usage not inside a select)
}


// End SalesTestCase.java
