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

package org.eigenbase.sql.parser;

import java.util.regex.Pattern;
import junit.framework.TestCase;
import junit.framework.AssertionFailedError;

import org.eigenbase.sql.SqlNode;
import org.eigenbase.util.Util;


/**
 * A <code>SqlParserTest</code> is a unit-test for {@link SqlParser the SQL
 * parser}.
 *
 * @author jhyde
 * @version $Id$
 *
 * @since Mar 19, 2003
 */
public class SqlParserTest extends TestCase
{
    //~ Static fields/initializers --------------------------------------------

    protected static final String NL = System.getProperty("line.separator");

    //~ Constructors ----------------------------------------------------------

    public SqlParserTest(String name)
    {
        super(name);
    }

    //~ Methods ---------------------------------------------------------------

    // Helper functions -------------------------------------------------------
    protected void check(
        String sql,
        String expected)
    {
        final SqlNode sqlNode;
        try {
            sqlNode = parseStmt(sql);
        } catch (ParseException e) {
            String message = "Received error while parsing SQL '" + sql +
                    "'; error is:" + NL + e.toString();
            throw new AssertionFailedError(message);
        }
        final String actual = sqlNode.toSqlString(null);
        assertEqualsUnabridged(expected, actual);
    }

    protected SqlNode parseStmt(String sql) throws ParseException {
        return new SqlParser(sql).parseStmt();
    }

    protected void checkExp(
        String sql,
        String expected)
    {
        final SqlNode sqlNode;
        try {
            sqlNode = parseExpression(sql);
        } catch (ParseException e) {
            String message = "Received error while parsing SQL '" + sql +
                    "'; error is:" + NL + e.toString();
            throw new AssertionFailedError(message);
        }
        final String actual = sqlNode.toSqlString(null);
        assertEqualsUnabridged(expected, actual);
    }

    protected SqlNode parseExpression(String sql) throws ParseException {
        return new SqlParser(sql).parseExpression();
    }

    protected void checkExpSame(String sql)
    {
        checkExp(sql, sql);
    }

    private void assertEqualsUnabridged(
        String expected,
        String actual)
    {
        if (!expected.equals(actual)) {
            // REVIEW jvs 2-Feb-2004:  I put this here because assertEquals
            // uses ellipses in its expected/actual reports, which makes
            // it very hard to find the problem with something like
            // a newline instead of a space.  Use diff-based testing instead;
            // it would also make updating the expected value much easier.
            String message =
                NL + "expected:<" + expected + ">" + NL + " but was:<"
                + actual + ">";
            fail(message);
        }
    }

    protected void checkFails(
        String sql,
        String exceptionPattern)
    {
        try {
            final SqlNode sqlNode = parseStmt(sql);
            Util.discard(sqlNode);
            fail("Expected query '" + sql + "' to throw exception matching '"
                + exceptionPattern + "'");
        } catch (Throwable e) {
            final String message = e.toString();
            if (!Pattern.matches(exceptionPattern, message)) {
                e.printStackTrace();
                fail("Expected query '" + sql
                    + "' to throw exception matching '" + exceptionPattern
                    + "', but it threw " + message);
            }
        }
    }

    protected void checkExpFails(String sql)
    {
        checkExpFails(sql, "(?s).*");
    }

    protected void checkExpFails(
        String sql,
        String exceptionPattern)
    {
        try {
            final SqlNode sqlNode = parseExpression(sql);
            Util.discard(sqlNode);
            fail("Expected expression '" + sql + "' to throw exception matching '"
                + exceptionPattern + "'");
        } catch (Throwable e) {
            final String message = e.toString();
            if (!Pattern.matches(exceptionPattern, message)) {
                e.printStackTrace();
                fail("Expected expression '" + sql
                    + "' to throw exception matching '" + exceptionPattern
                    + "', but it threw " + message);
            }
        }
    }

    public void _testDerivedColumnList()
    {
        check("select * from emp (empno, gender) where true", "foo");
    }

    public void _testDerivedColumnListInJoin()
    {
        check("select * from emp as e (empno, gender) join dept (deptno, dname) on emp.deptno = dept.deptno",
            "foo");
    }

    public void _testDerivedColumnListNoAs()
    {
        check("select * from emp e (empno, gender) where true", "foo");
    }

    public void _testDerivedColumnListWithAlias()
    {
        check("select * from emp as e (empno, gender) where true", "foo");
    }

    // jdbc syntax
    public void _testEmbeddedCall()
    {
        checkExp("{call foo(?, ?)}", "foo");
    }

    public void _testEmbeddedFunction()
    {
        checkExp("{? = call bar (?, ?)}", "foo");
    }

    public void testColumnAliasWithAs()
    {
        check("select 1 as foo from emp",
            "SELECT 1 AS `FOO`" + NL + "FROM `EMP`");
    }

    public void testColumnAliasWithoutAs()
    {
        check("select 1 foo from emp", "SELECT 1 AS `FOO`" + NL + "FROM `EMP`");
    }

    public void testEmbeddedDate()
    {
        checkExp("{d '1998-10-22'}", "DATE '1998-10-22'");
    }

    public void testEmbeddedTime()
    {
        checkExp("{t '16:22:34'}", "TIME '16:22:34'");
    }

    public void testEmbeddedTimestamp()
    {
        checkExp("{ts '1998-10-22 16:22:34'}",
            "TIMESTAMP '1998-10-22 16:22:34'");
    }

    public void testNot()
    {
        check("select not true, not false, not null, not unknown from t",
            "SELECT (NOT TRUE), (NOT FALSE), (NOT NULL), (NOT UNKNOWN)" + NL
            + "FROM `T`");
    }

    public void testBooleanPrecedenceAndAssociativity()
    {
        check("select * from t where true and false",
            "SELECT *" + NL + "FROM `T`" + NL + "WHERE (TRUE AND FALSE)");

        check("select * from t where null or unknown and unknown",
            "SELECT *" + NL + "FROM `T`" + NL
            + "WHERE (NULL OR (UNKNOWN AND UNKNOWN))");

        check("select * from t where true and (true or true) or false",
            "SELECT *" + NL + "FROM `T`" + NL
            + "WHERE ((TRUE AND (TRUE OR TRUE)) OR FALSE)");

        check("select * from t where 1 and true",
            "SELECT *" + NL + "FROM `T`" + NL + "WHERE (1 AND TRUE)");
    }

    public void testIsBooleans()
    {
        String [] inOut = { "NULL", "TRUE", "FALSE", "UNKNOWN" };

        for (int i = 0; i < inOut.length; i++) {
            check("select * from t where nOt fAlSe Is " + inOut[i],
                "SELECT *" + NL + "FROM `T`" + NL + "WHERE ((NOT FALSE) IS "
                + inOut[i] + ")");

            check("select * from t where c1=1.1 IS NOT " + inOut[i],
                "SELECT *" + NL + "FROM `T`" + NL
                + "WHERE ((`C1` = 1.1) IS NOT " + inOut[i] + ")");
        }
    }

    public void testIsBooleanPrecedenceAndAssociativity()
    {
        check("select * from t where x is unknown is not unknown",
            "SELECT *" + NL + "FROM `T`" + NL
            + "WHERE ((`X` IS UNKNOWN) IS NOT UNKNOWN)");

        check("select 1 from t where not true is unknown",
            "SELECT 1" + NL + "FROM `T`" + NL
            + "WHERE ((NOT TRUE) IS UNKNOWN)");

        check(
            "select * from t where x is unknown is not unknown is false is not false"
            + " is true is not true is null is not null",
            "SELECT *" + NL + "FROM `T`" + NL
            + "WHERE ((((((((`X` IS UNKNOWN) IS NOT UNKNOWN) IS FALSE) IS NOT FALSE) IS TRUE) IS NOT TRUE) IS NULL) IS NOT NULL)");

        // combine IS postfix operators with infix (AND) and prefix (NOT) ops
        check("select * from t where x is unknown is false and x is unknown is true or not y is unknown is not null",
            "SELECT *" + NL + "FROM `T`" + NL
            + "WHERE ((((`X` IS UNKNOWN) IS FALSE) AND ((`X` IS UNKNOWN) IS TRUE)) OR (((NOT `Y`) IS UNKNOWN) IS NOT NULL))");
    }

    public void testEqualNotEqual()
    {
        checkExp("'abc'=123", "('abc' = 123)");
        checkExp("'abc'<>123", "('abc' <> 123)");
        checkExp("'abc'<>123='def'<>456", "((('abc' <> 123) = 'def') <> 456)");
        checkExp("'abc'<>123=('def'<>456)", "(('abc' <> 123) = ('def' <> 456))");
    }

    public void testBetween()
    {
        check("select * from t where price between 1 and 2",
            "SELECT *" + NL + "FROM `T`" + NL
            + "WHERE (`PRICE` BETWEEN ASYMMETRIC 1 AND 2)");

        check("select * from t where price between symmetric 1 and 2",
            "SELECT *" + NL + "FROM `T`" + NL
            + "WHERE (`PRICE` BETWEEN SYMMETRIC 1 AND 2)");

        check("select * from t where price not between symmetric 1 and 2",
            "SELECT *" + NL + "FROM `T`" + NL
            + "WHERE (`PRICE` NOT BETWEEN SYMMETRIC 1 AND 2)");

        check("select * from t where price between ASYMMETRIC 1 and 2+2*2",
            "SELECT *" + NL + "FROM `T`" + NL
            + "WHERE (`PRICE` BETWEEN ASYMMETRIC 1 AND (2 + (2 * 2)))");

        check("select * from t where price > 5 and price not between 1 + 2 and 3 * 4 AnD price is null",
            "SELECT *" + NL + "FROM `T`" + NL
            + "WHERE (((`PRICE` > 5) AND (`PRICE` NOT BETWEEN ASYMMETRIC (1 + 2) AND (3 * 4))) AND (`PRICE` IS NULL))");

        check("select * from t where price > 5 and price between 1 + 2 and 3 * 4 + price is null",
            "SELECT *" + NL + "FROM `T`" + NL
            + "WHERE ((`PRICE` > 5) AND ((`PRICE` BETWEEN ASYMMETRIC (1 + 2) AND ((3 * 4) + `PRICE`)) IS NULL))");

        check("select * from t where price > 5 and price between 1 + 2 and 3 * 4 or price is null",
            "SELECT *" + NL + "FROM `T`" + NL
            + "WHERE (((`PRICE` > 5) AND (`PRICE` BETWEEN ASYMMETRIC (1 + 2) AND (3 * 4))) OR (`PRICE` IS NULL))");

        check("values a between c and d and e and f between g and h",
            "(VALUES (ROW((((`A` BETWEEN ASYMMETRIC `C` AND `D`) AND `E`) AND (`F` BETWEEN ASYMMETRIC `G` AND `H`)))))");

        checkFails("values a between b or c",
            ".*BETWEEN operator has no terminating AND; at line 1, column 18");

        checkFails("values a between",
            "(?s).*Encountered \"between <EOF>\" at line 1, column 10.*");

        checkFails("values a between symmetric 1",
            ".*BETWEEN operator has no terminating AND; at line 1, column 18");

        // precedence of BETWEEN is higher than AND and OR, but lower than '+'
        check("values a between b and c + 2 or d and e",
            "(VALUES (ROW(((`A` BETWEEN ASYMMETRIC `B` AND (`C` + 2)) OR (`D` AND `E`)))))");

        // '=' and BETWEEN have same precedence, and are left-assoc
        check("values x = a between b and c = d = e",
            "(VALUES (ROW(((((`X` = `A`) BETWEEN ASYMMETRIC `B` AND `C`) = `D`) = `E`))))");

        // AND doesn't match BETWEEN if it's between parentheses!
        check("values a between b or (c and d) or e and f",
            "(VALUES (ROW((`A` BETWEEN ASYMMETRIC ((`B` OR (`C` AND `D`)) OR `E`) AND `F`))))");
    }

    public void testOperateOnColumn()
    {
        check("select c1*1,c2  + 2,c3/3,c4-4,c5*c4  from t",
            "SELECT (`C1` * 1), (`C2` + 2), (`C3` / 3), (`C4` - 4), (`C5` * `C4`)"
            + NL + "FROM `T`");
    }

    public void testRow() {
        check("select t.r.\"EXPR$1\", t.r.\"EXPR$0\" from (select (1,2) r from sales.depts) t",
            "SELECT `T`.`R`.`EXPR$1`, `T`.`R`.`EXPR$0`"+ NL +
            "FROM (SELECT (ROW(1, 2)) AS `R`"+ NL +
            "FROM `SALES`.`DEPTS`) AS `T`");

        check("select t.r.\"EXPR$1\".\"EXPR$2\" " +
              "from (select ((1,2),(3,4,5)) r from sales.depts) t",
            "SELECT `T`.`R`.`EXPR$1`.`EXPR$2`" + NL +
            "FROM (SELECT (ROW((ROW(1, 2)), (ROW(3, 4, 5)))) AS `R`" + NL +
            "FROM `SALES`.`DEPTS`) AS `T`");

//        check("select t.r.\"EXPR$1\".\"EXPR$2\" " +
//              "from (select ((1,2),(3,4,5,6)) r from sales.depts) t",
//            "SELECT `T`.`R`.`EXPR$1`.`EXPR$2`" + NL +
//            "FROM (SELECT (ROW((ROW(1, 2)), (ROW(3, 4, 5, 6)))) AS `R`" + NL +
//            "FROM `SALES`.`DEPTS`) AS `T`");
    }

    public void testOverlaps()
    {
        checkExp("(x,xx) overlaps (y,yy)",
                 "((`X`, `XX`) OVERLAPS (`Y`, `YY`))");

        checkExp("(x,xx) overlaps (y,yy) or false",
                 "(((`X`, `XX`) OVERLAPS (`Y`, `YY`)) OR FALSE)");

        checkExp("true and not (x,xx) overlaps (y,yy) or false",
            "((TRUE AND (NOT ((`X`, `XX`) OVERLAPS (`Y`, `YY`)))) OR FALSE)");
    }

    public void testIsDistinctFrom()
    {
        check("select * from t where x is distinct from y",
            "SELECT *" + NL + "FROM `T`" + NL
            + "WHERE (`X` IS DISTINCT FROM `Y`)");

        check("select * from t where x is distinct from (4,5,6)",
            "SELECT *" + NL + "FROM `T`" + NL
            + "WHERE (`X` IS DISTINCT FROM (ROW(4, 5, 6)))");

        check("select * from t where true is distinct from true",
            "SELECT *" + NL + "FROM `T`" + NL
            + "WHERE (TRUE IS DISTINCT FROM TRUE)");

        check("select * from t where true is distinct from true is true",
            "SELECT *" + NL + "FROM `T`" + NL
            + "WHERE ((TRUE IS DISTINCT FROM TRUE) IS TRUE)");
    }

    public void testCast()
    {
        checkExp("cast(x as boolean)", "CAST(`X` AS BOOLEAN)");
        checkExp("cast(x as integer)", "CAST(`X` AS INTEGER)");
        checkExp("cast(x as varchar)", "CAST(`X` AS VARCHAR)");
        checkExp("cast(x as date)", "CAST(`X` AS DATE)");
        checkExp("cast(x as time)", "CAST(`X` AS TIME)");
        checkExp("cast(x as timestamp)", "CAST(`X` AS TIMESTAMP)");
        checkExp("cast(x as decimal)", "CAST(`X` AS DECIMAL)");
        checkExp("cast(x as char)", "CAST(`X` AS CHAR)");
        checkExp("cast(x as binary)", "CAST(`X` AS BINARY)");
        checkExp("cast(x as varbinary)", "CAST(`X` AS VARBINARY)");
        checkExp("cast(x as tinyint)", "CAST(`X` AS TINYINT)");
        checkExp("cast(x as smallint)", "CAST(`X` AS SMALLINT)");
        checkExp("cast(x as bigint)", "CAST(`X` AS BIGINT)");
        checkExp("cast(x as real)", "CAST(`X` AS REAL)");
        checkExp("cast(x as double)", "CAST(`X` AS DOUBLE)");
        checkExp("cast(x as bit)", "CAST(`X` AS BIT)");

        checkExp("cast(x as bit(123))", "CAST(`X` AS BIT(123))");
        checkExp("cast(x as decimal(1,2))", "CAST(`X` AS DECIMAL(1, 2))");

        checkExp("cast('foo' as bar)", "CAST('foo' AS `BAR`)");
    }

    public void testCastFails()
    {
    }

    public void testLikeAndSimilar()
    {
        check("select * from t where x like '%abc%'",
            "SELECT *" + NL + "FROM `T`" + NL + "WHERE (`X` LIKE '%abc%')");

        check("select * from t where x+1 not siMilaR to '%abc%' ESCAPE 'e'",
            "SELECT *" + NL + "FROM `T`" + NL
            + "WHERE ((`X` + 1) NOT SIMILAR TO '%abc%' ESCAPE 'e')");

        // LIKE has higher precedence than AND
        check("select * from t where price > 5 and x+2*2 like y*3+2 escape (select*from t)",
            "SELECT *" + NL + "FROM `T`" + NL
            + "WHERE ((`PRICE` > 5) AND ((`X` + (2 * 2)) LIKE ((`Y` * 3) + 2) ESCAPE (SELECT *"
            + NL + "FROM `T`)))");

        check("values a and b like c",
            "(VALUES (ROW((`A` AND (`B` LIKE `C`)))))");

        // LIKE has higher precedence than AND
        check("values a and b like c escape d and e",
            "(VALUES (ROW(((`A` AND (`B` LIKE `C` ESCAPE `D`)) AND `E`))))");

        // LIKE has same precedence as '='; LIKE is right-assoc, '=' is left
        check("values a = b like c = d",
            "(VALUES (ROW(((`A` = `B`) LIKE (`C` = `D`)))))");

        // Nested LIKE
        check("values a like b like c escape d",
            "(VALUES (ROW((`A` LIKE (`B` LIKE `C` ESCAPE `D`)))))");
        check("values a like b like c escape d and false",
            "(VALUES (ROW(((`A` LIKE (`B` LIKE `C` ESCAPE `D`)) AND FALSE))))");
        check("values a like b like c like d escape e escape f",
            "(VALUES (ROW((`A` LIKE (`B` LIKE (`C` LIKE `D` ESCAPE `E`) ESCAPE `F`)))))");

        // Mixed LIKE and SIMILAR TO
        check("values a similar to b like c similar to d escape e escape f",
            "(VALUES (ROW((`A` SIMILAR TO (`B` LIKE (`C` SIMILAR TO `D` ESCAPE `E`) ESCAPE `F`)))))");

        checkFails("select * from t where escape 'e'",
            "(?s).*Encountered \"escape\" at line 1, column 23.*");

        // LIKE with +
        check("values a like b + c escape d",
            "(VALUES (ROW((`A` LIKE (`B` + `C`) ESCAPE `D`))))");

        // LIKE with ||
        check("values a like b || c escape d",
            "(VALUES (ROW((`A` LIKE (`B` || `C`) ESCAPE `D`))))");

        // ESCAPE with no expression
        checkFails("values a like escape d",
            "(?s).*Encountered \"escape\" at line 1, column 15.*");

        // ESCAPE with no expression
        checkFails("values a like b || c escape and false",
            "(?s).*Encountered \"escape and\" at line 1, column 22.*");

        // basic SIMILAR TO
        check("select * from t where x similar to '%abc%'",
            "SELECT *" + NL + "FROM `T`" + NL
            + "WHERE (`X` SIMILAR TO '%abc%')");

        check("select * from t where x+1 not siMilaR to '%abc%' ESCAPE 'e'",
            "SELECT *" + NL + "FROM `T`" + NL
            + "WHERE ((`X` + 1) NOT SIMILAR TO '%abc%' ESCAPE 'e')");

        // SIMILAR TO has higher precedence than AND
        check("select * from t where price > 5 and x+2*2 SIMILAR TO y*3+2 escape (select*from t)",
            "SELECT *" + NL + "FROM `T`" + NL
            + "WHERE ((`PRICE` > 5) AND ((`X` + (2 * 2)) SIMILAR TO ((`Y` * 3) + 2) ESCAPE (SELECT *"
            + NL + "FROM `T`)))");

        // Mixed LIKE and SIMILAR TO
        check("values a similar to b like c similar to d escape e escape f",
            "(VALUES (ROW((`A` SIMILAR TO (`B` LIKE (`C` SIMILAR TO `D` ESCAPE `E`) ESCAPE `F`)))))");

        // SIMILAR TO with subquery
        check("values a similar to (select * from t where a like b escape c) escape d",
            "(VALUES (ROW((`A` SIMILAR TO (SELECT *" + NL + "FROM `T`" + NL
            + "WHERE (`A` LIKE `B` ESCAPE `C`)) ESCAPE `D`))))");
    }

    public void testFoo()
    {
    }

    public void testArthimeticOperators()
    {
        checkExp("1-2+3*4/5/6-7", "(((1 - 2) + (((3 * 4) / 5) / 6)) - 7)");
        checkExp("pow(2,3)", "POW(2, 3)");
        checkExp("aBs(-2.3e-2)", "ABS((- 2.3E-2))");
        checkExp("MOD(5             ,\t\f\r\n2)", "MOD(5, 2)");
        checkExp("ln(5.43  )", "LN(5.43)");
        checkExp("log(- -.2  )", "LOG((- (- 0.2)))");
    }

    public void testExists()
    {
        check("select * from dept where exists (select 1 from emp where emp.deptno = dept.deptno)",
            "SELECT *" + NL + "FROM `DEPT`" + NL + "WHERE (EXISTS (SELECT 1"
            + NL + "FROM `EMP`" + NL
            + "WHERE (`EMP`.`DEPTNO` = `DEPT`.`DEPTNO`)))");
    }

    public void testExistsInWhere()
    {
        check("select * from emp where 1 = 2 and exists (select 1 from dept) and 3 = 4",
            "SELECT *" + NL + "FROM `EMP`" + NL
            + "WHERE (((1 = 2) AND (EXISTS (SELECT 1" + NL
            + "FROM `DEPT`))) AND (3 = 4))");
    }

    public void testFromWithAs()
    {
        check("select 1 from emp as e where 1",
            "SELECT 1" + NL + "FROM `EMP` AS `E`" + NL + "WHERE 1");
    }

    public void testConcat()
    {
        checkExp("'a' || 'b'", "('a' || 'b')");
    }

    public void testReverseSolidus()
    {
        checkExp("'\\'", "'\\'");
    }

    public void testSubstring()
    {
        checkExp("substring('a' \n  FROM \t  1)", "SUBSTRING('a' FROM 1)");
        checkExp("substring('a' FROM 1 FOR 3)", "SUBSTRING('a' FROM 1 FOR 3)");
        checkExp("substring('a' FROM 'reg' FOR '\\')",
            "SUBSTRING('a' FROM 'reg' FOR '\\')");

        checkExp("substring('a', 'reg', '\\')",
            "SUBSTRING('a' FROM 'reg' FOR '\\')");
        checkExp("substring('a', 1, 2)", "SUBSTRING('a' FROM 1 FOR 2)");
        checkExp("substring('a' , 1)", "SUBSTRING('a' FROM 1)");
    }

    public void testFunction()
    {
        check("select substring('Eggs and ham', 1, 3 + 2) || ' benedict' from emp",
            "SELECT (SUBSTRING('Eggs and ham' FROM 1 FOR (3 + 2)) || ' benedict')"
            + NL + "FROM `EMP`");
        checkExp("log(1)\r\n+pow(2, mod(\r\n3\n\t\t\f\n,ln(4))*log(5)-6*log(7/abs(8)+9))*pow(10,11)",
            "(LOG(1) + (POW(2, ((MOD(3, LN(4)) * LOG(5)) - (6 * LOG(((7 / ABS(8)) + 9))))) * POW(10, 11)))");
    }

    public void testFunctionInFunction()
    {
        checkExp("ln(pow(2,2))", "LN(POW(2, 2))");
    }

    public void testGroup()
    {
        check("select deptno, min(foo) as x from emp group by deptno, gender",
            "SELECT `DEPTNO`, MIN(`FOO`) AS `X`" + NL + "FROM `EMP`" + NL
            + "GROUP BY `DEPTNO`, `GENDER`");
    }

    public void testHavingAfterGroup()
    {
        check("select deptno from emp group by deptno, emp having count(*) > 5 and 1 = 2 order by 5, 2",
            "(SELECT `DEPTNO`" + NL + "FROM `EMP`" + NL
            + "GROUP BY `DEPTNO`, `EMP`" + NL
            + "HAVING ((COUNT(*) > 5) AND (1 = 2)) ORDER BY 5, 2)");
    }

    public void testHavingBeforeGroupFails()
    {
        checkFails("select deptno from emp having count(*) > 5 and deptno < 4 group by deptno, emp",
            "(?s).*Encountered \"group\" at .*");
    }

    public void testHavingNoGroup()
    {
        check("select deptno from emp having count(*) > 5",
            "SELECT `DEPTNO`" + NL + "FROM `EMP`" + NL
            + "HAVING (COUNT(*) > 5)");
    }

    public void testIdentifier()
    {
        checkExp("ab", "`AB`");
        checkExp("     \"a  \"\" b!c\"", "`a  \" b!c`");
        checkExp("\"x`y`z\"", "`x``y``z`");
    }

    public void testInList()
    {
        check("select * from emp where deptno in (10, 20) and gender = 'F'",
            "SELECT *" + NL + "FROM `EMP`" + NL
            + "WHERE ((`DEPTNO` IN (10, 20)) AND (`GENDER` = 'F'))");
    }

    // NOTE jvs 15-Nov-2003:  I disabled this because SQL standard requires
    // lists to be non-empty.  Anything else would be an extension.  Is there a
    // good reason to support it?
    public void _testInListEmpty()
    {
        check("select * from emp where deptno in () and gender = 'F'",
            "SELECT *" + NL + "FROM `EMP`" + NL
            + "WHERE ((`DEPTNO` IN ()) AND (`GENDER` = 'F'))");
    }

    public void testInQuery()
    {
        check("select * from emp where deptno in (select deptno from dept)",
            "SELECT *" + NL + "FROM `EMP`" + NL
            + "WHERE (`DEPTNO` IN (SELECT `DEPTNO`" + NL + "FROM `DEPT`))");
    }

    public void testInSetop()
    {
        check(
            "select * from emp where deptno in ((select deptno from dept union select * from dept)"
            + "except select * from dept) and false",
            "SELECT *" + NL + "FROM `EMP`" + NL
            + "WHERE ((`DEPTNO` IN (((SELECT `DEPTNO`" + NL
            + "FROM `DEPT`) UNION (SELECT *" + NL
            + "FROM `DEPT`)) EXCEPT (SELECT *" + NL
            + "FROM `DEPT`))) AND FALSE)");
    }

    public void testUnion()
    {
        check("select * from a union select * from a",
            "((SELECT *" + NL + "FROM `A`) UNION (SELECT *" + NL
            + "FROM `A`))");
        check("select * from a union all select * from a",
            "((SELECT *" + NL + "FROM `A`) UNION ALL (SELECT *" + NL
            + "FROM `A`))");
        check("select * from a union distinct select * from a",
            "((SELECT *" + NL + "FROM `A`) UNION (SELECT *" + NL
            + "FROM `A`))");
    }

    public void testExcept()
    {
        check("select * from a except select * from a",
            "((SELECT *" + NL + "FROM `A`) EXCEPT (SELECT *" + NL
            + "FROM `A`))");
        check("select * from a except all select * from a",
            "((SELECT *" + NL + "FROM `A`) EXCEPT ALL (SELECT *" + NL
            + "FROM `A`))");
        check("select * from a except distinct select * from a",
            "((SELECT *" + NL + "FROM `A`) EXCEPT (SELECT *" + NL
            + "FROM `A`))");
    }

    public void testIntersect()
    {
        check("select * from a intersect select * from a",
            "((SELECT *" + NL + "FROM `A`) INTERSECT (SELECT *" + NL
            + "FROM `A`))");
        check("select * from a intersect all select * from a",
            "((SELECT *" + NL + "FROM `A`) INTERSECT ALL (SELECT *" + NL
            + "FROM `A`))");
        check("select * from a intersect distinct select * from a",
            "((SELECT *" + NL + "FROM `A`) INTERSECT (SELECT *" + NL
            + "FROM `A`))");
    }

    public void testJoinCross()
    {
        check("select * from a as a2 cross join b",
            "SELECT *" + NL + "FROM `A` AS `A2` CROSS JOIN `B`");
    }

    public void testJoinOn()
    {
        check("select * from a left join b on 1 = 1 and 2 = 2 where 3 = 3",
            "SELECT *" + NL
            + "FROM `A` LEFT JOIN `B` ON ((1 = 1) AND (2 = 2))" + NL
            + "WHERE (3 = 3)");
    }

    public void testOuterJoinNoiseword()
    {
        check("select * from a left outer join b on 1 = 1 and 2 = 2 where 3 = 3",
            "SELECT *" + NL
            + "FROM `A` LEFT JOIN `B` ON ((1 = 1) AND (2 = 2))" + NL
            + "WHERE (3 = 3)");
    }

    public void testJoinQuery()
    {
        check("select * from a join (select * from b) as b2 on true",
            "SELECT *" + NL + "FROM `A` INNER JOIN (SELECT *" + NL
            + "FROM `B`) AS `B2` ON TRUE");
    }

    public void testFullInnerJoinFails()
    {
        // cannot have more than one of INNER, FULL, LEFT, RIGHT, CROSS
        checkFails("select * from a full inner join b",
            "(\\s|.)*Encountered \"inner\" at line 1, column 22(\\s|.)*");
    }

    public void testFullOuterJoin()
    {
        // OUTER is an optional extra to LEFT, RIGHT, or FULL
        check("select * from a full outer join b",
            "SELECT *" + NL + "FROM `A` FULL JOIN `B`");
    }

    public void testInnerOuterJoinFails()
    {
        checkFails("select * from a inner outer join b",
            "(\\s|.)*Encountered \"outer\" at line 1, column 23(\\s|.)*");
    }

    public void _testJoinAssociativity()
    {
        // joins are left-associative
        // 1. no parens needed
        check("select * from (a natural left join b) left join c on b.c1 = c.c1",
            "SELECT *" + NL
            + "FROM (`A` NATURAL LEFT JOIN `B`) LEFT JOIN `C` ON (`B`.`C1` = `C`.`C1`)"
            + NL);

        // 2. parens needed
        check("select * from a natural left join (b left join c on b.c1 = c.c1)",
            "SELECT *" + NL
            + "FROM (`A` NATURAL LEFT JOIN `B`) LEFT JOIN `C` ON (`B`.`C1` = `C`.`C1`)"
            + NL);

        // 3. same as 1
        check("select * from a natural left join b left join c on b.c1 = c.c1",
            "SELECT *" + NL
            + "FROM (`A` NATURAL LEFT JOIN `B`) LEFT JOIN `C` ON (`B`.`C1` = `C`.`C1`)"
            + NL);
    }

    // Note: "select * from a natural cross join b" is actually illegal SQL
    // ("cross" is the only join type which cannot be modified with the
    // "natural") but the parser allows it; we and catch it at validate time
    public void testNaturalCrossJoin()
    {
        check("select * from a natural cross join b",
            "SELECT *" + NL + "FROM `A` NATURAL CROSS JOIN `B`");
    }

    public void testJoinUsing()
    {
        check("select * from a join b using (x)",
            "SELECT *" + NL + "FROM `A` INNER JOIN `B` USING ((`X`))");
        checkFails("select * from a join b using () where c = d",
            "(?s).*Encountered \"[)]\" at line 1, column 31.*");
    }

    public void testLiteral()
    {
        checkExpSame("'foo'");
        checkExpSame("100");
        check("select 1 as one, 'x' as x, null as n from emp",
            "SELECT 1 AS `ONE`, 'x' AS `X`, NULL AS `N`" + NL + "FROM `EMP`");

        // Even though it looks like a date, it's just a string.
        checkExp("'2004-06-01'", "'2004-06-01'");
        checkExp("-.25", "(- 0.25)");
        checkExpSame("TIMESTAMP '2004-06-01 15:55:55'");
        checkExpSame("TIMESTAMP '2004-06-01 15:55:55.900'");
        checkExp("TIMESTAMP '2004-06-01 15:55:55.1234'",
            "TIMESTAMP '2004-06-01 15:55:55.123'");
        checkExp("TIMESTAMP '2004-06-01 15:55:55.1236'",
            "TIMESTAMP '2004-06-01 15:55:55.124'");
        checkExp("TIMESTAMP '2004-06-01 15:55:55.9999'",
            "TIMESTAMP '2004-06-01 15:55:56.000'");
        checkExpSame("NULL");
    }

    public void testContinuedLiteral()
    {
        checkExp("'abba'\n'abba'", "'abba' 'abba'");
        checkExp("'abba'\n'0001'", "'abba' '0001'");
        checkExp("N'yabba'\n'dabba'\n'doo'", "_ISO-8859-1'yabba' 'dabba' 'doo'");
        checkExp("_iso-8859-1'yabba'\n'dabba'\n'doo'",
            "_ISO-8859-1'yabba' 'dabba' 'doo'");

        checkExp("B'0001'\n'0001'", "B'0001' '0001'");
        checkExp("x'01aa'\n'03ff'", "X'01AA' '03FF'");

        // a bad bitstring
        checkFails("B'0001'\n'3333'", ".*Invalid bit string '3333'.*");

        // a bad hexstring
        checkFails("x'01aa'\n'vvvv'", ".*Invalid binary string 'vvvv'.*");
    }

    public void testMixedFrom()
    {
        check("select * from a join b using (x), c join d using (y)",
            "SELECT *" + NL
            + "FROM `A` INNER JOIN `B` USING ((`X`)) , `C` INNER JOIN `D` USING ((`Y`))"); // is this valid?
    }

    public void testMixedStar()
    {
        check("select emp.*, 1 as foo from emp, dept",
            "SELECT `EMP`.*, 1 AS `FOO`" + NL + "FROM `EMP` , `DEPT`");
    }

    public void testNotExists()
    {
        check("select * from dept where not not exists (select * from emp) and true",
            "SELECT *" + NL + "FROM `DEPT`" + NL
            + "WHERE ((NOT (NOT (EXISTS (SELECT *" + NL
            + "FROM `EMP`)))) AND TRUE)");
    }

    public void testOrder()
    {
        check("select * from emp order by empno, gender desc, deptno asc, empno ascending, name descending",
            "(SELECT *" + NL + "FROM `EMP` "
            + "ORDER BY `EMPNO`, `GENDER` DESC, `DEPTNO`, `EMPNO`, `NAME` DESC)");
    }

    public void testOrderInternal()
    {
        check("(select * from emp order by empno) union select * from emp",
            "((SELECT *" + NL + "FROM `EMP` "
            + "ORDER BY `EMPNO`) UNION (SELECT *" + NL + "FROM `EMP`))");
    }

    public void testSqlInlineComment()
    {
        check("select 1 from t --this is a comment" + NL,
            "SELECT 1" + NL + "FROM `T`");
        check("select 1 from t--" + NL, "SELECT 1" + NL + "FROM `T`");
        check("select 1 from t--this is a comment" + NL
            + "where a>b-- this is comment" + NL,
            "SELECT 1" + NL + "FROM `T`" + NL + "WHERE (`A` > `B`)");
    }

    // expressions
    public void testParseNumber()
    {
        //Exacts
        checkExp("1", "1");
        checkExp("+1.", "(+ 1)");
        checkExp("-1", "(- 1)");
        checkExp("1.0", "1.0");
        checkExp("-3.2", "(- 3.2)");
        checkExp("1.", "1");
        checkExp(".1", "0.1");
        checkExp("2500000000", "2500000000");
        checkExp("5000000000", "5000000000");

        //Approxs
        checkExp("1e1", "1.0E1");
        checkExp("+1e1", "(+ 1.0E1)");
        checkExp("1.1e1", "1.1E1");
        checkExp("1.1e+1", "1.1E1");
        checkExp("1.1e-1", "1.1E-1");
        checkExp("+1.1e-1", "(+ 1.1E-1)");
        checkExp("1.E3", "1.000E3");
        checkExp("1.e-3", "1E-3");
        checkExp("1.e+3", "1.000E3");
        checkExp(".5E3", "5.00E2");
        checkExp("+.5e3", "(+ 5.00E2)");
        checkExp("-.5E3", "(- 5.00E2)");
        checkExp(".5e-32", "5E-33");

        //Mix integer/decimals/approx
        checkExp("3. + 2", "(3 + 2)");
        checkExp("1++2+3", "((1 + (+ 2)) + 3)");
        checkExp("1- -2", "(1 - (- 2))");
        checkExp("1++2.3e-4++.5e-6++.7++8",
            "((((1 + (+ 2.3E-4)) + (+ 5E-7)) + (+ 0.7)) + (+ 8))");
        checkExp("1- -2.3e-4 - -.5e-6  -" + NL + "-.7++8",
            "((((1 - (- 2.3E-4)) - (- 5E-7)) - (- 0.7)) + (+ 8))");
        checkExp("1+-2.*-3.e-1/-4", "(1 + (((- 2) * (- 3E-1)) / (- 4)))");
    }

    public void testParseNumberFails()
    {
        checkFails("SELECT 0.5e1.1 from t",
            "(?s).*Encountered .*\\.1.* at line 1.*");
    }

    public void testMinusPrefixInExpression()
    {
        checkExp("-(1+2)", "(- (1 + 2))");
    }

    // operator precedence
    public void testPrecedence0()
    {
        checkExp("1 + 2 * 3 * 4 + 5", "((1 + ((2 * 3) * 4)) + 5)");
    }

    public void testPrecedence1()
    {
        checkExp("1 + 2 * (3 * (4 + 5))", "(1 + (2 * (3 * (4 + 5))))");
    }

    public void testPrecedence2()
    {
        checkExp("- - 1", "(- (- 1))"); // two prefices
    }

    public void testPrecedence3()
    {
        checkExp("- 1 is null", "((- 1) IS NULL)"); // prefix vs. postfix
    }

    public void testPrecedence4()
    {
        checkExp("1 - -2", "(1 - (- 2))"); // infix, prefix '-'
    }

    public void testPrecedence5()
    {
        checkExp("1++2", "(1 + (+ 2))"); // infix, prefix '+'
        checkExp("1+ +2", "(1 + (+ 2))"); // infix, prefix '+'
    }

    public void testPrecedenceSetOps()
    {
        check("select * from a union " + "select * from b intersect "
            + "select * from c intersect " + "select * from d except "
            + "select * from e except " + "select * from f union "
            + "select * from g",
              "(((((SELECT *" + NL
            + "FROM `A`) UNION (((SELECT *" + NL
            + "FROM `B`) INTERSECT (SELECT *" + NL
            + "FROM `C`)) INTERSECT (SELECT *" + NL
            + "FROM `D`))) EXCEPT (SELECT *" + NL
            + "FROM `E`)) EXCEPT (SELECT *" + NL
            + "FROM `F`)) UNION (SELECT *" + NL
            + "FROM `G`))");
    }

    public void testQueryInFrom()
    {
        // one query with 'as', the other without
        check("select * from (select * from emp) as e join (select * from dept) d",
            "SELECT *" + NL + "FROM (SELECT *" + NL
            + "FROM `EMP`) AS `E` INNER JOIN (SELECT *" + NL
            + "FROM `DEPT`) AS `D`");
    }

    public void testQuotesInString()
    {
        checkExp("'a''b'", "'a''b'");
        checkExp("'''x'", "'''x'");
        checkExp("''", "''");
        checkExp("'Quoted strings aren''t \"hard\"'",
            "'Quoted strings aren''t \"hard\"'");
    }

    public void testScalarQueryInWhere()
    {
        check("select * from emp where 3 = (select count(*) from dept where dept.deptno = emp.deptno)",
            "SELECT *" + NL + "FROM `EMP`" + NL
            + "WHERE (3 = (SELECT COUNT(*)" + NL + "FROM `DEPT`" + NL
            + "WHERE (`DEPT`.`DEPTNO` = `EMP`.`DEPTNO`)))");
    }

    public void testSelectList()
    {
        check("select * from emp, dept",
            "SELECT *" + NL + "FROM `EMP` , `DEPT`");
    }

    public void testSelectList2()
    {
        check("select * from emp, dept",
            "SELECT *" + NL + "FROM `EMP` , `DEPT`");
    }

    public void testSelectList3()
    {
        check("select 1, emp.*, 2 from emp",
            "SELECT 1, `EMP`.*, 2" + NL + "FROM `EMP`");
    }

    public void testSelectList4()
    {
        checkFails("select from emp", "(?s).*Encountered \"from\" at line .*");
    }

    public void testStar()
    {
        check("select * from emp", "SELECT *" + NL + "FROM `EMP`");
    }

    public void testSelectDistinct()
    {
        check("select distinct foo from bar",
            "SELECT DISTINCT `FOO`" + NL + "FROM `BAR`");
    }

    public void testSelectAll()
    {
        // "unique" is the default -- so drop the keyword
        check("select * from (select all foo from bar) as xyz",
            "SELECT *" + NL + "FROM (SELECT ALL `FOO`" + NL
            + "FROM `BAR`) AS `XYZ`");
    }

    public void testWhere()
    {
        check("select * from emp where empno > 5 and gender = 'F'",
            "SELECT *" + NL + "FROM `EMP`" + NL
            + "WHERE ((`EMPNO` > 5) AND (`GENDER` = 'F'))");
    }

    public void testNestedSelect()
    {
        check("select * from (select * from emp)",
            "SELECT *" + NL + "FROM (SELECT *" + NL + "FROM `EMP`)");
    }

    public void testValues()
    {
        check("values(1,'two')", "(VALUES (ROW(1, 'two')))");
    }

    public void testValuesExplicitRow()
    {
        check("values row(1,'two')", "(VALUES (ROW(1, 'two')))");
    }

    public void testFromValues()
    {
        check("select * from values(1,'two'), 3, (4, 'five')",
            "SELECT *" + NL
            + "FROM (VALUES (ROW(1, 'two')), (ROW(3)), (ROW(4, 'five')))");
    }

    public void testEmptyValues()
    {
        checkFails("select * from values()",
            "(?s).*Encountered \"\\)\" at line .*");
    }

    public void testExplicitTable()
    {
        check("table emp", "(TABLE `EMP`)");
    }

    public void testExplicitTableOrdered()
    {
        check("table emp order by name", "((TABLE `EMP`) ORDER BY `NAME`)");
    }

    public void testSelectFromExplicitTable()
    {
        check("select * from (table emp)",
            "SELECT *" + NL + "FROM (TABLE `EMP`)");
    }

    public void testSelectFromBareExplicitTableFails()
    {
        checkFails("select * from table emp",
            "(?s).*Encountered \"table\" at line 1, column 15.*");
    }

    public void testExplain()
    {
        check("explain plan for select * from emps",
            "EXPLAIN PLAN FOR" + NL + "(SELECT *" + NL + "FROM `EMPS`)");
    }

    public void testExplainWithImpl()
    {
        check("explain plan with implementation for select * from emps",
            "EXPLAIN PLAN FOR" + NL + "(SELECT *" + NL + "FROM `EMPS`)");
    }

    public void testExplainWithoutImpl()
    {
        check("explain plan without implementation for select * from emps",
            "EXPLAIN PLAN WITHOUT IMPLEMENTATION FOR" + NL + "(SELECT *" + NL
            + "FROM `EMPS`)");
    }

    public void testInsertSelect()
    {
        check("insert into emps select * from emps",
            "INSERT INTO `EMPS`" + NL + "(SELECT *" + NL + "FROM `EMPS`)");
    }

    public void testInsertUnion()
    {
        check("insert into emps select * from emps1 union select * from emps2",
            "INSERT INTO `EMPS`" + NL + "((SELECT *" + NL + "FROM `EMPS1`)"
            + " UNION (SELECT *" + NL + "FROM `EMPS2`))");
    }

    public void testInsertValues()
    {
        check("insert into emps values (1,'Fredkin')",
            "INSERT INTO `EMPS`" + NL + "(VALUES (ROW(1, 'Fredkin')))");
    }

    public void testInsertColumnList()
    {
        check("insert into emps(x,y) select * from emps",
            "INSERT INTO `EMPS`(`X`, `Y`)" + NL + "(SELECT *" + NL
            + "FROM `EMPS`)");
    }

    public void testExplainInsert()
    {
        check("explain plan for insert into emps1 select * from emps2",
            "EXPLAIN PLAN FOR" + NL + "INSERT INTO `EMPS1`" + NL + "(SELECT *"
            + NL + "FROM `EMPS2`)");
    }

    public void testDelete()
    {
        check("delete from emps", "DELETE FROM `EMPS`");
    }

    public void testDeleteWhere()
    {
        check("delete from emps where empno=12",
            "DELETE FROM `EMPS`" + NL + "WHERE (`EMPNO` = 12)");
    }

    public void testBitString()
    {
        checkExp("b''=B'1001'", "(B'' = B'1001')");
        checkExp("b'1111111111'=B'1111111'", "(B'1111111111' = B'1111111')");
        checkExp("b'0101'\n'0110'", "B'0101' '0110'");
    }

    public void testBitStringFails()
    {
        checkFails("select b''=B'10FF' from t",
            "(?s).*Encountered .*FF.* at line 1, column ...*");
        checkFails("select B'3' from t",
            "(?s).*Encountered .*3.* at line 1, column ...*");
        checkFails("select b'1' B'0' from t",
            "(?s).*Encountered .B.*0.* at line 1, column 13.*");

        // checkFails("select b'1' '0' from t", "?"); validator error
    }

    public void testHexAndBinaryString()
    {
        checkExp("x''=X'2'", "(X'' = X'2')");
        checkExp("x'fffff'=X''", "(X'FFFFF' = X'')");
        checkExp("x'1' \t\t\f\r " + NL
            + "'2'--hi this is a comment'FF'\r\r\t\f " + NL + "'34'",
            "X'1' '2' '34'");
        checkExp("x'1' \t\t\f\r " + NL + "'000'--" + NL + "'01'",
            "X'1' '000' '01'");
        checkExp("x'1234567890abcdef'=X'fFeEdDcCbBaA'",
            "(X'1234567890ABCDEF' = X'FFEEDDCCBBAA')");
        checkExp("x'001'=X'000102'", "(X'001' = X'000102')"); //check so inital zeros dont get trimmed somehow
        if (false) {
            checkFails("select b'1a00' from t", "blah");
        }
        if (false) {
            checkFails("select x'FeedGoats' from t", "blah");
        }
    }

    public void testHexAndBinaryStringFails()
    {
        checkFails("select x'abcdefG' from t",
            "(?s).*Encountered .*G.* at line 1, column ...*");
        checkFails("select x'1' x'2' from t",
            "(?s).*Encountered .x.*2.* at line 1, column 13.*");

        //checkFails("select x'1' '2' from t",?); validator error
    }

    public void testStringLiteral()
    {
        checkExp("_latin1'hi'", "_LATIN1'hi'");
        checkExp("N'is it a plane? no it''s superman!'",
            "_ISO-8859-1'is it a plane? no it''s superman!'");
        checkExp("n'lowercase n'", "_ISO-8859-1'lowercase n'");
        checkExp("'boring string'", "'boring string'");
        checkExp("_iSo_8859-1'bye'", "_ISO_8859-1'bye'");
        checkExp("'three' \n ' blind'\n' mice'", "'three' ' blind' ' mice'");
        checkExp("'three' -- comment \n ' blind'\n' mice'",
            "'three' ' blind' ' mice'");
        checkExp("N'bye' \t\r\f\f\n' bye'", "_ISO-8859-1'bye' ' bye'");
        checkExp("_iso_8859-1'bye' \n\n--\n-- this is a comment\n' bye'",
            "_ISO_8859-1'bye' ' bye'");
    }

    public void testStringLiteralFails()
    {
        checkFails("select N 'space'",
            "(?s).*Encountered .*space.* at line 1, column ...*");
        checkFails("select _latin1 \n'newline'",
            "(?s).*Encountered.*newline.* at line 2, column ...*");
        checkFails("select _unknown-charset'' from values(true)",
            "(?s).*UnsupportedCharsetException.*.*UNKNOWN-CHARSET.*");

        // checkFails("select N'1' '2' from t", "?"); a validator error
    }

    public void testCaseExpression()
    {
        //implicit simple else null case
        checkExp("case \t col1 when 1 then 'one' end",
            "(CASE WHEN (`COL1` = 1) THEN 'one' ELSE NULL END)");

        //implicit searched elee null case
        checkExp("case when nbr is false then 'one' end",
            "(CASE WHEN (`NBR` IS FALSE) THEN 'one' ELSE NULL END)");

        //multiple whens
        checkExp("case col1 when \n1.2 then 'one' when 2 then 'two' else 'three' end",
            "(CASE WHEN (`COL1` = 1.2) THEN 'one' WHEN (`COL1` = 2) THEN 'two' ELSE 'three' END)");
    }

    public void testCaseExpressionFails()
    {
        //forget end
        checkFails("select case col1 when 1 then 'one' from t", "(?s).*from.*");

        //wrong when
        checkFails("select case col1 when1 then 'one' end from t",
            "(?s).*when1.*");
    }

    public void testNullIf()
    {
        checkExp("nullif(v1,v2)",
            "(CASE WHEN (`V1` = `V2`) THEN NULL ELSE `V1` END)");
        checkExpFails("nullif(1,2,3)", "(?s).*");
    }

    public void testCoalesce()
    {
        checkExp("coalesce(v1,v2)",
            "(CASE WHEN (`V1` IS NOT NULL) THEN `V1` ELSE `V2` END)");
        checkExp("coalesce(v1,v2,v3)",
            "(CASE WHEN (`V1` IS NOT NULL) THEN `V1` ELSE "
            + "(CASE WHEN (`V2` IS NOT NULL) THEN `V2` ELSE `V3` END) "
            + "END)");
    }

    public void testLiteralCollate()
    {
        checkExp("'string' collate latin1$sv_SE$mega_strength",
            "'string' COLLATE ISO-8859-1$sv_SE$mega_strength");
        checkExp("'a long '\n'string' collate latin1$sv_SE$mega_strength",
            "'a long ' 'string' COLLATE ISO-8859-1$sv_SE$mega_strength");
        checkExp("x collate iso-8859-6$ar_LB$1",
            "`X` COLLATE ISO-8859-6$ar_LB$1");
        checkExp("x.y.z collate shift_jis$ja_JP$2",
            "`X`.`Y`.`Z` COLLATE SHIFT_JIS$ja_JP$2");
        checkExp("'str1'='str2' collate latin1$sv_SE",
            "('str1' = 'str2' COLLATE ISO-8859-1$sv_SE$primary)");
        checkExp("'str1' collate latin1$sv_SE>'str2'",
            "('str1' COLLATE ISO-8859-1$sv_SE$primary > 'str2')");
        checkExp("'str1' collate latin1$sv_SE<='str2' collate latin1$sv_FI",
            "('str1' COLLATE ISO-8859-1$sv_SE$primary <= 'str2' COLLATE ISO-8859-1$sv_FI$primary)");
    }

    public void testCharLength()
    {
        checkExp("char_length('string')", "CHAR_LENGTH('string')");
        checkExp("character_length('string')", "CHARACTER_LENGTH('string')");
    }

    public void testPosition()
    {
        checkExp("posiTion('mouse' in 'house')", "POSITION('mouse' IN 'house')");
    }

    // check date/time functions.
    public void testTimeDate()
    {
        // CURRENT_TIME - returns time w/ timezone
        checkExp("CURRENT_TIME(3)", "CURRENT_TIME(3)");

        // checkFails("SELECT CURRENT_TIME() FROM foo", "SELECT CURRENT_TIME() FROM `FOO`");
        checkExp("CURRENT_TIME", "`CURRENT_TIME`");
        checkExp("CURRENT_TIME(x+y)", "CURRENT_TIME((`X` + `Y`))");

        // LOCALTIME returns time w/o TZ
        checkExp("LOCALTIME(3)", "LOCALTIME(3)");

        // checkFails("SELECT LOCALTIME() FROM foo", "SELECT LOCALTIME() FROM `FOO`");
        checkExp("LOCALTIME", "`LOCALTIME`");
        checkExp("LOCALTIME(x+y)", "LOCALTIME((`X` + `Y`))");

        // LOCALTIMESTAMP - returns timestamp w/o TZ
        checkExp("LOCALTIMESTAMP(3)", "LOCALTIMESTAMP(3)");

        // checkFails("SELECT LOCALTIMESTAMP() FROM foo", "SELECT LOCALTIMESTAMP() FROM `FOO`");
        checkExp("LOCALTIMESTAMP", "`LOCALTIMESTAMP`");
        checkExp("LOCALTIMESTAMP(x+y)", "LOCALTIMESTAMP((`X` + `Y`))");

        // CURRENT_DATE - returns DATE
        checkExp("CURRENT_DATE(3)", "CURRENT_DATE(3)");

        // checkFails("SELECT CURRENT_DATE() FROM foo", "SELECT CURRENT_DATE() FROM `FOO`");
        checkExp("CURRENT_DATE", "`CURRENT_DATE`");

        // checkFails("SELECT CURRENT_DATE(x+y) FROM foo", "CURRENT_DATE((`X` + `Y`))");
        // CURRENT_TIMESTAMP - returns timestamp w/ TZ
        checkExp("CURRENT_TIMESTAMP(3)", "CURRENT_TIMESTAMP(3)");

        // checkFails("SELECT CURRENT_TIMESTAMP() FROM foo", "SELECT CURRENT_TIMESTAMP() FROM `FOO`");
        checkExp("CURRENT_TIMESTAMP", "`CURRENT_TIMESTAMP`");
        checkExp("CURRENT_TIMESTAMP(x+y)", "CURRENT_TIMESTAMP((`X` + `Y`))");

        // Date literals
        checkExp("DATE '2004-12-01'", "DATE '2004-12-01'");
        checkExp("TIME '12:01:01'", "TIME '12:01:01'");
        checkExp("TIME '12:01:01.'", "TIME '12:01:01'");
        checkExp("TIME '12:01:01.000'", "TIME '12:01:01.000'");
        checkExp("TIME '12:01:01.001'", "TIME '12:01:01.001'");
        checkExp("TIMESTAMP '2004-12-01 12:01:01'",
            "TIMESTAMP '2004-12-01 12:01:01'");
        checkExp("TIMESTAMP '2004-12-01 12:01:01.1'",
            "TIMESTAMP '2004-12-01 12:01:01.1'");
        checkExp("TIMESTAMP '2004-12-01 12:01:01.'",
            "TIMESTAMP '2004-12-01 12:01:01'");
        checkExpSame("TIMESTAMP '2004-12-01 12:01:01.1'");

        // Failures.
        checkFails("DATE '12/21/99'", "(?s).*Illegal DATE literal.*");
        checkFails("TIME '1230:33'", "(?s).*Illegal TIME literal.*");
        checkFails("TIME '12:00:00 PM'", "(?s).*Illegal TIME literal.*");
        checkFails("TIMESTAMP '12-21-99, 12:30:00'",
            "(?s).*Illegal TIMESTAMP literal.*");
    }

    /**
     * Testing for casting to/from date/time types.
     */
    public void testDateTimeCast()
    {
        //   checkExp("CAST(DATE '2001-12-21' AS CHARACTER VARYING)", "CAST(2001-12-21)");
        checkExp("CAST('2001-12-21' AS DATE)", "CAST('2001-12-21' AS DATE)");
        checkExp("CAST(12 AS DATE)", "CAST(12 AS DATE)");
        checkFails("CAST('2000-12-21' AS DATE NOT NULL)",
            "(?s).*Encountered \"NOT\" at line 1, column 27.*");
        checkFails("CAST('foo' as 1)",
            "(?s).*Encountered \"1\" at line 1, column 15.*");
        checkExp("Cast(DATE '2004-12-21' AS VARCHAR(10))",
            "CAST(DATE '2004-12-21' AS VARCHAR(10))");
    }

    public void testTrim()
    {
        checkExp("trim('mustache' FROM 'beard')",
            "TRIM(BOTH 'mustache' FROM 'beard')");
        checkExp("trim('mustache')", "TRIM(BOTH ' ' FROM 'mustache')");
        checkExp("trim(TRAILING FROM 'mustache')",
            "TRIM(TRAILING ' ' FROM 'mustache')");
        checkExp("trim(bOth 'mustache' FROM 'beard')",
            "TRIM(BOTH 'mustache' FROM 'beard')");
        checkExp("trim( lEaDing       'mustache' FROM 'beard')",
            "TRIM(LEADING 'mustache' FROM 'beard')");
        checkExp("trim(\r\n\ttrailing\n  'mustache' FROM 'beard')",
            "TRIM(TRAILING 'mustache' FROM 'beard')");

        checkFails("trim(from 'beard')",
            "(?s).*'FROM' near line 1, column 6, without operands preceding it is illegal.*");
        checkFails("trim('mustache' in 'beard')",
            "(?s).*Encountered .in. at line 1, column 17.*");
    }

    public void testConvertAndTranslate()
    {
        checkExp("convert('abc' using conversion)",
            "CONVERT('abc' USING `CONVERSION`)");
        checkExp("translate('abc' using translation)",
            "TRANSLATE('abc' USING `TRANSLATION`)");
    }

    public void testOverlay()
    {
        checkExp("overlay('ABCdef' placing 'abc' from 1)",
            "OVERLAY('ABCdef' PLACING 'abc' FROM 1)");
        checkExp("overlay('ABCdef' placing 'abc' from 1 for 3)",
            "OVERLAY('ABCdef' PLACING 'abc' FROM 1 FOR 3)");
    }

    public void testJdbcFunctionCall()
    {
        checkExp("{fn apa(1,'1')}", "{fn APA(1, '1') }");
        checkExp("{ Fn apa(log(ln(1))+2)}", "{fn APA((LOG(LN(1)) + 2)) }");
        checkExp("{fN apa(*)}", "{fn APA(*) }");
        checkExp("{   FN\t\r\n apa()}", "{fn APA() }");
        checkExp("{fn insert()}", "{fn INSERT() }");
    }

    public void _testOver()
    {
        checkExp("sum(sal) over ()", "x");
        checkExp("sum(sal) over (partition by x, y)", "x");
        checkExp("sum(sal) over (order by x desc, y asc)", "x");
        checkExp("sum(sal) over (rows 5 preceding)", "x");
        checkExp("sum(sal) over (range between interval '1' second preceding and interval '1' second following)",
            "sum(sal) over (`emp` over (range between (interval '1' second preceding) and (interval '1' second following)))");
        checkExp("sum(sal) over (range between interval '1:03' hour preceding and interval '2' minute following)",
            "sum(sal) over (`emp` over (range between (interval '1:03' hour preceding) and (interval '2' minute following)))");
        checkExp("sum(sal) over (range between interval '5' day preceding and current row)",
            "sum(sal) over (`emp` over (range between (interval '5' day preceding) and current row))");
        checkExp("sum(sal) over (range interval '5' day preceding)",
            "sum(sal) over (`emp` over (range (interval '5' day preceding)))");
        checkExp("sum(sal) over (range between unbounded preceding and current row)",
            "sum(sal) over (`emp` over (range between unbounded preceding and current row))");
        checkExp("sum(sal) over (range unbounded preceding)",
            "sum(sal) over (`emp` over (range unbounded preceding))");
        checkExp("sum(sal) over (range between current row and unbounded preceding)",
            "sum(sal) over (`emp` over (range between current row and unbounded preceding))");
        checkExp("sum(sal) over (range between current row and unbounded following)",
            "sum(sal) over (`emp` over (range between current row and unbounded following))");
        checkExp("sum(sal) over (range between 6 preceding and interval '1:03' hour preceding)",
            "sum(sal) over (`emp` over (range between (6 preceding) and (interval '1:03' hour preceding)))");
        checkExp("sum(sal) over (range between interval '1' second following and interval '5' day following)",
            "sum(sal) over (`emp` over (range between (interval '1' second following) and (interval '5' day following)))");
    }

    public void testElementFunc() {
        checkExp("element(a)", "ELEMENT(`A`)");
    }

    public void testCardinalityFunc() {
        checkExp("cardinality(a)", "CARDINALITY(`A`)");
    }

    public void testMemberOf() {
        checkExp("a member of b", "(`A` MEMBER OF `B`)");
        checkExp("a member of multiset[b]", "(`A` MEMBER OF (MULTISET[`B`]))");
    }

    public void testSubMultisetrOf() {
        checkExp("a submultiset of b", "(`A` SUBMULTISET OF `B`)");
    }

    public void testIsASet() {
        checkExp("b is a set", "(`B` IS A SET)");
        checkExp("a is a set", "(`A` IS A SET)");
    }

    public void testMultiset() {
        checkExp("multiset[1]", "(MULTISET[1])");
        checkExp("multiset[1,2.3]", "(MULTISET[1, 2.3])");
        checkExp("multiset[1,    '2']", "(MULTISET[1, '2'])");
        checkExp("multiset[ROW(1,2)]", "(MULTISET[(ROW(1, 2))])");
        checkExp("multiset[ROW(1,2),ROW(3,4)]", "(MULTISET[(ROW(1, 2)), (ROW(3, 4))])");
    }

    public void testMultisetUnion()
    {
        checkExp("a multiset union b","(`A` MULTISET UNION `B`)");
        checkExp("a multiset union all b","(`A` MULTISET UNION ALL `B`)");
        checkExp("a multiset union distinct b","(`A` MULTISET UNION `B`)");
    }

    public void testMultisetExcept()
    {
        checkExp("a multiset EXCEPT b","(`A` MULTISET EXCEPT `B`)");
        checkExp("a multiset EXCEPT all b","(`A` MULTISET EXCEPT ALL `B`)");
        checkExp("a multiset EXCEPT distinct b","(`A` MULTISET EXCEPT `B`)");
    }

    public void testMultisetIntersect()
    {
        checkExp("a multiset INTERSECT b","(`A` MULTISET INTERSECT `B`)");
        checkExp("a multiset INTERSECT all b","(`A` MULTISET INTERSECT ALL `B`)");
        checkExp("a multiset INTERSECT distinct b","(`A` MULTISET INTERSECT `B`)");
    }

    public void testMultisetMixed() {
        checkExp("multiset[1] MULTISET union b", "((MULTISET[1]) MULTISET UNION `B`)");
        checkExp("a MULTISET union b multiset intersect c multiset except d multiset union e",
            "(((`A` MULTISET UNION (`B` MULTISET INTERSECT `C`)) MULTISET EXCEPT `D`) MULTISET UNION `E`)");
    }

    public void testIntervalQualifier() {
        checkExpFails("interval '1'","(?s).*");
        checkExp("interval '1' year","(INTERVAL '1' YEAR)");
        checkExp("interval '-1' year","(INTERVAL '-1' YEAR)");
        checkExp("interval -'0' year","(INTERVAL '-0' YEAR)");
        checkExp("interval '100' year(4)","(INTERVAL '100' YEAR(4))");
        checkExp("interval '1' month","(INTERVAL '1' MONTH)");
        checkExp("interval -'0' month","(INTERVAL '-0' MONTH)");
        checkExp("interval '21' month(3)","(INTERVAL '21' MONTH(3))");
        checkExp("interval '11-22' year to month","(INTERVAL '11-22' YEAR TO MONTH)");
        checkExp("interval '1-2' year(4) to month","(INTERVAL '1-2' YEAR(4) TO MONTH)");
        checkExp("interval '-1-2' year(4) to month","(INTERVAL '-1-2' YEAR(4) TO MONTH)");
        checkExp("interval -'1-2' year(4) to month","(INTERVAL '-1-2' YEAR(4) TO MONTH)");
        checkExpFails("interval '1-2' month to year","(?s).*");
        checkExpFails("interval '1-2' year to day","(?s).*");
        checkExpFails("interval '1-2' year to month(3)","(?s).*");

        checkExp("interval '1' day","(INTERVAL '1' DAY)");
        checkExp("interval '111 2' day to hour","(INTERVAL '111 2' DAY TO HOUR)");
        checkExp("interval '1 2:3' day to minute","(INTERVAL '1 2:3' DAY TO MINUTE)");
        checkExp("interval '1 2:3:4' day to second","(INTERVAL '1 2:3:4' DAY TO SECOND)");
        checkExp("interval '1 2:3:4.5' day to second","(INTERVAL '1 2:3:4.5' DAY TO SECOND)");

        checkExpFails("interval '1' day to hour");
        checkExpFails("interval '1 2' day to second");

        checkExp("interval '123' hour","(INTERVAL '123' HOUR)");
        checkExp("interval '1:2' hour to minute","(INTERVAL '1:2' HOUR TO MINUTE)");
        checkExpFails("interval '1 2' hour to minute","(?s).*Illegal INTERVAL literal.*");
        checkExp("interval '1' hour","(INTERVAL '1' HOUR)");
        checkExp("interval '1:2:3' hour(2) to second","(INTERVAL '1:2:3' HOUR(2) TO SECOND)");
        checkExp("interval '1:22222:3.4567' hour(2) to second","(INTERVAL '1:22222:3.4567' HOUR(2) TO SECOND)");

        checkExp("interval '1' minute","(INTERVAL '1' MINUTE)");
        checkExp("interval '1:2' minute to second","(INTERVAL '1:2' MINUTE TO SECOND)");
        checkExp("interval '1:2.3' minute to second","(INTERVAL '1:2.3' MINUTE TO SECOND)");
        checkExpFails("interval '1:2' minute to second");

        checkExp("interval '1' second","(INTERVAL '1' SECOND)");
        checkExp("interval '1' second(3)","(INTERVAL '1' SECOND(3))");
        checkExp("interval '1' second(2,3)","(INTERVAL '1' SECOND(2, 3))");
        checkExp("interval '1.2' second","(INTERVAL '1.2' SECOND)");
        checkExp("interval '-1.234' second","(INTERVAL '-1.234' SECOND)");
        checkExp("interval '-0.234' second","(INTERVAL '-0.234' SECOND)");
        checkExp("interval -'-0.234' second","(INTERVAL '0.234' SECOND)");
        checkExp("interval -'-1.234' second","(INTERVAL '1.234' SECOND)");

        checkExp("interval '1 2:3:4.567' day to second","(INTERVAL '1 2:3:4.567' DAY TO SECOND)");

        checkExpFails("interval '-' day","(?s).*");
        checkExpFails("interval '1 2:3:4.567' day to hour to second","(?s).*");
        checkExpFails("interval '1:2' minute to second(2, 2)","(?s).*");
        checkExpFails("interval '1:x' hour to minute","(?s).*");
        checkExpFails("interval '1:x:2' hour to second","(?s).*");
    }

    public void testIntervalOperators() {
        checkExp("-interval '1' day","(- (INTERVAL '1' DAY))");
        checkExp("interval '1' day + interval '1' day","((INTERVAL '1' DAY) + (INTERVAL '1' DAY))");
        checkExp("interval '1' day - interval '1:2:3' hour to second","((INTERVAL '1' DAY) - (INTERVAL '1:2:3' HOUR TO SECOND))");

        checkExp("interval -'1' day","(INTERVAL '-1' DAY)");
        checkExp("interval '-1' day","(INTERVAL '-1' DAY)");
        checkExpFails("interval 'wael was here'","(?s).*");
        checkExpFails("interval 'wael was here' HOUR","(?s).*Illegal INTERVAL literal .wael was here..*");

    }

    public void testDateMinusDate() {
        checkExp("(date1 - date2) INTERVAL HOUR", "((`DATE1` - `DATE2`) INTERVAL HOUR)");
        checkExp("(date1 - date2) INTERVAL YEAR TO MONTH", "((`DATE1` - `DATE2`) INTERVAL YEAR TO MONTH)");
        checkExp("(date1 - date2) INTERVAL HOUR > interval '1' HOUR",
                   "(((`DATE1` - `DATE2`) INTERVAL HOUR) > (INTERVAL '1' HOUR))");
    }

    public void testExtract() {
        checkExp("extract(year from x)","EXTRACT(YEAR FROM `X`)");
        checkExp("extract(month from x)","EXTRACT(MONTH FROM `X`)");
        checkExp("extract(day from x)","EXTRACT(DAY FROM `X`)");
        checkExp("extract(hour from x)","EXTRACT(HOUR FROM `X`)");
        checkExp("extract(minute from x)","EXTRACT(MINUTE FROM `X`)");
        checkExp("extract(second from x)","EXTRACT(SECOND FROM `X`)");

        checkExpFails("extract(day to second from x)");
    }

    public void testIntervalArithmetics() {
        checkExp("TIME '23:59:59' - interval '1' hour ", "(TIME '23:59:59' - (INTERVAL '1' HOUR))");
        checkExp("TIMESTAMP '2000-01-01 23:59:59.1' - interval '1' hour ", "(TIMESTAMP '2000-01-01 23:59:59.1' - (INTERVAL '1' HOUR))");
        checkExp("DATE '2000-01-01' - interval '1' hour ", "(DATE '2000-01-01' - (INTERVAL '1' HOUR))");

        checkExp("TIME '23:59:59' + interval '1' hour ", "(TIME '23:59:59' + (INTERVAL '1' HOUR))");
        checkExp("TIMESTAMP '2000-01-01 23:59:59.1' + interval '1' hour ", "(TIMESTAMP '2000-01-01 23:59:59.1' + (INTERVAL '1' HOUR))");
        checkExp("DATE '2000-01-01' + interval '1' hour ", "(DATE '2000-01-01' + (INTERVAL '1' HOUR))");

        checkExp("interval '1' hour + TIME '23:59:59' ", "((INTERVAL '1' HOUR) + TIME '23:59:59')");

        checkExp("interval '1' hour * 8", "((INTERVAL '1' HOUR) * 8)");
        checkExp("1 * interval '1' hour", "(1 * (INTERVAL '1' HOUR))");
        checkExp("interval '1' hour / 8", "((INTERVAL '1' HOUR) / 8)");
    }

    public void testIntervalCompare(){
        checkExp("interval '1' hour = interval '1' second", "((INTERVAL '1' HOUR) = (INTERVAL '1' SECOND))");
        checkExp("interval '1' hour <> interval '1' second", "((INTERVAL '1' HOUR) <> (INTERVAL '1' SECOND))");
        checkExp("interval '1' hour < interval '1' second", "((INTERVAL '1' HOUR) < (INTERVAL '1' SECOND))");
        checkExp("interval '1' hour <= interval '1' second", "((INTERVAL '1' HOUR) <= (INTERVAL '1' SECOND))");
        checkExp("interval '1' hour > interval '1' second", "((INTERVAL '1' HOUR) > (INTERVAL '1' SECOND))");
        checkExp("interval '1' hour >= interval '1' second", "((INTERVAL '1' HOUR) >= (INTERVAL '1' SECOND))");
    }

    public void testCastToInterval() {
        checkExp("cast(x as interval year)", "CAST(`X` AS YEAR)");
        checkExp("cast(x as interval month)", "CAST(`X` AS MONTH)");
        checkExp("cast(x as interval year to month)", "CAST(`X` AS YEAR TO MONTH)");
        checkExp("cast(x as interval day)", "CAST(`X` AS DAY)");
        checkExp("cast(x as interval hour)", "CAST(`X` AS HOUR)");
        checkExp("cast(x as interval minute)", "CAST(`X` AS MINUTE)");
        checkExp("cast(x as interval second)", "CAST(`X` AS SECOND)");
        checkExp("cast(x as interval day to hour)", "CAST(`X` AS DAY TO HOUR)");
        checkExp("cast(x as interval day to minute)", "CAST(`X` AS DAY TO MINUTE)");
        checkExp("cast(x as interval day to second)", "CAST(`X` AS DAY TO SECOND)");
        checkExp("cast(x as interval hour to minute)", "CAST(`X` AS HOUR TO MINUTE)");
        checkExp("cast(x as interval hour to second)", "CAST(`X` AS HOUR TO SECOND)");
        checkExp("cast(x as interval minute to second)", "CAST(`X` AS MINUTE TO SECOND)");
    }

}


// End SqlParserTest.java

