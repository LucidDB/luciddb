/*
// $Id$
// Saffron preprocessor and data engine
// (C) Copyright 2002-2003 Disruptive Technologies, Inc.
// (C) Copyright 2003-2004 John V. Sichi
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

package net.sf.saffron.sql.parser;

import junit.framework.TestCase;

import net.sf.saffron.sql.SqlNode;
import net.sf.saffron.util.Util;

import java.util.regex.Pattern;

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

    private static final String NL = System.getProperty("line.separator");

    //~ Constructors ----------------------------------------------------------

    public SqlParserTest(String name)
    {
        super(name);
    }

    //Helper functions-------------------------------------------------------------------

    private void check(String sql,String expected)
    {
        final SqlNode sqlNode;
        try {
            sqlNode = new SqlParser(sql).parseStmt();
        } catch (ParseException e) {
            throw Util.newInternal(e,"Error while parsing SQL '" + sql + "'");
        }
        final String actual = sqlNode.toSqlString(null);
        assertEqualsUnabridged(expected,actual);
    }

    private void checkExp(String sql,String expected)
    {
        final SqlNode sqlNode;
        try {
            sqlNode = new SqlParser(sql).parseExpression();
        } catch (ParseException e) {
            throw Util.newInternal(
                e,
                "Error while parsing SQL expression '" + sql + "'");
        }
        final String actual = sqlNode.toSqlString(null);
        assertEqualsUnabridged(expected,actual);
    }

    private void checkExpSame(String sql)
    {
        checkExp(sql,sql);
    }

    private void assertEqualsUnabridged(String expected,String actual)
    {
        if (!expected.equals(actual)) {
            // REVIEW jvs 2-Feb-2004:  I put this here because assertEquals
            // uses ellipses in its expected/actual reports, which makes
            // it very hard to find the problem with something like
            // a newline instead of a space.  Use diff-based testing instead;
            // it would also make updating the expected value much easier.
            String message = NL+"expected:<" + expected + ">" + NL + " but was:<"
                + actual + ">";
            fail(message);
        }
    }

    private void checkFails(String sql,String exceptionPattern)
    {
        try {
            final SqlNode sqlNode = new SqlParser(sql).parseStmt();
            Util.discard(sqlNode);
            throw Util.newInternal(
                "Expected query '" + sql + "' to throw exception matching '"
                + exceptionPattern + "'");
        } catch (ParseException e) {
            final String message = e.toString();
            if (!Pattern.matches(exceptionPattern,message)) {
                throw Util.newInternal(
                    "Expected query '" + sql
                    + "' to throw exception matching '" + exceptionPattern
                    + "', but it threw " + message);
            }
        }
        catch(java.nio.charset.UnsupportedCharsetException e){
            final String message = e.toString();
            if (!Pattern.matches(exceptionPattern,message)) {
                throw Util.newInternal(
                    "Expected query '" + sql
                    + "' to throw exception matching '" + exceptionPattern
                    + "', but it threw " + message);
            }
        }
    }

    //~ Methods ---------------------------------------------------------------

    public void _testDerivedColumnList()
    {
        check("select * from emp (empno, gender) where true","foo");
    }

    public void _testDerivedColumnListInJoin()
    {
        check(
            "select * from emp as e (empno, gender) join dept (deptno, dname) on emp.deptno = dept.deptno",
            "foo");
    }

    public void _testDerivedColumnListNoAs()
    {
        check("select * from emp e (empno, gender) where true","foo");
    }

    public void _testDerivedColumnListWithAlias()
    {
        check("select * from emp as e (empno, gender) where true","foo");
    }

    // jdbc syntax
    public void _testEmbeddedCall()
    {
        checkExp("{call foo(?, ?)}","foo");
    }

    public void _testEmbeddedFunction()
    {
        checkExp("{? = call bar (?, ?)}","foo");
    }

    public void testColumnAliasWithAs()
    {
        check(
            "select 1 as foo from emp",
            "SELECT 1 AS `FOO`" + NL + "FROM `EMP`");
    }

    public void testColumnAliasWithoutAs()
    {
        check("select 1 foo from emp","SELECT 1 AS `FOO`" + NL + "FROM `EMP`");
    }

    public void testEmbeddedDate()
    {
        checkExp("{d '1998-10-22'}","DATE '1998-10-22'");
    }

    public void testEmbeddedTime()
    {
        checkExp("{t '16:22:34'}","TIME '16:22:34'");
    }

    public void testEmbeddedTimestamp()
    {
        checkExp("{ts '1998-10-22 16:22:34'}","TIMESTAMP '1998-10-22 16:22:34'");
    }

    public void testNot()
    {
        check("select not true, not false, not null, not unknown from t",
              "SELECT (NOT TRUE), (NOT FALSE), (NOT NULL), (NOT UNKNOWN)"+NL+
              "FROM `T`");
    }

    public void testBooleanPrecedenceAndAssociativity()
    {
        check("select * from t where true and false",
              "SELECT *"+NL+
              "FROM `T`"+NL+
              "WHERE (TRUE AND FALSE)");

        check("select * from t where null or unknown and unknown",
              "SELECT *"+NL+
              "FROM `T`"+NL+
              "WHERE (NULL OR (UNKNOWN AND UNKNOWN))");

        check("select * from t where true and (true or true) or false",
              "SELECT *"+NL+
              "FROM `T`"+NL+
              "WHERE ((TRUE AND (TRUE OR TRUE)) OR FALSE)");

        check("select * from t where 1 and true",
              "SELECT *"+NL+
              "FROM `T`"+NL+
              "WHERE (1 AND TRUE)");
    }

    public void testIsBooleans()
    {
        String[] inOut = {"NULL","TRUE","FALSE", "UNKNOWN"};

        for (int i=0;i<inOut.length;i++) {
            check("select * from t where nOt fAlSe Is "+inOut[i],
                  "SELECT *"+NL+
                  "FROM `T`"+NL+
                  "WHERE ((NOT FALSE) IS "+inOut[i]+")");

            check("select * from t where c1=1.1 IS NOT "+inOut[i],
                  "SELECT *"+NL+
                  "FROM `T`"+NL+
                  "WHERE ((`C1` = 1.1) IS NOT "+inOut[i]+")");
        }
    }

    public void testIsBooleanPrecedenceAndAssociativity()
    {
        check("select * from t where x is unknown is not unknown",
              "SELECT *"+NL+
              "FROM `T`"+NL+
              "WHERE ((`X` IS UNKNOWN) IS NOT UNKNOWN)");

        check("select 1 from t where not true is unknown",
              "SELECT 1"+NL+
              "FROM `T`"+NL+
              "WHERE ((NOT TRUE) IS UNKNOWN)");

        check("select * from t where x is unknown is not unknown is false is not false"+
                " is true is not true is null is not null",
              "SELECT *"+NL+
              "FROM `T`"+NL+
              "WHERE ((((((((`X` IS UNKNOWN) IS NOT UNKNOWN) IS FALSE) IS NOT FALSE) IS TRUE) IS NOT TRUE) IS NULL) IS NOT NULL)");

        // combine IS postfix operators with infix (AND) and prefix (NOT) ops
        check("select * from t where x is unknown is false and x is unknown is true or not y is unknown is not null",
                "SELECT *" + NL +
                "FROM `T`" + NL +
                "WHERE ((((`X` IS UNKNOWN) IS FALSE) AND ((`X` IS UNKNOWN) IS TRUE)) OR (((NOT `Y`) IS UNKNOWN) IS NOT NULL))");
    }

    public void testEqualNotEqual() {
        checkExp("'abc'=123","('abc' = 123)");
        checkExp("'abc'<>123","('abc' <> 123)");
        checkExp("'abc'<>123='def'<>456","((('abc' <> 123) = 'def') <> 456)");
        checkExp("'abc'<>123=('def'<>456)","(('abc' <> 123) = ('def' <> 456))");
    }
    public void testBetween()
    {
        check("select * from t where price between 1 and 2",
                "SELECT *" + NL + "FROM `T`" + NL + "WHERE (`PRICE` BETWEEN ASYMMETRIC 1 AND 2)");

        check("select * from t where price between symmetric 1 and 2",
                "SELECT *" + NL + "FROM `T`" + NL + "WHERE (`PRICE` BETWEEN SYMMETRIC 1 AND 2)");

        check("select * from t where price not between symmetric 1 and 2",
                "SELECT *" + NL + "FROM `T`" + NL + "WHERE (NOT (`PRICE` BETWEEN SYMMETRIC 1 AND 2))");

        check("select * from t where price between ASYMMETRIC 1 and 2+2*2",
                "SELECT *" + NL + "FROM `T`" + NL + "WHERE (`PRICE` BETWEEN ASYMMETRIC 1 AND (2 + (2 * 2)))");

        check("select * from t where price > 5 and price not between 1 + 2 and 3 * 4 AnD price is null",
                  "SELECT *"+NL+
                  "FROM `T`"+NL+
                  "WHERE (((`PRICE` > 5) AND (NOT (`PRICE` BETWEEN ASYMMETRIC (1 + 2) AND (3 * 4)))) AND (`PRICE` IS NULL))");

        check("select * from t where price > 5 and price between 1 + 2 and 3 * 4 + price is null",
                  "SELECT *"+NL+
                  "FROM `T`"+NL+
                  "WHERE ((`PRICE` > 5) AND ((`PRICE` BETWEEN ASYMMETRIC (1 + 2) AND ((3 * 4) + `PRICE`)) IS NULL))");

        check("select * from t where price > 5 and price between 1 + 2 and 3 * 4 or price is null",
              "SELECT *"+NL+
              "FROM `T`"+NL+
              "WHERE (((`PRICE` > 5) AND (`PRICE` BETWEEN ASYMMETRIC (1 + 2) AND (3 * 4))) OR (`PRICE` IS NULL))");
    }

    public void testOperateOnColumn()
    {
        check("select c1*1,c2  + 2,c3/3,c4-4,c5*c4  from t",
              "SELECT (`C1` * 1), (`C2` + 2), (`C3` / 3), (`C4` - 4), (`C5` * `C4`)" + NL + "FROM `T`");
    }

    public void testOverlaps()
    {
        check("select * from t where x overlaps y",
                "SELECT *" + NL + "FROM `T`" + NL + "WHERE (`X` OVERLAPS `Y`)");

        check("select * from t where true and x overlaps y or false",
                "SELECT *" + NL + "FROM `T`" + NL + "WHERE ((TRUE AND (`X` OVERLAPS `Y`)) OR FALSE)");

        check("select * from t where true and not (x overlaps y or false)",
                "SELECT *" + NL + "FROM `T`" + NL + "WHERE (TRUE AND (NOT ((`X` OVERLAPS `Y`) OR FALSE)))");
    }

    public void testIsDistinctFrom()
    {
        check("select * from t where x is distinct from y",
                "SELECT *" + NL + "FROM `T`" + NL + "WHERE (`X` IS DISTINCT FROM `Y`)");

        check("select * from t where x is distinct from (4,5,6)",
                "SELECT *" + NL + "FROM `T`" + NL + "WHERE (`X` IS DISTINCT FROM (ROW(4, 5, 6)))");

        check("select * from t where true is distinct from true",
                "SELECT *" + NL + "FROM `T`" + NL + "WHERE (TRUE IS DISTINCT FROM TRUE)");

        check("select * from t where true is distinct from true is true",
                "SELECT *" + NL + "FROM `T`" + NL + "WHERE ((TRUE IS DISTINCT FROM TRUE) IS TRUE)");
    }

    public void testCast(){
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

        checkExp("cast('foo' as bar)","CAST('foo' AS `BAR`)");
    }

    public void testCastFails() {
    }

    public void testLikeAndSimilar()
    {
        check(
            "select * from t where x like '%abc%'",
            "SELECT *" + NL + "FROM `T`" + NL + "WHERE (`X` LIKE '%abc%')");

        check(
            "select * from t where x+1 not siMilaR to '%abc%' ESCAPE 'e'",
            "SELECT *" + NL + "FROM `T`" + NL + "WHERE (NOT ((`X` + 1) SIMILAR TO '%abc%' ESCAPE 'e'))");

        //TODO LIKE has higher precedence than AND
//        check("select * from t where price > 5 and x+2*2 like y*3+2 escape (select*from t)",
//              "SELECT *"+NL+
//              "FROM `T`"+NL+
//              "WHERE ((`PRICE` > 5) AND ((`X` LIKE ((`Y` * 3) + 2) ESCAPE (SELECT *"+NL+
//              "FROM `T`)))");

        checkFails("select * from t where escape 'e'",
                   "(?s).*Encountered \"escape\" at line 1, column 23.*");
    }

    public void testArthimeticOperators() {
        checkExp("1-2+3*4/5/6-7","(((1 - 2) + (((3 * 4) / 5) / 6)) - 7)");
        checkExp("pow(2,3)","POW(2, 3)");
        checkExp("aBs(-2.3e-2)","ABS((- 0.023))");
        checkExp("MOD(5             ,\t\f\r\n2)","MOD(5, 2)");
        checkExp("ln(5.43  )","LN(5.43)");
        checkExp("log(- -.2  )","LOG((- (- 0.2)))");
    }

    public void testExists()
    {
        check(
            "select * from dept where exists (select 1 from emp where emp.deptno = dept.deptno)",
            "SELECT *" + NL + "FROM `DEPT`" + NL + "WHERE (EXISTS (SELECT 1"
            + NL + "FROM `EMP`" + NL
            + "WHERE (`EMP`.`DEPTNO` = `DEPT`.`DEPTNO`)))");
    }

    public void testExistsInWhere()
    {
        check(
            "select * from emp where 1 = 2 and exists (select 1 from dept) and 3 = 4",
            "SELECT *" + NL + "FROM `EMP`" + NL
            + "WHERE (((1 = 2) AND (EXISTS (SELECT 1" + NL
            + "FROM `DEPT`))) AND (3 = 4))");
    }

    public void testFromWithAs()
    {
        check(
            "select 1 from emp as e where 1",
            "SELECT 1" + NL + "FROM `EMP` AS `E`" + NL + "WHERE 1");
    }

    public void testConcat() {
        checkExp("'a' || 'b'","('a' || 'b')");
    }

    public void testReverseSolidus() {
        checkExp("'\\'","'\\'");
    }

    public void testSubstring() {
        checkExp("substring('a' \n  FROM \t  1)","SUBSTRING('a' FROM 1)") ;
        checkExp("substring('a' FROM 1 FOR 3)","SUBSTRING('a' FROM 1 FOR 3)") ;
        checkExp("substring('a' FROM 'reg' FOR '\\')","SUBSTRING('a' FROM 'reg' FOR '\\')") ;

        checkExp("substring('a', 'reg', '\\')","SUBSTRING('a' FROM 'reg' FOR '\\')") ;
        checkExp("substring('a', 1, 2)","SUBSTRING('a' FROM 1 FOR 2)") ;
        checkExp("substring('a' , 1)","SUBSTRING('a' FROM 1)") ;
    }

    public void testFunction()
    {
        check("select substring('Eggs and ham', 1, 3 + 2) || ' benedict' from emp",
              "SELECT (SUBSTRING('Eggs and ham' FROM 1 FOR (3 + 2)) || ' benedict')"
              + NL + "FROM `EMP`");
        checkExp("log(1)\r\n+pow(2, mod(\r\n3\n\t\t\f\n,ln(4))*log(5)-6*log(7/abs(8)+9))*pow(10,11)",
                 "(LOG(1) + (POW(2, ((MOD(3, LN(4)) * LOG(5)) - (6 * LOG(((7 / ABS(8)) + 9))))) * POW(10, 11)))");
    }

    public void testFunctionInFunction() {
        checkExp("ln(pow(2,2))","LN(POW(2, 2))");
    }

    public void testGroup()
    {
        check(
            "select deptno, min(foo) as x from emp group by deptno, gender",
            "SELECT `DEPTNO`, MIN(`FOO`) AS `X`" + NL + "FROM `EMP`" + NL
            + "GROUP BY `DEPTNO`, `GENDER`");
    }

    public void testHavingAfterGroup()
    {
        check(
            "select deptno from emp group by deptno, emp having count(*) > 5 and 1 = 2 order by 5, 2",
            "(SELECT `DEPTNO`" + NL + "FROM `EMP`" + NL
            + "GROUP BY `DEPTNO`, `EMP`" + NL
            + "HAVING ((COUNT(*) > 5) AND (1 = 2)) ORDER BY 5, 2)");
    }

    public void testHavingBeforeGroupFails()
    {
        checkFails(
            "select deptno from emp having count(*) > 5 and deptno < 4 group by deptno, emp",
            "(?s).*Encountered \"group\" at .*");
    }

    public void testHavingNoGroup()
    {
        check(
            "select deptno from emp having count(*) > 5",
            "SELECT `DEPTNO`" + NL + "FROM `EMP`" + NL
            + "HAVING (COUNT(*) > 5)");
    }

    public void testIdentifier()
    {
        checkExp("ab","`AB`");
        checkExp("     \"a  \"\" b!c\"","`a  \" b!c`");
        checkExp("\"x`y`z\"","`x``y``z`");
    }

    public void testInList()
    {
        check(
            "select * from emp where deptno in (10, 20) and gender = 'F'",
            "SELECT *" + NL + "FROM `EMP`" + NL
            + "WHERE ((`DEPTNO` IN (10, 20)) AND (`GENDER` = 'F'))");
    }

    // NOTE jvs 15-Nov-2003:  I disabled this because SQL standard requires
    // lists to be non-empty.  Anything else would be an extension.  Is there a
    // good reason to support it?
    public void _testInListEmpty()
    {
        check(
            "select * from emp where deptno in () and gender = 'F'",
            "SELECT *" + NL + "FROM `EMP`" + NL
            + "WHERE ((`DEPTNO` IN ()) AND (`GENDER` = 'F'))");
    }

    public void testInQuery()
    {
        check(
            "select * from emp where deptno in (select deptno from dept)",
            "SELECT *" + NL + "FROM `EMP`" + NL
            + "WHERE (`DEPTNO` IN (SELECT `DEPTNO`" + NL + "FROM `DEPT`))");
    }

    public void testInSetop()
    {
        check(
            "select * from emp where deptno in ((select deptno from dept union select * from dept)"+
                "except select * from dept) and false",
            "SELECT *" + NL + "FROM `EMP`" + NL
            + "WHERE ((`DEPTNO` IN (((SELECT `DEPTNO`" + NL
            + "FROM `DEPT`) UNION (SELECT *" + NL
            + "FROM `DEPT`)) EXCEPT (SELECT *" + NL
            + "FROM `DEPT`))) AND FALSE)");
    }

    public void testUnionAll()
    {
        check(
            "select * from emp union all select * from emp",
            "((SELECT *" + NL + "FROM `EMP`) UNION ALL (SELECT *" + NL
            + "FROM `EMP`))");
    }

    public void testJoinCross()
    {
        check(
            "select * from a as a2 cross join b",
            "SELECT *" + NL + "FROM `A` AS `A2` CROSS JOIN `B`");
    }

    public void testJoinOn()
    {
        check(
            "select * from a left join b on 1 = 1 and 2 = 2 where 3 = 3",
            "SELECT *" + NL
            + "FROM `A` LEFT JOIN `B` ON ((1 = 1) AND (2 = 2))" + NL
            + "WHERE (3 = 3)");
    }

    public void testOuterJoinNoiseword()
    {
        check(
            "select * from a left outer join b on 1 = 1 and 2 = 2 where 3 = 3",
            "SELECT *" + NL
            + "FROM `A` LEFT JOIN `B` ON ((1 = 1) AND (2 = 2))" + NL
            + "WHERE (3 = 3)");
    }

    public void testJoinQuery()
    {
        check(
            "select * from a join (select * from b) as b2 on true",
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
                "SELECT *" + NL +
                "FROM `A` FULL JOIN `B`");
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
                "SELECT *" + NL +
                "FROM (`A` NATURAL LEFT JOIN `B`) LEFT JOIN `C` ON (`B`.`C1` = `C`.`C1`)" + NL);
        // 2. parens needed
        check("select * from a natural left join (b left join c on b.c1 = c.c1)",
                "SELECT *" + NL +
                "FROM (`A` NATURAL LEFT JOIN `B`) LEFT JOIN `C` ON (`B`.`C1` = `C`.`C1`)" + NL);
        // 3. same as 1
        check("select * from a natural left join b left join c on b.c1 = c.c1",
                "SELECT *" + NL +
                "FROM (`A` NATURAL LEFT JOIN `B`) LEFT JOIN `C` ON (`B`.`C1` = `C`.`C1`)" + NL);
    }

    // Note: "select * from a natural cross join b" is actually illegal SQL
    // ("cross" is the only join type which cannot be modified with the
    // "natural") but the parser allows it; we and catch it at validate time
    public void testNaturalCrossJoin()
    {
        check("select * from a natural cross join b",
                "SELECT *" + NL +
                "FROM `A` NATURAL CROSS JOIN `B`");
    }

    public void testJoinUsing()
    {
        check("select * from a join b using (x)",
            "SELECT *" + NL + "FROM `A` INNER JOIN `B` USING ((`X`))");
    }

    public void testLiteral()
    {
        checkExpSame("'foo'");
        checkExpSame("100");
        check(
            "select 1 as one, 'x' as x, null as n from emp",
            "SELECT 1 AS `ONE`, 'x' AS `X`, NULL AS `N`" + NL + "FROM `EMP`");

        // Even though it looks like a date, it's just a string.
        checkExp("'2004-06-01'", "'2004-06-01'");
        checkExp("-.25", "(- 0.25)");
        checkExpSame("TIMESTAMP '2004-06-01 15:55:55'");
        checkExpSame("TIMESTAMP '2004-06-01 15:55:55.900'");
        checkExp("TIMESTAMP '2004-06-01 15:55:55.1234'", "TIMESTAMP '2004-06-01 15:55:55.123'");
        checkExp("TIMESTAMP '2004-06-01 15:55:55.1236'", "TIMESTAMP '2004-06-01 15:55:55.124'");
        checkExp("TIMESTAMP '2004-06-01 15:55:55.9999'", "TIMESTAMP '2004-06-01 15:55:56.000'");
        checkExpSame("NULL");

    }

    public void testMixedFrom()
    {
        check(
            "select * from a join b using (x), c join d using (y)",
            "SELECT *" + NL
            + "FROM `A` INNER JOIN `B` USING ((`X`)) , `C` INNER JOIN `D` USING ((`Y`))"); // is this valid?
    }

    public void testMixedStar()
    {
        check(
            "select emp.*, 1 as foo from emp, dept",
            "SELECT `EMP`.*, 1 AS `FOO`" + NL + "FROM `EMP` , `DEPT`");
    }

    public void testNotExists()
    {
        check(
            "select * from dept where not not exists (select * from emp) and true",
            "SELECT *" + NL + "FROM `DEPT`" + NL
            + "WHERE ((NOT (NOT (EXISTS (SELECT *" + NL
            + "FROM `EMP`)))) AND TRUE)");
    }

    public void testOrder()
    {
        check(
            "select * from emp order by empno, gender desc, deptno asc, empno ascending, name descending",
            "(SELECT *" + NL + "FROM `EMP` "
            + "ORDER BY `EMPNO`, `GENDER` DESC, `DEPTNO`, `EMPNO`, `NAME` DESC)");
    }

    public void testOrderInternal()
    {
        check(
            "(select * from emp order by empno) union select * from emp",
            "((SELECT *" + NL + "FROM `EMP` "
            + "ORDER BY `EMPNO`) UNION (SELECT *" + NL + "FROM `EMP`))");
    }

    public void testSqlInlineComment(){
        check("select 1 from t --this is a comment"+NL,"SELECT 1"+NL+"FROM `T`");
        check("select 1 from t--"+NL,"SELECT 1"+NL+"FROM `T`");
        check("select 1 from t--this is a comment"+NL+
              "where a>b-- this is comment"+NL,
              "SELECT 1"+NL+"FROM `T`"+NL+"WHERE (`A` > `B`)");
    }

    // expressions
    public void testParseNumber()
    {
        //Exacts
        checkExp("1","1");
        checkExp("+1.","(+ 1)");
        checkExp("-1","(- 1)");
        checkExp("1.0","1.0");
        checkExp("-3.2","(- 3.2)");
        checkExp("1.","1");
        checkExp(".1","0.1");
        checkExp("2500000000","2500000000");
        checkExp("5000000000","5000000000");
        //Approxs
        checkExp("1e1","10");
        checkExp("+1e1","(+ 10)");
        checkExp("1.1e1","11");
        checkExp("1.1e+1","11");
        checkExp("1.1e-1","0.11");
        checkExp("+1.1e-1","(+ 0.11)");
        checkExp("1.E3","1000");
        checkExp("1.e-3","0.001");
        checkExp("1.e+3","1000");
        checkExp(".5E3","500");
        checkExp("+.5e3","(+ 500)");
        checkExp("-.5E3","(- 500)");
        checkExp(".5e-32","0.000000000000000000000000000000005");
        //Mix integer/decimals/approx
        checkExp("3. + 2","(3 + 2)");
        checkExp("1++2+3","((1 + (+ 2)) + 3)");
        checkExp("1- -2","(1 - (- 2))");
        checkExp("1++2.3e-4++.5e-6++.7++8","((((1 + (+ 0.00023)) + (+ 0.0000005)) + (+ 0.7)) + (+ 8))");
        checkExp("1- -2.3e-4 - -.5e-6  -"+NL+"-.7++8","((((1 - (- 0.00023)) - (- 0.0000005)) - (- 0.7)) + (+ 8))");
        checkExp("1+-2.*-3.e-1/-4","(1 + (((- 2) * (- 0.3)) / (- 4)))");

    }

    public void testParseNumberFails() {
        checkFails("SELECT 0.5e1.1 from t","(?s).*Encountered .*\\.1.* at line 1.*");
    }

    public void testMinusPrefixInExpression(){
        checkExp("-(1+2)","(- (1 + 2))");
    }
    // operator precedence
    public void testPrecedence0()
    {
        checkExp("1 + 2 * 3 * 4 + 5","((1 + ((2 * 3) * 4)) + 5)");
    }

    public void testPrecedence1()
    {
        checkExp("1 + 2 * (3 * (4 + 5))","(1 + (2 * (3 * (4 + 5))))");
    }

    public void testPrecedence2()
    {
        checkExp("- - 1","(- (- 1))"); // two prefix
    }

    public void testPrecedence3()
    {
        checkExp("- 1 is null","((- 1) IS NULL)"); // prefix vs. postfix
    }

    public void testPrecedence4()
    {
        checkExp("1 - -2","(1 - (- 2))"); // potential confusion between infix, prefix '-'
    }

    public void testPrecedence5()
    {
        checkExp("1++2","(1 + (+ 2))"); // potential confusion between infix, prefix '+'
        checkExp("1+ +2","(1 + (+ 2))"); // potential confusion between infix, prefix '+'
    }

    public void testPrecedenceSetOps()
    {
        check(
            "select * from a union " + "select * from b intersect "
            + "select * from c intersect " + "select * from d except "
            + "select * from e except " + "select * from f union "
            + "select * from g",
            "(((SELECT *" + NL + "FROM `A`) UNION (((((SELECT *" + NL
            + "FROM `B`) INTERSECT (SELECT *" + NL
            + "FROM `C`)) INTERSECT (SELECT *" + NL
            + "FROM `D`)) EXCEPT (SELECT *" + NL
            + "FROM `E`)) EXCEPT (SELECT *" + NL
            + "FROM `F`))) UNION (SELECT *" + NL + "FROM `G`))");
    }

    public void testQueryInFrom()
    {
        // one query with 'as', the other without
        check(
            "select * from (select * from emp) as e join (select * from dept) d",
            "SELECT *" + NL + "FROM (SELECT *" + NL
            + "FROM `EMP`) AS `E` INNER JOIN (SELECT *" + NL
            + "FROM `DEPT`) AS `D`");
    }

    public void testQuotesInString()
    {
        checkExp("'a''b'","'a''b'");
        checkExp("'''x'","'''x'");
        checkExp("''","''");
        checkExp(
            "'Quoted strings aren''t \"hard\"'",
            "'Quoted strings aren''t \"hard\"'");
    }

    public void testScalarQueryInWhere()
    {
        check(
            "select * from emp where 3 = (select count(*) from dept where dept.deptno = emp.deptno)",
            "SELECT *" + NL + "FROM `EMP`" + NL
            + "WHERE (3 = (SELECT COUNT(*)" + NL + "FROM `DEPT`" + NL
            + "WHERE (`DEPT`.`DEPTNO` = `EMP`.`DEPTNO`)))");
    }

    public void testSelectList()
    {
        check(
            "select * from emp, dept",
            "SELECT *" + NL + "FROM `EMP` , `DEPT`");
    }

    public void testSelectList2()
    {
        check(
            "select * from emp, dept",
            "SELECT *" + NL + "FROM `EMP` , `DEPT`");
    }

    public void testSelectList3()
    {
        check(
            "select 1, emp.*, 2 from emp",
            "SELECT 1, `EMP`.*, 2" + NL + "FROM `EMP`");
    }

    public void testSelectList4()
    {
        checkFails("select from emp","(?s).*Encountered \"from\" at line .*");
    }

    public void testStar()
    {
        check("select * from emp","SELECT *" + NL + "FROM `EMP`");
    }

    public void testSelectDistinct()
    {
        check("select distinct foo from bar",
                "SELECT DISTINCT `FOO`" + NL +
                "FROM `BAR`");
    }

    public void testSelectUnique()
    {
        // "unique" is the default -- so drop the keyword
        check("select * from (select unique foo from bar) as xyz",
                "SELECT *" + NL +
                "FROM (SELECT `FOO`" + NL + "FROM `BAR`) AS `XYZ`");
    }

    public void testWhere()
    {
        check(
            "select * from emp where empno > 5 and gender = 'F'",
            "SELECT *" + NL + "FROM `EMP`" + NL
            + "WHERE ((`EMPNO` > 5) AND (`GENDER` = 'F'))");
    }

    public void testNestedSelect()
    {
        check(
            "select * from (select * from emp)",
            "SELECT *" + NL + "FROM (SELECT *" + NL + "FROM `EMP`)");
    }

    public void testValues()
    {
        check("values(1,'two')",
            "(VALUES (ROW(1, 'two')))");
    }

    public void testValuesExplicitRow()
    {
        check("values row(1,'two')",
            "(VALUES (ROW(1, 'two')))");
    }

    public void testFromValues()
    {
        check("select * from values(1,'two'), 3, (4, 'five')",
            "SELECT *" + NL + "FROM (VALUES (ROW(1, 'two')), (ROW(3)), (ROW(4, 'five')))");
    }

    public void testEmptyValues()
    {
        checkFails(
            "select * from values()",
            "(?s).*Encountered \"\\)\" at line .*");
    }

    public void testExplicitTable()
    {
        check("table emp","(TABLE `EMP`)");
    }

    public void testExplicitTableOrdered()
    {
        check("table emp order by name","((TABLE `EMP`) ORDER BY `NAME`)");
    }

    public void testSelectFromExplicitTable()
    {
        check(
            "select * from (table emp)",
            "SELECT *" +NL +"FROM (TABLE `EMP`)");
    }

    public void testSelectFromBareExplicitTableFails()
    {
        checkFails(
            "select * from table emp",
            "(?s).*Encountered \"table\" at line 1, column 15.*");
    }

    public void testExplain()
    {
        check(
            "explain plan for select * from emps",
            "EXPLAIN PLAN FOR" + NL + "(SELECT *" + NL + "FROM `EMPS`)");
    }

    public void testExplainWithImpl()
    {
        check(
            "explain plan with implementation for select * from emps",
            "EXPLAIN PLAN FOR" + NL + "(SELECT *" + NL + "FROM `EMPS`)");
    }

    public void testExplainWithoutImpl()
    {
        check(
            "explain plan without implementation for select * from emps",
            "EXPLAIN PLAN WITHOUT IMPLEMENTATION FOR" + NL +
            "(SELECT *" + NL + "FROM `EMPS`)");
    }

    public void testInsertSelect()
    {
        check(
            "insert into emps select * from emps",
            "INSERT INTO `EMPS`" + NL + "(SELECT *" + NL + "FROM `EMPS`)");
    }

    public void testInsertUnion()
    {
        check(
            "insert into emps select * from emps1 union select * from emps2",
            "INSERT INTO `EMPS`" + NL + "((SELECT *" + NL + "FROM `EMPS1`)"
            + " UNION (SELECT *" + NL +"FROM `EMPS2`))");
    }

    public void testInsertValues()
    {
        check(
            "insert into emps values (1,'Fredkin')",
            "INSERT INTO `EMPS`" + NL + "(VALUES (ROW(1, 'Fredkin')))");
    }

    public void testInsertColumnList()
    {
        check(
            "insert into emps(x,y) select * from emps",
            "INSERT INTO `EMPS`(`X`, `Y`)"
            + NL + "(SELECT *" + NL + "FROM `EMPS`)");
    }

    public void testExplainInsert()
    {
        check(
            "explain plan for insert into emps1 select * from emps2",
            "EXPLAIN PLAN FOR" + NL + "INSERT INTO `EMPS1`"
            + NL + "(SELECT *" + NL + "FROM `EMPS2`)");
    }

    public void testDelete()
    {
        check(
            "delete from emps",
            "DELETE FROM `EMPS`");
    }

    public void testDeleteWhere()
    {
        check(
            "delete from emps where empno=12",
            "DELETE FROM `EMPS`" + NL +"WHERE (`EMPNO` = 12)");
    }

    public void testBitString(){
        checkExp("b''=B'1001'","(B'' = B'1001')");
        checkExp("b'1111111111'=B'1111111'","(B'1111111111' = B'1111111')");
    }

    public void testBitStringFails(){
        checkFails("select b''=B'10FF' from t","(?s).*Encountered .*FF.* at line 1, column ...*");
        checkFails("select B'3' from t","(?s).*Encountered .*3.* at line 1, column ...*");
        checkFails("select b'1' B'0' from t","(?s).*Encountered .B.*0.* at line 1, column 13.*");
//todo newline required for separator        checkFails("select b'1' '0' from t",?);
    }

    public void testHexAndBinaryString(){
        checkExp("x''=X'2'","(X'' = X'2')");
        checkExp("x'fffff'=X''","(X'FFFFF' = X'')");
//todo        checkExp("x'1' \t\t\f\r "+NL+"'2'--hi this is a comment'FF'\r\r\t\f "+NL+"'34'","X'1234'");
//todo        checkExp("x'1' \t\t\f\r "+NL+"'000'--"+NL+"'01'","B'100001'");
        checkExp("x'1234567890abcdef'=X'fFeEdDcCbBaA'","(X'1234567890ABCDEF' = X'FFEEDDCCBBAA')");
        checkExp("x'001'=X'000102'","(X'001' = X'000102')"); //check so inital zeros dont get trimmed somehow
        if (false) checkFails("select b'1a00' from t", "blah");
        if (false) checkFails("select x'FeedGoats' from t", "blah");
    }

    public void testHexAndBinaryStringFails(){
        checkFails("select x'abcdefG' from t","(?s).*Encountered .*G.* at line 1, column ...*");
        checkFails("select x'1' x'2' from t","(?s).*Encountered .x.*2.* at line 1, column 13.*");
//todo newline required for separator        checkFails("select x'1' '2' from t",?);
    }

    public void testStringLiteral(){
        checkExp("_latin1'hi'","_LATIN1'hi'");
        checkExp("N'is it a plane? no it''s superman!'","_ISO-8859-1'is it a plane? no it''s superman!'");
        checkExp("n'lowercase n'","_ISO-8859-1'lowercase n'");
        checkExp("'boring string'","'boring string'");
        checkExp("_iSo_8859-1'bye'","_ISO_8859-1'bye'");
//        checkExp("N'bye' \t\r\f\f\n' bye'","_LATIN1'bye bye'"); todo
//        checkExp("_iso_8859-1'bye' \n\n--\n-- this is a comment\n' bye'","_ISO_8859-1'bye bye'"); todo
    }

    public void testStringLiteralFails(){
        checkFails("select N 'space'","(?s).*Encountered .*space.* at line 1, column ...*");
        checkFails("select _latin1 \n'newline'","(?s).*Encountered.*newline.* at line 2, column ...*");
        checkFails("select _unknown-charset'' from values(true)","(?s).*UnsupportedCharsetException.*.*UNKNOWN-CHARSET.*");
//todo  checkFails("select N'1' '2' from t",?); //need a newline in separtor
    }

    public void testCaseExpression() {
        //implicit simple else null case
        checkExp("case \t col1 when 1 then 'one' end","(CASE WHEN (`COL1` = 1) THEN 'one' ELSE NULL END)");
        //implicit searched elee null case
        checkExp("case when nbr is false then 'one' end","(CASE WHEN (`NBR` IS FALSE) THEN 'one' ELSE NULL END)");
        //multiple whens
        checkExp("case col1 when \n1.2 then 'one' when 2 then 'two' else 'three' end",
                 "(CASE WHEN (`COL1` = 1.2) THEN 'one' WHEN (`COL1` = 2) THEN 'two' ELSE 'three' END)");
    }

    public void testCaseExpressionFails() {
        //forget end
        checkFails("select case col1 when 1 then 'one' from t","(?s).*from.*");
        //wrong when
        checkFails("select case col1 when1 then 'one' end from t","(?s).*when1.*");
    }

    public void testNullIf(){
        checkExp("nullif(v1,v2)","(CASE WHEN (`V1` = `V2`) THEN NULL ELSE `V1` END)");
    }

    public void testCoalesce(){
        checkExp("coalesce(v1,v2)","(CASE WHEN (`V1` IS NOT NULL) THEN `V1` ELSE `V2` END)");
        checkExp("coalesce(v1,v2,v3)","(CASE WHEN (`V1` IS NOT NULL) THEN `V1` ELSE "+
                                            "(CASE WHEN (`V2` IS NOT NULL) THEN `V2` ELSE `V3` END) "+
                                      "END)");
    }

    public void testLiteralCollate(){
        checkExp("'string' collate latin1$sv_SE$mega_strength","'string' COLLATE ISO-8859-1$sv_SE$mega_strength");
        checkExp("x collate iso-8859-6$ar_LB$1","`X` COLLATE ISO-8859-6$ar_LB$1");
        checkExp("x.y.z collate shift_jis$ja_JP$2","`X`.`Y`.`Z` COLLATE SHIFT_JIS$ja_JP$2");
        checkExp("'str1'='str2' collate latin1$sv_SE","('str1' = 'str2' COLLATE ISO-8859-1$sv_SE$primary)");
        checkExp("'str1' collate latin1$sv_SE>'str2'","('str1' COLLATE ISO-8859-1$sv_SE$primary > 'str2')");
        checkExp("'str1' collate latin1$sv_SE<='str2' collate latin1$sv_FI",
                 "('str1' COLLATE ISO-8859-1$sv_SE$primary <= 'str2' COLLATE ISO-8859-1$sv_FI$primary)");

    }

    public void testCharLength() {
        checkExp("char_length('string')","CHAR_LENGTH('string')");
        checkExp("character_length('string')","CHARACTER_LENGTH('string')");
    }

    public void testPosition() {
        checkExp("posiTion('mouse' in 'house')","POSITION('mouse' IN 'house')");
    }
	// check date/time functions.
	public void testTimeDate() {
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
        checkExp("TIMESTAMP '2004-12-01 12:01:01'", "TIMESTAMP '2004-12-01 12:01:01'");
        checkExp("TIMESTAMP '2004-12-01 12:01:01.1'", "TIMESTAMP '2004-12-01 12:01:01.1'");
        checkExp("TIMESTAMP '2004-12-01 12:01:01.'", "TIMESTAMP '2004-12-01 12:01:01'");
        checkExpSame("TIMESTAMP '2004-12-01 12:01:01.1'");

        // Failures.
        checkFails("DATE '12/21/99'", "(?s).*Illegal DATE literal.*");
        checkFails("TIME '1230:33'","(?s).*Illegal TIME literal.*");
        checkFails("TIME '12:00:00 PM'","(?s).*Illegal TIME literal.*");
        checkFails("TIMESTAMP '12-21-99, 12:30:00'", "(?s).*Illegal TIMESTAMP literal.*");

	}

    /**
     * Testing for casting to/from date/time types.
     */
    public void testDateTimeCast() {
     //   checkExp("CAST(DATE '2001-12-21' AS CHARACTER VARYING)", "CAST(2001-12-21)");
        checkExp("CAST('2001-12-21' AS DATE)","CAST('2001-12-21' AS DATE)");
        checkExp("CAST(12 AS DATE)","CAST(12 AS DATE)");
        checkFails("CAST('2000-12-21' AS DATE NOT NULL)", "(?s).*Encountered \"NOT\" at line 1, column 27.*");
        checkFails("CAST('foo' as 1)","(?s).*Encountered \"1\" at line 1, column 15.*");
        checkExp("Cast(DATE '2004-12-21' AS VARCHAR(10))", "CAST(DATE '2004-12-21' AS VARCHAR(10))");

    }

    public void testTrim() {
        checkExp("trim('mustache' FROM 'beard')","TRIM(BOTH 'mustache' FROM 'beard')");
        checkExp("trim('mustache')","TRIM(BOTH ' ' FROM 'mustache')");
        checkExp("trim(TRAILING FROM 'mustache')","TRIM(TRAILING ' ' FROM 'mustache')");
        checkExp("trim(bOth 'mustache' FROM 'beard')","TRIM(BOTH 'mustache' FROM 'beard')");
        checkExp("trim( lEaDing       'mustache' FROM 'beard')","TRIM(LEADING 'mustache' FROM 'beard')");
        checkExp("trim(\r\n\ttrailing\n  'mustache' FROM 'beard')","TRIM(TRAILING 'mustache' FROM 'beard')");

        checkFails("trim(from 'beard')","(?s).*'FROM' near line 1 col 6, without operands preceding it is illegal.*");
        checkFails("trim('mustache' in 'beard')","(?s).*Encountered .in. at line 1, column 17.*");
    }

    public void testConvertAndTranslate() {
        checkExp("convert('abc' using conversion)","CONVERT('abc' USING `CONVERSION`)");
        checkExp("translate('abc' using translation)","TRANSLATE('abc' USING `TRANSLATION`)");
    }

    public void testOverlay() {
        checkExp("overlay('ABCdef' placing 'abc' from 1)","OVERLAY('ABCdef' PLACING 'abc' FROM 1)");
        checkExp("overlay('ABCdef' placing 'abc' from 1 for 3)","OVERLAY('ABCdef' PLACING 'abc' FROM 1 FOR 3)");
    }

    public void testJdbcFunctionCall() {
        checkExp("{fn apa(1,'1')}","{fn APA(1, '1') }");
        checkExp("{ Fn apa(log(ln(1))+2)}","{fn APA((LOG(LN(1)) + 2)) }");
        checkExp("{fN apa(*)}","{fn APA(*) }");
        checkExp("{   FN\t\r\n apa()}","{fn APA() }");
        checkExp("{fn insert()}","{fn INSERT() }");
    }
}


// End SqlParserTest.java
