/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2002-2005 Disruptive Tech
// Copyright (C) 2005-2005 LucidEra, Inc.
// Portions Copyright (C) 2003-2005 John V. Sichi
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
package org.eigenbase.sql.test;

import org.eigenbase.sql.type.SqlTypeName;
import org.eigenbase.util.Util;

import java.util.regex.Pattern;

/**
 * Holder for operator unit-tests.
 *
 * <p>This class is not a JUnit test. Each of the methods is named after an
 * operator, and is called from that operator's
 * {@link org.eigenbase.sql.SqlOperator#test} method. There is a unit test
 * ({@link net.sf.farrago.test.FarragoSqlOperatorsTest})
 * which invokes the <code>test()</code> method of every operator.
 *
 * @author Julian Hyde
 * @since October 1, 2004
 * @version $Id$
 */
public class SqlOperatorTests
{
    /**
     * Regular expression for a SQL TIME(0) value.
     */
    public static final Pattern timePattern = Pattern.compile(
        "[0-9][0-9]:[0-9][0-9]:[0-9][0-9]");
    /**
     * Regular expression for a SQL TIMESTAMP(3) value.
     */
    public static final Pattern timestampPattern = Pattern.compile(
        "[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9] " +
        "[0-9][0-9]:[0-9][0-9]:[0-9][0-9]\\.[0-9]+");
    /**
     * Regular expression for a SQL DATE value.
     */
    public static final Pattern datePattern = Pattern.compile(
        "[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]");

    public static void testBetween(SqlTester tester)
    {
        tester.checkBoolean("2 between 1 and 3", Boolean.TRUE);
        tester.checkBoolean("2 between 3 and 2", Boolean.FALSE);
        tester.checkBoolean("2 between symmetric 3 and 2", Boolean.TRUE);
        tester.checkBoolean("3 between 1 and 3", Boolean.TRUE);
        tester.checkBoolean("4 between 1 and 3", Boolean.FALSE);
        tester.checkBoolean("1 between 4 and -3", Boolean.FALSE);
        tester.checkBoolean("1 between -1 and -3", Boolean.FALSE);
        tester.checkBoolean("1 between -1 and 3", Boolean.TRUE);
        tester.checkBoolean("1 between 1 and 1", Boolean.TRUE);
        tester.checkBoolean("x'' between x'' and x''", Boolean.TRUE);
        tester.checkNull("cast(null as integer) between -1 and 2");
        tester.checkNull("1 between -1 and cast(null as integer)");
        tester.checkNull(
            "1 between cast(null as integer) and cast(null as integer)");
        tester.checkNull("1 between cast(null as integer) and 1");
    }

    public static void testNotBetween(SqlTester tester)
    {
        tester.checkBoolean("2 not between 1 and 3", Boolean.FALSE);
        tester.checkBoolean("3 not between 1 and 3", Boolean.FALSE);
        tester.checkBoolean("4 not between 1 and 3", Boolean.TRUE);
    }

    public static void testCast(SqlTester tester)
    {
        tester.checkScalarExact("cast(1.0 as integer)", "1");
        tester.checkScalarApprox("cast(1 as double)", "1.0");
        tester.checkScalarApprox("cast(1.0 as double)", "1.0");
        tester.checkNull("cast(null as double)");
        tester.checkNull("cast(null as date)");
    }

    public static void testCase(SqlTester tester)
    {
        tester.checkScalarExact("case when 'a'='a' then 1 end", "1");

        // FIXME jvs 26-Jan-2004:  disabled because of calculator
        // assertion after I changed the type of string literals from
        // VARCHAR to CHAR (see dtbug 278)
        if (false) {
            tester.checkString("case 2 when 1 then 'a' when 2 then 'b' end", "b");
        }
        tester.checkScalarExact("case 'a' when 'a' then 1 end", "1");
        tester.checkNull("case 'a' when 'b' then 1 end");
        tester.checkScalarExact(
            "case when 'a'=cast(null as varchar(1)) then 1 else 2 end", "2");
    }

    public static void testJdbcFn(SqlTester tester)
    {
        // TODO:
    }

    public static void testSelect(SqlTester tester)
    {
        tester.check("select * from values(1)", "1", SqlTypeName.Integer);
    }

    public static void testLiteralChain(SqlTester tester)
    {
        tester.checkString("'buttered'\n' toast'", "buttered toast");
        tester.checkString("'corned'\n' beef'\n' on'\n' rye'",
            "corned beef on rye");
        tester.checkString("_latin1'Spaghetti'\n' all''Amatriciana'",
            "Spaghetti all'Amatriciana");
        tester.checkBoolean("x'1234'\n'abcd' = x'1234abcd'", Boolean.TRUE);
        tester.checkBoolean("x'1234'\n'' = x'1234'", Boolean.TRUE);
        tester.checkBoolean("x''\n'ab' = x'ab'", Boolean.TRUE);
    }

    public static void testRow()
    {
        // todo:
    }

    public static void testAndOperator(SqlTester tester)
    {
        tester.checkBoolean("true and false", Boolean.FALSE);
        tester.checkBoolean("true and true", Boolean.TRUE);
        tester.checkBoolean("cast(null as boolean) and false", Boolean.FALSE);
        tester.checkBoolean("false and cast(null as boolean)", Boolean.FALSE);
        tester.checkNull("cast(null as boolean) and true");
        tester.checkBoolean("true and (not false)", Boolean.TRUE);
    }

    public static void testConcatOperator(SqlTester tester)
    {
        tester.checkString(" 'a'||'b' ", "ab");

        if (false) {
            // not yet implemented
            tester.checkString(" x'f'||x'f' ", "X'FF");
            tester.checkNull("x'ff' || cast(null as varbinary)");
        }
    }

    public static void testDivideOperator(SqlTester tester)
    {
        tester.checkScalarExact("10 / 5", "2");
        tester.checkScalarExact("-10 / 5", "-2");
        tester.checkScalarApprox("10.0 / 5", "2.0");
        tester.checkNull("1e1 / cast(null as float)");
    }

    public static void testEqualsOperator(SqlTester tester)
    {
        tester.checkBoolean("1=1", Boolean.TRUE);
        tester.checkBoolean("'a'='b'", Boolean.FALSE);
        tester.checkNull("cast(null as boolean)=cast(null as boolean)");
        tester.checkNull("cast(null as integer)=1");
    }

    public static void testGreaterThanOperator(SqlTester tester)
    {
        tester.checkBoolean("1>2", Boolean.FALSE);
        tester.checkBoolean("-1>1", Boolean.FALSE);
        tester.checkBoolean("1>1", Boolean.FALSE);
        tester.checkBoolean("2>1", Boolean.TRUE);
        tester.checkNull("3.0>cast(null as double)");
    }

    public static void testIsDistinctFromOperator(SqlTester tester)
    {
        tester.checkBoolean("1 is distinct from 1", Boolean.FALSE);
        tester.checkBoolean("1 is distinct from 1.0", Boolean.FALSE);
        tester.checkBoolean("1 is distinct from 2", Boolean.TRUE);
        tester.checkBoolean("cast(null as integer) is distinct from 2", Boolean.TRUE);
        tester.checkBoolean("cast(null as integer) is distinct from cast(null as integer)", Boolean.FALSE);
//        tester.checkBoolean("row(1,1) is distinct from row(1,1)", Boolean.TRUE);
//        tester.checkBoolean("row(1,1) is distinct from row(1,2)", Boolean.FALSE);
    }

    public static void testIsNotDistinctFromOperator(SqlTester tester)
    {
        tester.checkBoolean("1 is not distinct from 1", Boolean.TRUE);
        tester.checkBoolean("1 is not distinct from 1.0", Boolean.TRUE);
        tester.checkBoolean("1 is not distinct from 2", Boolean.FALSE);
        tester.checkBoolean("cast(null as integer) is not distinct from 2", Boolean.FALSE);
        tester.checkBoolean("cast(null as integer) is not distinct from cast(null as integer)", Boolean.TRUE);
//        tester.checkBoolean("row(1,1) is not distinct from row(1,1)", Boolean.FALSE);
//        tester.checkBoolean("row(1,1) is not distinct from row(1,2)", Boolean.TRUE);
    }

    public static void testGreaterThanOrEqualOperator(SqlTester tester)
    {
        tester.checkBoolean("1>=2", Boolean.FALSE);
        tester.checkBoolean("-1>=1", Boolean.FALSE);
        tester.checkBoolean("1>=1", Boolean.TRUE);
        tester.checkBoolean("2>=1", Boolean.TRUE);
        tester.checkNull("cast(null as real)>=999");
    }

    public static void testInOperator(SqlTester tester)
    {
        // TODO:
        Util.discard(tester);
    }

    public static void testOverlapsOperator(SqlTester tester)
    {
        //?todo
        Util.discard(tester);
    }

    public static void testLessThanOperator(SqlTester tester)
    {
        tester.checkBoolean("1<2", Boolean.TRUE);
        tester.checkBoolean("-1<1", Boolean.TRUE);
        tester.checkBoolean("1<1", Boolean.FALSE);
        tester.checkBoolean("2<1", Boolean.FALSE);
        tester.checkNull("123<cast(null as bigint)");
    }

    public static void testLessThanOrEqualOperator(SqlTester tester)
    {
        tester.checkBoolean("1<=2", Boolean.TRUE);
        tester.checkBoolean("1<=1", Boolean.TRUE);
        tester.checkBoolean("-1<=1", Boolean.TRUE);
        tester.checkBoolean("2<=1", Boolean.FALSE);
        tester.checkNull("cast(null as integer)<=3");
    }

    public static void testMinusOperator(SqlTester tester)
    {
        tester.checkScalarExact("-2-1", "-3");
        tester.checkScalarExact("2-1", "1");
        tester.checkScalarApprox("2.0-1", "1.0");
        tester.checkScalarExact("1-2", "-1");
        tester.checkNull("1e1-cast(null as double)");
    }

    public static void testMinusDateOperator(SqlTester tester)
    {
        //todo
    }

    public static void testMultiplyOperator(SqlTester tester)
    {
        tester.checkScalarExact("2*3", "6");
        tester.checkScalarExact("2*-3", "-6");
        tester.checkScalarExact("+2*3", "6");
        tester.checkScalarExact("2*0", "0");
        tester.checkScalarApprox("2.0*3", "6.0");
        tester.checkNull("2e-3*cast(null as integer)");
    }

    public static void testNotEqualsOperator(SqlTester tester)
    {
        tester.checkBoolean("1<>1", Boolean.FALSE);
        tester.checkBoolean("'a'<>'A'", Boolean.TRUE);
        tester.checkNull("'a'<>cast(null as varchar(1))");
    }

    public static void testOrOperator(SqlTester tester)
    {
        tester.checkBoolean("true or false", Boolean.TRUE);
        tester.checkBoolean("false or false", Boolean.FALSE);
        tester.checkBoolean("true or cast(null as boolean)", Boolean.TRUE);
        tester.checkNull("false or cast(null as boolean)");
    }

    public static void testPlusOperator(SqlTester tester)
    {
        tester.checkScalarExact("1+2", "3");
        tester.checkScalarExact("-1+2", "1");
        tester.checkScalarApprox("1+2.0", "3.0");
        tester.checkNull("cast(null as tinyint)+1");
        tester.checkNull("1e-2+cast(null as double)");
    }

    public static void testDescendingOperator(SqlTester tester)
    {
        Util.discard(tester);
        // TODO:
    }

    public static void testIsNotNullOperator(SqlTester tester)
    {
        tester.checkBoolean("true is not null", Boolean.TRUE);
        tester.checkBoolean("cast(null as boolean) is not null",
            Boolean.FALSE);
    }

    public static void testIsNullOperator(SqlTester tester)
    {
        tester.checkBoolean("true is null", Boolean.FALSE);
        tester.checkBoolean("cast(null as boolean) is null",
            Boolean.TRUE);
    }

    public static void testIsNotTrueOperator(SqlTester tester)
    {
        tester.checkBoolean("true is not true", Boolean.FALSE);
        tester.checkBoolean("false is not true", Boolean.TRUE);
        tester.checkBoolean("cast(null as boolean) is not true",
            Boolean.TRUE);
    }

    public static void testIsTrueOperator(SqlTester tester)
    {
        tester.checkBoolean("true is true", Boolean.TRUE);
        tester.checkBoolean("false is true", Boolean.FALSE);
        tester.checkBoolean("cast(null as boolean) is true",
            Boolean.FALSE);
    }

    public static void testIsNotFalseOperator(SqlTester tester)
    {
        tester.checkBoolean("false is not false", Boolean.FALSE);
        tester.checkBoolean("true is not false", Boolean.TRUE);
        tester.checkBoolean("cast(null as boolean) is not false",
            Boolean.TRUE);
    }

    public static void testIsFalseOperator(SqlTester tester)
    {
        tester.checkBoolean("false is false", Boolean.TRUE);
        tester.checkBoolean("true is false", Boolean.FALSE);
        tester.checkBoolean("cast(null as boolean) is false",
            Boolean.FALSE);
    }

    public static void testIsNotUnknownOperator(SqlTester tester)
    {
        tester.checkBoolean("false is not unknown", Boolean.TRUE);
        tester.checkBoolean("true is not unknown", Boolean.TRUE);
        tester.checkBoolean("cast(null as boolean) is not unknown",
            Boolean.FALSE);
        tester.checkBoolean("unknown is not unknown", Boolean.FALSE);
    }

    public static void testIsUnknownOperator(SqlTester tester)
    {
        tester.checkBoolean("false is unknown", Boolean.FALSE);
        tester.checkBoolean("true is unknown", Boolean.FALSE);
        tester.checkBoolean("cast(null as boolean) is unknown",
            Boolean.TRUE);
        tester.checkBoolean("unknown is unknown", Boolean.TRUE);
    }

    public static void testNotOperator(SqlTester tester)
    {
        tester.checkBoolean("not true", Boolean.FALSE);
        tester.checkBoolean("not false", Boolean.TRUE);
        tester.checkBoolean("not unknown", null);
        tester.checkNull("not cast(null as boolean)");
    }

    public static void testPrefixMinusOperator(SqlTester tester)
    {
        tester.checkScalarExact("-1", "-1");
        tester.checkScalarApprox("-1.0", "-1.0");
        tester.checkNull("-cast(null as integer)");
    }

    public static void testPrefixPlusOperator(SqlTester tester)
    {
        tester.checkScalarExact("+1", "1");
        tester.checkScalarApprox("+1.0", "1.0");
        tester.checkNull("+cast(null as integer)");
    }

    public static void testExplicitTableOperator(SqlTester tester)
    {
        Util.discard(tester);
        // TODO:
    }

    public static void testValuesOperator(SqlTester tester)
    {
        tester.check("select 'abc' from values(true)", "abc",
            SqlTypeName.Varchar);
    }

    public static void testNotLikeOperator(SqlTester tester)
    {
        tester.checkBoolean("'abc' not like '_b_'", Boolean.FALSE);
    }

    public static void testLikeOperator(SqlTester tester)
    {
        tester.checkBoolean("''  like ''", Boolean.TRUE);
        tester.checkBoolean("'a' like 'a'", Boolean.TRUE);
        tester.checkBoolean("'a' like 'b'", Boolean.FALSE);
        tester.checkBoolean("'a' like 'A'", Boolean.FALSE);
        tester.checkBoolean("'a' like 'a_'", Boolean.FALSE);
        tester.checkBoolean("'a' like '_a'", Boolean.FALSE);
        tester.checkBoolean("'a' like '%a'", Boolean.TRUE);
        tester.checkBoolean("'a' like '%a%'", Boolean.TRUE);
        tester.checkBoolean("'a' like 'a%'", Boolean.TRUE);
        tester.checkBoolean("'ab'   like 'a_'", Boolean.TRUE);
        tester.checkBoolean("'abc'  like 'a_'", Boolean.FALSE);
        tester.checkBoolean("'abcd' like 'a%'", Boolean.TRUE);
        tester.checkBoolean("'ab'   like '_b'", Boolean.TRUE);
        tester.checkBoolean("'abcd' like '_d'", Boolean.FALSE);
        tester.checkBoolean("'abcd' like '%d'", Boolean.TRUE);
    }

    public static void testNotSimilarToOperator(SqlTester tester)
    {
        tester.checkBoolean("'ab' not similar to 'a_'", Boolean.FALSE);
    }

    public static void testSimilarToOperator(SqlTester tester)
    {
        // like LIKE
        tester.checkBoolean("''  similar to ''", Boolean.TRUE);
        tester.checkBoolean("'a' similar to 'a'", Boolean.TRUE);
        tester.checkBoolean("'a' similar to 'b'", Boolean.FALSE);
        tester.checkBoolean("'a' similar to 'A'", Boolean.FALSE);
        tester.checkBoolean("'a' similar to 'a_'", Boolean.FALSE);
        tester.checkBoolean("'a' similar to '_a'", Boolean.FALSE);
        tester.checkBoolean("'a' similar to '%a'", Boolean.TRUE);
        tester.checkBoolean("'a' similar to '%a%'", Boolean.TRUE);
        tester.checkBoolean("'a' similar to 'a%'", Boolean.TRUE);
        tester.checkBoolean("'ab'   similar to 'a_'", Boolean.TRUE);
        tester.checkBoolean("'abc'  similar to 'a_'", Boolean.FALSE);
        tester.checkBoolean("'abcd' similar to 'a%'", Boolean.TRUE);
        tester.checkBoolean("'ab'   similar to '_b'", Boolean.TRUE);
        tester.checkBoolean("'abcd' similar to '_d'", Boolean.FALSE);
        tester.checkBoolean("'abcd' similar to '%d'", Boolean.TRUE);

        // simple regular expressions
        // ab*c+d matches acd, abcd, acccd, abcccd but not abd, aabc
        tester.checkBoolean("'acd'    similar to 'ab*c+d'", Boolean.TRUE);
        tester.checkBoolean("'abcd'   similar to 'ab*c+d'", Boolean.TRUE);
        tester.checkBoolean("'acccd'  similar to 'ab*c+d'", Boolean.TRUE);
        tester.checkBoolean("'abcccd' similar to 'ab*c+d'", Boolean.TRUE);
        tester.checkBoolean("'abd'    similar to 'ab*c+d'", Boolean.FALSE);
        tester.checkBoolean("'aabc'   similar to 'ab*c+d'", Boolean.FALSE);

        // compound regular expressions
        // x(ab|c)*y matches xy, xccy, xababcy but not xbcy
        tester.checkBoolean("'xy'      similar to 'x(ab|c)*y'", Boolean.TRUE);
        tester.checkBoolean("'xccy'    similar to 'x(ab|c)*y'", Boolean.TRUE);
        tester.checkBoolean("'xababcy' similar to 'x(ab|c)*y'", Boolean.TRUE);
        tester.checkBoolean("'xbcy'    similar to 'x(ab|c)*y'", Boolean.FALSE);

        // x(ab|c)+y matches xccy, xababcy but not xy, xbcy
        tester.checkBoolean("'xy'      similar to 'x(ab|c)+y'", Boolean.FALSE);
        tester.checkBoolean("'xccy'    similar to 'x(ab|c)+y'", Boolean.TRUE);
        tester.checkBoolean("'xababcy' similar to 'x(ab|c)+y'", Boolean.TRUE);
        tester.checkBoolean("'xbcy'    similar to 'x(ab|c)+y'", Boolean.FALSE);
    }

    public static void testEscapeOperator(SqlTester tester)
    {
        Util.discard(tester);
        // TODO:
    }

    public static void testConvertFunc(SqlTester tester)
    {
        Util.discard(tester);
        //todo: implement when convert exist in the calculator
    }

    public static void testTranslateFunc(SqlTester tester)
    {
        Util.discard(tester);
        //todo: implement when translate exist in the calculator
    }

    public static void testOverlayFunc(SqlTester tester)
    {
        tester.checkString("overlay('ABCdef' placing 'abc' from 1)",
            "abcdef");
        tester.checkString("overlay('ABCdef' placing 'abc' from 1 for 2)",
            "abcCdef");
        tester.checkNull(
            "overlay('ABCdef' placing 'abc' from 1 for cast(null as integer))");
        tester.checkNull(
            "overlay(cast(null as varchar(1)) placing 'abc' from 1)");

        if (false) {
            // hex strings not yet implemented in calc
            tester.checkNull("overlay(x'abc' placing x'abc' from cast(null as integer))");
        }
    }

    public static void testPositionFunc(SqlTester tester)
    {
        tester.checkScalarExact("position('b' in 'abc')", "2");
        tester.checkScalarExact("position('' in 'abc')", "1");

        tester.checkNull("position(cast(null as varchar(1)) in '0010')");
        tester.checkNull("position('a' in cast(null as varchar(1)))");
    }

    public static void testCharLengthFunc(SqlTester tester)
    {
        tester.checkScalarExact("char_length('abc')", "3");
        tester.checkNull("char_length(cast(null as varchar(1)))");
    }

    public static void testCharacterLengthFunc(SqlTester tester)
    {
        tester.checkScalarExact("CHARACTER_LENGTH('abc')", "3");
        tester.checkNull("CHARACTER_LENGTH(cast(null as varchar(1)))");
    }

    public static void testUpperFunc(SqlTester tester)
    {
        tester.checkString("upper('a')", "A");
        tester.checkString("upper('A')", "A");
        tester.checkString("upper('1')", "1");
        tester.checkNull("upper(cast(null as varchar(1)))");
    }

    public static void testLowerFunc(SqlTester tester)
    {
        tester.checkString("lower('A')", "a");
        tester.checkString("lower('a')", "a");
        tester.checkString("lower('1')", "1");
        tester.checkNull("lower(cast(null as varchar(1)))");
    }

    public static void testInitcapFunc(SqlTester tester)
    {
        Util.discard(tester);
        //not yet supported
        //                    tester.checkString("initcap('aA')", "'Aa'");
        //                    tester.checkString("initcap('Aa')", "'Aa'");
        //                    tester.checkString("initcap('1a')", "'1a'");
        //                    tester.checkString("initcap('ab cd Ef 12')", "'Ab Cd Ef 12'");
        //                    tester.checkNull("initcap(cast(null as varchar(1)))");
    }

    public static void testPowFunc(SqlTester tester)
    {
        tester.checkScalarApprox("pow(2,-2)", "0.25");
        tester.checkNull("pow(cast(null as integer),2)");
        tester.checkNull("pow(2,cast(null as double))");
    }

    public static void testModFunc(SqlTester tester)
    {
        tester.checkScalarExact("mod(4,2)", "0");
        tester.checkNull("mod(cast(null as integer),2)");
        tester.checkNull("mod(4,cast(null as tinyint))");
    }

    public static void testLnFunc(SqlTester tester)
    {
        //todo not very platform independant
        tester.checkScalarApprox("ln(2.71828)", "0.999999327347282");
        tester.checkNull("ln(cast(null as tinyint))");
    }

    public static void testLogFunc(SqlTester tester)
    {
        tester.checkScalarApprox("log(10)", "1.0");
        tester.checkNull("log(cast(null as real))");
    }

    public static void testAbsFunc(SqlTester tester)
    {
        tester.checkScalarExact("abs(-1)", "1");
        tester.checkNull("abs(cast(null as double))");
    }

    public static void testNullifFunc(SqlTester tester)
    {
        tester.checkNull("nullif(1,1)");
        tester.checkString("nullif('a','b')", "a");
        tester.checkString("nullif('a',cast(null as varchar(1)))", "a");
        tester.checkNull("nullif(cast(null as varchar(1)),'a')");
    }

    public static void testCoalesceFunc(SqlTester tester)
    {
        tester.checkString("coalesce('a','b')", "a");
        tester.checkScalarExact("coalesce(null,null,3)", "3");
    }

    public static void testUserFunc(SqlTester tester)
    {
        tester.checkScalar("USER", null, "VARCHAR(30) NOT NULL");
    }

    public static void testCurrentUserFunc(SqlTester tester)
    {
        tester.checkScalar("CURRENT_USER", null, "VARCHAR(30) NOT NULL");
    }

    public static void testSessionUserFunc(SqlTester tester)
    {
        tester.checkScalar("SESSION_USER", null, "VARCHAR(30) NOT NULL");
    }

    public static void testSystemUserFunc(SqlTester tester)
    {
        String user = System.getProperty("user.name"); // e.g. "jhyde"
        tester.checkScalar("SYSTEM_USER", user, "VARCHAR(30) NOT NULL");
    }

    public static void testCurrentPathFunc(SqlTester tester)
    {
        tester.checkScalar("CURRENT_PATH", "", "VARCHAR(30) NOT NULL");
    }

    public static void testCurrentRoleFunc(SqlTester tester)
    {
        // We don't have roles yet, so the CURRENT_ROLE function returns
        // the empty string.
        tester.checkScalar("CURRENT_ROLE", "", "VARCHAR(30) NOT NULL");
    }

    public static void testLocalTimeFunc(SqlTester tester)
    {
        tester.checkScalar("LOCALTIME", timePattern, "TIME(0) NOT NULL");
        //TODO: tester.checkFails("LOCALTIME()", "?", SqlTypeName.Time);
        tester.checkScalar("LOCALTIME(1)", timePattern,
            "TIME(1) NOT NULL");
    }

    public static void testLocalTimestampFunc(SqlTester tester)
    {
        tester.checkScalar("LOCALTIMESTAMP", timestampPattern,
            "TIMESTAMP(0) NOT NULL");
        tester.checkFails("LOCALTIMESTAMP()", "?");
        tester.checkScalar("LOCALTIMESTAMP(1)", timestampPattern,
            "TIMESTAMP(1) NOT NULL");
    }

    public static void testCurrentTimeFunc(SqlTester tester)
    {
        tester.checkScalar("CURRENT_TIME", timePattern,
            "TIME(0) NOT NULL");
        tester.checkFails("CURRENT_TIME()", "?");
        tester.checkScalar("CURRENT_TIME(1)", timePattern,
            "TIME(1) NOT NULL");
    }

    public static void testCurrentTimestampFunc(SqlTester tester)
    {
        tester.checkScalar("CURRENT_TIMESTAMP", timestampPattern,
            "TIMESTAMP(0) NOT NULL");
        tester.checkFails("CURRENT_TIMESTAMP()", "?");
        tester.checkScalar("CURRENT_TIMESTAMP(1)", timestampPattern,
            "TIMESTAMP(1) NOT NULL");
    }

    public static void testCurrentDateFunc(SqlTester tester)
    {
        tester.checkScalar("CURRENT_DATE", datePattern, "DATE NOT NULL");
    }

    public static void testSubstringFunction(SqlTester tester)
    {
        tester.checkString("substring('abc' from 1 for 2)", "ab");
        tester.checkString("substring('abc' from 2)", "bc");

        //substring reg exp not yet supported
        //                    tester.checkString("substring('foobar' from '%#\"o_b#\"%' for '#')", "oob");
        tester.checkNull("substring(cast(null as varchar(1)),1,2)");
    }

    public static void testTrimFunc(SqlTester tester)
    {
        tester.checkString("trim('a' from 'aAa')", "A");
        tester.checkString("trim(both 'a' from 'aAa')", "A");
        tester.checkString("trim(leading 'a' from 'aAa')", "Aa");
        tester.checkString("trim(trailing 'a' from 'aAa')", "aA");
        tester.checkNull("trim(cast(null as varchar(1)) from 'a')");
        tester.checkNull("trim('a' from cast(null as varchar(1)))");
    }

    public static void testWindow(SqlTester tester) {
        tester.check("select sum(1) over () from values (true)", "1",
                SqlTypeName.Integer);
    }

    public static void testElementFunc(SqlTester tester)
    {
//        tester.checkString("element(multiset['abc']))","abc");
//        tester.checkNull("element(multiset[cast(null as integer)]))");
    }

    public static void testCardinalityFunc(SqlTester tester) {
//        tester.checkScalarExact("cardinality(multiset[cast(null as integer),2]))","2");
    }

    public static void testMemberOfOperator(SqlTester tester) {
//        tester.checkBoolean("1 member of multiset[1]",Boolean.TRUE);
//        tester.checkBoolean("'2' member of multiset['1']",Boolean.FALSE);
//        tester.checkBoolean("cast(null as double) member of multiset[cast(null as double)]",Boolean.TRUE);
//        tester.checkBoolean("cast(null as double) member of multiset[1.1]",Boolean.FALSE);
//        tester.checkBoolean("1.1 member of multiset[cast(null as double)]",Boolean.FALSE);
    }

    public static void testExtractFunc(SqlTester tester) {
        //todo
    }

    public static void testCeilFunc(SqlTester tester)
    {
        //To change body of created methods use File | Settings | File Templates.
    }
    
    public static void testFloorFunc(SqlTester tester)
    {
        // Add in calls wwhne function is implemented
    }
}

// End SqlOperatorTests.java
