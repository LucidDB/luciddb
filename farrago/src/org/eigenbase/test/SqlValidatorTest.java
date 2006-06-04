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

package org.eigenbase.test;

import junit.framework.TestCase;
import org.eigenbase.sql.SqlCollation;
import org.eigenbase.sql.SqlIntervalQualifier;
import org.eigenbase.util.Bug;

import java.nio.charset.Charset;
import java.util.logging.Logger;


/**
 * Concrete child class of {@link SqlValidatorTestCase}, containing lots of
 * unit tests.
 *
 * <p>If you want to run these same tests in a different environment, create
 * a derived class whose {@link #getTester} returns a different implementation
 * of {@link Tester}.
 *
 * @author Wael Chatila
 * @since Jan 14, 2004
 * @version $Id$
 **/
public class SqlValidatorTest extends SqlValidatorTestCase
{
    /**
     * @deprecated Deprecated so that usages of this constant will show up in
     * yellow in Intellij and maybe someone will fix them.
     */
    protected static final boolean todo = false;
    public static final boolean todoTypeInference = false;

    protected final Logger logger = Logger.getLogger(getClass().getName());

    //~ Methods ---------------------------------------------------------------

    public void testMultipleSameAsPass()
    {
        check("select 1 as again,2 as \"again\", 3 as AGAiN from (values (true))");
    }

    public void testMultipleDifferentAs()
    {
        check("select 1 as c1,2 as c2 from (values(true))");
    }

    public void testTypeOfAs()
    {
        checkColumnType("select 1 as c1 from (values (true))", "INTEGER NOT NULL");
        checkColumnType("select 'hej' as c1 from (values (true))", "CHAR(3) NOT NULL");
        checkColumnType("select x'deadbeef' as c1 from (values (true))", "BINARY(4) NOT NULL");
        checkColumnType("select cast(null as boolean) as c1 from (values (true))", "BOOLEAN");
    }

    public void testTypesLiterals()
    {
        checkExpType("'abc'", "CHAR(3) NOT NULL");
        checkExpType("n'abc'", "CHAR(3) NOT NULL");
        checkExpType("_iso_8859-2'abc'", "CHAR(3) NOT NULL");
        checkExpType("'ab '" + NL + "' cd'", "CHAR(6) NOT NULL");
        checkExpType("'ab'" + NL + "'cd'" + NL + "'ef'" + NL + "'gh'" + NL
            + "'ij'" + NL + "'kl'", "CHAR(12) NOT NULL");
        checkExpType("n'ab '" + NL + "' cd'", "CHAR(6) NOT NULL");
        checkExpType("_iso_8859-2'ab '" + NL + "' cd'", "CHAR(6) NOT NULL");

        checkExpFails("^x'abc'^",
            "Binary literal string must contain an even number of hexits");
        checkExpType("x'abcd'", "BINARY(2) NOT NULL");
        checkExpType("x'abcd'" + NL + "'ff001122aabb'", "BINARY(8) NOT NULL");
        checkExpType("x'aaaa'" + NL + "'bbbb'" + NL + "'0000'" + NL + "'1111'",
            "BINARY(8) NOT NULL");

        checkExpType("1234567890", "INTEGER NOT NULL");
        checkExpType("123456.7890", "DECIMAL(10, 4) NOT NULL");
        checkExpType("123456.7890e3", "DOUBLE NOT NULL");
        checkExpType("true", "BOOLEAN NOT NULL");
        checkExpType("false", "BOOLEAN NOT NULL");
        checkExpType("unknown", "BOOLEAN");
    }

    public void testBooleans()
    {
        check("select TRUE OR unknowN from (values(true))");
        check("select false AND unknown from (values(true))");
        check("select not UNKNOWn from (values(true))");
        check("select not true from (values(true))");
        check("select not false from (values(true))");
    }

    public void testAndOrIllegalTypesFails()
    {
        //TODO need col+line number
        checkWholeExpFails("'abc' AND FaLsE",
            "(?s).*'<CHAR.3.> AND <BOOLEAN>'.*");

        checkWholeExpFails("TRUE OR 1", "(?s).*");

        checkWholeExpFails("unknown OR 1.0",
            "(?s).*");

        checkWholeExpFails("true OR 1.0e4",
            "(?s).*");

        if (todo) {
            checkWholeExpFails("TRUE OR (TIME '12:00' AT LOCAL)",
                "(?s).*");
        }
    }

    public void testNotIllegalTypeFails()
    {
        //TODO need col+line number
        assertExceptionIsThrown("select NOT 3.141 from (values(true))",
            "(?s).*'NOT<DECIMAL.4, 3.>'.*");

        assertExceptionIsThrown("select NOT 'abc' from (values(true))", "(?s).*");

        assertExceptionIsThrown("select NOT 1 from (values(true))", "(?s).*");
    }

    public void testIs()
    {
        check("select TRUE IS FALSE FROM (values(true))");
        check("select false IS NULL FROM (values(true))");
        check("select UNKNOWN IS NULL FROM (values(true))");
        check("select FALSE IS UNKNOWN FROM (values(true))");

        check("select TRUE IS NOT FALSE FROM (values(true))");
        check("select TRUE IS NOT NULL FROM (values(true))");
        check("select false IS NOT NULL FROM (values(true))");
        check("select UNKNOWN IS NOT NULL FROM (values(true))");
        check("select FALSE IS NOT UNKNOWN FROM (values(true))");

        check("select 1 IS NULL FROM (values(true))");
        check("select 1.2 IS NULL FROM (values(true))");
        checkExpFails("'abc' IS NOT UNKNOWN", "(?s).*Cannot apply.*");
    }

    public void testIsFails()
    {
        assertExceptionIsThrown("select 1 IS TRUE FROM (values(true))",
            "(?s).*'<INTEGER> IS TRUE'.*");

        assertExceptionIsThrown("select 1.1 IS NOT FALSE FROM (values(true))",
            "(?s).*");

        assertExceptionIsThrown("select 1.1e1 IS NOT FALSE FROM (values(true))",
            "(?s).*Cannot apply 'IS NOT FALSE' to arguments of type '<DOUBLE> IS NOT FALSE'.*");

        assertExceptionIsThrown("select 'abc' IS NOT TRUE FROM (values(true))",
            "(?s).*");
    }

    public void testScalars()
    {
        check("select 1  + 1 from (values(true))");
        check("select 1  + 2.3 from (values(true))");
        check("select 1.2+3 from (values(true))");
        check("select 1.2+3.4 from (values(true))");

        check("select 1  - 1 from (values(true))");
        check("select 1  - 2.3 from (values(true))");
        check("select 1.2-3 from (values(true))");
        check("select 1.2-3.4 from (values(true))");

        check("select 1  * 2 from (values(true))");
        check("select 1.2* 3 from (values(true))");
        check("select 1  * 2.3 from (values(true))");
        check("select 1.2* 3.4 from (values(true))");

        check("select 1  / 2 from (values(true))");
        check("select 1  / 2.3 from (values(true))");
        check("select 1.2/ 3 from (values(true))");
        check("select 1.2/3.4 from (values(true))");
    }

    public void testScalarsFails()
    {
        //TODO need col+line number
        assertExceptionIsThrown("select 1+TRUE from (values(true))",
            "(?s).*Cannot apply '\\+' to arguments of type '<INTEGER> \\+ <BOOLEAN>'\\. Supported form\\(s\\):.*");
    }

    public void testNumbers()
    {
        check("select 1+-2.*-3.e-1/-4>+5 AND true from (values(true))");
    }

    public void testPrefix()
    {
        checkExpType("+interval '1' second","INTERVAL SECOND NOT NULL");
        checkExpType("-interval '1' month","INTERVAL MONTH NOT NULL");
        checkFails("SELECT -'abc' from (values(true))",
            "(?s).*Cannot apply '-' to arguments of type '-<CHAR.3.>'.*");
        checkFails("SELECT +'abc' from (values(true))",
            "(?s).*Cannot apply '\\+' to arguments of type '\\+<CHAR.3.>'.*");
    }

    public void testEqualNotEqual()
    {
        checkExp("''=''");
        checkExp("'abc'=n''");
        checkExp("''=_latin1''");
        checkExp("n''=''");
        checkExp("n'abc'=n''");
        checkExp("n''=_latin1''");
        checkExp("_latin1''=''");
        checkExp("_latin1''=n''");
        checkExp("_latin1''=_latin1''");

        checkExp("''<>''");
        checkExp("'abc'<>n''");
        checkExp("''<>_latin1''");
        checkExp("n''<>''");
        checkExp("n'abc'<>n''");
        checkExp("n''<>_latin1''");
        checkExp("_latin1''<>''");
        checkExp("_latin1'abc'<>n''");
        checkExp("_latin1''<>_latin1''");

        checkExp("true=false");
        checkExp("unknown<>true");

        checkExp("1=1");
        checkExp("1=.1");
        checkExp("1=1e-1");
        checkExp("0.1=1");
        checkExp("0.1=0.1");
        checkExp("0.1=1e1");
        checkExp("1.1e1=1");
        checkExp("1.1e1=1.1");
        checkExp("1.1e-1=1e1");

        checkExp("''<>''");
        checkExp("1<>1");
        checkExp("1<>.1");
        checkExp("1<>1e-1");
        checkExp("0.1<>1");
        checkExp("0.1<>0.1");
        checkExp("0.1<>1e1");
        checkExp("1.1e1<>1");
        checkExp("1.1e1<>1.1");
        checkExp("1.1e-1<>1e1");
    }

    public void testEqualNotEqualFails()
    {
        checkExpFails("''<>1",
            "(?s).*Cannot apply '<>' to arguments of type '<CHAR.0.> <> <INTEGER>'.*");
        checkExpFails("'1'>=1",
            "(?s).*Cannot apply '>=' to arguments of type '<CHAR.1.> >= <INTEGER>'.*");
        checkExpFails("1<>n'abc'",
            "(?s).*Cannot apply '<>' to arguments of type '<INTEGER> <> <CHAR.3.>'.*");
        checkExpFails("''=.1",
            "(?s).*Cannot apply '=' to arguments of type '<CHAR.0.> = <DECIMAL.1..1.>'.*");
        checkExpFails("true<>1e-1",
            "(?s).*Cannot apply '<>' to arguments of type '<BOOLEAN> <> <DOUBLE>'.*");
        checkExpFails("false=''",
            "(?s).*Cannot apply '=' to arguments of type '<BOOLEAN> = <CHAR.0.>'.*");
        checkExpFails("x'a4'=0.01",
            "(?s).*Cannot apply '=' to arguments of type '<BINARY.1.> = <DECIMAL.3, 2.>'.*");
        checkExpFails("x'a4'=1",
            "(?s).*Cannot apply '=' to arguments of type '<BINARY.1.> = <INTEGER>'.*");
        checkExpFails("x'13'<>0.01",
            "(?s).*Cannot apply '<>' to arguments of type '<BINARY.1.> <> <DECIMAL.3, 2.>'.*");
        checkExpFails("x'abcd'<>1",
            "(?s).*Cannot apply '<>' to arguments of type '<BINARY.2.> <> <INTEGER>'.*");
    }

    public void testBinaryString()
    {
        check("select x'face'=X'' from (values(true))");
        check("select x'ff'=X'' from (values(true))");
    }

    public void testBinaryStringFails()
    {
        assertExceptionIsThrown("select x'ffee'='abc' from (values(true))",
            "(?s).*Cannot apply '=' to arguments of type '<BINARY.2.> = <CHAR.3.>'.*");
        assertExceptionIsThrown("select x'ff'=88 from (values(true))",
            "(?s).*Cannot apply '=' to arguments of type '<BINARY.1.> = <INTEGER>'.*");
        assertExceptionIsThrown("select x''<>1.1e-1 from (values(true))",
            "(?s).*Cannot apply '<>' to arguments of type '<BINARY.0.> <> <DOUBLE>'.*");
        assertExceptionIsThrown("select x''<>1.1 from (values(true))",
            "(?s).*Cannot apply '<>' to arguments of type '<BINARY.0.> <> <DECIMAL.2, 1.>'.*");
    }

    public void testStringLiteral()
    {
        check("select n''=_iso_8859-1'abc' from (values(true))");
        check("select N'f'<>'''' from (values(true))");
    }

    public void testStringLiteralBroken()
    {
        check("select 'foo'" + NL + "'bar' from (values(true))");
        check("select 'foo'\r'bar' from (values(true))");
        check("select 'foo'\n\r'bar' from (values(true))");
        check("select 'foo'\r\n'bar' from (values(true))");
        check("select 'foo'\n'bar' from (values(true))");
        checkFails("select 'foo' /* comment */ ^'bar'^ from (values(true))",
            "String literal continued on same line");
        check("select 'foo' -- comment\r from (values(true))");
        checkFails("select 'foo' ^'bar'^ from (values(true))",
            "String literal continued on same line");
    }

    public void testArithmeticOperators()
    {
        checkExp("pow(2,3)");
        checkExp("aBs(-2.3e-2)");
        checkExp("MOD(5             ,\t\f\r\n2)");
        checkExp("ln(5.43  )");
        checkExp("log10(- -.2  )");

        checkExp("mod(5.1, 3)");
        checkExp("mod(2,5.1)");
        checkExp("exp(3.67)");
    }

    public void testArithmeticOperatorsTypes()
    {
        // todo: move these tests to SqlOperatorTests
        checkExpType("pow(2,3)", "todo: DOUBLE");
        checkExpType("aBs(-2.3e-2)", "todo: DOUBLE");
        checkExpType("aBs(5000000000)", "todo: BIGINT");
        checkExpType("aBs(-interval '1-1' year to month)", "todo: INTERVAL YEAR TO MONTH");
        checkExpType("aBs(+interval '1:1' hour to minute)", "todo: INTERVAL HOUR TO MINUTE");
        checkExpType("MOD(5,2)", "todo: INTEGER");
        checkExpType("ln(5.43  )", "todo: DOUBLE");
        checkExpType("log10(- -.2  )", "todo: DOUBLE");
        checkExpType("exp(3)", "todo: DOUBLE");
    }

    public void testArithmeticOperatorsFails()
    {
        checkExpFails("pow(2,'abc')",
            "(?s).*Cannot apply 'POW' to arguments of type 'POW.<INTEGER>, <CHAR.3.>.*");
        checkExpFails("pow(true,1)",
            "(?s).*Cannot apply 'POW' to arguments of type 'POW.<BOOLEAN>, <INTEGER>.*");
        checkExpFails("mod(x'1100',1)",
            "(?s).*Cannot apply 'MOD' to arguments of type 'MOD.<BINARY.2.>, <INTEGER>.*");
        checkExpFails("mod(1, x'1100')",
            "(?s).*Cannot apply 'MOD' to arguments of type 'MOD.<INTEGER>, <BINARY.2.>.*");
        checkExpFails("abs(x'')",
            "(?s).*Cannot apply 'ABS' to arguments of type 'ABS.<BINARY.0.>.*");
        checkExpFails("ln(x'face12')",
            "(?s).*Cannot apply 'LN' to arguments of type 'LN.<BINARY.3.>.*");
        checkExpFails("log10(x'fa')",
            "(?s).*Cannot apply 'LOG10' to arguments of type 'LOG10.<BINARY.1.>.*");
        checkExpFails("exp('abc')",
            "(?s).*Cannot apply 'EXP' to arguments of type 'EXP.<CHAR.3.>.*");
    }

    public void testCaseExpression()
    {
        checkExp("case 1 when 1 then 'one' end");
        checkExp("case 1 when 1 then 'one' else null end");
        checkExp("case 1 when 1 then 'one' else 'more' end");
        checkExp("case 1 when 1 then 'one' when 2 then null else 'more' end");
        checkExp("case when TRUE then 'true' else 'false' end");
        check("values case when TRUE then 'true' else 'false' end");
        checkExp(
            "CASE 1 WHEN 1 THEN cast(null as integer) WHEN 2 THEN null END");
        checkExp(
            "CASE 1 WHEN 1 THEN cast(null as integer) WHEN 2 THEN cast(null as integer) END");
        checkExp(
            "CASE 1 WHEN 1 THEN null WHEN 2 THEN cast(null as integer) END");
        checkExp(
            "CASE 1 WHEN 1 THEN cast(null as integer) WHEN 2 THEN cast(cast(null as tinyint) as integer) END");
    }

    public void testCaseExpressionTypes()
    {
        checkExpType("case 1 when 1 then 'one' else 'not one' end",
            "CHAR(7) NOT NULL");
        checkExpType("case when 2<1 then 'impossible' end", "CHAR(10)");
        checkExpType("case 'one' when 'two' then 2.00 when 'one' then 1.3 else 3.2 end",
            "DECIMAL(3, 2) NOT NULL");
        checkExpType("case 'one' when 'two' then 2 when 'one' then 1.00 else 3 end",
            "DECIMAL(12, 2) NOT NULL");
        checkExpType("case 1 when 1 then 'one' when 2 then null else 'more' end",
            "CHAR(4)");
        checkExpType("case when TRUE then 'true' else 'false' end",
            "CHAR(5) NOT NULL");
        checkExpType("CASE 1 WHEN 1 THEN cast(null as integer) END", "INTEGER");
        checkExpType("CASE 1 WHEN 1 THEN NULL WHEN 2 THEN cast(cast(null as tinyint) as integer) END",
            "INTEGER");
        checkExpType("CASE 1 WHEN 1 THEN cast(null as integer) WHEN 2 THEN cast(null as integer) END",
            "INTEGER");
        checkExpType("CASE 1 WHEN 1 THEN cast(null as integer) WHEN 2 THEN cast(cast(null as tinyint) as integer) END",
            "INTEGER");
        ;
    }

    public void testCaseExpressionFails()
    {
        //varchar not comparable with bit string
        checkExpFails("case 'string' when x'01' then 'zero one' else 'something' end",
            "(?s).*Cannot apply '=' to arguments of type '<CHAR.6.> = <BINARY.1.>'.*");

        //all thens and else return null
        checkExpFails("case 1 when 1 then null else null end",
            "(?s).*ELSE clause or at least one THEN clause must be non-NULL.*");

        //all thens and else return null
        checkExpFails("case 1 when 1 then null end",
            "(?s).*ELSE clause or at least one THEN clause must be non-NULL.*");
        checkWholeExpFails(
            "case when true and true then 1 "
            + "when false then 2 "
            + "when false then true " + "else "
            + "case when true then 3 end end",
            "Illegal mixing of types in CASE or COALESCE statement");
    }

    public void testNullIf()
    {
        checkExp("nullif(1,2)");
        checkExpType("nullif(1,2)", "INTEGER");
        checkExpType("nullif('a','b')", "CHAR(1)");
        checkExpType("nullif(345.21, 2)", "DECIMAL(5, 2)");
        checkExpType("nullif(345.21, 2e0)", "DECIMAL(5, 2)");
    }

    public void testCoalesce()
    {
        checkExp("coalesce('a','b')");
        checkExpType("coalesce('a','b','c')", "CHAR(1) NOT NULL");
    }

    public void testCoalesceFails()
    {
        checkWholeExpFails("coalesce('a',1)",
            "Illegal mixing of types in CASE or COALESCE statement");
        checkWholeExpFails("coalesce('a','b',1)",
            "Illegal mixing of types in CASE or COALESCE statement");
    }

    public void testStringCompare()
    {
        checkExp("'a' = 'b'");
        checkExp("'a' <> 'b'");
        checkExp("'a' > 'b'");
        checkExp("'a' < 'b'");
        checkExp("'a' >= 'b'");
        checkExp("'a' <= 'b'");

        checkExp("cast('' as varchar(1))>cast('' as char(1))");
        checkExp("cast('' as varchar(1))<cast('' as char(1))");
        checkExp("cast('' as varchar(1))>=cast('' as char(1))");
        checkExp("cast('' as varchar(1))<=cast('' as char(1))");
        checkExp("cast('' as varchar(1))=cast('' as char(1))");
        checkExp("cast('' as varchar(1))<>cast('' as char(1))");
    }

    public void testStringCompareType()
    {
        checkExpType("'a' = 'b'", "todo: BOOLEAN"); // todo: should it be "BOOLEAN NOT NULL" since args are not null?
        checkExpType("'a' <> 'b'", "todo: BOOLEAN");
        checkExpType("'a' > 'b'", "todo: BOOLEAN");
        checkExpType("'a' < 'b'", "todo: BOOLEAN");
        checkExpType("'a' >= 'b'", "todo: BOOLEAN");
        checkExpType("'a' <= 'b'", "todo: BOOLEAN");
        checkExpType("CAST(NULL AS VARCHAR(33)) > 'foo'", "BOOLEAN");
    }

    public void testConcat()
    {
        checkExp("'a'||'b'");
        checkExp("x'12'||x'34'");
        checkExpType("'a'||'b'", "todo: VARCHAR(2)"); // should it be "VARCHAR(2) NOT NULL"?
        checkExpType("cast('a' as char(1))||cast('b' as char(2))", "todo: VARCHAR(3)");
        checkExpType("'a'||'b'||'c'", "todo: VARCHAR(3)");
        checkExpType("'a'||'b'||'cde'||'f'", "todo: VARCHAR(6)");
        checkExp("_iso-8859-6'a'||_iso-8859-6'b'||_iso-8859-6'c'");
    }

    public void testConcatWithCharset()
    {
        checkCharset(
            "_iso-8859-6'a'||_iso-8859-6'b'||_iso-8859-6'c'",
            Charset.forName("ISO-8859-6"));
    }

    public void testConcatFails()
    {
        checkExpFails("'a'||x'ff'",
            "(?s).*Cannot apply '\\|\\|' to arguments of type '<CHAR.1.> \\|\\| <BINARY.1.>'"
            + ".*Supported form.s.: '<STRING> \\|\\| <STRING>.*'");
    }

    public void testBetween()
    {
        checkExp("1 between 2 and 3");
        checkExp("'a' between 'b' and 'c'");
        checkExpFails("'' between 2 and 3", "(?s).*Cannot apply.*");
    }

    public void testCharsetMismatch()
    {
        checkExpFails("''=_shift_jis''",
            "(?s).*Cannot apply .* to the two different charsets.*");
        checkExpFails("''<>_shift_jis''",
            "(?s).*Cannot apply .* to the two different charsets.*");
        checkExpFails("''>_shift_jis''",
            "(?s).*Cannot apply .* to the two different charsets.*");
        checkExpFails("''<_shift_jis''",
            "(?s).*Cannot apply .* to the two different charsets.*");
        checkExpFails("''<=_shift_jis''",
            "(?s).*Cannot apply .* to the two different charsets.*");
        checkExpFails("''>=_shift_jis''",
            "(?s).*Cannot apply .* to the two different charsets.*");
        checkExpFails("''||_shift_jis''", "(?s).*");
        checkExpFails("'a'||'b'||_iso-8859-6'c'", "(?s).*");
    }

    // FIXME jvs 2-Feb-2005: all collation-related tests are disabled due to
    // dtbug 280

    public void _testSimpleCollate()
    {
        checkExp("'s' collate latin1$en$1");
        checkExpType("'s' collate latin1$en$1", "CHAR(1)");
        checkCollation("'s'", "ISO-8859-1$en_US$primary",
            SqlCollation.Coercibility.Coercible);
        checkCollation("'s' collate latin1$sv$3", "ISO-8859-1$sv$3",
            SqlCollation.Coercibility.Explicit);
    }

    public void _testCharsetAndCollateMismatch()
    {
        //todo
        checkExpFails("_shift_jis's' collate latin1$en$1", "?");
    }

    public void _testDyadicCollateCompare()
    {
        checkExp("'s' collate latin1$en$1 < 't'");
        checkExp("'t' > 's' collate latin1$en$1");
        checkExp("'s' collate latin1$en$1 <> 't' collate latin1$en$1");
    }

    public void _testDyadicCompareCollateFails()
    {
        //two different explicit collations. difference in strength
        checkExpFails("'s' collate latin1$en$1 <= 't' collate latin1$en$2",
            "(?s).*Two explicit different collations.*are illegal.*");

        //two different explicit collations. difference in language
        checkExpFails("'s' collate latin1$sv$1 >= 't' collate latin1$en$1",
            "(?s).*Two explicit different collations.*are illegal.*");
    }

    public void _testDyadicCollateOperator()
    {
        checkCollation("'a' || 'b'", "ISO-8859-1$en_US$primary",
            SqlCollation.Coercibility.Coercible);
        checkCollation("'a' collate latin1$sv$3 || 'b'", "ISO-8859-1$sv$3",
            SqlCollation.Coercibility.Explicit);
        checkCollation("'a' collate latin1$sv$3 || 'b' collate latin1$sv$3",
            "ISO-8859-1$sv$3", SqlCollation.Coercibility.Explicit);
    }

    public void testCharLength()
    {
        checkExp("char_length('string')");
        checkExp("char_length(_shift_jis'string')");
        checkExp("character_length('string')");
        checkExpType("char_length('string')", "INTEGER NOT NULL");
        checkExpType("character_length('string')", "INTEGER NOT NULL");
    }

    public void testUpperLower()
    {
        checkExp("upper(_shift_jis'sadf')");
        checkExp("lower(n'sadf')");
        checkExpType("lower('sadf')", "CHAR(4) NOT NULL");
        checkExpFails("upper(123)",
            "(?s).*Cannot apply 'UPPER' to arguments of type 'UPPER.<INTEGER>.'.*");
    }

    public void testPosition()
    {
        checkExp("position('mouse' in 'house')");
        checkExp("position(x'11' in x'100110')");
        checkExp("position(x'abcd' in x'')");
        checkExpType("position('mouse' in 'house')", "INTEGER NOT NULL");
        checkExpFails("position(x'1234' in '110')",
            "(?s).*Cannot apply 'POSITION' to arguments of type 'POSITION.<BINARY.2.> IN <CHAR.3.>.'.*");
    }

    public void testTrim()
    {
        checkExp("trim('mustache' FROM 'beard')");
        checkExp("trim(both 'mustache' FROM 'beard')");
        checkExp("trim(leading 'mustache' FROM 'beard')");
        checkExp("trim(trailing 'mustache' FROM 'beard')");
        checkExpType("trim('mustache' FROM 'beard')", "todo: CHAR(5)");

        if (todo) {
            final SqlCollation.Coercibility expectedCoercibility = null;
            checkCollation("trim('mustache' FROM 'beard')", "CHAR(5)", expectedCoercibility);
        }

    }

    public void testTrimFails()
    {
        checkExpFails("trim(123 FROM 'beard')",
            "(?s).*Cannot apply 'TRIM' to arguments of type.*");
        checkExpFails("trim('a' FROM 123)",
            "(?s).*Cannot apply 'TRIM' to arguments of type.*");
        checkExpFails("trim('a' FROM _shift_jis'b')",
            "(?s).*not comparable to each other.*");
    }

    public void _testConvertAndTranslate()
    {
        checkExp("convert('abc' using conversion)");
        checkExp("translate('abc' using translation)");
    }

    public void testOverlay()
    {
        checkExp("overlay('ABCdef' placing 'abc' from 1)");
        checkExp("overlay('ABCdef' placing 'abc' from 1 for 3)");
        checkExpFails("overlay('ABCdef' placing 'abc' from '1' for 3)",
            "(?s).*OVERLAY\\(<STRING> PLACING <STRING> FROM <INTEGER>\\).*");
        checkExpType("overlay('ABCdef' placing 'abc' from 1 for 3)",
            "todo: CHAR(9)");

        if (todo) {
            checkCollation("overlay('ABCdef' placing 'abc' collate latin1$sv from 1 for 3)",
                "ISO-8859-1$sv", SqlCollation.Coercibility.Explicit);
        }
    }

    public void testSubstring()
    {
        checkExp("substring('a' FROM 1)");
        checkExp("substring('a' FROM 1 FOR 3)");
        checkExp("substring('a' FROM 'reg' FOR '\\')");
        checkExp("substring(x'ff' FROM 1  FOR 2)"); //binary string

        checkExpType("substring('10' FROM 1  FOR 2)", "VARCHAR(2) NOT NULL");
        checkExpType("substring('1000' FROM '1'  FOR 'w')", "VARCHAR(4) NOT NULL");
        checkExpType("substring(cast(' 100 ' as CHAR(99)) FROM '1'  FOR 'w')",
            "VARCHAR(99) NOT NULL");
        checkExpType("substring(x'10456b' FROM 1  FOR 2)", "VARBINARY(3) NOT NULL");

        checkCharset(
            "substring('10' FROM 1  FOR 2)",
            Charset.forName("latin1"));
        checkCharset(
            "substring(_shift_jis'10' FROM 1  FOR 2)",
            Charset.forName("SHIFT_JIS"));
    }


    public void testSubstringFails()
    {
        checkExpFails("substring('a' from 1 for 'b')",
            "(?s).*Cannot apply 'SUBSTRING' to arguments of type.*");
        checkExpFails("substring(_shift_jis'10' FROM '0' FOR '\\')",
            "(?s).* not comparable to each other.*");
        checkExpFails("substring('10' FROM _shift_jis'0' FOR '\\')",
            "(?s).* not comparable to each other.*");
        checkExpFails("substring('10' FROM '0' FOR _shift_jis'\\')",
            "(?s).* not comparable to each other.*");
    }

    public void testLikeAndSimilar()
    {
        checkExp("'a' like 'b'");
        checkExp("'a' like 'b'");
        checkExp("'a' similar to 'b'");
        checkExp("'a' similar to 'b' escape 'c'");
    }

    public void _testLikeAndSimilarFails()
    {
        checkExpFails("'a' like _shift_jis'b'  escape 'c'",
            "(?s).*Operands _ISO-8859-1.a. COLLATE ISO-8859-1.en_US.primary, _SHIFT_JIS.b..*");
        checkExpFails("'a' similar to _shift_jis'b'  escape 'c'",
            "(?s).*Operands _ISO-8859-1.a. COLLATE ISO-8859-1.en_US.primary, _SHIFT_JIS.b..*");

        checkExpFails("'a' similar to 'b' collate shift_jis$jp  escape 'c'",
            "(?s).*Operands _ISO-8859-1.a. COLLATE ISO-8859-1.en_US.primary, _ISO-8859-1.b. COLLATE SHIFT_JIS.jp.primary.*");
    }

    public void testNull()
    {
        checkFails("values 1.0 + NULL", "(?s).*Illegal use of .NULL.*");
        checkExpFails("1.0 + NULL", "(?s).*Illegal use of .NULL.*");
    }

    public void testNullCast()
    {
        checkExpType("cast(null as tinyint)", "TINYINT");
        checkExpType("cast(null as smallint)", "SMALLINT");
        checkExpType("cast(null as integer)", "INTEGER");
        checkExpType("cast(null as bigint)", "BIGINT");
        checkExpType("cast(null as float)", "FLOAT");
        checkExpType("cast(null as real)", "REAL");
        checkExpType("cast(null as double)", "DOUBLE");
        checkExpType("cast(null as boolean)", "BOOLEAN");
        checkExpType("cast(null as varchar(1))", "VARCHAR(1)");
        checkExpType("cast(null as char(1))", "CHAR(1)");
        checkExpType("cast(null as binary(1))", "BINARY(1)");
        checkExpType("cast(null as date)", "DATE");
        checkExpType("cast(null as time)", "TIME(0)");
        checkExpType("cast(null as timestamp)", "TIMESTAMP(0)");
        checkExpType("cast(null as decimal)", "DECIMAL(19, 0)");
        checkExpType("cast(null as varbinary(1))", "VARBINARY(1)");

        checkExp("cast(null as integer), cast(null as char(1))");
    }

    public void testCastTypeToType()
    {
        checkExpType("cast(123 as varchar(3))", "VARCHAR(3) NOT NULL");
        checkExpType("cast(123 as char(3))", "CHAR(3) NOT NULL");
        checkExpType("cast('123' as integer)", "INTEGER NOT NULL");
        checkExpType("cast('123' as double)", "DOUBLE NOT NULL");
        checkExpType("cast('1.0' as real)", "REAL NOT NULL");
        checkExpType("cast(1.0 as tinyint)", "TINYINT NOT NULL");
        checkExpType("cast(1 as tinyint)", "TINYINT NOT NULL");
        checkExpType("cast(1.0 as smallint)", "SMALLINT NOT NULL");
        checkExpType("cast(1 as integer)", "INTEGER NOT NULL");
        checkExpType("cast(1.0 as integer)", "INTEGER NOT NULL");
        checkExpType("cast(1.0 as bigint)", "BIGINT NOT NULL");
        checkExpType("cast(1 as bigint)", "BIGINT NOT NULL");
        checkExpType("cast(1.0 as float)", "FLOAT NOT NULL");
        checkExpType("cast(1 as float)", "FLOAT NOT NULL");
        checkExpType("cast(1.0 as real)", "REAL NOT NULL");
        checkExpType("cast(1 as real)", "REAL NOT NULL");
        checkExpType("cast(1.0 as double)", "DOUBLE NOT NULL");
        checkExpType("cast(1 as double)", "DOUBLE NOT NULL");
        checkExpType("cast(123 as decimal(6,4))", "DECIMAL(6, 4) NOT NULL");
        checkExpType("cast(123 as decimal(6))", "DECIMAL(6, 0) NOT NULL");
        checkExpType("cast(123 as decimal)", "DECIMAL(19, 0) NOT NULL");
        checkExpType("cast(1.234 as decimal(2,5))", "DECIMAL(2, 5) NOT NULL");
        checkExpType("cast('4.5' as decimal(3,1))", "DECIMAL(3, 1) NOT NULL");
        checkExpType("cast(null as boolean)", "BOOLEAN");
        checkExpType("cast('abc' as varchar(1))", "VARCHAR(1) NOT NULL");
        checkExpType("cast('abc' as char(1))", "CHAR(1) NOT NULL");
        checkExpType("cast(x'ff' as binary(1))", "BINARY(1) NOT NULL");
        checkExpType("cast(multiset[1] as double multiset)", "DOUBLE NOT NULL MULTISET NOT NULL");
        checkExpType("cast(multiset['abc'] as integer multiset)", "INTEGER NOT NULL MULTISET NOT NULL");
    }

    public void testCastFails()
    {
        checkExpFails("cast('foo' as bar)",
            "(?s).*Unknown datatype name 'BAR'");
        checkExpFails("cast(multiset[1] as integer)",
            "(?s).*Cast function cannot convert value of type INTEGER MULTISET to type INTEGER");
        checkExpFails("cast(x'ff' as decimal(5,2))",
            "(?s).*Cast function cannot convert value of type BINARY\\(1\\) to type DECIMAL\\(5, 2\\)");
        // TODO: Enable tests when data types are validated in non-DDL context
        if (todo) {
        checkExpFails("cast(43 as decimal(1,20))",
            "(?s).*Scale 20 exceeds maximum of 19.*");
        checkExpFails("cast(43 as decimal(20,19))",
            "(?s).*Precision 20 exceeds maximum of 19");
        checkExpFails("cast(43 as decimal(0,2))",
            "(?s).*Precision must be positive.*");
        }

        checkExpFails("cast(1 as boolean)",
            "(?s).*Cast function cannot convert value of type INTEGER to type BOOLEAN.*");
        checkExpFails("cast(1.0e1 as boolean)",
            "(?s).*Cast function cannot convert value of type DOUBLE to type BOOLEAN.*");
        checkExpFails("cast(true as numeric)",
            "(?s).*Cast function cannot convert value of type BOOLEAN to type DECIMAL.*");
        checkExpFails("cast(DATE '1243-12-01' as TIME)",
            "(?s).*Cast function cannot convert value of type DATE to type TIME.*");
        checkExpFails("cast(TIME '12:34:01' as DATE)",
            "(?s).*Cast function cannot convert value of type TIME\\(0\\) to type DATE.*");


    }

    public void testDateTime()
    {
        // LOCAL_TIME
        checkExp("LOCALTIME(3)");
        checkExp("LOCALTIME"); //    fix sqlcontext later.
        checkExpFails("LOCALTIME(1+2)",
            "Argument to function 'LOCALTIME' must be a literal");
        checkWholeExpFails("LOCALTIME()",
            "No match found for function signature LOCALTIME..");
        checkExpType("LOCALTIME", "TIME(0) NOT NULL"); //  with TZ ?
        checkExpFails("LOCALTIME(-1)",
            "Argument to function 'LOCALTIME' must be a positive integer literal");
        checkExpFails("LOCALTIME(100000000000000)",
            "(?s).*Numeric literal '100000000000000' out of range.*");
        checkExpFails("LOCALTIME(4)",
            "Argument to function 'LOCALTIME' must be a valid precision between '0' and '3'");
        checkExpFails("LOCALTIME('foo')",
            "(?s).*Cannot apply.*");

        // LOCALTIMESTAMP
        checkExp("LOCALTIMESTAMP(3)");
        checkExp("LOCALTIMESTAMP"); //    fix sqlcontext later.
        checkExpFails("LOCALTIMESTAMP(1+2)",
            "Argument to function 'LOCALTIMESTAMP' must be a literal");
        checkWholeExpFails("LOCALTIMESTAMP()",
            "No match found for function signature LOCALTIMESTAMP..");
        checkExpType("LOCALTIMESTAMP", "TIMESTAMP(0) NOT NULL"); //  with TZ ?
        checkExpFails("LOCALTIMESTAMP(-1)",
            "Argument to function 'LOCALTIMESTAMP' must be a positive integer literal");
        checkExpFails("LOCALTIMESTAMP(100000000000000)",
            "(?s).*Numeric literal '100000000000000' out of range.*");
        checkExpFails("LOCALTIMESTAMP(4)",
            "Argument to function 'LOCALTIMESTAMP' must be a valid precision between '0' and '3'");
        checkExpFails("LOCALTIMESTAMP('foo')",
            "(?s).*Cannot apply.*");

        // CURRENT_DATE
        checkWholeExpFails("CURRENT_DATE(3)",
            "No match found for function signature CURRENT_DATE..NUMERIC..");
        checkExp("CURRENT_DATE"); //    fix sqlcontext later.
        checkWholeExpFails("CURRENT_DATE(1+2)",
            "No match found for function signature CURRENT_DATE..NUMERIC..");
        checkWholeExpFails("CURRENT_DATE()",
            "No match found for function signature CURRENT_DATE..");
        checkExpType("CURRENT_DATE", "DATE NOT NULL"); //  with TZ?
        checkWholeExpFails("CURRENT_DATE(-1)",
            "No match found for function signature CURRENT_DATE..NUMERIC.."); // i guess -s1 is an expression?
        checkExpFails("CURRENT_DATE('foo')", "(?s).*");

        // current_time
        checkExp("current_time(3)");
        checkExp("current_time"); //    fix sqlcontext later.
        checkExpFails("current_time(1+2)",
            "Argument to function 'CURRENT_TIME' must be a literal");
        checkWholeExpFails("current_time()",
            "No match found for function signature CURRENT_TIME..");
        checkExpType("current_time", "TIME(0) NOT NULL"); //  with TZ ?
        checkExpFails("current_time(-1)",
            "Argument to function 'CURRENT_TIME' must be a positive integer literal");
        checkExpFails("CURRENT_TIME(100000000000000)",
            "(?s).*Numeric literal '100000000000000' out of range.*");
        checkExpFails("CURRENT_TIME(4)",
            "Argument to function 'CURRENT_TIME' must be a valid precision between '0' and '3'");
        checkExpFails("current_time('foo')",
            "(?s).*Cannot apply.*");

        // current_timestamp
        checkExp("CURRENT_TIMESTAMP(3)");
        checkExp("CURRENT_TIMESTAMP"); //    fix sqlcontext later.
        check("SELECT CURRENT_TIMESTAMP AS X FROM (VALUES (1))");
        checkExpFails("CURRENT_TIMESTAMP(1+2)",
            "Argument to function 'CURRENT_TIMESTAMP' must be a literal");
        checkWholeExpFails("CURRENT_TIMESTAMP()",
            "No match found for function signature CURRENT_TIMESTAMP..");
        checkExpType("CURRENT_TIMESTAMP", "TIMESTAMP(0) NOT NULL"); //  with TZ ?
        checkExpType("CURRENT_TIMESTAMP(2)", "TIMESTAMP(2) NOT NULL"); //  with TZ ?
        checkExpFails("CURRENT_TIMESTAMP(-1)",
            "Argument to function 'CURRENT_TIMESTAMP' must be a positive integer literal");
        checkExpFails("CURRENT_TIMESTAMP(100000000000000)",
            "(?s).*Numeric literal '100000000000000' out of range.*");
        checkExpFails("CURRENT_TIMESTAMP(4)",
            "Argument to function 'CURRENT_TIMESTAMP' must be a valid precision between '0' and '3'");
        checkExpFails("CURRENT_TIMESTAMP('foo')",
            "(?s).*Cannot apply.*");

        // Date literals
        checkExp("DATE '2004-12-01'");
        checkExp("TIME '12:01:01'");
        checkExp("TIME '11:59:59.99'");
        checkExp("TIME '12:01:01.001'");
        checkExp("TIMESTAMP '2004-12-01 12:01:01'");
        checkExp("TIMESTAMP '2004-12-01 12:01:01.001'");

        // REVIEW: Can't think of any date/time/ts literals that will parse, but not validate.
    }

    /**
     * Tests casting to/from date/time types.
     */
    public void testDateTimeCast()
    {
        checkExpFails("CAST(1 as DATE)",
            "Cast function cannot convert value of type INTEGER to type DATE");
        checkExp("CAST(DATE '2001-12-21' AS VARCHAR(10))");
        checkExp("CAST( '2001-12-21' AS DATE)");
        checkExp("CAST( TIMESTAMP '2001-12-21 10:12:21' AS VARCHAR(20))");
        checkExp("CAST( TIME '10:12:21' AS VARCHAR(20))");
        checkExp("CAST( '10:12:21' AS TIME)");
        checkExp("CAST( '2004-12-21 10:12:21' AS TIMESTAMP)");
    }

    public void testInvalidFunction()
    {
        checkWholeExpFails("foo()",
            "No match found for function signature FOO..");
        checkWholeExpFails("mod(123)",
            "Invalid number of arguments to function 'MOD'. Was expecting 2 arguments");
    }

    public void testJdbcFunctionCall()
    {
        checkExp("{fn log10(1)}");
        checkExp("{fn locate('','')}");
        checkExp("{fn insert('',1,2,'')}");
        checkExpFails("{fn insert('','',1,2)}", "(?s).*.*");
        checkExpFails("{fn insert('','',1)}", "(?s).*4.*");
        checkExpFails("{fn locate('','',1)}", "(?s).*"); //todo this is legal jdbc syntax, just that currently the 3 ops call is not implemented in the system
        checkExpFails("{fn log10('1')}",
            "(?s).*Cannot apply.*fn LOG10..<CHAR.1.>.*");
        checkExpFails("{fn log10(1,1)}",
            "(?s).*Encountered .fn LOG10. with 2 parameter.s.; was expecting 1 parameter.s.*");
        checkExpFails("{fn fn(1)}",
            "(?s).*Function '.fn FN.' is not defined.*");
        checkExpFails("{fn hahaha(1)}",
            "(?s).*Function '.fn HAHAHA.' is not defined.*");
    }

    // REVIEW jvs 2-Feb-2005:  I am disabling this test because I removed
    // the corresponding support from the parser.  Where in the standard
    // does it state that you're supposed to be able to quote keywords
    // for builtin functions?
    public void _testQuotedFunction()
    {
        checkExp("\"CAST\"(1 as double)");
        checkExp("\"POSITION\"('b' in 'alphabet')");

        //convert and translate not yet implemented
        //        checkExp("\"CONVERT\"('b' using converstion)");
        //        checkExp("\"TRANSLATE\"('b' using translation)");
        checkExp("\"OVERLAY\"('a' PLAcing 'b' from 1)");
        checkExp("\"SUBSTRING\"('a' from 1)");
        checkExp("\"TRIM\"('b')");
    }

    public void testRowtype()
    {
        check("values (1),(2),(1)");
        check("values (1,'1'),(2,'2')");
        checkFails("^values ('1'),(2)^",
            "Values passed to VALUES operator must have compatible types");
        if (todo) {
            checkColumnType("values (1),(2.0),(3)", "ROWTYPE(DOUBLE)");
        }
    }

    public void testMultiset()
    {
        checkExpType("multiset[1]","INTEGER NOT NULL MULTISET NOT NULL");
        checkExpType("multiset[1, CAST(null AS DOUBLE)]", "DOUBLE MULTISET NOT NULL");
        checkExpType("multiset[1.3,2.3]","DECIMAL(2, 1) NOT NULL MULTISET NOT NULL");
        checkExpType("multiset[1,2.3, cast(4 as bigint)]","DECIMAL(19, 0) NOT NULL MULTISET NOT NULL");
        checkExpType(
            "multiset['1','22', '333','22']",
            "CHAR(3) NOT NULL MULTISET NOT NULL");
        checkExpFails("^multiset[1, '2']^", "Parameters must be of the same type");
        checkExpType(
            "multiset[ROW(1,2)]",
            "RecordType(INTEGER NOT NULL EXPR$0, INTEGER NOT NULL EXPR$1) NOT NULL MULTISET NOT NULL");
        checkExpType(
            "multiset[ROW(1,2),ROW(2,5)]",
            "RecordType(INTEGER NOT NULL EXPR$0, INTEGER NOT NULL EXPR$1) NOT NULL MULTISET NOT NULL");
        checkExpType(
            "multiset[ROW(1,2),ROW(3.4,5.4)]",
            "RecordType(DECIMAL(11, 1) NOT NULL EXPR$0, DECIMAL(11, 1) NOT NULL EXPR$1) NOT NULL MULTISET NOT NULL");
        checkExpType(
            "multiset(select*from emp)",
            "RecordType(INTEGER NOT NULL EMPNO," +
            " VARCHAR(20) NOT NULL ENAME," +
            " VARCHAR(10) NOT NULL JOB," +
            " INTEGER NOT NULL MGR," +
            " DATE NOT NULL HIREDATE," +
            " INTEGER NOT NULL SAL," +
            " INTEGER NOT NULL COMM," +
            " INTEGER NOT NULL DEPTNO) NOT NULL MULTISET NOT NULL");
    }

    public void testMultisetSetOperators()
    {
        checkExp("multiset[1] multiset union multiset[1,2.3]");
        checkExpType("multiset[324.2] multiset union multiset[23.2,2.32]",
            "DECIMAL(5, 2) NOT NULL MULTISET NOT NULL");
        checkExpType("multiset[1] multiset union multiset[1,2.3]",
            "DECIMAL(11, 1) NOT NULL MULTISET NOT NULL");
        checkExp("multiset[1] multiset union all multiset[1,2.3]");
        checkExp("multiset[1] multiset except multiset[1,2.3]");
        checkExp("multiset[1] multiset except all multiset[1,2.3]");
        checkExp("multiset[1] multiset intersect multiset[1,2.3]");
        checkExp("multiset[1] multiset intersect all multiset[1,2.3]");

        checkExpFails("^multiset[1, '2']^ multiset union multiset[1]", "Parameters must be of the same type");
        checkExp("multiset[ROW(1,2)] multiset intersect multiset[row(3,4)]");
        if (todo) {
            checkWholeExpFails("multiset[ROW(1,'2')] multiset union multiset[ROW(1,2)]",
                "Parameters must be of the same type");
        }
    }

    public void testSubMultisetOf()
    {
        checkExpType("multiset[1] submultiset of multiset[1,2.3]", "BOOLEAN NOT NULL");
        checkExpType("multiset[1] submultiset of multiset[1]", "BOOLEAN NOT NULL");

        checkExpFails("^multiset[1, '2']^ submultiset of multiset[1]", "Parameters must be of the same type");
        checkExp("multiset[ROW(1,2)] submultiset of multiset[row(3,4)]");
    }

    public void testElement()
    {
        checkExpType("element(multiset[1])", "INTEGER NOT NULL");
        checkExpType("1.0+element(multiset[1])", "DECIMAL(12, 1) NOT NULL");
        checkExpType("element(multiset['1'])", "CHAR(1) NOT NULL");
        checkExpType("element(multiset[1e-2])", "DOUBLE NOT NULL");
        checkExpType("element(multiset[multiset[cast(null as tinyint)]])", "TINYINT MULTISET NOT NULL");
    }

    public void testMemberOf()
    {
        checkExpType("1 member of multiset[1]", "BOOLEAN NOT NULL");
        checkWholeExpFails("1 member of multiset['1']", "Cannot compare values of types 'INTEGER', 'CHAR\\(1\\)'");
    }

    public void testIsASet()
    {
        checkExp("multiset[1] is a set");
        checkExp("multiset['1'] is a set");
        checkExpFails("'a' is a set", ".*Cannot apply 'IS A SET' to.*");
    }

    public void testCardinality()
    {
        checkExpType("cardinality(multiset[1])", "INTEGER NOT NULL");
        checkExpType("cardinality(multiset['1'])", "INTEGER NOT NULL");
        checkWholeExpFails("cardinality('a')", "Cannot apply 'CARDINALITY' to arguments of type 'CARDINALITY.<CHAR.1.>.'. Supported form.s.: 'CARDINALITY.<MULTISET>.'");
    }

    public void testIntervalTimeUnitEnumeration()
    {
        // Since there is validation code relaying on the fact that the
        // enumerated time unit ordinals in SqlIntervalQualifier starts with 0
        // and ends with 5, this test is here to make sure that if someone
        // changes how the time untis are setup, an early feedback will be
        // generated by this test.
        assertEquals(0, SqlIntervalQualifier.TimeUnit.Year.getOrdinal());
        assertEquals(1, SqlIntervalQualifier.TimeUnit.Month.getOrdinal());
        assertEquals(2, SqlIntervalQualifier.TimeUnit.Day.getOrdinal());
        assertEquals(3, SqlIntervalQualifier.TimeUnit.Hour.getOrdinal());
        assertEquals(4, SqlIntervalQualifier.TimeUnit.Minute.getOrdinal());
        assertEquals(5, SqlIntervalQualifier.TimeUnit.Second.getOrdinal());
        boolean b =
            (SqlIntervalQualifier.TimeUnit.Year.getOrdinal() <
            SqlIntervalQualifier.TimeUnit.Month.getOrdinal())
            &&
            (SqlIntervalQualifier.TimeUnit.Month.getOrdinal() <
            SqlIntervalQualifier.TimeUnit.Day.getOrdinal())
            &&
            (SqlIntervalQualifier.TimeUnit.Day.getOrdinal() <
            SqlIntervalQualifier.TimeUnit.Hour.getOrdinal())
            &&
            (SqlIntervalQualifier.TimeUnit.Hour.getOrdinal() <
            SqlIntervalQualifier.TimeUnit.Minute.getOrdinal())
            &&
            (SqlIntervalQualifier.TimeUnit.Minute.getOrdinal() <
            SqlIntervalQualifier.TimeUnit.Second.getOrdinal());
        assertTrue(b);
    }

    public void testIntervalMillisConversion()
    {
        checkIntervalConv("INTERVAL '1' DAY", "86400000");
        checkIntervalConv("INTERVAL '1' HOUR", "3600000");
        checkIntervalConv("INTERVAL '1' MINUTE", "60000");
        checkIntervalConv("INTERVAL '1' SECOND", "1000");
        checkIntervalConv("INTERVAL '1:05' HOUR TO MINUTE", "3900000");
        checkIntervalConv("INTERVAL '1:05' MINUTE TO SECOND", "65000");
        checkIntervalConv("INTERVAL '1 1' DAY TO HOUR", "90000000");
        checkIntervalConv("INTERVAL '1 1:05' DAY TO MINUTE", "90300000");
        checkIntervalConv("INTERVAL '1 1:05:03' DAY TO SECOND", "90303000");
        checkIntervalConv("INTERVAL '1 1:05:03.12345' DAY TO SECOND", "90303123");
        checkIntervalConv("INTERVAL '1.12345' SECOND", "1123");
        checkIntervalConv("INTERVAL '1:05.12345' MINUTE TO SECOND", "65123");
        checkIntervalConv("INTERVAL '1:05:03' HOUR TO SECOND", "3903000");
        checkIntervalConv("INTERVAL '1:05:03.12345' HOUR TO SECOND", "3903123");
    }

    public void testIntervalLiteral()
    {
        checkExpType("INTERVAL '1' DAY", "INTERVAL DAY NOT NULL");
        checkExpType("INTERVAL '1' DAY(4)", "INTERVAL DAY(4) NOT NULL");
        checkExpType("INTERVAL '1' HOUR", "INTERVAL HOUR NOT NULL");
        checkExpType("INTERVAL '1' MINUTE", "INTERVAL MINUTE NOT NULL");
        checkExpType("INTERVAL '1' SECOND", "INTERVAL SECOND NOT NULL");
        checkExpType("INTERVAL '1' SECOND(3)", "INTERVAL SECOND(3) NOT NULL");
        checkExpType("INTERVAL '1' SECOND(3, 4)", "INTERVAL SECOND(3, 4) NOT NULL");
        checkExpType("INTERVAL '1 2:3:4' DAY TO SECOND", "INTERVAL DAY TO SECOND NOT NULL");
        checkExpType("INTERVAL '1 2:3:4' DAY(4) TO SECOND(4)", "INTERVAL DAY(4) TO SECOND(4) NOT NULL");

        checkExpType("INTERVAL '1' YEAR", "INTERVAL YEAR NOT NULL");
        checkExpType("INTERVAL '1' MONTH", "INTERVAL MONTH NOT NULL");
        checkExpType("INTERVAL '1-2' YEAR TO MONTH", "INTERVAL YEAR TO MONTH NOT NULL");

        // FIXME Error message should contain quotes:
        //    Illegal interval literal format '1:2' for INTERVAL DAY TO HOUR
        // FIXME Position is wrong
        checkExpFails("interval 'wael was here' ^HOUR^",
            "(?s).*Illegal interval literal format wael was here for INTERVAL HOUR.*");
        checkExpFails("interval '1' day to ^hour^",
            "Illegal interval literal format 1 for INTERVAL DAY TO HOUR");
        checkExpFails("interval '1 2' day to ^second^",
            "Illegal interval literal format 1 2 for INTERVAL DAY TO SECOND");
        checkExpFails("interval '1 2' hour to ^minute^",
            "(?s).*Illegal interval literal format 1 2 for INTERVAL HOUR TO MINUTE.*");
        checkExp("interval '1:2' minute to second");
        checkExpFails("interval '-' ^day^",
            "(?s).*Illegal interval literal format - for INTERVAL DAY.*");
        checkExpFails("interval '1:x' hour to ^minute^",
            "(?s).*Illegal interval literal format 1:x for INTERVAL HOUR TO MINUTE.*");
        checkExpFails("interval '1:x:2' hour to ^second^",
            "(?s).*Illegal interval literal format 1:x:2 for INTERVAL HOUR TO SECOND.*");
    }

    public void testIntervalOperators()
    {
        checkExpType("interval '1' day + interval '1' DAY(4)", "INTERVAL DAY(4) NOT NULL");
        checkExpType("interval '1' day(5) + interval '1' DAY", "INTERVAL DAY(5) NOT NULL");
        checkExpType("interval '1' day + interval '1' HOUR(10)", "INTERVAL DAY TO HOUR NOT NULL");
        checkExpType("interval '1' day + interval '1' MINUTE", "INTERVAL DAY TO MINUTE NOT NULL");
        checkExpType("interval '1' day + interval '1' second", "INTERVAL DAY TO SECOND NOT NULL");

        checkExpType("interval '1:2' hour to minute + interval '1' second", "INTERVAL HOUR TO SECOND NOT NULL");
        checkExpType("interval '1:3' hour to minute + interval '1 1:2:3.4' day to second", "INTERVAL DAY TO SECOND NOT NULL");
        checkExpType("interval '1:2' hour to minute + interval '1 1' day to hour", "INTERVAL DAY TO MINUTE NOT NULL");
        checkExpType("interval '1:2' hour to minute + interval '1 1' day to hour", "INTERVAL DAY TO MINUTE NOT NULL");
        checkExpType("interval '1 2' day to hour + interval '1:1' minute to second", "INTERVAL DAY TO SECOND NOT NULL");

        checkExpType("interval '1' year + interval '1' month", "INTERVAL YEAR TO MONTH NOT NULL");
        checkExpType("interval '1' day - interval '1' hour", "INTERVAL DAY TO HOUR NOT NULL");
        checkExpType("interval '1' year - interval '1' month", "INTERVAL YEAR TO MONTH NOT NULL");
        checkExpType("interval '1' month - interval '1' year", "INTERVAL YEAR TO MONTH NOT NULL");
        checkExpFails("interval '1' year + interval '1' day",
            "(?s).*Cannot apply '\\+' to arguments of type '<INTERVAL YEAR> \\+ <INTERVAL DAY>'.*");
        checkExpFails("interval '1' month + interval '1' second",
            "(?s).*Cannot apply '\\+' to arguments of type '<INTERVAL MONTH> \\+ <INTERVAL SECOND>'.*");
        checkExpFails("interval '1' year - interval '1' day",
            "(?s).*Cannot apply '-' to arguments of type '<INTERVAL YEAR> - <INTERVAL DAY>'.*");
        checkExpFails("interval '1' month - interval '1' second",
            "(?s).*Cannot apply '-' to arguments of type '<INTERVAL MONTH> - <INTERVAL SECOND>'.*");

        // mixing between datetime and interval
//todo        checkExpType("date '1234-12-12' + INTERVAL '1' month + interval '1' day","DATE");
//todo        checkExpFails("date '1234-12-12' + (INTERVAL '1' month + interval '1' day)","?");

        // multiply operator
        checkExpType("interval '1' year * 2", "INTERVAL YEAR NOT NULL");
        checkExpType("1.234*interval '1 1:2:3' day to second ", "INTERVAL DAY TO SECOND NOT NULL");

        // division operator
        checkExpType("interval '1' month / 0.1", "INTERVAL MONTH NOT NULL");
        checkExpType("interval '1-2' year TO month / 0.1e-9", "INTERVAL YEAR TO MONTH NOT NULL");
        checkExpFails("1.234/interval '1 1:2:3' day to second ", "(?s).*Cannot apply '/' to arguments of type '<DECIMAL.4, 3.> / <INTERVAL DAY TO SECOND>'.*");
    }

    public void testNumericOperators()
    {
        // unary operator
        checkExpType("- cast(1 as TINYINT)", "TINYINT NOT NULL");
        checkExpType("+ cast(1 as INT)", "INTEGER NOT NULL");
        checkExpType("- cast(1 as FLOAT)", "FLOAT NOT NULL");
        checkExpType("+ cast(1 as DOUBLE)", "DOUBLE NOT NULL");
        checkExpType("-1.643", "DECIMAL(4, 3) NOT NULL");
        checkExpType("+1.643", "DECIMAL(4, 3) NOT NULL");

        // addition operator
        checkExpType("cast(1 as TINYINT) + cast(5 as INTEGER)", "INTEGER NOT NULL");
        checkExpType("cast(null as SMALLINT) + cast(5 as BIGINT)", "BIGINT");
        checkExpType("cast(1 as REAL) + cast(5 as INTEGER)", "REAL NOT NULL");
        checkExpType("cast(null as REAL) + cast(5 as DOUBLE)", "DOUBLE");
        checkExpType("cast(null as REAL) + cast(5 as REAL)", "REAL");

        checkExpType("cast(1 as DECIMAL(5, 2)) + cast(1 as REAL)", "DOUBLE NOT NULL");
        checkExpType("cast(1 as DECIMAL(5, 2)) + cast(1 as DOUBLE)", "DOUBLE NOT NULL");
        checkExpType("cast(null as DECIMAL(5, 2)) + cast(1 as DOUBLE)", "DOUBLE");

        checkExpType("1.543 + 2.34", "DECIMAL(5, 3) NOT NULL");
        checkExpType("cast(1 as DECIMAL(5, 2)) + cast(1 as BIGINT)", "DECIMAL(19, 2) NOT NULL");
        checkExpType("cast(1 as NUMERIC(5, 2)) + cast(1 as INTEGER)", "DECIMAL(13, 2) NOT NULL");
        checkExpType("cast(1 as DECIMAL(5, 2)) + cast(null as SMALLINT)", "DECIMAL(8, 2)");
        checkExpType("cast(1 as DECIMAL(5, 2)) + cast(1 as TINYINT)", "DECIMAL(6, 2) NOT NULL");

        checkExpType("cast(1 as DECIMAL(5, 2)) + cast(1 as DECIMAL(5, 2))", "DECIMAL(6, 2) NOT NULL");
        checkExpType("cast(1 as DECIMAL(5, 2)) + cast(1 as DECIMAL(6, 2))", "DECIMAL(7, 2) NOT NULL");
        checkExpType("cast(1 as DECIMAL(4, 2)) + cast(1 as DECIMAL(6, 4))", "DECIMAL(7, 4) NOT NULL");
        checkExpType("cast(null as DECIMAL(4, 2)) + cast(1 as DECIMAL(6, 4))", "DECIMAL(7, 4)");
        checkExpType("cast(1 as DECIMAL(19, 2)) + cast(1 as DECIMAL(19, 2))", "DECIMAL(19, 2) NOT NULL");

        // substraction operator
        checkExpType("cast(1 as TINYINT) - cast(5 as BIGINT)", "BIGINT NOT NULL");
        checkExpType("cast(null as INTEGER) - cast(5 as SMALLINT)", "INTEGER");
        checkExpType("cast(1 as INTEGER) - cast(5 as REAL)", "REAL NOT NULL");
        checkExpType("cast(null as REAL) - cast(5 as DOUBLE)", "DOUBLE");
        checkExpType("cast(null as REAL) - cast(5 as REAL)", "REAL");

        checkExpType("cast(1 as DECIMAL(5, 2)) - cast(1 as DOUBLE)", "DOUBLE NOT NULL");
        checkExpType("cast(null as DOUBLE) - cast(1 as DECIMAL)", "DOUBLE");

        checkExpType("1.543 - 24", "DECIMAL(14, 3) NOT NULL");
        checkExpType("cast(1 as DECIMAL(5)) - cast(1 as BIGINT)", "DECIMAL(19, 0) NOT NULL");
        checkExpType("cast(1 as DECIMAL(5, 2)) - cast(1 as INTEGER)", "DECIMAL(13, 2) NOT NULL");
        checkExpType("cast(1 as DECIMAL(5, 2)) - cast(null as SMALLINT)", "DECIMAL(8, 2)");
        checkExpType("cast(1 as DECIMAL(5, 2)) - cast(1 as TINYINT)", "DECIMAL(6, 2) NOT NULL");

        checkExpType("cast(1 as DECIMAL(5, 2)) - cast(1 as DECIMAL(7))", "DECIMAL(10, 2) NOT NULL");
        checkExpType("cast(1 as DECIMAL(5, 2)) - cast(1 as DECIMAL(6, 2))", "DECIMAL(7, 2) NOT NULL");
        checkExpType("cast(1 as DECIMAL(4, 2)) - cast(1 as DECIMAL(6, 4))", "DECIMAL(7, 4) NOT NULL");
        checkExpType("cast(null as DECIMAL) - cast(1 as DECIMAL(6, 4))", "DECIMAL(19, 4)");
        checkExpType("cast(1 as DECIMAL(19, 2)) - cast(1 as DECIMAL(19, 2))", "DECIMAL(19, 2) NOT NULL");

        // multiply operator
        checkExpType("cast(1 as TINYINT) * cast(5 as INTEGER)", "INTEGER NOT NULL");
        checkExpType("cast(null as SMALLINT) * cast(5 as BIGINT)", "BIGINT");
        checkExpType("cast(1 as REAL) * cast(5 as INTEGER)", "REAL NOT NULL");
        checkExpType("cast(null as REAL) * cast(5 as DOUBLE)", "DOUBLE");

        checkExpType("cast(1 as DECIMAL(7, 3)) * 1.654", "DECIMAL(11, 6) NOT NULL");
        checkExpType("cast(null as DECIMAL(7, 3)) * cast (1.654 as DOUBLE)", "DOUBLE");

        checkExpType("cast(null as DECIMAL(5, 2)) * cast(1 as BIGINT)", "DECIMAL(19, 2)");
        checkExpType("cast(1 as DECIMAL(5, 2)) * cast(1 as INTEGER)", "DECIMAL(15, 2) NOT NULL");
        checkExpType("cast(1 as DECIMAL(5, 2)) * cast(1 as SMALLINT)", "DECIMAL(10, 2) NOT NULL");
        checkExpType("cast(1 as DECIMAL(5, 2)) * cast(1 as TINYINT)", "DECIMAL(8, 2) NOT NULL");

        checkExpType("cast(1 as DECIMAL(5, 2)) * cast(1 as DECIMAL(5, 2))", "DECIMAL(10, 4) NOT NULL");
        checkExpType("cast(1 as DECIMAL(5, 2)) * cast(1 as DECIMAL(6, 2))", "DECIMAL(11, 4) NOT NULL");
        checkExpType("cast(1 as DECIMAL(4, 2)) * cast(1 as DECIMAL(6, 4))", "DECIMAL(10, 6) NOT NULL");
        checkExpType("cast(null as DECIMAL(4, 2)) * cast(1 as DECIMAL(6, 4))", "DECIMAL(10, 6)");
        checkExpType("cast(1 as DECIMAL(4, 10)) * cast(null as DECIMAL(6, 10))", "DECIMAL(10, 19)");
        checkExpType("cast(1 as DECIMAL(19, 2)) * cast(1 as DECIMAL(19, 2))", "DECIMAL(19, 4) NOT NULL");

        // divide operator
        checkExpType("cast(1 as TINYINT) / cast(5 as INTEGER)", "INTEGER NOT NULL");
        checkExpType("cast(null as SMALLINT) / cast(5 as BIGINT)", "BIGINT");
        checkExpType("cast(1 as REAL) / cast(5 as INTEGER)", "REAL NOT NULL");
        checkExpType("cast(null as REAL) / cast(5 as DOUBLE)", "DOUBLE");
        checkExpType("cast(1 as DECIMAL(7, 3)) / 1.654", "DECIMAL(15, 8) NOT NULL");
        checkExpType("cast(null as DECIMAL(7, 3)) / cast (1.654 as DOUBLE)", "DOUBLE");

        checkExpType("cast(null as DECIMAL(5, 2)) / cast(1 as BIGINT)", "DECIMAL(19, 16)");
        checkExpType("cast(1 as DECIMAL(5, 2)) / cast(1 as INTEGER)", "DECIMAL(16, 13) NOT NULL");
        checkExpType("cast(1 as DECIMAL(5, 2)) / cast(1 as SMALLINT)", "DECIMAL(11, 8) NOT NULL");
        checkExpType("cast(1 as DECIMAL(5, 2)) / cast(1 as TINYINT)", "DECIMAL(9, 6) NOT NULL");

        checkExpType("cast(1 as DECIMAL(5, 2)) / cast(1 as DECIMAL(5, 2))", "DECIMAL(13, 8) NOT NULL");
        checkExpType("cast(1 as DECIMAL(5, 2)) / cast(1 as DECIMAL(6, 2))", "DECIMAL(14, 9) NOT NULL");
        checkExpType("cast(1 as DECIMAL(4, 2)) / cast(1 as DECIMAL(6, 4))", "DECIMAL(15, 9) NOT NULL");
        checkExpType("cast(null as DECIMAL(4, 2)) / cast(1 as DECIMAL(6, 4))", "DECIMAL(15, 9)");
        checkExpType("cast(1 as DECIMAL(4, 10)) / cast(null as DECIMAL(6, 19))", "DECIMAL(19, 6)");
        checkExpType("cast(1 as DECIMAL(19, 2)) / cast(1 as DECIMAL(19, 2))", "DECIMAL(19, 0) NOT NULL");
    }

    protected void checkWin(String sql, String expectedMsgPattern)
    {
        logger.info(sql);
        checkFails(sql, expectedMsgPattern);
    }

    public void checkWinClauseExp(String sql, String expectedMsgPattern)
    {
        sql = "select * from emp " + sql;
        checkWin(sql, expectedMsgPattern);
    }

    public void checkWinFuncExpWithWinClause(String sql, String expectedMsgPattern)
    {
        sql = "select " + sql + " from emp window w as (order by deptno)";
        checkWin(sql, expectedMsgPattern);
    }

    public void checkWinFuncExp(String sql, String expectedMsgPattern)
    {
        sql = "select " + sql + " from emp";
        checkWin(sql, expectedMsgPattern);
    }

    // test window partition clause. See SQL 2003 specification for detail
    public void _testWinPartClause()
    {
        checkWinClauseExp(
            "window w as (w2 order by deptno), w2 as (^rang^e 100 preceding)",
            "Referenced window cannot have framing declarations");
        // Test specified collation, window clause syntax rule 4,5.
    }

    public void testWindowFunctions()
    {
        // SQL 03 Section 6.10

        // Window functions may only appear in the <select list> of a
        // <query specification> or <select statement: single row>,
        // or the <order by clause> of a simple table query.
        // See 4.15.3 for detail
        // todo: test case for rule 1

        // rule 3, a)
        checkWin("select sal from emp order by sum(sal) over (partition by deptno order by deptno)",null);

        // scope reference

        // rule 4,
        // valid window functions
        checkWinFuncExpWithWinClause("sum(sal)", null);

        // row_number function
        checkWinFuncExpWithWinClause("row_number() over (order by deptno)", null);

        // rank function type
        checkWinFuncExpWithWinClause("dense_rank()", null);
        checkWinFuncExpWithWinClause("rank() over (order by empno)", null);
        checkWinFuncExpWithWinClause("percent_rank() over (order by empno)", null);
        checkWinFuncExpWithWinClause("cume_dist() over (order by empno)", null);

        // rule 6a
        // ORDER BY required with RANK & DENSE_RANK
        checkWin("select rank() over ^(partition by deptno)^ from emp",
            "RANK or DENSE_RANK functions require ORDER BY clause in window specification");
        checkWin("select dense_rank() over ^(partition by deptno)^ from emp ",
            "RANK or DENSE_RANK functions require ORDER BY clause in window specification");
        // The following fail but it is reported as window needing OBC due to test sequence so
        // not really failing due to 6a
        //checkWin("select rank() over w from emp window w as ^(partition by deptno)^",
        //    "RANK or DENSE_RANK functions require ORDER BY clause in window specification");
        //checkWin("select dense_rank() over w from emp window w as ^(partition by deptno)^",
        //    "RANK or DENSE_RANK functions require ORDER BY clause in window specification");

        // rule 6b
        // Framing not allowed with RANK & DENSE_RANK functions
        // window framing defined in window clause
        checkWin("select rank() over w from emp window w as (order by empno ^rows^ 2 preceding )",
            "ROW/RANGE not allowed with RANK or DENSE_RANK functions");
        checkWin("select dense_rank() over w from emp window w as (order by empno ^rows^ 2 preceding)",
            "ROW/RANGE not allowed with RANK or DENSE_RANK functions");
        checkWin("select percent_rank() over w from emp window w as (rows 2 preceding )", null);
        checkWin("select cume_dist() over w from emp window w as (rows 2 preceding)", null);
        // window framing defined in in-line window
        checkWin("select rank() over (order by empno ^range^ 2 preceding ) from emp ",
            "ROW/RANGE not allowed with RANK or DENSE_RANK functions");
        checkWin("select dense_rank() over (order by empno ^rows^ 2 preceding ) from emp ",
            "ROW/RANGE not allowed with RANK or DENSE_RANK functions");
        checkWin("select percent_rank() over (rows 2 preceding ) from emp", null);
        checkWin("select cume_dist() over (rows 2 preceding ) from emp ", null);

        // invalid column reference
        checkWinFuncExpWithWinClause("sum(^invalidColumn^)",
            "Unknown identifier \'INVALIDCOLUMN\'");

        // invalid window functions
        checkWinFuncExpWithWinClause("^invalidFun(sal)^",
            "No match found for function signature INVALIDFUN\\(<NUMERIC>\\)");

        // 6.10 rule 10. no distinct allowed aggreagate function
        // Fails in parser.
        // checkWinFuncExpWithWinClause(" sum(distinct sal) over w ", null);

        // 7.11 rule 10c
        checkWin("select sum(sal) over (w partition by ^deptno^)"+NL+
            " from emp window w as (order by empno rows 2 preceding )",
            "PARTITION BY not allowed with existing window reference");
        // 7.11 rule 10d
        checkWin("select sum(sal) over (w order by ^empno^)"+NL+
            " from emp window w as (order by empno rows 2 preceding )",
            "ORDER BY not allowed in both base and referenced windows");
        // 7.11 rule 10e
        checkWin("select sum(sal) over (w) "+NL+
            " from emp window w as (order by empno ^rows^ 2 preceding )",
            "Referenced window cannot have framing declarations");
    }

    public void testInlineWinDef()
    {
        // the <window specification> used by windowed agg functions is
        // fully defined in SQL 03 Std. section 7.1 <window clause>
        check("select sum(sal) over (partition by deptno order by empno) from emp order by empno");
        checkWinFuncExp("sum(sal) OVER (" +
            "partition by deptno " +
            "order by empno " +
            "rows 2 preceding )", null);
        checkWinFuncExp("sum(sal) OVER (" +
            "order by 1 " +
            "rows 2 preceding )", null);
        checkWinFuncExp("sum(sal) OVER (" +
            "order by 'b' " +
            "rows 2 preceding )", null);
        checkWinFuncExp("sum(sal) over ("+
            "partition by deptno "+
            "order by 1+1 rows 26 preceding)",null);
        checkWinFuncExp("sum(sal) over (order by deptno rows unbounded preceding)",null);
        checkWinFuncExp("sum(sal) over (order by deptno rows current row)",null);
        checkWinFuncExp("sum(sal) over ("+
            "order by deptno "+
            "rows between unbounded preceding and unbounded following)",null);
        checkWinFuncExp("sum(sal) over ("+
            "order by deptno "+
            "rows between CURRENT ROW and unbounded following)",null);
        checkWinFuncExp("sum(sal) over ("+
            "order by deptno "+
            "rows between unbounded preceding and CURRENT ROW)",null);
        checkWinFuncExpWithWinClause("sum(sal) OVER (w " +
            "rows 2 preceding )", null) ;
        checkWinFuncExp("sum(sal) over (order by deptno range 2.0 preceding)",null);

        // Failure mode tests
        checkWinFuncExp("sum(sal) over (order by deptno "+
            "rows between ^UNBOUNDED FOLLOWING^ and unbounded preceding)",
            "UNBOUNDED FOLLOWING cannot be specified for the lower frame boundary");
        checkWinFuncExp("sum(sal) over ("+
            "order by deptno "+
            "rows between 2 preceding and ^UNBOUNDED PRECEDING^)",
            "UNBOUNDED PRECEDING cannot be specified for the upper frame boundary");
        checkWinFuncExp("sum(sal) over ("+
            "order by deptno "+
            "rows between CURRENT ROW and ^2 preceding^)",
            "Upper frame boundary cannot be PRECEDING when lower boundary is CURRENT ROW");
        checkWinFuncExp("sum(sal) over ("+
            "order by deptno "+
            "rows between 2 following and ^CURRENT ROW^)",
            "Upper frame boundary cannot be CURRENT ROW when lower boundary is FOLLOWING");
        checkWinFuncExp("sum(sal) over ("+
            "order by deptno "+
            "rows between 2 following and ^2 preceding^)",
            "Upper frame boundary cannot be PRECEDING when lower boundary is FOLLOWING");
        checkWinFuncExp("sum(sal) over ("+
            "order by deptno "+
            "RANGE BETWEEN INTERVAL '1' ^SECOND^ PRECEDING AND INTERVAL '1' SECOND FOLLOWING)",
            "Data Type mismatch between ORDER BY and RANGE clause");
        checkWinFuncExp("sum(sal) over ("+
            "order by empno "+
            "RANGE BETWEEN INTERVAL '1' ^SECOND^ PRECEDING AND INTERVAL '1' SECOND FOLLOWING)",
            "Data Type mismatch between ORDER BY and RANGE clause");
        checkWinFuncExp("sum(sal) over (order by deptno, empno ^range^ 2 preceding)",
            "RANGE clause cannot be used with compound ORDER BY clause");
        checkWinFuncExp("sum(sal) over ^(partition by deptno range 5 preceding)^",
            "Window specification must contain an ORDER BY clause");
        checkWinFuncExp("sum(sal) over ^w1^",
            "Window 'W1' not found");
        checkWinFuncExp("sum(sal) OVER (^w1^ " +
            "partition by deptno " +
            "order by empno " +
            "rows 2 preceding )",
            "Window 'W1' not found") ;
    }

    public void testWindowClause()
    {
        // -----------------------------------
        // --   positive testings           --
        // -----------------------------------
        // correct syntax:
        checkWinFuncExpWithWinClause("sum(sal) as sumsal",null);
        checkWinClauseExp("window w as (partition by sal order by deptno rows 2 preceding)", null);
        // define window on an existing window
        checkWinClauseExp("window w as (order by sal), w1 as (w)", null);

        // -----------------------------------
        // --   negative testings           --
        // -----------------------------------
        // Test fails in parser
        //checkWinClauseExp("window foo.w as (range 100 preceding) "+
        //    "Window name must be a simple identifier\");

        // rule 11
        // a)
        // missing window order clause.
        checkWinClauseExp("window w as (range 100 preceding)",
            "Window specification must contain an ORDER BY clause");
        // order by number
        checkWinClauseExp("window w as (order by sal range 100 preceding)", null);
        // order by date
        checkWinClauseExp("window w as (order by hiredate range 100 preceding)",
            "Data Type mismatch between ORDER BY and RANGE clause");
        // order by string, should fail
        checkWinClauseExp("window w as (order by ename range 100 preceding)",
            "Data type of ORDER BY prohibits use of RANGE clause");
        // todo: interval test ???


        // b)
        // valid
        checkWinClauseExp("window w as (rows 2 preceding)", null);

        // invalid tests
        // exact numeric for the unsigned value specification
        // The followoing two test fail as they should but in the parser
        //checkWinClauseExp("window w as (rows -2.5 preceding)", null);
        //checkWinClauseExp("window w as (rows -2 preceding)", null);

        // This test should fail as per 03 Std. but we pass it and plan
        // to apply the FLOOR function before window processing
        checkWinClauseExp("window w as (rows 2.5 preceding)", null);

        // -----------------------------------
        // --   negative testings           --
        // -----------------------------------
        // reference undefined xyz column
        checkWinClauseExp("window w as (partition by ^xyz^)",
            "Column 'XYZ' not found in any table");

        // window defintion is empty when applied to unsorted table
        checkWinClauseExp("window w as ^( /* boo! */  )^",
            "Window specification must contain an ORDER BY clause");

        // duplidate window name
        checkWinClauseExp("window w as (order by empno), ^w^ as (order by empno)",
            "Duplicate window names not allowed");
        checkWinClauseExp("window win1 as (order by empno), ^win1^ as (order by empno)",
            "Duplicate window names not allowed");

        // syntax rule 6
        checkFails("select min(sal) over (order by deptno) from emp group by deptno,sal", null);
        checkFails("select min(sal) over (order by ^deptno^) from emp group by sal",
            "Expression 'DEPTNO' is not being grouped");
        checkFails("select min(sal) over "+NL+
            "(partition by comm order by deptno) from emp group by deptno,sal,comm",null);
        checkFails("select min(sal) over "+NL+
            "(partition by ^comm^ order by deptno) from emp group by deptno,sal",
            "Expression 'COMM' is not being grouped");

        // syntax rule 7
        checkWinClauseExp("window w as (order by rank() over (order by sal))",null);

        // ------------------------------------
        // ---- window frame between tests ----
        // ------------------------------------
        // bound 1 shall not specify UNBOUNDED FOLLOWING
        checkWinClauseExp("window w as (rows between ^unbounded following^ and 5 following)",
            "UNBOUNDED FOLLOWING cannot be specified for the lower frame boundary");

        // bound 2 shall not specify UNBOUNDED PRECEDING
        checkWinClauseExp("window w as ("+
            "order by deptno "+
            "rows between 2 preceding and ^UNBOUNDED PRECEDING^)",
            "UNBOUNDED PRECEDING cannot be specified for the upper frame boundary");
        checkWinClauseExp("window w as ("+
            "order by deptno "+
            "rows between 2 following and ^2 preceding^)",
            "Upper frame boundary cannot be PRECEDING when lower boundary is FOLLOWING");
        checkWinClauseExp("window w as ("+
            "order by deptno "+
            "rows between CURRENT ROW and ^2 preceding^)",
            "Upper frame boundary cannot be PRECEDING when lower boundary is CURRENT ROW");
        checkWinClauseExp("window w as ("+
            "order by deptno "+
            "rows between 2 following and ^CURRENT ROW^)",
            "Upper frame boundary cannot be CURRENT ROW when lower boundary is FOLLOWING");

        // Sql '03 rule 10 c)
        // assertExceptionIsThrown("select deptno as d, sal as s from emp window w as (partition by deptno order by sal), w2 as (w partition by deptno)", null);
        // checkWinClauseExp("window w as (partition by sal order by deptno), w2 as (w partition by sal)", null);
        // d)
        // valid because existing window does not have an ORDER BY clause
        checkWinClauseExp("window w as (w2 range 2 preceding ), w2 as (order by sal)", null);
        checkWinClauseExp("window w as ^(partition by sal)^, w2 as (w order by deptno)",
            "Window specification must contain an ORDER BY clause");
        checkWinClauseExp("window w as (w2 partition by ^sal^), w2 as (order by deptno)",
            "PARTITION BY not allowed with existing window reference");
        checkWinClauseExp("window w as (partition by sal order by deptno), w2 as (w order by ^deptno^)",
            "ORDER BY not allowed in both base and referenced windows");
        // e)
        checkWinClauseExp("window w as (w2 order by deptno), w2 as (^range^ 100 preceding)",
            "Referenced window cannot have framing declarations");


        // rule 12, todo: test scope of window
        // assertExceptionIsThrown("select deptno as d from emp window d as (partition by deptno)", null);

        // rule 13
        checkWinClauseExp("window w as (order by sal)",null);
        checkWinClauseExp("window w as (order by ^non_exist_col^)",
            "Column 'NON_EXIST_COL' not found in any table");
        checkWinClauseExp("window w as (partition by ^non_exist_col^ order by sal)",
            "Column 'NON_EXIST_COL' not found in any table");
    }

    public void testWindowClause2()
    {
        // 7.10 syntax rule 2
        //<new window name> NWN1 shall not be contained in the scope of another <new window name> NWN2
        //such that NWN1 and NWN2 are equivalent.
        checkWinClauseExp("window "+NL+
            "w  as (partition by deptno order by empno rows 2 preceding), "+NL+
            "w2 as (partition by deptno order by empno rows 2 preceding)"+NL,
            "Duplicate window specification not allowed in the same window clause");
    }

    public void testOneWinFunc()
    {
        checkWinClauseExp("window w as (partition by sal order by deptno rows 2 preceding)", null);
    }

    public void testNameResolutionInValuesClause()
    {
        final String emps = "(select 1 as empno, 'x' as name, 10 as deptno, 'M' as gender, 'San Francisco' as city, 30 as empid, 25 as age from (values (1)))";
        final String depts = "(select 10 as deptno, 'Sales' as name from (values (1)))";

        checkFails("select * from " + emps + " join " + depts + NL +
            " on ^emps^.deptno = deptno",
            "Table 'EMPS' not found");
        // this is ok
        check("select * from " + emps + " as e" + NL +
            " join " + depts + " as d" + NL +
            " on e.deptno = d.deptno");
        // fail: ambiguous column in WHERE
        checkFails("select * from " + emps + " as emps," + NL +
            " " + depts + NL +
            "where ^deptno^ > 5",
            "Column 'DEPTNO' is ambiguous");
        // fail: ambiguous column reference in ON clause
        checkFails("select * from " + emps + " as e" + NL +
            " join " + depts + " as d" + NL +
            " on e.deptno = ^deptno^",
            "Column 'DEPTNO' is ambiguous");
        // ok: column 'age' is unambiguous
        check("select * from " + emps + " as e" + NL +
            " join " + depts + " as d" + NL +
            " on e.deptno = age");
        // ok: reference to derived column
        check("select * from " + depts + NL +
            " join (select mod(age, 30) as agemod from " + emps + ") " + NL +
            "on deptno = agemod");
        // fail: deptno is ambiguous
        checkFails("select name from " + depts + " " + NL +
            "join (select mod(age, 30) as agemod, deptno from " + emps + ") " + NL +
            "on ^deptno^ = agemod",
            "Column 'DEPTNO' is ambiguous");
        // fail: lateral reference
        checkFails("select * from " + emps + " as e," + NL +
            " (select 1, ^e^.deptno from (values(true))) as d",
            "Unknown identifier 'E'");
    }

    public void testNestedFrom()
    {
        checkColumnType("values (true)", "BOOLEAN NOT NULL");
        checkColumnType("select * from (values(true))", "BOOLEAN NOT NULL");
        checkColumnType("select * from (select * from (values(true)))", "BOOLEAN NOT NULL");
        checkColumnType("select * from (select * from (select * from (values(true))))", "BOOLEAN NOT NULL");
        checkColumnType(
            "select * from (" +
            "  select * from (" +
            "    select * from (values(true))" +
            "    union" +
            "    select * from (values (false)))" +
            "  except" +
            "  select * from (values(true)))", "BOOLEAN NOT NULL");
    }

    public void testAmbiguousColumn()
    {
        checkFails("select * from emp join dept" + NL +
            " on emp.deptno = ^deptno^",
            "Column 'DEPTNO' is ambiguous");
        // this is ok
        check("select * from emp as e" + NL +
            " join dept as d" + NL +
            " on e.deptno = d.deptno");
        // fail: ambiguous column in WHERE
        checkFails("select * from emp as emps, dept" + NL +
            "where ^deptno^ > 5",
            "Column 'DEPTNO' is ambiguous");
        // fail: alias 'd' obscures original table name 'dept'
        checkFails("select * from emp as emps, dept as d" + NL +
            "where ^dept^.deptno > 5",
            "Unknown identifier 'DEPT'");
        // fail: ambiguous column reference in ON clause
        checkFails("select * from emp as e" + NL +
            " join dept as d" + NL +
            " on e.deptno = ^deptno^",
            "Column 'DEPTNO' is ambiguous");
        // ok: column 'comm' is unambiguous
        check("select * from emp as e" + NL +
            " join dept as d" + NL +
            " on e.deptno = comm");
        // ok: reference to derived column
        check("select * from dept" + NL +
            " join (select mod(comm, 30) as commmod from emp) " + NL +
            "on deptno = commmod");
        // fail: deptno is ambiguous
        checkFails("select name from dept " + NL +
            "join (select mod(comm, 30) as commmod, deptno from emp) " + NL +
            "on ^deptno^ = commmod",
            "Column 'DEPTNO' is ambiguous");
        // fail: lateral reference
        checkFails("select * from emp as e," + NL +
            " (select 1, ^e^.deptno from (values(true))) as d",
            "Unknown identifier 'E'");
    }

    public void testExpandStar()
    {
        // dtbug 282
        // "select r.* from sales.depts" gives NPE.
        checkFails("select ^r.*^ from dept",
            "Unknown identifier 'R'");

        check("select e.* from emp as e");
        check("select emp.* from emp");

        // Error message could be better (EMPNO does exist, but it's a column).
        checkFails("select ^empno .  *^ from emp",
            "Unknown identifier 'EMPNO'");
    }

    // todo: implement IN
    public void _testAmbiguousColumnInIn()
    {
        // ok: cyclic reference
        check("select * from emp as e" + NL +
            "where e.deptno in (" + NL +
            "  select 1 from (values(true)) where e.empno > 10)");
        // ok: cyclic reference
        check("select * from emp as e" + NL +
            "where e.deptno in (" + NL +
            "  select e.deptno from (values(true)))");
    }

    private final String ERR_IN_VALUES_INCOMPATIBLE =
        "Values in expression list must have compatible types";
        
    private final String ERR_IN_OPERANDS_INCOMPATIBLE =
            "Values passed to IN operator must have compatible types";
        
    public void testInList()
    {
        check("select * from emp where empno in (10,20)");
        // "select * from emp where empno in ()" is invalid -- see parser test
        check("select * from emp where empno in (10 + deptno, cast(null as integer))");
        checkFails("select * from emp where empno ^in (10, '20')^",
            ERR_IN_VALUES_INCOMPATIBLE);

        checkExpType("1 in (2, 3, 4)", "BOOLEAN NOT NULL");
        checkExpType("cast(null as integer) in (2, 3, 4)", "BOOLEAN");
        checkExpType("1 in (2, cast(null as integer) , 4)", "BOOLEAN");
        checkExpType("1 in (2.5, 3.14)", "BOOLEAN NOT NULL");
        checkExpType("true in (false, unknown)", "BOOLEAN");
        checkExpType("true in (false, false or unknown)", "BOOLEAN");
        checkExpType("true in (false, true)", "BOOLEAN NOT NULL");
        checkExpType("(1,2) in ((1,2), (3,4))", "BOOLEAN NOT NULL");
        checkExpType("'medium' in (cast(null as varchar(10)), 'bc')", "BOOLEAN");

        // nullability depends on nullability of both sides
        checkColumnType("select empno in (1, 2) from emp", "BOOLEAN NOT NULL");
        checkColumnType("select nullif(empno,empno) in (1, 2) from emp", "BOOLEAN");
        checkColumnType("select empno in (1, nullif(empno,empno), 2) from emp", "BOOLEAN");

        checkExpFails("1 in (2, 'c')",
            ERR_IN_VALUES_INCOMPATIBLE);
        checkExpFails("1 in ((2), (3,4))",
            ERR_IN_VALUES_INCOMPATIBLE);
        checkExpFails("false and ^1 in ('b', 'c')^",
            ERR_IN_OPERANDS_INCOMPATIBLE);
        checkExpFails("1 > 5 ^or (1, 2) in (3, 4)^",
            ERR_IN_OPERANDS_INCOMPATIBLE);
    }

    public void testInSubquery()
    {
        check("select * from emp where deptno in (select deptno from dept)");
        check(
            "select * from emp where (empno,deptno)"
            + " in (select deptno,deptno from dept)");
        
        checkFails(
            "select * from emp where deptno in "
            + "(select deptno,deptno from dept)",
            "Values passed to IN operator must have compatible types");
    }
    
    public void testDoubleNoAlias()
    {
        check("select * from emp join dept on true");
        check("select * from emp, dept");
        check("select * from emp cross join dept");
    }

    // TODO: is this legal? check that standard
    public void _testDuplicateColumnAliasFails()
    {
        checkFails("select 1 as a, 2 as b, 3 as a from emp", "xyz");
    }

    public void testInvalidGroupBy(TestCase test)
    {
        checkFails("select empno, deptno from emp group by deptno", "xyz");
    }

    public void testSingleNoAlias()
    {
        check("select * from emp");
    }

    public void testObscuredAliasFails()
    {
        // It is an error to refer to a table which has been given another
        // alias.
        checkFails("select * from emp as e where exists (" + NL +
            "  select 1 from dept where dept.deptno = ^emp^.deptno)",
            "Table 'EMP' not found");
    }

    public void testFromReferenceFails()
    {
        // You cannot refer to a table ('e2') in the parent scope of a query in
        // the from clause.
        checkFails("select * from emp as e1 where exists (" + NL
            + "  select * from emp as e2, " + NL
            + "    (select * from dept where dept.deptno = ^e2^.deptno))",
            "Table 'E2' not found");
    }

    public void testWhereReference()
    {
        // You can refer to a table ('e1') in the parent scope of a query in
        // the from clause.
        //
        // Note: Oracle10g does not allow this query.
        check("select * from emp as e1 where exists (" + NL
            + "  select * from emp as e2, " + NL
            + "    (select * from dept where dept.deptno = e1.deptno))");
    }

    public void testUnionNameResolution()
    {
        checkFails(
            "select * from emp as e1 where exists (" + NL +
            "  select * from emp as e2, " + NL +
            "  (select deptno from dept as d" + NL +
            "   union" + NL +
            "   select deptno from emp as e3 where deptno = ^e2^.deptno))",
            "Table 'E2' not found");

        checkFails("select * from emp" + NL +
            "union" + NL +
            "select * from dept where ^empno^ < 10",
            "Unknown identifier 'EMPNO'");
    }

    public void testUnionCountMismatchFails()
    {
        checkFails("select 1,2 from emp" + NL +
            "union" + NL +
            "select ^3^ from dept",
            "Column count mismatch in UNION");
    }

    public void testUnionTypeMismatchFails()
    {
        checkFails("select 1, ^2^ from emp union select deptno, name from dept",
            "Type mismatch in column 2 of UNION");
    }

    public void testUnionTypeMismatchWithStarFails()
    {
        checkFails("select ^*^ from dept union select 1, 2 from emp",
            "Type mismatch in column 2 of UNION");

        checkFails("select ^dept.*^ from dept union select 1, 2 from emp",
            "Type mismatch in column 2 of UNION");
    }

    public void testUnionTypeMismatchWithValuesFails()
    {
        checkFails("values (1, ^2^, 3), (3, 4, 5), (6, 7, 8) union " + NL +
            "select deptno, name, deptno from dept",
            "Type mismatch in column 2 of UNION");
    }

    public void testNaturalCrossJoinFails()
    {
        checkFails("select * from emp natural cross ^join^ dept",
            "Cannot specify condition \\(NATURAL keyword, or ON or USING clause\\) following CROSS JOIN");
    }

    public void testCrossJoinUsingFails()
    {
        checkFails("select * from emp cross join dept ^using (deptno)^",
            "Cannot specify condition \\(NATURAL keyword, or ON or USING clause\\) following CROSS JOIN");
    }

    public void testJoinUsing()
    {
        check("select * from emp join dept using (deptno)");
        // fail: comm exists on one side not the other
        // todo: The error message could be improved.
        checkFails("select * from emp join dept using (deptno, ^comm^)",
            "Column 'COMM' not found in any table");
        // ok to repeat (ok in Oracle10g too)
        check("select * from emp join dept using (deptno, deptno)");
        // inherited column, not found in either side of the join, in the
        // USING clause
        checkFails("select * from dept where exists (" + NL +
            "select 1 from emp join bonus using (^dname^))",
            "Column 'DNAME' not found in any table");
        // inherited column, found in only one side of the join, in the
        // USING clause
        checkFails("select * from dept where exists (" + NL +
            "select 1 from emp join bonus using (^deptno^))",
            "Column 'DEPTNO' not found in any table");
    }

    public void testCrossJoinOnFails()
    {
        checkFails("select * from emp cross join dept" + NL +
            " ^on emp.deptno = dept.deptno^",
            "Cannot specify condition \\(NATURAL keyword, or ON or USING clause\\) following CROSS JOIN");
    }

    public void testInnerJoinWithoutUsingOrOnFails()
    {
        checkFails("select * from emp inner ^join^ dept "+ NL +
            "where emp.deptno = dept.deptno",
            "INNER, LEFT, RIGHT or FULL join requires a condition \\(NATURAL keyword or ON or USING clause\\)");
    }

    public void testJoinUsingInvalidColsFails()
    {
        // todo: Improve error msg
        checkFails("select * from emp left join dept using (^gender^)",
            "Column 'GENDER' not found in any table");
    }

    // todo: Cannot handle '(a join b)' yet -- we see the '(' and expect to
    // see 'select'.
    public void _testJoinUsing()
    {
        check("select * from (emp join bonus using (job))" + NL +
            "join dept using (deptno)");
        // cannot alias a JOIN (actually this is a parser error, but who's
        // counting?)
        checkFails("select * from (emp join bonus using (job)) as x" + NL +
            "join dept using (deptno)",
            "as wrong here");
        checkFails("select * from (emp join bonus using (job))" + NL +
            "join dept using (^dname^)",
            "dname not found in lhs");
        // Needs real Error Message and error marks in query
        checkFails("select * from (emp join bonus using (job))" + NL +
            "join (select 1 as job from (true)) using (job)",
            "ambig");
    }

    public void testWhere()
    {
        checkFails("select * from emp where ^sal^",
            "WHERE clause must be a condition");
    }

    public void testHaving()
    {
        checkFails("select * from emp having ^sum(sal)^",
            "HAVING clause must be a condition");
        checkFails("select ^*^ from emp having sum(sal) > 10",
            "Expression '\\*' is not being grouped");
        // agg in select and having, no group by
        check("select sum(sal + sal) from emp having sum(sal) > 10");
        checkFails("SELECT deptno FROM emp GROUP BY deptno HAVING ^sal^ > 10",
            "Expression 'SAL' is not being grouped");
    }

    public void testHavingBetween()
    {
        // FRG-115: having clause with between not working
        if (Bug.Frg115Fixed)
        check("select deptno from emp group by deptno having deptno between 10 and 12");
        // this worked even before FRG-115 was fixed
        check("select deptno from emp group by deptno having deptno + 5 > 10");
    }

    public void testOrder()
    {
        final Compatible compatible = getCompatible();
        final boolean sortByOrdinal =
            compatible == Compatible.Oracle10g ||
            compatible == Compatible.Strict92 ||
            compatible == Compatible.Pragmatic99;
        final boolean sortByAlias =
            compatible == Compatible.Default ||
            compatible == Compatible.Oracle10g ||
            compatible == Compatible.Strict92;
        final boolean sortByAliasObscures =
            compatible == Compatible.Strict92;

        check("select empno as x from emp order by empno");

        // In sql92, empno is obscured by the alias.
        // Otherwise valid.
        // Checked Oracle10G -- is it valid.
        checkFails("select empno as x from emp order by empno",
            // in sql92, empno is obscured by the alias
            sortByAliasObscures ? "unknown column empno" :
            // otherwise valid
            null);
        checkFails("select empno as x from emp order by x",
            // valid in oracle and pre-99 sql
            sortByAlias ? null :
            // invalid in sql:2003
            "column 'x' not found");

        checkFails("select empno as x from emp order by 10",
            // invalid in oracle and pre-99
            sortByOrdinal ? "offset out of range" :
            // valid from sql:99 onwards (but sorting by constant achieves
            // nothing!)
            null);

        // Has different meanings in different dialects (which makes it very
        // confusing!) but is always valid.
        check("select empno + 1 as empno from emp order by empno");

        // Always fails
        checkFails("select empno as x from emp, dept order by ^deptno^",
            "Column 'DEPTNO' is ambiguous");

        checkFails("select empno as deptno from emp, dept order by deptno",
            // Alias 'deptno' is closer in scope than 'emp.deptno'
            // and 'dept.deptno', and is therefore not ambiguous.
            // Checked Oracle10G -- it is valid.
            sortByAlias ? null :
            // Ambiguous in SQL:2003
            "col ambig");

        check(
            "select deptno from dept" + NL +
            "union" + NL +
            "select empno from emp" + NL +
            "order by deptno");

        checkFails(
            "select deptno from dept" + NL +
            "union" + NL +
            "select empno from emp" + NL +
            "order by ^empno^",
            "Column 'EMPNO' not found in any table");

        checkFails(
            "select deptno from dept" + NL +
            "union" + NL +
            "select empno from emp" + NL +
            "order by 10",
            // invalid in oracle and pre-99
            sortByOrdinal ? "offset out of range" :
            null);

        // Sort by scalar subquery
        check(
            "select * from emp " + NL +
            "order by (select name from dept where deptno = emp.deptno)");
        checkFails(
            "select * from emp " + NL +
            "order by (select name from dept where deptno = emp.foo)",
            "Column 'FOO' not found in table EMP");

        // Sort by aggregate. Oracle allows this.
        check("select 1 from emp order by sum(sal)");
    }

    public void testGroup()
    {
        checkFails("select empno from emp where ^sum(sal)^ > 50",
            "Aggregate expression is illegal in WHERE clause");

        checkFails("select ^empno^ from emp group by deptno",
            "Expression 'EMPNO' is not being grouped");

        checkFails("select ^*^ from emp group by deptno",
            "Expression '\\*' is not being grouped");

        // This query tries to reference an agg expression from within a
        // subquery as a correlating expression, but the SQL syntax rules say
        // that the agg function SUM always applies to the current scope.
        // As it happens, the query is valid.
        check("select deptno " + NL +
            "from emp " + NL +
            "group by deptno " + NL +
            "having exists (select sum(emp.sal) > 10 from (values(true)))");

        // if you reference a column from a subquery, it must be a group col
        check("select deptno " +
            "from emp " +
            "group by deptno " +
            "having exists (select 1 from (values(true)) where emp.deptno = 10)");

        // Needs proper error message text and error markers in query
        if (todo) {
            checkFails("select deptno " +
                "from emp " +
                "group by deptno " +
                "having exists (select 1 from (values(true)) where emp.empno = 10)",
                "xx");
        }
        // constant expressions
        check("select cast(1 as integer) + 2 from emp group by deptno");
        check("select localtime, deptno + 3 from emp group by deptno");
    }

    public void testGroupExpressionEquivalence()
    {
        // operator equivalence
        check("select empno + 1 from emp group by empno + 1");
        checkFails("select 1 + ^empno^ from emp group by empno + 1",
            "Expression 'EMPNO' is not being grouped");
        // datatype equivalence
        check("select cast(empno as VARCHAR(10)) from emp group by cast(empno as VARCHAR(10))");
        checkFails("select cast(^empno^ as VARCHAR(11)) from emp group by cast(empno as VARCHAR(10))",
            "Expression 'EMPNO' is not being grouped");
    }

    public void testGroupExpressionEquivalenceId()
    {
        // identifier equivalence
        check("select case empno when 10 then deptno else null end from emp " +
            "group by case empno when 10 then deptno else null end");
        // matches even when one column is qualified (checked on Oracle10.1)
        if (todo)
            check("select case empno when 10 then deptno else null end from emp " +
                "group by case empno when 10 then emp.deptno else null end");
        // note that expression appears unchanged in error msg
        checkFails("select case ^emp^.empno when 10 then deptno else null end from emp " +
            "group by case emp.empno when 10 then emp.deptno else null end",
            "Expression 'EMP.EMPNO' is not being grouped");
        checkFails("select case ^empno^ when 10 then deptno else null end from emp " +
            "group by case emp.empno when 10 then emp.deptno else null end",
            "Expression 'EMPNO' is not being grouped");

    }

    // todo: enable when correlating variables work
    public void _testGroupExpressionEquivalenceCorrelated()
    {
        // dname comes from dept, so it is constant within the subquery, and
        // is so is a valid expr in a group-by query
        check("select * from dept where exists (" +
            "select dname from emp group by empno)");
        check("select * from dept where exists (" +
            "select dname + empno + 1 from emp group by empno, dept.deptno)");
    }

    // todo: enable when params are implemented
    public void _testGroupExpressionEquivalenceParams()
    {
        check("select cast(? as integer) from emp group by cast(? as integer)");
    }

    public void testGroupExpressionEquivalenceLiteral()
    {
        // The purpose of this test is to see whether the validator
        // regards a pair of constants as equivalent. If we just used the raw
        // constants the validator wouldn't care ('SELECT 1 FROM emp GROUP BY
        // 2' is legal), so we combine a column and a constant into the same
        // CASE expression.

        // literal equivalence
        check("select case empno when 10 then date '1969-04-29' else null end from emp " +
            "group by case empno when 10 then date '1969-04-29' else null end");
        // this query succeeds in oracle 10.1 because 1 and 1.0 have the same type
        checkFails("select case ^empno^ when 10 then 1 else null end from emp " +
            "group by case empno when 10 then 1.0 else null end",
            "Expression 'EMPNO' is not being grouped");
        // 3.1415 and 3.14150 are different literals (I don't care either way)
        checkFails("select case ^empno^ when 10 then 3.1415 else null end from emp " +
            "group by case empno when 10 then 3.14150 else null end",
            "Expression 'EMPNO' is not being grouped");
        // 3 and 03 are the same literal (I don't care either way)
        check("select case empno when 10 then 03 else null end from emp " +
            "group by case empno when 10 then 3 else null end");
        checkFails("select case ^empno^ when 10 then 1 else null end from emp " +
            "group by case empno when 10 then 2 else null end",
            "Expression 'EMPNO' is not being grouped");
        check("select case empno when 10 then timestamp '1969-04-29 12:34:56.0' else null end from emp " +
            "group by case empno when 10 then timestamp '1969-04-29 12:34:56' else null end");
    }

    public void testGroupExpressionEquivalenceStringLiteral()
    {
        check("select case empno when 10 then 'foo bar' else null end from emp " +
            "group by case empno when 10 then 'foo bar' else null end");

        if (Bug.Frg78Fixed)
        check("select case empno when 10 then _iso-8859-1'foo bar' collate latin1$en$1 else null end from emp " +
            "group by case empno when 10 then _iso-8859-1'foo bar' collate latin1$en$1 else null end");

        checkFails("select case ^empno^ when 10 then _iso-8859-1'foo bar' else null end from emp " +
            "group by case empno when 10 then _iso-8859-2'foo bar' else null end",
            "Expression 'EMPNO' is not being grouped");

        if (Bug.Frg78Fixed)
        checkFails("select case ^empno^ when 10 then 'foo bar' collate latin1$en$1 else null end from emp " +
            "group by case empno when 10 then 'foo bar' collate latin1$fr$1 else null end",
            "Expression 'EMPNO' is not being grouped");
    }

    public void testCorrelatingVariables()
    {
        // reference to unqualified correlating column
        checkFails("select * from emp where exists (" + NL +
            "select * from dept where deptno = sal)",
            "Unknown identifier 'SAL'");

        // reference to qualified correlating column
        check("select * from emp where exists (" + NL +
            "select * from dept where deptno = emp.sal)");
    }

    public void testIntervalCompare()
    {
        checkExpType("interval '1' hour = interval '1' day", "BOOLEAN NOT NULL");
        checkExpType("interval '1' hour <> interval '1' hour", "BOOLEAN NOT NULL");
        checkExpType("interval '1' hour < interval '1' second", "BOOLEAN NOT NULL");
        checkExpType("interval '1' hour <= interval '1' minute", "BOOLEAN NOT NULL");
        checkExpType("interval '1' minute > interval '1' second", "BOOLEAN NOT NULL");
        checkExpType("interval '1' second >= interval '1' day", "BOOLEAN NOT NULL");
        checkExpType("interval '1' year >= interval '1' year", "BOOLEAN NOT NULL");
        checkExpType("interval '1' month = interval '1' year", "BOOLEAN NOT NULL");
        checkExpType("interval '1' month <> interval '1' month", "BOOLEAN NOT NULL");
        checkExpType("interval '1' year >= interval '1' month", "BOOLEAN NOT NULL");

        checkExpFails("interval '1' second >= interval '1' year", "(?s).*Cannot apply '>=' to arguments of type '<INTERVAL SECOND> >= <INTERVAL YEAR>'.*");
        checkExpFails("interval '1' month = interval '1' day", "(?s).*Cannot apply '=' to arguments of type '<INTERVAL MONTH> = <INTERVAL DAY>'.*");
    }

    // disabled(dtbug 334): works in farrago but not from aspen
    public void _testOverlaps()
    {
        checkExpType("(date '1-2-3', date '1-2-3') overlaps (date '1-2-3', date '1-2-3')","BOOLEAN NOT NULL");
        checkExp("(date '1-2-3', date '1-2-3') overlaps (date '1-2-3', interval '1' year)");
        checkExp("(time '1:2:3', interval '1' second) overlaps (time '23:59:59', time '1:2:3')");
        checkExp("(timestamp '1-2-3 4:5:6', timestamp '1-2-3 4:5:6' ) overlaps (timestamp '1-2-3 4:5:6', interval '1 2:3:4.5' day to second)");

        checkExpFails("(timestamp '1-2-3 4:5:6', timestamp '1-2-3 4:5:6' ) overlaps (time '4:5:6', interval '1 2:3:4.5' day to second)",
            "(?s).*Cannot apply 'OVERLAPS' to arguments of type '.<TIMESTAMP.0.>, <TIMESTAMP.0.>. OVERLAPS .<TIME.0.>, <INTERVAL DAY TO SECOND>.*");
        checkExpFails("(time '4:5:6', timestamp '1-2-3 4:5:6' ) overlaps (time '4:5:6', interval '1 2:3:4.5' day to second)",
            "(?s).*Cannot apply 'OVERLAPS' to arguments of type '.<TIME.0.>, <TIMESTAMP.0.>. OVERLAPS .<TIME.0.>, <INTERVAL DAY TO SECOND>.'.*");
        checkExpFails("(time '4:5:6', time '4:5:6' ) overlaps (time '4:5:6', date '1-2-3')",
            "(?s).*Cannot apply 'OVERLAPS' to arguments of type '.<TIME.0.>, <TIME.0.>. OVERLAPS .<TIME.0.>, <DATE>.'.*");
    }

    public void testExtract()
    {
        // The reason why extract returns double is because we can have
        // seconds fractions
        checkExpType("extract(year from interval '1-2' year to month)","DOUBLE NOT NULL");
        checkExp("extract(minute from interval '1.1' second)");

        checkExpFails("extract(minute from interval '11' month)","(?s).*Cannot apply.*");
        checkExpFails("extract(year from interval '11' second)","(?s).*Cannot apply.*");
    }

    public void testCastToInterval()
    {
        checkExpType("cast(interval '1' month as interval year)", "INTERVAL YEAR NOT NULL");
        checkExpType("cast(interval '1-1' year to month as interval month)", "INTERVAL MONTH NOT NULL");
        checkExpType("cast(interval '1:1' hour to minute as interval day)", "INTERVAL DAY NOT NULL");
        checkExpType("cast(interval '1:1' hour to minute as interval minute to second)", "INTERVAL MINUTE TO SECOND NOT NULL");

        checkExpFails("cast(interval '1:1' hour to minute as interval month)", "Cast function cannot convert value of type INTERVAL HOUR TO MINUTE to type INTERVAL MONTH");
        checkExpFails("cast(interval '1-1' year to month as interval second)", "Cast function cannot convert value of type INTERVAL YEAR TO MONTH to type INTERVAL SECOND");
    }

    public void testMinusDateOperator()
    {
        checkExpType("(CURRENT_DATE - CURRENT_DATE) HOUR", "INTERVAL HOUR NOT NULL");
        checkExpType("(CURRENT_DATE - CURRENT_DATE) YEAR TO MONTH", "INTERVAL YEAR TO MONTH NOT NULL");
        checkExpFails("(CURRENT_DATE - LOCALTIME) YEAR TO MONTH", "(?s).*Parameters must be of the same type.*");
    }

    public void testBind()
    {
        check("select * from emp where deptno = ?");
        check("select * from emp where deptno = ? and sal < 100000");
        if (todoTypeInference)
        check("select case when deptno = ? then 1 else 2 end from emp");
        if (todoTypeInference)
        check("select deptno from emp group by substring(name from ? for ?)");
        if (todoTypeInference)
        check("select deptno from emp group by case when deptno = ? then 1 else 2 end");
        check("select 1 from emp having sum(sal) < ?");
    }

    public void testUnnest()
    {
        checkColumnType("select*from unnest(multiset[1])","INTEGER NOT NULL");
        checkColumnType("select*from unnest(multiset[1, 2])","INTEGER NOT NULL");
        checkColumnType("select*from unnest(multiset[321.3, 2.33])","DECIMAL(5, 2) NOT NULL");
        checkColumnType("select*from unnest(multiset[321.3, 4.23e0])","DOUBLE NOT NULL");
        checkColumnType("select*from unnest(multiset[43.2e1, cast(null as decimal(4,2))])","DOUBLE");
        checkColumnType("select*from unnest(multiset[1, 2.3, 1])","DECIMAL(11, 1) NOT NULL");
        checkColumnType("select*from unnest(multiset['1','22','333'])","CHAR(3) NOT NULL");
        checkColumnType("select*from unnest(multiset['1','22','333','22'])","CHAR(3) NOT NULL");
        checkFails("select*from unnest(1)","(?s).*Cannot apply 'UNNEST' to arguments of type 'UNNEST.<INTEGER>.'.*");
        check("select*from unnest(multiset(select*from dept))");
    }

    public void testCorrelationJoin()
    {
        check("select *," +
            "         multiset(select * from emp where deptno=dept.deptno) " +
            "               as empset" +
            "      from dept");
        check("select*from unnest(select multiset[8] from dept)");
        check("select*from unnest(select multiset[deptno] from dept)");
    }

    public void testStructuredTypes()
    {
        checkColumnType(
            "values new address()",
            "ObjectSqlType(ADDRESS) NOT NULL");
        checkColumnType(
            "select home_address from emp_address",
            "ObjectSqlType(ADDRESS) NOT NULL");
        checkColumnType(
            "select ea.home_address.zip from emp_address ea",
            "INTEGER NOT NULL");
        checkColumnType(
            "select ea.mailing_address.city from emp_address ea",
            "VARCHAR(20) NOT NULL");
    }

    public void testLateral()
    {
        checkFails("select * from emp, (select * from dept where emp.deptno=dept.deptno)","(?s).*Unknown identifier 'EMP'.*");

        check("select * from emp, LATERAL (select * from dept where emp.deptno=dept.deptno)");
        check("select * from emp, LATERAL (select * from dept where emp.deptno=dept.deptno) as ldt");
        check("select * from emp, LATERAL (select * from dept where emp.deptno=dept.deptno) ldt");

    }

    public void testCollect()
    {
        check("select collect(deptno) from emp");
        check("select collect(multiset[3]) from emp");
        // todo. COLLECT is an aggregate function. test that validator only
        // can take set operators in its select list once aggregation support is
        // complete
    }

    public void testFusion()
    {
        checkFails("select fusion(deptno) from emp","(?s).*Cannot apply 'FUSION' to arguments of type 'FUSION.<INTEGER>.'.*");
        check("select fusion(multiset[3]) from emp");
        // todo. FUSION is an aggregate function. test that validator only
        // can take set operators in its select list once aggregation support is
        // complete
    }

    public void testCountFunction()
    {
        check("select count(*) from emp");
        check("select count(ename) from emp");
        check("select count(sal) from emp");
        check("select count(1) from emp");
        checkFails("select ^count(sal,ename)^ from emp",
            "Invalid number of arguments to function 'COUNT'. Was expecting 1 arguments");
    }

    public void testLastFunction()
    {
        check("select LAST_VALUE(sal) over (order by empno) from emp");
        check("select LAST_VALUE(ename) over (order by empno) from emp");

        check("select FIRST_VALUE(sal) over (order by empno) from emp");
        check("select FIRST_VALUE(ename) over (order by empno) from emp");
    }

    public void testMinMaxFunctions()
    {
        checkFails("SELECT MIN(^true^) from emp",
            "The MIN function does not support the BOOLEAN data type.");
        checkFails("SELECT MAX(^false^) from emp",
            "The MAX function does not support the BOOLEAN data type.");

        check("SELECT MIN(sal+deptno) FROM emp");
        check("SELECT MAX(ename) FROM emp");
        check("SELECT MIN(5.5) FROM emp");
        check("SELECT MAX(5) FROM emp");
    }

    public void testFunctionalDistinct()
    {
        check("select count(distinct sal) from emp");
        checkFails("select COALESCE(^distinct^ sal) from emp",
            "DISTINCT/ALL not allowed with COALESCE function");
    }

    public void testSelectDistinct()
    {
        check("SELECT DISTINCT deptno FROM emp");
        check("SELECT DISTINCT deptno, sal FROM emp");
        check("SELECT DISTINCT deptno FROM emp GROUP BY deptno");
        checkFails("SELECT DISTINCT ^deptno^ FROM emp GROUP BY sal",
            "Expression 'DEPTNO' is not being grouped");
        check("SELECT DISTINCT avg(sal) from emp");
        checkFails("SELECT DISTINCT ^deptno^, avg(sal) from emp",
            "Expression 'DEPTNO' is not being grouped");
        check ("SELECT DISTINCT deptno, sal from emp GROUP BY sal, deptno");
        check("SELECT deptno FROM emp GROUP BY deptno HAVING deptno > 55");
        check("SELECT DISTINCT deptno, 33 FROM emp GROUP BY deptno HAVING deptno > 55");
        checkFails("SELECT DISTINCT deptno, 33 FROM emp HAVING ^deptno^ > 55",
            "Expression 'DEPTNO' is not being grouped");
        check("SELECT DISTINCT * from emp");
        checkFails("SELECT DISTINCT ^*^ from emp GROUP BY deptno",
            "Expression '\\*' is not being grouped");
        check("SELECT DISTINCT 5, 10+5, 'string' from emp");
    }

    public void testExplicitTable()
    {
        final String empRecordType =
            "RecordType(INTEGER NOT NULL EMPNO," +
            " VARCHAR(20) NOT NULL ENAME," +
            " VARCHAR(10) NOT NULL JOB," +
            " INTEGER NOT NULL MGR," +
            " DATE NOT NULL HIREDATE," +
            " INTEGER NOT NULL SAL," +
            " INTEGER NOT NULL COMM," +
            " INTEGER NOT NULL DEPTNO) NOT NULL";
        checkResultType("select * from (table emp)", empRecordType);
        checkResultType("table emp", empRecordType);
        checkFails("table ^nonexistent^",
            "Table 'NONEXISTENT' not found");
    }

    public void testCollectionTable()
    {
        checkResultType("select * from table(ramp(3))",
            "RecordType(INTEGER NOT NULL I) NOT NULL");

        checkFails("select * from table(^ramp('3')^)",
            "Cannot apply 'RAMP' to arguments of type 'RAMP\\(<CHAR\\(1\\)>\\)'\\. Supported form\\(s\\): 'RAMP\\(<NUMERIC>\\)'");

        checkFails("select * from table(^nonExistentRamp('3')^)",
            "No match found for function signature NONEXISTENTRAMP\\(<CHARACTER>\\)");
    }

    public void testCollectionTableWithCursorParam()
    {
        checkResultType(
            "select * from table(dedup(cursor(select * from emp),'ename'))",
            "RecordType(VARCHAR(1024) NOT NULL NAME) NOT NULL");
        checkFails(
            "select * from table(dedup(cursor(select * from ^bloop^),'ename'))",
            "Table 'BLOOP' not found");
    }

    public void testScalarSubQuery()
    {
        check("SELECT  ename,(select name from dept where deptno=1) FROM emp");
        checkFails("SELECT ename,(select losal,^hisal^ from salgrade where grade=1) FROM emp",
            "Only scalar subqueries allowed in select list.");

    }

    public void testRecordType()
    {
        // Have to qualify columns with table name.
        checkFails("SELECT ^coord^.x, coord.y FROM customer.contact",
            "Table 'COORD' not found");

        checkResultType(
            "SELECT contact.coord.x, contact.coord.y FROM customer.contact",
            "RecordType(INTEGER NOT NULL X, INTEGER NOT NULL Y) NOT NULL");

        // Qualifying with schema is OK.
        if (Bug.Frg140Fixed)
        checkResultType(
            "SELECT customer.contact.coord.x, customer.contact.email, contact.coord.y FROM customer.contact",
            "RecordType(INTEGER NOT NULL X, INTEGER NOT NULL Y) NOT NULL");
    }

    public void testNew()
    {
        // (To debug invidual statements, paste them into this method.)
        //            1         2         3         4         5         6
        //   12345678901234567890123456789012345678901234567890123456789012345
    }
}

// End SqlValidatorTest.java

