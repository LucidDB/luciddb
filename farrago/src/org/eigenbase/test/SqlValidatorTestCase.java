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
import org.eigenbase.reltype.RelDataType;
import org.eigenbase.sql.*;
import org.eigenbase.sql.parser.ParseException;
import org.eigenbase.sql.parser.SqlParser;
import org.eigenbase.util.EnumeratedValues;

import java.nio.charset.Charset;
import java.util.regex.Pattern;
import java.util.regex.Matcher;

/**
 * An abstract base class for implementing tests against
 * {@link org.eigenbase.sql.SqlValidator} and derived classes.
 *
 * @author Wael Chatila
 * @since Jan 12, 2004
 * @version $Id$
 **/
public abstract class SqlValidatorTestCase extends TestCase
{
    //~ Static fields/initializers --------------------------------------------

    private static final String NL = System.getProperty("line.separator");
    private final Pattern lineColPattern =
        Pattern.compile("At line (.*), column (.*)");

    //~ Methods ---------------------------------------------------------------

    abstract public SqlValidator getValidator();

    abstract public SqlParser getParser(String sql)
        throws ParseException;

    public void check(String sql)
    {
        assertExceptionIsThrown(sql, null, -1, -1);
    }

    public void checkExp(String sql)
    {
        sql = "select " + sql + " from values(true)";
        assertExceptionIsThrown(sql, null, -1, -1);
    }

    public final void checkFails(
        String sql,
        String expected)
    {
        assertExceptionIsThrown(sql, expected, -1, -1);
    }

    /**
     * Asserts that a query throws an exception matching a given pattern.
     */
    public void checkFails(
        String sql,
        String expected,
        int line,
        int column)
    {
        assertExceptionIsThrown(sql, expected, line, column);
    }

    public final void checkExpFails(
        String sql,
        String expected)
    {
        checkExpFails(sql, expected, -1, -1);
    }

    public final void checkExpFails(String sql)
    {
        checkExpFails(sql, "(?s).*");
    }

    public void checkExpFails(
        String sql,
        String expected,
        int line,
        int column)
    {
        sql = "select " + sql + " from values(true)";
        assertExceptionIsThrown(sql, expected, line, column);
    }

    public void checkExpType(
        String sql,
        String expected)
    {
        sql = "select " + sql + " from values(true)";
        checkType(sql, expected);
    }

    public void checkType(
        String sql,
        String expected)
    {
        RelDataType actualType = getResultType(sql);
        String actual = actualType.toString();
        // REVIEW (jhyde, 2004/8/4): Why not use assertEquals
        if (!expected.equals(actual)) {
            String msg =
                NL + "Expected=" + expected + NL + "   actual=" + actual;
            fail(msg);
        }
    }

    public void checkCollation(
        String sql,
        String expectedCollationName,
        SqlCollation.Coercibility expectedCoercibility)
    {
        sql = "select " + sql + " from values(true)";
        RelDataType actualType = getResultType(sql);
        SqlCollation collation = actualType.getCollation();

        String actualName = collation.getCollationName();
        int actualCoercibility = collation.getCoercibility().getOrdinal();
        int expectedCoercibilityOrd = expectedCoercibility.getOrdinal();
        assertEquals(expectedCollationName, actualName);
        assertEquals(expectedCoercibilityOrd, actualCoercibility);
    }

    public void checkCharset(
        String sql,
        Charset expectedCharset)
    {
        sql = "select " + sql + " from values(true)";
        RelDataType actualType = getResultType(sql);
        Charset actualCharset = actualType.getCharset();

        if (!expectedCharset.equals(actualCharset)) {
            fail(NL + "Expected=" + expectedCharset.name() + NL + "  actual="
                + actualCharset.name());
        }
    }

    private RelDataType getResultType(String sql)
    {
        SqlParser parser;
        SqlValidator validator;
        SqlNode sqlNode;
        try {
            parser = getParser(sql);
            sqlNode = parser.parseQuery();
            validator = getValidator();
        } catch (ParseException ex) {
            ex.printStackTrace();
            fail("SqlValidationTest: Parse Error while parsing query=" + sql
                + "\n" + ex.getMessage());
            return null;
        } catch (Throwable e) {
            e.printStackTrace(System.err);
            fail(
                "SqlValidationTest: Failed while trying to connect or get statement");
            return null;
        }
        SqlNode n = validator.validate(sqlNode);

        RelDataType actualType;
        if (false) {
            actualType = validator.getValidatedNodeType(
                ((SqlNodeList) ((SqlCall) n).getOperands()[1]).get(0));
        } else {
            final RelDataType rowType = validator.getValidatedNodeType(n);
            actualType = rowType.getFields()[0].getType();
        }
        return actualType;
    }

    protected final void assertExceptionIsThrown(
        String sql,
        String expectedMsgPattern)
    {
        assertExceptionIsThrown(sql, expectedMsgPattern, -1, -1);
    }

    /**
     * Asserts either if a sql query is valid or not.
     * @param sql
     * @param expectedMsgPattern If this parameter is null the query must be
     *   valid for the test to pass;
     *   If this parameter is not null the query must be malformed and the msg
     *   pattern must match the the error raised for the test to pass.
     */
    protected void assertExceptionIsThrown(
        String sql,
        String expectedMsgPattern,
        int expectedLine,
        int expectedColumn)
    {
        SqlParser parser;
        SqlValidator validator;
        SqlNode sqlNode;
        try {
            parser = getParser(sql);
            sqlNode = parser.parseQuery();
            validator = getValidator();
        } catch (ParseException ex) {
            ex.printStackTrace();
            fail("SqlValidationTest: Parse Error while parsing query=" + sql
                + "\n" + ex.getMessage());
            return;
        } catch (Throwable e) {
            e.printStackTrace(System.err);
            fail(
                "SqlValidationTest: Failed while trying to connect or get statement");
            return;
        }

        Throwable actualException = null;
        int actualLine = -1;
        int actualColumn = -1;
        try {
            validator.validate(sqlNode);
        } catch (Throwable ex) {
            final String message = ex.getMessage();
            final Matcher matcher = lineColPattern.matcher(message);
            if (message != null && matcher.matches()) {
                actualException = ex.getCause();
                actualLine = Integer.parseInt(matcher.group(1));
                actualColumn = Integer.parseInt(matcher.group(2));
            } else {
                actualException = ex;
            }
        }

        if (null == expectedMsgPattern) {
            if ((null != actualException)) {
                actualException.printStackTrace();
                String actualMessage = actualException.getMessage();
                fail("SqlValidationTest: Validator threw unexpected exception" +
                    "; query [" + sql +
                    "]; exception ["+ actualMessage +
                    "]; line [" + actualLine +
                    "]; column [" + actualColumn + "]");
            }
        } else if (null != expectedMsgPattern) {
            if (null == actualException) {
                fail("SqlValidationTest: Expected validator to throw " +
                        "exception, but it did not; query [" + sql +
                        "]; expected [" + expectedMsgPattern + "]");
            } else {
                String actualMessage = actualException.getMessage();
                if (actualMessage == null ||
                        !actualMessage.matches(expectedMsgPattern)) {
                    actualException.printStackTrace();
                    fail("SqlValidationTest: Validator threw different " +
                        "exception than expected; query [" + sql +
                        "]; expected [" + expectedMsgPattern +
                        "]; actual [" + actualMessage  +
                        "]; line [" + actualLine +
                        "]; column [" + actualColumn + "]");
                } else if ((expectedLine != -1 &&
                    actualLine != expectedLine) ||
                    (expectedColumn != -1 &&
                    actualColumn != expectedColumn)) {
                    fail("SqlValidationTest: Validator threw expected " +
                        "exception [" + actualMessage +
                        "]; but at line [" + actualLine +
                        "]; column [" + actualColumn + "]");
                }
            }
		}
    }

    //-- tests -----------------------------------
    public void testMultipleSameAsPass()
    {
        checkExp("1 as again,2 as \"again\", 3 as AGAiN");
    }

    public void testMultipleDifferentAs()
    {
        check("select 1 as c1,2 as c2 from values(true)");
    }

    public void testTypeOfAs()
    {
        checkExpType("1 as c1", "INTEGER");
        checkExpType("'hej' as c1", "VARCHAR(3)");
        checkExpType("b'111' as c1", "BIT(3)");
    }

    public void testTypesLiterals()
    {
        checkExpType("'abc'", "VARCHAR(3)");
        checkExpType("n'abc'", "VARCHAR(3)");
        checkExpType("_iso_8859-2'abc'", "VARCHAR(3)");
        checkExpType("'ab '" + NL + "' cd'", "VARCHAR(6)");
        checkExpType("'ab'" + NL + "'cd'" + NL + "'ef'" + NL + "'gh'" + NL
            + "'ij'" + NL + "'kl'", "VARCHAR(12)");
        checkExpType("n'ab '" + NL + "' cd'", "VARCHAR(6)");
        checkExpType("_iso_8859-2'ab '" + NL + "' cd'", "VARCHAR(6)");

        checkExpType("x'abc'", "BIT(12)");
        checkExpType("x'abcd'", "VARBINARY(2)");
        checkExpType("x'abcd'" + NL + "'ff001122aabb'", "VARBINARY(8)");
        checkExpType("x'aaaa'" + NL + "'bbbb'" + NL + "'0000'" + NL + "'1111'",
            "VARBINARY(8)");

        checkExpType("b'1001'", "BIT(4)");
        checkExpType("b'1001'" + NL + "'0110'", "BIT(8)");
        checkExpType("B'0000'" + NL + "'0001'" + NL + "'0000'" + NL + "'1111'",
            "BIT(16)");

        checkExpType("1234567890", "INTEGER");
        checkExpType("123456.7890", "DECIMAL(10, 4)");
        checkExpType("123456.7890e3", "DOUBLE");
        checkExpType("true", "BOOLEAN");
        checkExpType("false", "BOOLEAN");
        checkExpType("unknown", "BOOLEAN");
    }

    public void testBooleans()
    {
        check("select TRUE OR unknowN from values(true)");
        check("select false AND unknown from values(true)");
        check("select not UNKNOWn from values(true)");
        check("select not true from values(true)");
        check("select not false from values(true)");
    }

    public void testAndOrIllegalTypesFails()
    {
        //TODO need col+line number
        assertExceptionIsThrown("select 'abc' AND FaLsE from values(true)",
            "(?s).*'<VARCHAR.3.> AND <BOOLEAN>'.*");

        assertExceptionIsThrown("select TRUE OR 1 from values(true)", "(?s).*");

        assertExceptionIsThrown("select unknown OR 1.0 from values(true)",
            "(?s).*");

        assertExceptionIsThrown("select true OR 1.0e4 from values(true)",
            "(?s).*");

        if (false) {
            //        todo
            assertExceptionIsThrown("select TRUE OR (TIME '12:00' AT LOCAL) from values(true)",
                                    "some error msg with line + col");
        }
    }

    public void testNotIlleagalTypeFails()
    {
        //TODO need col+line number
        assertExceptionIsThrown("select NOT 3.141 from values(true)",
            "(?s).*'NOT<DECIMAL.4, 3.>'.*");

        assertExceptionIsThrown("select NOT 'abc' from values(true)", "(?s).*");

        assertExceptionIsThrown("select NOT 1 from values(true)", "(?s).*");
    }

    public void testIs()
    {
        check("select TRUE IS FALSE FROM values(true)");
        check("select false IS NULL FROM values(true)");
        check("select UNKNOWN IS NULL FROM values(true)");
        check("select FALSE IS UNKNOWN FROM values(true)");

        check("select TRUE IS NOT FALSE FROM values(true)");
        check("select TRUE IS NOT NULL FROM values(true)");
        check("select false IS NOT NULL FROM values(true)");
        check("select UNKNOWN IS NOT NULL FROM values(true)");
        check("select FALSE IS NOT UNKNOWN FROM values(true)");

        check("select 1 IS NULL FROM values(true)");
        check("select 1.2 IS NULL FROM values(true)");
        checkExpFails("'abc' IS NOT UNKNOWN", "(?s).*Cannot apply.*");
    }

    public void testIsFails()
    {
        assertExceptionIsThrown("select 1 IS TRUE FROM values(true)",
            "(?s).*'<INTEGER> IS TRUE'.*");

        assertExceptionIsThrown("select 1.1 IS NOT FALSE FROM values(true)",
            "(?s).*");

        assertExceptionIsThrown("select 1.1e1 IS NOT FALSE FROM values(true)",
            "(?s).*Cannot apply 'IS NOT FALSE' to arguments of type '<DOUBLE> IS NOT FALSE'.*");

        assertExceptionIsThrown("select 'abc' IS NOT TRUE FROM values(true)",
            "(?s).*");
    }

    public void testScalars()
    {
        check("select 1  + 1 from values(true)");
        check("select 1  + 2.3 from values(true)");
        check("select 1.2+3 from values(true)");
        check("select 1.2+3.4 from values(true)");

        check("select 1  - 1 from values(true)");
        check("select 1  - 2.3 from values(true)");
        check("select 1.2-3 from values(true)");
        check("select 1.2-3.4 from values(true)");

        check("select 1  * 2 from values(true)");
        check("select 1.2* 3 from values(true)");
        check("select 1  * 2.3 from values(true)");
        check("select 1.2* 3.4 from values(true)");

        check("select 1  / 2 from values(true)");
        check("select 1  / 2.3 from values(true)");
        check("select 1.2/ 3 from values(true)");
        check("select 1.2/3.4 from values(true)");
    }

    public void testScalarsFails()
    {
        //TODO need col+line number
        assertExceptionIsThrown("select 1+TRUE from values(true)",
            "(?s).*Cannot apply '\\+' to arguments of type '<INTEGER> \\+ <BOOLEAN>'\\. Supported form\\(s\\):.*");
    }

    public void testNumbers()
    {
        check("select 1+-2.*-3.e-1/-4>+5 AND true from values(true)");
    }

    public void testPrefixFails()
    {
        assertExceptionIsThrown("SELECT -'abc' from values(true)",
            "(?s).*Cannot apply '-' to arguments of type '-<VARCHAR.3.>'.*");
        assertExceptionIsThrown("SELECT +'abc' from values(true)",
            "(?s).*Cannot apply '\\+' to arguments of type '\\+<VARCHAR.3.>'.*");
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
            "(?s).*Cannot apply '<>' to arguments of type '<VARCHAR.0.> <> <INTEGER>'.*");
        checkExpFails("'1'>=1",
            "(?s).*Cannot apply '>=' to arguments of type '<VARCHAR.1.> >= <INTEGER>'.*");
        checkExpFails("1<>n'abc'",
            "(?s).*Cannot apply '<>' to arguments of type '<INTEGER> <> <VARCHAR.3.>'.*");
        checkExpFails("''=.1",
            "(?s).*Cannot apply '=' to arguments of type '<VARCHAR.0.> = <DECIMAL.1..1.>'.*");
        checkExpFails("true<>1e-1",
            "(?s).*Cannot apply '<>' to arguments of type '<BOOLEAN> <> <DOUBLE>'.*");
        checkExpFails("false=''",
            "(?s).*Cannot apply '=' to arguments of type '<BOOLEAN> = <VARCHAR.0.>'.*");
        checkExpFails("b'1'=0.01",
            "(?s).*Cannot apply '=' to arguments of type '<BIT.1.> = <DECIMAL.3, 2.>'.*");
        checkExpFails("b'1'=1",
            "(?s).*Cannot apply '=' to arguments of type '<BIT.1.> = <INTEGER>'.*");
        checkExpFails("b'1'<>0.01",
            "(?s).*Cannot apply '<>' to arguments of type '<BIT.1.> <> <DECIMAL.3, 2.>'.*");
        checkExpFails("b'1'<>1",
            "(?s).*Cannot apply '<>' to arguments of type '<BIT.1.> <> <INTEGER>'.*");
    }

    public void testHexBitBinaryString()
    {
        check("select x'f'=x'abc' from values(true)");
        check("select x'f'=X'' from values(true)");
        check("select x'ff'=X'' from values(true)");
        check("select x'ff'=X'f' from values(true)");
        check("select x'ff'=b'10' from values(true)");
        check("select b'000'=X'f' from values(true)");
    }

    public void testHexBitBinaryStringFails()
    {
        assertExceptionIsThrown("select x'f'='abc' from values(true)",
            "(?s).*Cannot apply '=' to arguments of type '<BIT.4.> = <VARCHAR.3.>'.*");
        assertExceptionIsThrown("select x'ff'=88 from values(true)",
            "(?s).*Cannot apply '=' to arguments of type '<VARBINARY.1.> = <INTEGER>'.*");
        assertExceptionIsThrown("select x''<>1.1e-1 from values(true)",
            "(?s).*Cannot apply '<>' to arguments of type '<VARBINARY.0.> <> <DOUBLE>'.*");
        assertExceptionIsThrown("select b''<>1.1 from values(true)",
            "(?s).*Cannot apply '<>' to arguments of type '<BIT.0.> <> <DECIMAL.2, 1.>'.*");
    }

    public void testStringLiteral()
    {
        check("select n''=_iso_8859-1'abc' from values(true)");
        check("select N'f'<>'''' from values(true)");
    }

    public void testStringLiteralBroken()
    {
        check("select 'foo'" + NL + "'bar' from values (true)");
        checkFails("select 'foo' 'bar' from values (true)",
            "String literal continued on same line", 1, 14);
    }

    public void testArthimeticOperators()
    {
        checkExp("pow(2,3)");
        checkExp("aBs(-2.3e-2)");
        checkExp("MOD(5             ,\t\f\r\n2)");
        checkExp("ln(5.43  )");
        checkExp("log(- -.2  )");

        checkExpFails("mod(5.1, 3)", "(?s).*Cannot apply.*");
        checkExpFails("mod(2,5.1)", "(?s).*Cannot apply.*");
    }

    public void testArthimeticOperatorsTypes()
    {
        checkExpType("pow(2,3)", "DOUBLE");
        checkExpType("aBs(-2.3e-2)", "DOUBLE");
        checkExpType("aBs(5000000000)", "BIGINT");
        checkExpType("MOD(5,2)", "INTEGER");
        checkExpType("ln(5.43  )", "DOUBLE");
        checkExpType("log(- -.2  )", "DOUBLE");
    }

    public void testArthimeticOperatorsFails()
    {
        checkExpFails("pow(2,'abc')",
            "(?s).*Cannot apply 'POW' to arguments of type 'POW.<INTEGER>, <VARCHAR.3.>.*");
        checkExpFails("pow(true,1)",
            "(?s).*Cannot apply 'POW' to arguments of type 'POW.<BOOLEAN>, <INTEGER>.*");
        checkExpFails("mod(b'11001',1)",
            "(?s).*Cannot apply 'MOD' to arguments of type 'MOD.<BIT.5.>, <INTEGER>.*");
        checkExpFails("mod(1, b'11001')",
            "(?s).*Cannot apply 'MOD' to arguments of type 'MOD.<INTEGER>, <BIT.5.>.*");
        checkExpFails("abs(x'')",
            "(?s).*Cannot apply 'ABS' to arguments of type 'ABS.<VARBINARY.0.>.*");
        checkExpFails("ln(x'f')",
            "(?s).*Cannot apply 'LN' to arguments of type 'LN.<BIT.4.>.*");
        checkExpFails("log(x'f')",
            "(?s).*Cannot apply 'LOG' to arguments of type 'LOG.<BIT.4.>.*");
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
            "VARCHAR(7)");
        checkExpType("case when 2<1 then 'impossible' end", "VARCHAR(10)");
        checkExpType("case 'one' when 'two' then 2.00 when 'one' then 1 else 3 end",
            "DECIMAL(3, 2)");
        checkExpType("case 'one' when 'two' then 2 when 'one' then 1.00 else 3 end",
            "DECIMAL(3, 2)");
        checkExpType("case 1 when 1 then 'one' when 2 then null else 'more' end",
            "VARCHAR(4)");
        checkExpType("case when TRUE then 'true' else 'false' end",
            "VARCHAR(5)");
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
        checkExpFails("case 'string' when b'01' then 'zero one' else 'something' end",
            "(?s).*Cannot apply '=' to arguments of type '<VARCHAR.6.> = <BIT.2.>'.*");

        //all thens and else return null
        checkExpFails("case 1 when 1 then null else null end",
            "(?s).*ELSE clause or at least one THEN clause must be non-NULL.*");

        //all thens and else return null
        checkExpFails("case 1 when 1 then null end",
            "(?s).*ELSE clause or at least one THEN clause must be non-NULL.*");
        checkExpFails("case when true and true then 1 " + "when false then 2 "
            + "when false then true " + "else "
            + "case when true then 3 end end",
            "Illegal mixing of types in CASE or COALESCE statement", 1, 8);
    }

    public void testNullIf()
    {
        checkExp("nullif(1,2)");
        checkExpType("nullif(1,2)", "INTEGER");
        checkExpType("nullif('a','b')", "VARCHAR(1)");
    }

    public void _testNullIfFails()
    {
        //todo
        checkExpFails("nullif(1,2,3)", "(?s)Invalid number of Arguments.*");
    }

    public void testCoalesce()
    {
        checkExp("coalesce('a','b')");
        checkExpType("coalesce('a','b','c')", "VARCHAR(1)");
    }

    public void testCoalesceFails()
    {
        checkExpFails("coalesce('a',1)",
            "Illegal mixing of types in CASE or COALESCE statement", 1, 8);
        checkExpFails("coalesce('a','b',1)",
            "Illegal mixing of types in CASE or COALESCE statement", 1, 8);
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
        checkExpType("'a' = 'b'", "BOOLEAN");
        checkExpType("'a' <> 'b'", "BOOLEAN");
        checkExpType("'a' > 'b'", "BOOLEAN");
        checkExpType("'a' < 'b'", "BOOLEAN");
        checkExpType("'a' >= 'b'", "BOOLEAN");
        checkExpType("'a' <= 'b'", "BOOLEAN");
    }

    public void testConcat()
    {
        checkExp("'a'||'b'");
        checkExp("b'1'||b'1'");
        checkExp("x'1'||x'1'");
        checkExpType("'a'||'b'", "VARCHAR(2)");
        checkExpType("cast('a' as char(1))||cast('b' as char(2))", "VARCHAR(3)");
        checkExpType("'a'||'b'||'c'", "VARCHAR(3)");
        checkExpType("'a'||'b'||'cde'||'f'", "VARCHAR(6)");
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
            "(?s).*Cannot apply '\\|\\|' to arguments of type '<VARCHAR.1.> \\|\\| <VARBINARY.1.>'"
            + ".*Supported form.s.: '<CHAR> \\|\\| <CHAR>'"
            + ".*'<VARCHAR> \\|\\| <VARCHAR>'"
            + ".*'<BIT> \\|\\| <BIT>'" + ".*'<BINARY> \\|\\| <BINARY>'"
            + ".*'<VARBINARY> \\|\\| <VARBINARY>'.*");
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

    public void testSimpleCollate()
    {
        checkExp("'s' collate latin1$en$1");
        checkExpType("'s' collate latin1$en$1", "VARCHAR(1)");
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

    public void testDyadicCollateCompare()
    {
        checkExp("'s' collate latin1$en$1 < 't'");
        checkExp("'t' > 's' collate latin1$en$1");
        checkExp("'s' collate latin1$en$1 <> 't' collate latin1$en$1");
    }

    public void testDyadicCompareCollateFails()
    {
        //two different explicit collations. difference in strength
        checkExpFails("'s' collate latin1$en$1 <= 't' collate latin1$en$2",
            "(?s).*Two explicit different collations.*are illegal.*");

        //two different explicit collations. difference in language
        checkExpFails("'s' collate latin1$sv$1 >= 't' collate latin1$en$1",
            "(?s).*Two explicit different collations.*are illegal.*");
    }

    public void testDyadicCollateOperator()
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
        checkExpType("char_length('string')", "INTEGER");
        checkExpType("character_length('string')", "INTEGER");
    }

    public void testUpperLower()
    {
        checkExp("upper(_shift_jis'sadf')");
        checkExp("lower(n'sadf')");
        checkExpType("lower('sadf')", "VARCHAR(4)");
        checkExpFails("upper(123)",
            "(?s).*Cannot apply 'UPPER' to arguments of type 'UPPER.<INTEGER>.'.*");
    }

    public void testPosition()
    {
        checkExp("position('mouse' in 'house')");
        checkExp("position(b'1' in b'1010')");
        checkExp("position(x'1' in x'110')");
        checkExpType("position('mouse' in 'house')", "INTEGER");

        //review wael 29 March 2004: is x'1' (hexstring) and x'1010' (bytestring) a type mismatch?
        checkExpFails("position(x'1' in x'1010')",
            "(?s).*Cannot apply 'POSITION' to arguments of type 'POSITION.<BIT.4.> IN <VARBINARY.2.>.'.*");
        checkExpFails("position(x'1' in '110')",
            "(?s).*Cannot apply 'POSITION' to arguments of type 'POSITION.<BIT.4.> IN <VARCHAR.3.>.'.*");
    }

    public void testTrim()
    {
        checkExp("trim('mustache' FROM 'beard')");
        checkExp("trim(both 'mustache' FROM 'beard')");
        checkExp("trim(leading 'mustache' FROM 'beard')");
        checkExp("trim(trailing 'mustache' FROM 'beard')");
        checkExpType("trim('mustache' FROM 'beard')", "VARCHAR(5)");

        if (false) {
            final SqlCollation.Coercibility expectedCoercibility = null; // todo
            checkCollation("trim('mustache' FROM 'beard')","VARCHAR(5)", expectedCoercibility);
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
            "(?s).*OVERLAY.<BIT> PLACING <BIT> FROM <INTEGER>..*");
        checkExpType("overlay('ABCdef' placing 'abc' from 1 for 3)",
            "VARCHAR(9)");

        if (false) {
            //todo
            checkCollation("overlay('ABCdef' placing 'abc' collate latin1$sv from 1 for 3)",
                           "ISO-8859-1$sv", SqlCollation.Coercibility.Explicit);
        }
    }

    public void testSubstring()
    {
        checkExp("substring('a' FROM 1)");
        checkExp("substring('a' FROM 1 FOR 3)");
        checkExp("substring('a' FROM 'reg' FOR '\\')");
        checkExp("substring(b'0' FROM 1  FOR 2)"); //bit string
        checkExp("substring(x'f' FROM 1  FOR 2)"); //hexstring
        checkExp("substring(x'ff' FROM 1  FOR 2)"); //binary string

        checkExpType("substring('10' FROM 1  FOR 2)", "VARCHAR(2)");
        checkExpType("substring('1000' FROM '1'  FOR 'w')", "VARCHAR(4)");
        checkExpType("substring(cast(' 100 ' as CHAR(99)) FROM '1'  FOR 'w')",
            "VARCHAR(99)");
        checkExpType("substring(b'10' FROM 1  FOR 2)", "VARBIT(2)");
        checkExpType("substring(x'10' FROM 1  FOR 2)", "VARBINARY(1)");

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

    public void testLikeAndSimilarFails()
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
        checkExpType("cast(null as bit)", "BIT(0)");
        checkExpType("cast(null as boolean)", "BOOLEAN");
        checkExpType("cast(null as varchar)", "VARCHAR(0)");
        checkExpType("cast(null as char)", "CHAR(0)");
        checkExpType("cast(null as binary)", "BINARY(0)");
        checkExpType("cast(null as date)", "DATE");
        checkExpType("cast(null as time)", "TIME");
        checkExpType("cast(null as timestamp)", "TIMESTAMP");
        checkExpType("cast(null as decimal)", "DECIMAL");
        checkExpType("cast(null as varbinary)", "VARBINARY(0)");

        checkExp("cast(null as integer), cast(null as char)");
    }

    public void testCastTypeToType()
    {
        checkExpType("cast(123 as varchar(3))", "VARCHAR(3)");
        checkExpType("cast(123 as char(3))", "CHAR(3)");
        checkExpType("cast('123' as integer)", "INTEGER");
        checkExpType("cast('123' as double)", "DOUBLE");
        checkExpType("cast('1.0' as real)", "REAL");
        checkExpType("cast(1.0 as tinyint)", "TINYINT");
        checkExpType("cast(1 as tinyint)", "TINYINT");
        checkExpType("cast(1.0 as smallint)", "SMALLINT");
        checkExpType("cast(1 as integer)", "INTEGER");
        checkExpType("cast(1.0 as integer)", "INTEGER");
        checkExpType("cast(1.0 as bigint)", "BIGINT");
        checkExpType("cast(1 as bigint)", "BIGINT");
        checkExpType("cast(1.0 as float)", "FLOAT");
        checkExpType("cast(1 as float)", "FLOAT");
        checkExpType("cast(1.0 as real)", "REAL");
        checkExpType("cast(1 as real)", "REAL");
        checkExpType("cast(1.0 as double)", "DOUBLE");
        checkExpType("cast(1 as double)", "DOUBLE");
        checkExpType("cast(null as boolean)", "BOOLEAN");
        checkExpType("cast('abc' as varchar)", "VARCHAR(0)"); //return type precision is not correct
        checkExpType("cast('abc' as char)", "CHAR(0)"); //return type precision is not correct
        checkExpType("cast(x'ff' as binary)", "BINARY(0)");
    }

    public void testCastFails()
    {
        checkExpFails("cast('foo' as bar)",
            "(?s).*Unknown datatype name 'BAR'");
    }

    public void testDateTime()
    {
        // LOCAL_TIME
        checkExp("LOCALTIME(3)");
        checkExp("LOCALTIME"); //    fix sqlcontext later.
        checkExpFails("LOCALTIME(1+2)",
            "Argument to function 'LOCALTIME' must be a positive integer literal");
        checkExpFails("LOCALTIME()",
            "Invalid number of arguments to function 'LOCALTIME'. Was expecting 0 arguments", 1, 8);
        checkExpType("LOCALTIME", "TIME(0)"); //  NOT NULL, with TZ ?
        checkExpFails("LOCALTIME(-1)",
            "Argument to function 'LOCALTIME' must be a positive integer literal"); // i guess -s1 is an expression?
        checkExpFails("LOCALTIME('foo')",
            "Argument to function 'LOCALTIME' must be a positive integer literal");

        // LOCALTIMESTAMP
        checkExp("LOCALTIMESTAMP(3)");
        checkExp("LOCALTIMESTAMP"); //    fix sqlcontext later.
        checkExpFails("LOCALTIMESTAMP(1+2)",
            "Argument to function 'LOCALTIMESTAMP' must be a positive integer literal");
        checkExpFails("LOCALTIMESTAMP()",
            "Invalid number of arguments to function 'LOCALTIMESTAMP'. Was expecting 0 arguments", 1, 8);
        checkExpType("LOCALTIMESTAMP", "TIMESTAMP(0)"); //  NOT NULL, with TZ ?
        checkExpFails("LOCALTIMESTAMP(-1)",
            "Argument to function 'LOCALTIMESTAMP' must be a positive integer literal"); // i guess -s1 is an expression?
        checkExpFails("LOCALTIMESTAMP('foo')",
            "Argument to function 'LOCALTIMESTAMP' must be a positive integer literal");

        // CURRENT_DATE
        checkExpFails("CURRENT_DATE(3)",
            "Invalid number of arguments to function 'CURRENT_DATE'. Was expecting 0 arguments", 1, 8);
        checkExp("CURRENT_DATE"); //    fix sqlcontext later.
        checkExpFails("CURRENT_DATE(1+2)",
            "Invalid number of arguments to function 'CURRENT_DATE'. Was expecting 0 arguments", 1, 8);
        checkExpFails("CURRENT_DATE()",
            "Invalid number of arguments to function 'CURRENT_DATE'. Was expecting 0 arguments", 1, 8);
        checkExpType("CURRENT_DATE", "DATE"); //  NOT NULL, with TZ?
        checkExpFails("CURRENT_DATE(-1)",
            "Invalid number of arguments to function 'CURRENT_DATE'. Was expecting 0 arguments", 1, 8); // i guess -s1 is an expression?
        checkExpFails("CURRENT_DATE('foo')", "(?s).*");

        // current_time
        checkExp("current_time(3)");
        checkExp("current_time"); //    fix sqlcontext later.
        checkExpFails("current_time(1+2)",
            "Argument to function 'CURRENT_TIME' must be a positive integer literal");
        checkExpFails("current_time()",
            "Invalid number of arguments to function 'CURRENT_TIME'. Was expecting 0 arguments", 1, 8);
        checkExpType("current_time", "TIME(0)"); //  NOT NULL, with TZ ?
        checkExpFails("current_time(-1)",
            "Argument to function 'CURRENT_TIME' must be a positive integer literal");
        checkExpFails("current_time('foo')",
            "Argument to function 'CURRENT_TIME' must be a positive integer literal");

        // current_timestamp
        checkExp("CURRENT_TIMESTAMP(3)");
        checkExp("CURRENT_TIMESTAMP"); //    fix sqlcontext later.
        checkExpFails("CURRENT_TIMESTAMP(1+2)",
            "Argument to function 'CURRENT_TIMESTAMP' must be a positive integer literal");
        checkExpFails("CURRENT_TIMESTAMP()",
            "Invalid number of arguments to function 'CURRENT_TIMESTAMP'. Was expecting 0 arguments", 1, 8);
        checkExpType("CURRENT_TIMESTAMP", "TIMESTAMP(0)"); //  NOT NULL, with TZ ?
        checkExpType("CURRENT_TIMESTAMP(2)", "TIMESTAMP(2)"); //  NOT NULL, with TZ ?
        checkExpFails("CURRENT_TIMESTAMP(-1)",
            "Argument to function 'CURRENT_TIMESTAMP' must be a positive integer literal");
        checkExpFails("CURRENT_TIMESTAMP('foo')",
            "Argument to function 'CURRENT_TIMESTAMP' must be a positive integer literal");

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
     * Testing for casting to/from date/time types.
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
        checkExpFails("foo()",
            "Reference to unknown function 'FOO'", 1, 8);
        checkExpFails("mod(123)",
            "Invalid number of arguments to function 'MOD'. Was expecting 2 arguments", 1, 8);
    }

    public void testJdbcFunctionCall()
    {
        checkExp("{fn log(1)}");
        checkExp("{fn locate('','')}");
        checkExp("{fn insert('',1,2,'')}");
        checkExpFails("{fn insert('','',1,2)}", "(?s).*.*");
        checkExpFails("{fn insert('','',1)}", "(?s).*4.*");
        checkExpFails("{fn locate('','',1)}", "(?s).*"); //todo this is legal jdbc syntax, just that currently the 3 ops call is not implemented in the system
        checkExpFails("{fn log('1')}",
            "(?s).*Cannot apply.*fn LOG..<VARCHAR.1.>.*");
        checkExpFails("{fn log(1,1)}",
            "(?s).*Encountered .fn LOG. with 2 parameter.s.; was expecting 1 parameter.s.*");
        checkExpFails("{fn fn(1)}",
            "(?s).*Function '.fn FN.' is not defined.*");
        checkExpFails("{fn hahaha(1)}",
            "(?s).*Function '.fn HAHAHA.' is not defined.*");
    }

    public void testQuotedFunction()
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
        checkFails("values ('1'),(2)",
            "Values passed to VALUES operator must have compatible types", 1, 1);
        if (false) {
            checkType("values (1),(2.0),(3)","ROWTYPE(DOUBLE)");
        }
    }

    public void testMultiset() {
        checkExp("multiset[1]");
        checkExp("multiset[1,2.3]");
        checkExpFails("multiset[1, '2']","Parameters must be of the same type", 1, 23);
        checkExp("multiset[ROW(1,2)]");
        checkExp("multiset[ROW(1,2),ROW(2,5)]");
        checkExp("multiset[ROW(1,2),ROW(3.4,5.4)]");
    }

    public void testMultisetSetOperators() {
        checkExp("multiset[1] multiset union multiset[1,2.3]");
        checkExpType("multiset[1] multiset union multiset[1,2.3]","DECIMAL(2, 1) MULTISET");
        checkExp("multiset[1] multiset union all multiset[1,2.3]");
        checkExp("multiset[1] multiset except multiset[1,2.3]");
        checkExp("multiset[1] multiset except all multiset[1,2.3]");
        checkExp("multiset[1] multiset intersect multiset[1,2.3]");
        checkExp("multiset[1] multiset intersect all multiset[1,2.3]");

        checkExpFails("multiset[1, '2'] multiset union multiset[1]","Parameters must be of the same type", 1, 23);
        checkExp("multiset[ROW(1,2)] multiset intersect multiset[row(3,4)]");
        if (false) {
            //TODO
            checkExpFails("multiset[ROW(1,'2')] multiset union multiset[ROW(1,2)]","Parameters must be of the same type", 1, 23);
        }
    }

    public void testSubMultisetOf() {
        checkExpType("multiset[1] submultiset of multiset[1,2.3]","BOOLEAN");
        checkExpType("multiset[1] submultiset of multiset[1]","BOOLEAN");

        checkExpFails("multiset[1, '2'] submultiset of multiset[1]","Parameters must be of the same type", 1, 23);
        checkExp("multiset[ROW(1,2)] submultiset of multiset[row(3,4)]");
    }

    public void testElement() {
        checkExpType("element(multiset[1])", "INTEGER");
        checkExpType("1.0+element(multiset[1])", "DECIMAL(2, 1)");
        checkExpType("element(multiset['1'])", "VARCHAR(1)");
        checkExpType("element(multiset[1e-2])", "DOUBLE");
        checkExpType("element(multiset[multiset[cast(null as tinyint)]])", "TINYINT MULTISET");
    }

    public void testMemberOf() {
        checkExpType("1 member of multiset[1]","BOOLEAN");
        checkExpFails("1 member of multiset['1']","Cannot compare values of types 'INTEGER', 'VARCHAR\\(1\\)'", 1, 32);
    }

    public void testIsASet() {
        checkExp("multiset[1] is a set");
        checkExp("multiset['1'] is a set");
        checkExpFails("'a' is a set",".*Cannot apply 'IS A SET' to.*");
    }

    public void testCardinality() {
        checkExpType("cardinality(multiset[1])","INTEGER");
        checkExpType("cardinality(multiset['1'])","INTEGER");
        checkExpFails("cardinality('a')","Cannot apply 'CARDINALITY' to arguments of type 'CARDINALITY.<VARCHAR.1.>.'. Supported form.s.: 'CARDINALITY.<MULTISET>.'", 1, 8);
    }

    public void testIntervalTimeUnitEnumeration() {
        // Since there is validation code relaying on the fact that the
        // enumerated time unit ordinals in SqlIntervalQualifier starts with 0
        // and ends with 5, this test is here to make sure that if someone
        // changes how the time untis are setup, an early feedback will be
        // generated by this test.
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
        assertEquals(0, SqlIntervalQualifier.TimeUnit.Year.getOrdinal());
        assertEquals(1, SqlIntervalQualifier.TimeUnit.Month.getOrdinal());
        assertEquals(2, SqlIntervalQualifier.TimeUnit.Day.getOrdinal());
        assertEquals(3, SqlIntervalQualifier.TimeUnit.Hour.getOrdinal());
        assertEquals(4, SqlIntervalQualifier.TimeUnit.Minute.getOrdinal());
        assertEquals(5, SqlIntervalQualifier.TimeUnit.Second.getOrdinal());
    }

    public void testIntervalLiteral() {
        checkExpType("INTERVAL '1' DAY", "INTERVAL DAY");
        checkExpType("INTERVAL '1' DAY(4)", "INTERVAL DAY(4)");
        checkExpType("INTERVAL '1' HOUR", "INTERVAL HOUR");
        checkExpType("INTERVAL '1' MINUTE", "INTERVAL MINUTE");
        checkExpType("INTERVAL '1' SECOND", "INTERVAL SECOND");
        checkExpType("INTERVAL '1' SECOND(3)", "INTERVAL SECOND(3)");
        checkExpType("INTERVAL '1' SECOND(3, 4)", "INTERVAL SECOND(3, 4)");
        checkExpType("INTERVAL '1 2:3:4' DAY TO SECOND", "INTERVAL DAY TO SECOND");
        checkExpType("INTERVAL '1 2:3:4' DAY(4) TO SECOND(4)", "INTERVAL DAY(4) TO SECOND(4)");

        checkExpType("INTERVAL '1' YEAR", "INTERVAL YEAR");
        checkExpType("INTERVAL '1' MONTH", "INTERVAL MONTH");
        checkExpType("INTERVAL '1-2' YEAR TO MONTH", "INTERVAL YEAR TO MONTH");
    }

    public void testIntervalOperators() {
        checkExpType("interval '1' day + interval '1' DAY(4)", "INTERVAL DAY(4)");
        checkExpType("interval '1' day(5) + interval '1' DAY", "INTERVAL DAY(5)");
        checkExpType("interval '1' day + interval '1' HOUR(10)", "INTERVAL DAY TO HOUR");
        checkExpType("interval '1' day + interval '1' MINUTE", "INTERVAL DAY TO MINUTE");
        checkExpType("interval '1' day + interval '1' second", "INTERVAL DAY TO SECOND");

        checkExpType("interval '1:2' hour to minute + interval '1' second", "INTERVAL HOUR TO SECOND");
        checkExpType("interval '1:3' hour to minute + interval '1 1:2:3.4' day to second", "INTERVAL DAY TO SECOND");
        checkExpType("interval '1:2' hour to minute + interval '1 1' day to hour", "INTERVAL DAY TO MINUTE");
        checkExpType("interval '1:2' hour to minute + interval '1 1' day to hour", "INTERVAL DAY TO MINUTE");
        checkExpType("interval '1 2' day to hour + interval '1:1' minute to second", "INTERVAL DAY TO SECOND");

        checkExpType("interval '1' year + interval '1' month", "INTERVAL YEAR TO MONTH");
        checkExpType("interval '1' day - interval '1' hour", "INTERVAL DAY TO HOUR");
        checkExpType("interval '1' year - interval '1' month", "INTERVAL YEAR TO MONTH");
        checkExpType("interval '1' month - interval '1' year", "INTERVAL YEAR TO MONTH");
        checkExpFails("interval '1' year + interval '1' day");
        checkExpFails("interval '1' month + interval '1' second");
        checkExpFails("interval '1' year - interval '1' day");
        checkExpFails("interval '1' month - interval '1' second");
    }

    public void checkWinClauseExp(String sql, String expectedMsgPattern) {
        sql = "select * from emp " + sql;
        assertExceptionIsThrown(sql, expectedMsgPattern);
    }

    public void checkWindowExpFails(String sql, String expectedMsgPattern) {
        sql = "select * from emp " + sql;
        assertExceptionIsThrown(sql, expectedMsgPattern);
    }

    public void _testWindowClause() {
        checkWinClauseExp("window as w (partition by sal order by deptno rows 2 preceding)", "expectedMsgPattern");

        // check syntax rules
        checkWindowExpFails("window as w ()", "");
        // syntax rule 2
        checkWindowExpFails("window as w (partition by sal), window as w (partition by sal)", "");
//        checkWindowExpFails("window as w (partition by sal),
//        checkWindowExpFails("window as w (partition by non_exist_col order by deptno)");
//        checkWindowExpFails("window as w (partition by sal order by non_exist_col)");
        // unambiguously reference a column in the window clause
        // select * from emp, emp window w as (partition by sal);
        // select * from emp empalias window w as (partition by sal);
    }

    public void checkWinFuncExp(String sql)
    {
        sql = "select " + sql + " from emp";
        assertExceptionIsThrown(sql, null);
    }

    public void testOneWinFunc()
    {
        checkWinFuncExp("abs(2) over (partition by sal)");
    }

    public void testNameResolutionInValuesClause()
    {
        final String emps = "(select 1 as empno, 'x' as name, 10 as deptno, 'M' as gender, 'San Francisco' as city, 30 as empid, 25 as age from values (1))";
        final String depts = "(select 10 as deptno, 'Sales' as name from values (1))";

        checkFails("select * from " + emps + " join " + depts + NL +
            " on emps.deptno = deptno",
            "Table 'EMPS' not found", 2, 5);
        // this is ok
        check("select * from " + emps + " as e" + NL +
            " join " + depts + " as d" + NL +
            " on e.deptno = d.deptno");
        // fail: ambiguous column in WHERE
        checkFails("select * from " + emps + " as emps," + NL +
            " " + depts + NL +
            "where deptno > 5",
            "Column 'DEPTNO' is ambiguous", 3, 7);
        // fail: ambiguous column reference in ON clause
        checkFails("select * from " + emps + " as e" + NL +
            " join " + depts + " as d" + NL +
            " on e.deptno = deptno",
            "Column 'DEPTNO' is ambiguous", 3, 16);
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
            "on deptno = agemod",
            "Column 'DEPTNO' is ambiguous", 3, 4);
        // fail: lateral reference
        checkFails("select * from " + emps + " as e," + NL +
            " (select 1, e.deptno from values (true)) as d",
            "Table 'E' not found", 2, 13);
    }

    public void testNestedFrom()
    {
        checkType("values (true)", "BOOLEAN");
        checkType("select * from values (true)", "BOOLEAN");
        checkType("select * from (select * from values (true))", "BOOLEAN");
        checkType("select * from (select * from (select * from values (true)))", "BOOLEAN");
        checkType(
            "select * from (" +
            "  select * from (" +
            "    select * from values (true)" +
            "    union" +
            "    select * from values (false))" +
            "  except" +
            "  select * from values (true))", "BOOLEAN");
    }

    public void testAmbiguousColumn()
    {
        checkFails("select * from emp join dept" + NL +
            " on emp.deptno = deptno",
            "Column 'DEPTNO' is ambiguous", 2, 18);
        // this is ok
        check("select * from emp as e" + NL +
            " join dept as d" + NL +
            " on e.deptno = d.deptno");
        // fail: ambiguous column in WHERE
        checkFails("select * from emp as emps, dept" + NL +
            "where deptno > 5",
            "Column 'DEPTNO' is ambiguous", 2, 7);
        // fail: alias 'd' obscures original table name 'dept'
        checkFails("select * from emp as emps, dept as d" + NL +
            "where dept.deptno > 5",
            "Table 'DEPT' not found", 2, 7);
        // fail: ambiguous column reference in ON clause
        checkFails("select * from emp as e" + NL +
            " join dept as d" + NL +
            " on e.deptno = deptno",
            "Column 'DEPTNO' is ambiguous", 3, 16);
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
            "on deptno = commmod",
            "Column 'DEPTNO' is ambiguous", 3, 4);
        // fail: lateral reference
        checkFails("select * from emp as e," + NL +
            " (select 1, e.deptno from values (true)) as d",
            "Table 'E' not found", 2, 13);
    }

    // todo: implement IN
    public void _testAmbiguousColumnInIn()
    {
        // ok: cyclic reference
        check("select * from emp as e" + NL +
            "where e.deptno in (" + NL +
            "  select 1 from values (true) where e.empno > 10)");
        // ok: cyclic reference
        check("select * from emp as e" + NL +
            "where e.deptno in (" + NL +
            "  select e.deptno from values (true))");
    }

    public void testDoubleNoAlias() {
        check("select * from emp join dept on true");
        check("select * from emp, dept");
        check("select * from emp cross join dept");
    }

    // TODO: is this legal? check that standard
    public void _testDuplicateColumnAliasFails() {
        checkFails("select 1 as a, 2 as b, 3 as a from emp", "xyz");
    }

    // NOTE jvs 20-May-2003 -- this is just here as a reminder that GROUP BY
    // validation isn't implemented yet
    public void testInvalidGroupBy(TestCase test) {
        try {
            check("select empno, deptno from emp group by deptno");
        } catch (RuntimeException ex) {
            return;
        }
        test.fail("Expected validation error");
    }

    public void testSingleNoAlias() {
        check("select * from emp");
    }

    public void testObscuredAliasFails() {
        // It is an error to refer to a table which has been given another
        // alias.
        checkFails("select * from emp as e where exists ("
            + "  select 1 from dept where dept.deptno = emp.deptno)",
            "Table 'EMP' not found", 1, 79);
    }

    public void testFromReferenceFails() {
        // You cannot refer to a table ('e2') in the parent scope of a query in
        // the from clause.
        checkFails("select * from emp as e1 where exists (" + NL
            + "  select * from emp as e2, " + NL
            + "    (select * from dept where dept.deptno = e2.deptno))",
            "Table 'E2' not found", 3, 45);
    }

    public void testWhereReference() {
        // You can refer to a table ('e1') in the parent scope of a query in
        // the from clause.
		//
		// Note: Oracle10g does not allow this query.
        check("select * from emp as e1 where exists (" + NL
            + "  select * from emp as e2, " + NL
            + "    (select * from dept where dept.deptno = e1.deptno))");
    }

    public void testUnionNameResolution() {
        checkFails(
            "select * from emp as e1 where exists (" + NL +
            "  select * from emp as e2, " + NL +
            "  (select deptno from dept as d" + NL +
            "   union" + NL +
            "   select deptno from emp as e3 where deptno = e2.deptno))",
            "Table 'E2' not found", 5, 48);

        checkFails("select * from emp" + NL +
            "union" + NL +
            "select * from dept where empno < 10",
            "Column 'EMPNO' not found in any table", 3, 26);
    }

    // TODO: implement UNION
    public void _testIncompatibleUnionFails() {
        checkFails("select 1,2 from emp union select 3 from dept", "xyz");
    }

    // TODO: implement UNION
    public void _testUnionOfNonQueryFails() {
        checkFails("select 1 from emp union 2", "xyz");
    }

    public void _testInTooManyColumnsFails() {
        checkFails("select * from emp where deptno in (select deptno,deptno from dept)",
            "xyz");
    }

    public void testNaturalCrossJoinFails() {
        checkFails("select * from emp natural cross join dept",
            "Cannot specify condition \\(NATURAL keyword, or ON or USING clause\\) following CROSS JOIN", 1, 33);
    }

    public void testCrossJoinUsingFails() {
        checkFails("select * from emp cross join dept using (deptno)",
            "Cannot specify condition \\(NATURAL keyword, or ON or USING clause\\) following CROSS JOIN", 1, 48);
    }

    public void testJoinUsing() {
        check("select * from emp join dept using (deptno)");
        // fail: comm exists on one side not the other
        // todo: The error message could be improved.
        checkFails("select * from emp join dept using (deptno, comm)",
            "Column 'COMM' not found in any table", 1, 44);
        // ok to repeat (ok in Oracle10g too)
        check("select * from emp join dept using (deptno, deptno)");
        // inherited column, not found in either side of the join, in the
        // USING clause
        checkFails("select * from dept where exists (" + NL +
            "select 1 from emp join bonus using (dname))",
            "Column 'DNAME' not found in any table", 2, 37);
        // inherited column, found in only one side of the join, in the
        // USING clause
        checkFails("select * from dept where exists (" + NL +
            "select 1 from emp join bonus using (deptno))",
            "Column 'DEPTNO' not found in any table", 2, 37);
    }

    public void testCrossJoinOnFails() {
        checkFails("select * from emp cross join dept" + NL +
            " on emp.deptno = dept.deptno",
            "Cannot specify condition \\(NATURAL keyword, or ON or USING clause\\) following CROSS JOIN", 2, 23);
    }

    public void testInnerJoinWithoutUsingOrOnFails() {
        checkFails("select * from emp inner join dept "
            + "where emp.deptno = dept.deptno",
            "INNER, LEFT, RIGHT or FULL join requires a condition \\(NATURAL keyword or ON or USING clause\\)", 1, 25);
    }

    public void testJoinUsingInvalidColsFails() {
        // todo: Improve error msg
        checkFails("select * from emp left join dept using (gender)",
            "Column 'GENDER' not found in any table", 1, 41);
    }

    // todo: Cannot handle '(a join b)' yet -- we see the '(' and expect to
    // see 'select'.
    public void _testJoinUsing() {
        check("select * from (emp join bonus using (job))" + NL +
            "join dept using (deptno)");
        // cannot alias a JOIN (actually this is a parser error, but who's
        // counting?)
        checkFails("select * from (emp join bonus using (job)) as x" + NL +
            "join dept using (deptno)",
            "as wrong here");
        checkFails("select * from (emp join bonus using (job))" + NL +
            "join dept using (dname)",
            "dname not found in lhs", 1, 41);
        checkFails("select * from (emp join bonus using (job))" + NL +
            "join (select 1 as job from (true)) using (job)",
            "ambig", 1, 1);
    }

    /**
     * Describes the valid SQL compatiblity modes.
     */
    public static class Compatible extends EnumeratedValues.BasicValue {
        private Compatible(String name, int ordinal) {
            super(name, ordinal, null);
        }

        public static final int Default_ordinal = 0;
        public static final int Strict92_ordinal = 1;
        public static final int Strict99_ordinal = 2;
        public static final int Pragmatic99_ordinal = 3;
        public static final int Oracle10g_ordinal = 4;
        public static final int Sql2003_ordinal = 5;

        public static final Compatible Strict92 =
            new Compatible("Strict92", Strict92_ordinal);
        public static final Compatible Strict99 =
            new Compatible("Strict99", Strict99_ordinal);
        public static final Compatible Pragmatic99 =
            new Compatible("Pragmatic99", Pragmatic99_ordinal);
        public static final Compatible Oracle10g =
            new Compatible("Oracle10g", Oracle10g_ordinal);
        public static final Compatible Sql2003 =
            new Compatible("Sql2003", Sql2003_ordinal);
        public static final Compatible Default =
            new Compatible("Default", Default_ordinal);
    }

    protected Compatible getCompatible() {
        return Compatible.Default;
    }

	public void testOrder() {
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
        checkFails("select empno as x from emp, dept order by deptno",
            "Column 'DEPTNO' is ambiguous", 1, 43);

        checkFails("select empno as deptno from emp, dept order by deptno",
            // Alias 'deptno' is closer in scope than 'emp.deptno'
            // and 'dept.deptno', and is therefore not ambiguous.
            // Checked Oracle10G -- it is valid.
            sortByAlias ? null :
            // Ambiguous in SQL:2003
            "col ambig");

        checkFails(
            "select deptno from dept" + NL +
            "union" + NL +
            "select empno from emp" + NL +
            "order by empno",
            "Column 'EMPNO' not found in any table", 4, 10);

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
    }

    public void testNew() {
        // Oracle allows this.
        check("select 1 from emp order by sum(sal)");
    }
}



// End SqlValidatorTestCase.java
