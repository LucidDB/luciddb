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

import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.charset.Charset;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Iterator;
import java.util.Vector;

import junit.framework.TestCase;

import org.eigenbase.reltype.RelDataType;
import org.eigenbase.reltype.RelDataTypeFactory;
import org.eigenbase.sql.*;
import org.eigenbase.sql.parser.ParseException;
import org.eigenbase.sql.parser.SqlParser;


/**
 * An abstract base class for implementing tests against {@link org.eigenbase.sql.SqlValidator} and derived classes.
 *
 * @author Wael Chatila
 * @since Jan 12, 2004
 * @version $Id$
 **/
public abstract class SqlValidatorTestCase extends TestCase
{
    //~ Static fields/initializers --------------------------------------------

    private static final String NL = System.getProperty("line.separator");

    //~ Instance fields -------------------------------------------------------

    private final String UNKNOWN_FUNC =
        "(?s).*Reference to unknown function.*encountered near line 1, column 8.*";
    private final String INVALID_NUMBER_OF_ARGS =
        "(?s).*Invalid number of arguments to function '.*'; encountered near line 1, column 8. Was expecting . arguments.*";

    //~ Methods ---------------------------------------------------------------

    abstract public SqlValidator getValidator();

    abstract public SqlParser getParser(String sql)
        throws ParseException;

    public void check(String sql)
    {
        assertExceptionIsThrown(sql, null);
    }

    public void checkExp(String sql)
    {
        sql = "select " + sql + " from values(true)";
        assertExceptionIsThrown(sql, null);
    }

    public void checkFails(
        String sql,
        String expected)
    {
        assertExceptionIsThrown(sql, expected);
    }

    public void checkExpFails(
        String sql,
        String expected)
    {
        sql = "select " + sql + " from values(true)";
        assertExceptionIsThrown(sql, expected);
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
        RelDataType actualType =
            validator.getValidatedNodeType(
                ((SqlNodeList) ((SqlCall) n).getOperands()[1]).get(0));
        return actualType;
    }

    /**
     * Asserts either if a sql query is valid or not.
     * @param sql
     * @param expectedMsgPattern If this parameter is null the query must be valid for the test to pass<br>
     * If this parameter is not null the query must be malformed and the msg pattern must
     * match the the error raised for the test to pass.
     */
    protected void assertExceptionIsThrown(
        String sql,
        String expectedMsgPattern)
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
        try {
            validator.validate(sqlNode);
        } catch (Throwable ex) {
            actualException = ex;
        }

        if (null == expectedMsgPattern) {
            if ((null != actualException)) {
                actualException.printStackTrace();
                String actualMessage = actualException.getMessage();
                fail("SqlValidationTest: Validator threw unexpected exception" +
                        "; query [" + sql +
                        "]; exception ["+ actualMessage + "]");
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
                            "]; actual [" + actualMessage + "]");
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

        //        todo
        //        assertExceptionIsThrown("select TRUE OR (TIME '12:00' AT LOCAL) from values(true)",
        //                                "some error msg with line + col");
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
        checkExpFails("'abc' IS NOT UNKNOWN", "(?s).*Can not apply.*");
    }

    public void testIsFails()
    {
        assertExceptionIsThrown("select 1 IS TRUE FROM values(true)",
            "(?s).*'<INTEGER> IS TRUE'.*");

        assertExceptionIsThrown("select 1.1 IS NOT FALSE FROM values(true)",
            "(?s).*");

        assertExceptionIsThrown("select 1.1e1 IS NOT FALSE FROM values(true)",
            "(?s).*Can not apply 'IS NOT FALSE' to arguments of type '<DOUBLE> IS NOT FALSE'.*");

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
            "(?s).*Can not apply '\\+' to arguments of type '<INTEGER> \\+ <BOOLEAN>'\\. Supported form\\(s\\):.*");
    }

    public void testNumbers()
    {
        check("select 1+-2.*-3.e-1/-4>+5 AND true from values(true)");
    }

    public void testPrefixFails()
    {
        assertExceptionIsThrown("SELECT -'abc' from values(true)",
            "(?s).*Can not apply '-' to arguments of type '-<VARCHAR.3.>'.*");
        assertExceptionIsThrown("SELECT +'abc' from values(true)",
            "(?s).*Can not apply '\\+' to arguments of type '\\+<VARCHAR.3.>'.*");
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
            "(?s).*Can not apply '<>' to arguments of type '<VARCHAR.0.> <> <INTEGER>'.*");
        checkExpFails("'1'>=1",
            "(?s).*Can not apply '>=' to arguments of type '<VARCHAR.1.> >= <INTEGER>'.*");
        checkExpFails("1<>n'abc'",
            "(?s).*Can not apply '<>' to arguments of type '<INTEGER> <> <VARCHAR.3.>'.*");
        checkExpFails("''=.1",
            "(?s).*Can not apply '=' to arguments of type '<VARCHAR.0.> = <DECIMAL.1..1.>'.*");
        checkExpFails("true<>1e-1",
            "(?s).*Can not apply '<>' to arguments of type '<BOOLEAN> <> <DOUBLE>'.*");
        checkExpFails("false=''",
            "(?s).*Can not apply '=' to arguments of type '<BOOLEAN> = <VARCHAR.0.>'.*");
        checkExpFails("b'1'=0.01",
            "(?s).*Can not apply '=' to arguments of type '<BIT.1.> = <DECIMAL.3, 2.>'.*");
        checkExpFails("b'1'=1",
            "(?s).*Can not apply '=' to arguments of type '<BIT.1.> = <INTEGER>'.*");
        checkExpFails("b'1'<>0.01",
            "(?s).*Can not apply '<>' to arguments of type '<BIT.1.> <> <DECIMAL.3, 2.>'.*");
        checkExpFails("b'1'<>1",
            "(?s).*Can not apply '<>' to arguments of type '<BIT.1.> <> <INTEGER>'.*");
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
            "(?s).*Can not apply '=' to arguments of type '<BIT.4.> = <VARCHAR.3.>'.*");
        assertExceptionIsThrown("select x'ff'=88 from values(true)",
            "(?s).*Can not apply '=' to arguments of type '<VARBINARY.1.> = <INTEGER>'.*");
        assertExceptionIsThrown("select x''<>1.1e-1 from values(true)",
            "(?s).*Can not apply '<>' to arguments of type '<VARBINARY.0.> <> <DOUBLE>'.*");
        assertExceptionIsThrown("select b''<>1.1 from values(true)",
            "(?s).*Can not apply '<>' to arguments of type '<BIT.0.> <> <DECIMAL.2, 1.>'.*");
    }

    public void testStringLiteral()
    {
        check("select n''=_iso_8859-1'abc' from values(true)");
        check("select N'f'<>'''' from values(true)");
    }

    public void testArthimeticOperators()
    {
        checkExp("pow(2,3)");
        checkExp("aBs(-2.3e-2)");
        checkExp("MOD(5             ,\t\f\r\n2)");
        checkExp("ln(5.43  )");
        checkExp("log(- -.2  )");

        checkExpFails("mod(5.1, 3)", "(?s).*Can not apply.*");
        checkExpFails("mod(2,5.1)", "(?s).*Can not apply.*");
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
            "(?s).*Can not apply 'POW' to arguments of type 'POW.<INTEGER>, <VARCHAR.3.>.*");
        checkExpFails("pow(true,1)",
            "(?s).*Can not apply 'POW' to arguments of type 'POW.<BOOLEAN>, <INTEGER>.*");
        checkExpFails("mod(b'11001',1)",
            "(?s).*Can not apply 'MOD' to arguments of type 'MOD.<BIT.5.>, <INTEGER>.*");
        checkExpFails("mod(1, b'11001')",
            "(?s).*Can not apply 'MOD' to arguments of type 'MOD.<INTEGER>, <BIT.5.>.*");
        checkExpFails("abs(x'')",
            "(?s).*Can not apply 'ABS' to arguments of type 'ABS.<VARBINARY.0.>.*");
        checkExpFails("ln(x'f')",
            "(?s).*Can not apply 'LN' to arguments of type 'LN.<BIT.4.>.*");
        checkExpFails("log(x'f')",
            "(?s).*Can not apply 'LOG' to arguments of type 'LOG.<BIT.4.>.*");
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
            "(?s).*Can not apply '=' to arguments of type '<VARCHAR.6.> = <BIT.2.>'.*");

        //all thens and else return null
        checkExpFails("case 1 when 1 then null else null end",
            "(?s).*ELSE clause or at least one THEN clause must be non-NULL.*");

        //all thens and else return null
        checkExpFails("case 1 when 1 then null end",
            "(?s).*ELSE clause or at least one THEN clause must be non-NULL.*");
        checkExpFails("case when true and true then 1 " + "when false then 2 "
            + "when false then true " + "else "
            + "case when true then 3 end end",
            "(?s).*Illegal mixing of types found in statement starting near: line 1, column 8.*");
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
            "(?s).*Illegal mixing of types found in statement starting near: line 1, column 8.*");
        checkExpFails("coalesce('a','b',1)",
            "(?s).*Illegal mixing of types found in statement starting near: line 1, column 8.*");
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
            "(?s).*Can not apply '\\|\\|' to arguments of type '<VARCHAR.1.> \\|\\| <VARBINARY.1.>'"
            + ".*Supported form.s.: '<CHAR> \\|\\| <CHAR>'"
            + ".*'<VARCHAR> \\|\\| <VARCHAR>'"            
            + ".*'<BIT> \\|\\| <BIT>'" + ".*'<BINARY> \\|\\| <BINARY>'"
            + ".*'<VARBINARY> \\|\\| <VARBINARY>'.*");
    }

    public void testBetween()
    {
        checkExp("1 between 2 and 3");
        checkExp("'a' between 'b' and 'c'");
        checkExpFails("'' between 2 and 3", "(?s).*Can not apply.*");
    }

    public void testCharsetMismatch()
    {
        checkExpFails("''=_shift_jis''",
            "(?s).*Can not apply .* to the two different charsets.*");
        checkExpFails("''<>_shift_jis''",
            "(?s).*Can not apply .* to the two different charsets.*");
        checkExpFails("''>_shift_jis''",
            "(?s).*Can not apply .* to the two different charsets.*");
        checkExpFails("''<_shift_jis''",
            "(?s).*Can not apply .* to the two different charsets.*");
        checkExpFails("''<=_shift_jis''",
            "(?s).*Can not apply .* to the two different charsets.*");
        checkExpFails("''>=_shift_jis''",
            "(?s).*Can not apply .* to the two different charsets.*");
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
            "(?s).*Can not apply 'UPPER' to arguments of type 'UPPER.<INTEGER>.'.*");
    }

    public void testPosition()
    {
        checkExp("position('mouse' in 'house')");
        checkExp("position(b'1' in b'1010')");
        checkExp("position(x'1' in x'110')");
        checkExpType("position('mouse' in 'house')", "INTEGER");

        //review wael 29 March 2004: is x'1' (hexstring) and x'1010' (bytestring) a type mismatch?
        checkExpFails("position(x'1' in x'1010')",
            "(?s).*Can not apply 'POSITION' to arguments of type 'POSITION.<BIT.4.> IN <VARBINARY.2.>.'.*");
        checkExpFails("position(x'1' in '110')",
            "(?s).*Can not apply 'POSITION' to arguments of type 'POSITION.<BIT.4.> IN <VARCHAR.3.>.'.*");
    }

    public void testTrim()
    {
        checkExp("trim('mustache' FROM 'beard')");
        checkExp("trim(both 'mustache' FROM 'beard')");
        checkExp("trim(leading 'mustache' FROM 'beard')");
        checkExp("trim(trailing 'mustache' FROM 'beard')");
        checkExpType("trim('mustache' FROM 'beard')", "VARCHAR(5)");

        //todo checkCollation("trim('mustache' FROM 'beard')","VARCHAR(5)",...);
    }

    public void testTrimFails()
    {
        checkExpFails("trim(123 FROM 'beard')",
            "(?s).*Can not apply 'TRIM' to arguments of type.*");
        checkExpFails("trim('a' FROM 123)",
            "(?s).*Can not apply 'TRIM' to arguments of type.*");
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

        //todo checkCollation("overlay('ABCdef' placing 'abc' collate latin1$sv from 1 for 3)",
        //               "ISO-8859-1$sv", SqlCollation.COERCIBILITY_EXPLICIT);
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
            "(?s).*Can not apply 'SUBSTRING' to arguments of type.*");
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
            "(?s).*Unknown datatype name: BAR.*");
    }

    public void testDateTime()
    {
        // LOCAL_TIME
        checkExp("LOCALTIME(3)");
        checkExp("LOCALTIME"); //    fix sqlcontext later.
        checkExpFails("LOCALTIME(1+2)",
            "Argument to function 'LOCALTIME' must be a positive integer literal");
        checkExpFails("LOCALTIME()", INVALID_NUMBER_OF_ARGS);
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
        checkExpFails("LOCALTIMESTAMP()", INVALID_NUMBER_OF_ARGS);
        checkExpType("LOCALTIMESTAMP", "TIMESTAMP(0)"); //  NOT NULL, with TZ ?
        checkExpFails("LOCALTIMESTAMP(-1)",
            "Argument to function 'LOCALTIMESTAMP' must be a positive integer literal"); // i guess -s1 is an expression?
        checkExpFails("LOCALTIMESTAMP('foo')",
            "Argument to function 'LOCALTIMESTAMP' must be a positive integer literal");

        // CURRENT_DATE
        checkExpFails("CURRENT_DATE(3)", INVALID_NUMBER_OF_ARGS);
        checkExp("CURRENT_DATE"); //    fix sqlcontext later.
        checkExpFails("CURRENT_DATE(1+2)", INVALID_NUMBER_OF_ARGS);
        checkExpFails("CURRENT_DATE()", INVALID_NUMBER_OF_ARGS);
        checkExpType("CURRENT_DATE", "DATE"); //  NOT NULL, with TZ?
        checkExpFails("CURRENT_DATE(-1)", INVALID_NUMBER_OF_ARGS); // i guess -s1 is an expression?
        checkExpFails("CURRENT_DATE('foo')", "(?s).*");

        // current_time
        checkExp("current_time(3)");
        checkExp("current_time"); //    fix sqlcontext later.
        checkExpFails("current_time(1+2)",
            "Argument to function 'CURRENT_TIME' must be a positive integer literal");
        checkExpFails("current_time()", INVALID_NUMBER_OF_ARGS);
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
        checkExpFails("CURRENT_TIMESTAMP()", INVALID_NUMBER_OF_ARGS);
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
        checkExpFails("foo()", UNKNOWN_FUNC);
        checkExpFails("mod(123)",
            "(?s).*Invalid number of arguments to function .MOD.. encountered near line 1, column 8. Was expecting 2 arguments.*");
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
            "(?s).*Can not apply.*fn LOG..<VARCHAR.1.>.*");
        checkExpFails("{fn log(1,1)}",
            "(?s).*Encountered .fn LOG. with 2 parameter.s., was expecting 1 parameter.s.*");
        checkExpFails("{fn fn(1)}", "(?s).*Function .fn FN. is not defined.*");
        checkExpFails("{fn hahaha(1)}",
            "(?s).*Function .fn HAHAHA. is not defined.*");
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
            "(?s).*Values passed to VALUES operator near line 1, column 1 must have compatible types.*");

        //        checkType("values (1),(2.0),(3)","ROWTYPE(DOUBLE)");
    }
}


// End SqlValidatorTestCase.java
