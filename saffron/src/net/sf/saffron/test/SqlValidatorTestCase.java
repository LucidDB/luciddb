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
package net.sf.saffron.test;

import junit.framework.TestCase;

import java.sql.Connection;
import java.sql.Statement;
import java.sql.SQLException;
import java.util.Vector;
import java.util.Iterator;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.charset.Charset;

import net.sf.saffron.sql.*;
import net.sf.saffron.sql.parser.SqlParser;
import net.sf.saffron.sql.parser.ParseException;
import net.sf.saffron.core.SaffronTypeFactory;
import net.sf.saffron.core.SaffronType;


/**
 * An abstract base class for implementing tests against {@link net.sf.saffron.sql.SqlValidator} and derived classes.
 *
 * @author wael
 * @since Jan 12, 2004
 * @version $Id$
 **/
public abstract class SqlValidatorTestCase extends TestCase {

	abstract public SqlValidator getValidator();
	abstract public SqlParser getParser(String sql) throws ParseException;

    private static final String NL = System.getProperty("line.separator");

    void check(String sql)
    {
        assertExceptionIsThrown(sql, null);
    }

    void checkExp(String sql)
    {
        sql = "select "+sql+" from values(true)";
        assertExceptionIsThrown(sql, null);
    }

    void checkExpFails(String sql, String expected)
    {
        sql = "select "+sql+" from values(true)";
        assertExceptionIsThrown(sql, expected);
    }

    void checkType(String sql,String expected){
        sql="select "+sql+" from values(true)";
        SaffronType actualType = getResultType(sql);
        String actual = actualType.toString();
        if (!expected.equals(actual)) {
            String msg = NL+"Excpected="+expected+NL+"   actual="+actual;
            fail(msg);
        }
    }

    void checkCollation(String sql,String expectedCollationName,
                        SqlCollation.Coercibility expectedCoercibility){
        sql="select "+sql+" from values(true)";
        SaffronType actualType = getResultType(sql);
        SqlCollation collation = actualType.getCollation();

        String actualName = collation.getCollationName();
        int actualCoercibility = collation.getCoercibility().getOrdinal();
        int expectedCoercibilityOrd = expectedCoercibility.getOrdinal();
        assertEquals(expectedCollationName,actualName);
        assertEquals(expectedCoercibilityOrd,actualCoercibility);
    }

    void checkCharset(String sql,Charset expectedCharset){
        sql="select "+sql+" from values(true)";
        SaffronType actualType = getResultType(sql);
        Charset actualCharset = actualType.getCharset();

        if (!expectedCharset.equals(actualCharset)) {
            fail(NL+"Expected="+expectedCharset.name()+NL+"  actual="+actualCharset.name());
        }
    }

    private SaffronType getResultType(String sql) {
        SqlParser parser;
		SqlValidator validator;
		SqlNode sqlNode;
		try {
			parser = getParser(sql);
			sqlNode = parser.parseQuery();
			validator = getValidator();
		}
		catch (ParseException ex) {
            ex.printStackTrace();
			fail("SqlValidationTest: Parse Error while parsing query="+sql+"\n"+ex.getMessage());
			return null;
		}
		catch (Throwable e) {
			e.printStackTrace(System.err);
			fail("SqlValidationTest: Failed while trying to connect or get statement");
			return null;
        }
        SqlNode n=validator.validate(sqlNode);
        SaffronType actualType = validator.getValidatedNodeType(((SqlNodeList)((SqlCall)n).getOperands()[1]).get(0));
        return actualType;
    }

    /**
     * Asserts either if a sql query is valid or not.
     * @param sql
     * @param expectedMsgPattern If this parameter is null the query must be valid for the test to pass<br>
     * If this parameter is not null the query must be malformed and the msg pattern must
     * match the the error raised for the test to pass.
     */
	protected void assertExceptionIsThrown(String sql, String expectedMsgPattern)
	{
        SqlParser parser;
		SqlValidator validator;
		SqlNode sqlNode;
		try {
			parser = getParser(sql);
			sqlNode = parser.parseQuery();
			validator = getValidator();
		}
		catch (ParseException ex) {
            ex.printStackTrace();
			fail("SqlValidationTest: Parse Error while parsing query="+sql+"\n"+ex.getMessage());
			return;
		}
		catch (Throwable e) {
			e.printStackTrace(System.err);
			fail("SqlValidationTest: Failed while trying to connect or get statement");
			return;
        }

        Throwable exceptionHappended = null;
        try {
            validator.validate(sqlNode);
        } catch (Throwable ex) {
	        exceptionHappended = ex;
        }

        if ((null == expectedMsgPattern) && (null != exceptionHappended)) {
            exceptionHappended.printStackTrace();
            fail("SqlValidationTest: Validator throw unexpected exception while executing query='"+sql+"'"+
                "\n"+exceptionHappended.getMessage());
        }
        else if (null != expectedMsgPattern) {
            if (null == exceptionHappended) {
                fail("SqlValidationTest: Validator didn't throw exception as expected while executing query='"+sql+"'");
            }
            else if (!exceptionHappended.getMessage().matches(expectedMsgPattern)) {
                exceptionHappended.printStackTrace();
                String actual =exceptionHappended.getMessage();
                fail("SqlValidationTest: The thrown exception was unexpected"
                    +" Expected='"+expectedMsgPattern
                    +"' Actual='"+actual
                    +"' query='"+sql+"'");
			}
		}
	}

    //-- tests -----------------------------------
	public void testMultipleSameAsFails() {
        assertExceptionIsThrown("select 1 as c1,2 as c1 from values(1)",
                                "(?s)(?i).*more than one column has alias 'c1'");
	}

	public void testMultipleDifferentAs() {
        check("select 1 as c1,2 as c2 from values(true)");
	}

    public void testTypeOfAs() {
        checkType("1 as c1","INTEGER");
        checkType("'hej' as c1","VARCHAR(3)");
        checkType("b'111' as c1","BIT(3)");
	}

    public void testTypesLiterals(){
        checkType("'abc'","VARCHAR(3)");
        checkType("n'abc'","VARCHAR(3)");
        checkType("_iso_8859-2'abc'","VARCHAR(3)");
        checkType("x'abc'","BIT(12)");
        checkType("x'abcd'","VARBINARY(2)");
        checkType("b'1001'","BIT(4)");
        checkType("1234567890","INTEGER");
        checkType("123456.7890","DECIMAL(10, 4)");
        checkType("123456.7890e3","DOUBLE");
        checkType("true","BOOLEAN");
        checkType("false","BOOLEAN");
        checkType("unknown","BOOLEAN");
    }

	public void testBooleans() {
        check("select TRUE OR unknowN from values(true)");
        check("select false AND unknown from values(true)");
        check("select not UNKNOWn from values(true)");
        check("select not true from values(true)");
        check("select not false from values(true)");
	}

	public void testAndOrIllegalTypesFails() {
        //TODO need col+line number
        assertExceptionIsThrown("select 'abc' AND FaLsE from values(true)",
                                "(?s).*'<VARCHAR.3.> AND <BOOLEAN>'.*");

        assertExceptionIsThrown("select TRUE OR 1 from values(true)",
                                "(?s).*");

        assertExceptionIsThrown("select unknown OR 1.0 from values(true)",
                                "(?s).*");

        assertExceptionIsThrown("select true OR 1.0e4 from values(true)",
                                "(?s).*");

//        todo
//        assertExceptionIsThrown("select TRUE OR (TIME '12:00' AT LOCAL) from values(true)",
//                                "some error msg with line + col");

	}

    public void testNotIlleagalTypeFails() {
        //TODO need col+line number
        assertExceptionIsThrown("select NOT 3.141 from values(true)",
                                "(?s).*'NOT<DECIMAL.4, 3.>'.*");

        assertExceptionIsThrown("select NOT 'abc' from values(true)",
                                "(?s).*");

        assertExceptionIsThrown("select NOT 1 from values(true)",
                                "(?s).*");

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
        check("select 'abc' IS NOT UNKNOWN FROM values(true)");
    }

    public void testIsFails(){
        assertExceptionIsThrown("select 1 IS TRUE FROM values(true)",
                                "(?s).*'<INTEGER> IS TRUE'.*");

        assertExceptionIsThrown("select 1.1 IS NOT FALSE FROM values(true)",
                                "(?s).*");

        assertExceptionIsThrown("select 1.1e1 IS NOT FALSE FROM values(true)",
                                "(?s).*'<DOUBLE> IS FALSE'.*"); //todo doesnt map to NOT

        assertExceptionIsThrown("select 'abc' IS NOT TRUE FROM values(true)",
                                "(?s).*");
    }

	public void testScalars() {
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

    public void testScalarsFails() {
        //TODO need col+line number
        assertExceptionIsThrown("select 1+TRUE from values(true)",
                                "(?s).*Can not apply '\\+' to arguments of type '<INTEGER> \\+ <BOOLEAN>'\\. Supported form\\(s\\): "+
                                "'<INTEGER> \\+ <INTEGER>'"+NL+
                                "'<INTEGER> \\+ <BIGINT>'"+NL+
                                "'<INTEGER> \\+ <DECIMAL>'"+NL+
                                "'<INTEGER> \\+ <REAL>'"+NL+
                                "'<INTEGER> \\+ <DOUBLE>'"+NL+
                                "'<BIGINT> \\+ <INTEGER>'"+NL+
                                "'<BIGINT> \\+ <BIGINT>'"+NL+
                                "'<BIGINT> \\+ <DECIMAL>'"+NL+
                                "'<BIGINT> \\+ <REAL>'"+NL+
                                "'<BIGINT> \\+ <DOUBLE>'"+NL+
                                "'<DECIMAL> \\+ <INTEGER>'"+NL+
                                "'<DECIMAL> \\+ <BIGINT>'"+NL+
                                "'<DECIMAL> \\+ <DECIMAL>'"+NL+
                                "'<DECIMAL> \\+ <REAL>'"+NL+
                                "'<DECIMAL> \\+ <DOUBLE>'"+NL+
                                "'<REAL> \\+ <INTEGER>'"+NL+
                                "'<REAL> \\+ <BIGINT>'"+NL+
                                "'<REAL> \\+ <DECIMAL>'"+NL+
                                "'<REAL> \\+ <REAL>'"+NL+
                                "'<REAL> \\+ <DOUBLE>'"+NL+
                                "'<DOUBLE> \\+ <INTEGER>'"+NL+
                                "'<DOUBLE> \\+ <BIGINT>'"+NL+
                                "'<DOUBLE> \\+ <DECIMAL>'"+NL+
                                "'<DOUBLE> \\+ <REAL>'"+NL+
                                "'<DOUBLE> \\+ <DOUBLE>'");
	}

    public void testNumbers() {
        check("select 1+-2.*-3.e-1/-4>+5 AND true from values(true)");
    }

    public void testPrefixFails(){
        assertExceptionIsThrown("SELECT -'abc' from values(true)","(?s).*Can not apply '-' to arguments of type '-<VARCHAR.3.>'.*");
        assertExceptionIsThrown("SELECT +'abc' from values(true)","(?s).*Can not apply '\\+' to arguments of type '\\+<VARCHAR.3.>'.*");
    }

    public void testEqualNotEqual() {
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

    public void testEqualNotEqualFails() {
        checkExpFails("''<>1"       ,"(?s).*Can not apply '<>' to arguments of type '<VARCHAR.0.> <> <INTEGER>'.*");
        checkExpFails("1<>n'abc'"   ,"(?s).*Can not apply '<>' to arguments of type '<INTEGER> <> <VARCHAR.3.>'.*");
        checkExpFails("''=.1"       ,"(?s).*Can not apply '=' to arguments of type '<VARCHAR.0.> = <DECIMAL.1..1.>'.*");
        checkExpFails("true<>1e-1"  ,"(?s).*Can not apply '<>' to arguments of type '<BOOLEAN> <> <DOUBLE>'.*");
        checkExpFails("false=''"    ,"(?s).*Can not apply '=' to arguments of type '<BOOLEAN> = <VARCHAR.0.>'.*");
    }

    public void testHexBitBinaryString(){
        check("select x'f'=x'abc' from values(true)");
        check("select x'f'=X'' from values(true)");
        check("select x'ff'=X'' from values(true)");
        check("select x'ff'=X'f' from values(true)");
        check("select x'ff'=b'10' from values(true)");
        check("select b'000'=X'f' from values(true)");
    }

    public void testHexBitBinaryStringFails(){
        assertExceptionIsThrown("select x'f'='abc' from values(true)","(?s).*Parameters must be of same type.*");
        assertExceptionIsThrown("select x'ff'=88 from values(true)","(?s).*Parameters must be of same type.*");
        assertExceptionIsThrown("select x''<>1.1e-1 from values(true)","(?s).*Parameters must be of same type.*");
        assertExceptionIsThrown("select b''<>1.1 from values(true)","(?s).*Parameters must be of same type.*");
    }

    public void testStringLiteral() {
        check("select n''=_iso_8859-1'abc' from values(true)");
        check("select N'f'<>'''' from values(true)");
    }

    public void testArthimeticOperators() {
        checkExp("pow(2,3)");
        checkExp("aBs(-2.3e-2)");
        checkExp("MOD(5             ,\t\f\r\n2)");
        checkExp("ln(5.43  )");
        checkExp("log(- -.2  )");
    }

    public void testArthimeticOperatorsTypes() {
        checkType("pow(2,3)","INTEGER");
        checkType("aBs(-2.3e-2)","DOUBLE");
        checkType("MOD(5,2)","INTEGER");
        checkType("ln(5.43  )","DOUBLE");
        checkType("log(- -.2  )","DOUBLE");
    }

    public void testArthimeticOperatorsFails() {
        checkExpFails("pow(2,'abc')","(?s).*Can not apply 'POW' to arguments of type 'POW.<INTEGER>, <VARCHAR.3.>.*");
        checkExpFails("pow(true,1)","(?s).*Can not apply 'POW' to arguments of type 'POW.<BOOLEAN>, <INTEGER>.*");
        checkExpFails("mod(b'11001',1)","(?s).*Can not apply 'MOD' to arguments of type 'MOD.<BIT.5.>, <INTEGER>.*");
        checkExpFails("mod(1, b'11001')","(?s).*Can not apply 'MOD' to arguments of type 'MOD.<INTEGER>, <BIT.5.>.*");
        checkExpFails("abs(x'')","(?s).*Can not apply 'ABS' to arguments of type 'ABS.<VARBINARY.0.>.*");
        checkExpFails("ln(x'f')","(?s).*Can not apply 'LN' to arguments of type 'LN.<BIT.4.>.*");
        checkExpFails("log(x'f')","(?s).*Can not apply 'LOG' to arguments of type 'LOG.<BIT.4.>.*");
    }

    public void testCaseExpression(){
        checkExp("case 1 when 1 then 'one' end");
        checkExp("case 1 when 1 then 'one' else null end");
        checkExp("case 1 when 1 then 'one' else 'more' end");
        checkExp("case 1 when 1 then 'one' when 2 then null else 'more' end");
    }

    public void testCaseExpressionTypes(){
        checkType("case 1 when 1 then 'one' else 'more' end","VARCHAR(4)");
        checkType("case when 2<1 then 'impossible' end","VARCHAR(10)");
        checkType("case 'one' when 'two' then 2.00 when 'one' then 1 else 3 end","DECIMAL(3, 2)");
        checkType("case 1 when 1 then 'one' when 2 then null else 'more' end","VARCHAR(4)");
    }

    public void testCaseExpressionFails(){
        //varchar not comparable with bit string
        checkExpFails("case 'string' when b'01' then 'zero one' else 'something' end",
                      "(?s).*Can not apply '=' to arguments of type '<VARCHAR.6.> = <BIT.2.>'.*");
        //all thens and else return null
        checkExpFails("case 1 when 1 then null else null end",
                      "(?s).*ELSE clause or at least one THEN clause must be non-NULL.*");
        //all thens and else return null
        checkExpFails("case 1 when 1 then null end",
                      "(?s).*ELSE clause or at least one THEN clause must be non-NULL.*");
    }

    public void testNullIf(){
        checkExp("nullif(1,2)");
        checkType("nullif(1,2)","INTEGER");
    }

    public void testCoalesce(){
        checkExp("coalesce('a','b')");
        checkType("coalesce('a','b','c')","VARCHAR(1)");
    }

    public void testStringCompare() {
        checkExp("'a' = 'b'");
        checkExp("'a' <> 'b'");
        checkExp("'a' > 'b'");
        checkExp("'a' < 'b'");
        checkExp("'a' >= 'b'");
        checkExp("'a' <= 'b'");
    }

    public void testStringCompareType() {
        checkType("'a' = 'b'", "BOOLEAN");
        checkType("'a' <> 'b'", "BOOLEAN");
        checkType("'a' > 'b'", "BOOLEAN");
        checkType("'a' < 'b'", "BOOLEAN");
        checkType("'a' >= 'b'", "BOOLEAN");
        checkType("'a' <= 'b'", "BOOLEAN");
    }

    public void testConcat() {
        checkExp("'a'||'b'");
        checkExp("b'1'||b'1'");
        checkExp("x'1'||x'1'");
//        checkType("'a'||'b'", "VARCHAR(2)"); //todo
        checkExp("_iso-8859-6'a'||_iso-8859-6'b'||_iso-8859-6'c'");
    }

    public void testConcatWithCharset() {
        checkCharset("_iso-8859-6'a'||_iso-8859-6'b'||_iso-8859-6'c'",Charset.forName("ISO-8859-6"));
    }

    public void testConcatFails() {
        checkExpFails("'a'||x'ff'","(?s).*Can not apply '\\|\\|' to arguments of type '<VARCHAR.1.> \\|\\| <VARBINARY.1.>'"+
                              ".*Supported form.s.: '<VARCHAR> \\|\\| <VARCHAR>'"+
                              ".*'<BIT> \\|\\| <BIT>'"+
                              ".*'<BINARY> \\|\\| <BINARY>'"+
                              ".*'<VARBINARY> \\|\\| <VARBINARY>'.*");
    }

    public void testCharsetMismatch() {
        checkExpFails("''=_shift_jis''", "(?s).*Can not apply .* to the two differnet charsets.*");
        checkExpFails("''<>_shift_jis''", "(?s).*Can not apply .* to the two differnet charsets.*");
        checkExpFails("''>_shift_jis''", "(?s).*Can not apply .* to the two differnet charsets.*");
        checkExpFails("''<_shift_jis''", "(?s).*Can not apply .* to the two differnet charsets.*");
        checkExpFails("''<=_shift_jis''", "(?s).*Can not apply .* to the two differnet charsets.*");
        checkExpFails("''>=_shift_jis''", "(?s).*Can not apply .* to the two differnet charsets.*");
        checkExpFails("''||_shift_jis''", "(?s).*Can not apply .* to the two differnet charsets.*");
        checkExpFails("'a'||'b'||_iso-8859-6'c'", "(?s).*Can not apply .* to the two differnet charsets.*");
    }

    public void testSimpleCollate(){
        checkExp("'s' collate latin1$en$1");
        checkType("'s' collate latin1$en$1","VARCHAR(1)");
        checkCollation("'s'","ISO-8859-1$en_US$primary", SqlCollation.Coercibility.Coercible);
        checkCollation("'s' collate latin1$sv$3","ISO-8859-1$sv$3", SqlCollation.Coercibility.Explicit);
    }

    public void _testCharsetAndCollateMismatch(){
        //todo
        checkExpFails("_shift_jis's' collate latin1$en$1","?");
    }

    public void testDyadicCollateCompare(){
        checkExp("'s' collate latin1$en$1 < 't'");
        checkExp("'t' > 's' collate latin1$en$1");
        checkExp("'s' collate latin1$en$1 <> 't' collate latin1$en$1");
    }

    public void testDyadicCompareCollateFails(){
        //two different explicit collations. difference in strength
        checkExpFails("'s' collate latin1$en$1 <= 't' collate latin1$en$2",
                      "(?s).*Two explicit different collations.*are illegal.*");
        //two different explicit collations. difference in language
        checkExpFails("'s' collate latin1$sv$1 >= 't' collate latin1$en$1",
                      "(?s).*Two explicit different collations.*are illegal.*");
    }

    public void testDyadicCollateOperator() {
        checkCollation("'a' || 'b'","ISO-8859-1$en_US$primary", SqlCollation.Coercibility.Coercible);
        checkCollation("'a' collate latin1$sv$3 || 'b'","ISO-8859-1$sv$3", SqlCollation.Coercibility.Explicit);
        checkCollation("'a' collate latin1$sv$3 || 'b' collate latin1$sv$3",
                       "ISO-8859-1$sv$3", SqlCollation.Coercibility.Explicit);
    }

    public void testCharLength() {
        checkExp("char_length('string')");
        checkExp("char_length(_shift_jis'string')");
        checkExp("character_length('string')");
        checkType("char_length('string')","INTEGER");
        checkType("character_length('string')","INTEGER");
    }

    public void testUpperLower(){
        checkExp("upper(_shift_jis'sadf')");
        checkExp("lower(n'sadf')");
        checkType("lower('sadf')","VARCHAR(4)");
        checkExpFails("upper(123)","(?s).*Can not apply 'UPPER' to arguments of type 'UPPER.<INTEGER>.'.*");
    }

    public void testPosition() {
        checkExp("position('mouse' in 'house')");
        checkExp("position(b'1' in b'1010')");
        checkExp("position(x'1' in x'110')");
        checkType("position('mouse' in 'house')","INTEGER");
        //review wael 29 March 2004: is x'1' (hexstring) and x'1010' (bytestring) a type mismatch?
        checkExpFails("position(x'1' in x'1010')",
                      "(?s).*Can not apply 'POSITION' to arguments of type 'POSITION.<BIT.4.>, <VARBINARY.2.>.'.*");
        checkExpFails("position(x'1' in '110')",
                      "(?s).*Can not apply 'POSITION' to arguments of type 'POSITION.<BIT.4.>, <VARCHAR.3.>.'.*");
    }

    public void testTrim() {
        checkExp("trim('mustache' FROM 'beard')");
        checkExp("trim(both 'mustache' FROM 'beard')");
        checkExp("trim(leading 'mustache' FROM 'beard')");
        checkExp("trim(trailing 'mustache' FROM 'beard')");
        checkType("trim('mustache' FROM 'beard')","VARCHAR(5)");
        //todo checkCollation("trim('mustache' FROM 'beard')","VARCHAR(5)",...);
    }

    public void testTrimFails(){
        checkExpFails("trim(123 FROM 'beard')","(?s).*Can not apply 'TRIM' to arguments of type.*");
        checkExpFails("trim('a' FROM 123)","(?s).*Can not apply 'TRIM' to arguments of type.*");
        checkExpFails("trim('a' FROM 'b' collate latin1$sv)","(?s).*is not comparable to.*");
    }

    public void _testConvertAndTranslate() {
        checkExp("convert('abc' using conversion)");
        checkExp("translate('abc' using translation)");
    }

    public void testOverlay() {
        checkExp("overlay('ABCdef' placing 'abc' from 1)");
        checkExp("overlay('ABCdef' placing 'abc' from 1 for 3)");
        //todo checkCollation("overlay('ABCdef' placing 'abc' collate latin1$sv from 1 for 3)",
        //               "ISO-8859-1$sv", SqlCollation.COERCIBILITY_EXPLICIT);
    }

    public void testSubstring() {
        checkExp("substring('a' FROM 1)") ;
        checkExp("substring('a' FROM 1 FOR 3)") ;
        checkExp("substring('a' FROM 'reg' FOR '\\')") ;
        checkExp("substring(b'0' FROM 1  FOR 2)") ; //bit string
        checkExp("substring(x'f' FROM 1  FOR 2)") ; //hexstring
        checkExp("substring(x'ff' FROM 1  FOR 2)") ; //binary string

        checkType("substring('10' FROM 1  FOR 2)","VARCHAR(2)");
        checkType("substring('10' FROM '1'  FOR 'w')","VARCHAR(2)");
        checkType("substring(b'10' FROM 1  FOR 2)","BIT(2)");
        checkType("substring(x'10' FROM 1  FOR 2)","VARBINARY(1)");

        checkCharset("substring('10' FROM 1  FOR 2)",Charset.forName("latin1"));
        checkCharset("substring(_shift_jis'10' FROM 1  FOR 2)",Charset.forName("SHIFT_JIS"));
    }

    public void testSubstringFails() {
        checkExpFails("substring('a' from 1 for 'b')","(?s).*Can not apply 'SUBSTRING' to arguments of type.*");
        checkExpFails("substring(_shift_jis'10' FROM '0' FOR '\\')","(?s).* not comparable to eachother.*");
        checkExpFails("substring('10' FROM '0' collate latin1$sv FOR '\\')","(?s).* not comparable to eachother.*");
        checkExpFails("substring('10' FROM '0' FOR '\\' collate latin1$sv)","(?s).* not comparable to eachother.*");
    }

    public void testDateTime() {
        // LOCAL_TIME
        checkExp("LOCALTIME(3)");
        checkExp("LOCALTIME");                     //    fix sqlcontext later.
        checkExpFails("LOCALTIME(1+2)","Argument to function 'LOCALTIME' must be a literal") ;
        checkExpFails("LOCALTIME()","Function 'LOCALTIME' does not exist");
        checkType("LOCALTIME","TIME"); //  NOT NULL, with TZ ?
        checkExpFails("LOCALTIME(-1)", "Argument to function 'LOCALTIME' must be a literal"); // i guess -s1 is an expression?
// this next one fails because i can't get the error string to match.  dunno why.
//        checkExpFails("LOCALTIME('foo')","Validation Error: Can not apply 'LOCALTIME' to arguments of type 'LOCALTIME(<VARCHAR(3)>)'. Supported form(s): 'LOCALTIME(<INTEGER>)'");

        // LOCALTIMESTAMP
        checkExp("LOCALTIMESTAMP(3)");
        checkExp("LOCALTIMESTAMP");                     //    fix sqlcontext later.
        checkExpFails("LOCALTIMESTAMP(1+2)","Argument to function 'LOCALTIMESTAMP' must be a literal") ;
        checkExpFails("LOCALTIMESTAMP()","Function 'LOCALTIMESTAMP' does not exist");
        checkType("LOCALTIMESTAMP","TIMESTAMP"); //  NOT NULL, with TZ ?
        checkExpFails("LOCALTIMESTAMP(-1)", "Argument to function 'LOCALTIMESTAMP' must be a literal"); // i guess -s1 is an expression?
//        checkExpFails("LOCALTIMESTAMP('foo')","Validation Error: Can not apply 'LOCALTIMESTAMP' to arguments of type 'LOCALTIMESTAMP(<VARCHAR(3)>)'. " +
    //            "Supported form(s): 'LOCALTIMESTAMP(<INTEGER>)'");

        // CURRENT_DATE
        checkExpFails("CURRENT_DATE(3)","Function 'CURRENT_DATE' does not exist");
        checkExp("CURRENT_DATE");                     //    fix sqlcontext later.
        checkExpFails("CURRENT_DATE(1+2)","Function 'CURRENT_DATE' does not exist") ;
        checkExp("CURRENT_DATE()"); // FIXME: works, but shouldn't
        checkType("CURRENT_DATE","DATE"); //  NOT NULL, with TZ?
        checkExpFails("CURRENT_DATE(-1)", "Function 'CURRENT_DATE' does not exist"); // i guess -s1 is an expression?
 //       checkExpFails("CURRENT_DATE('foo')","Validation Error: Can not apply 'CURRENT_DATE' to arguments of type 'CURRENT_DATE(<VARCHAR(3)>)'. " +
 //               "Supported form(s): 'CURRENT_DATE(<INTEGER>)'");

        // current_time
        checkExp("current_time(3)");
        checkExp("current_time");                     //    fix sqlcontext later.
        checkExpFails("current_time(1+2)","Argument to function 'CURRENT_TIME' must be a literal") ;
        checkExpFails("current_time()","Function 'CURRENT_TIME' does not exist");
        checkType("current_time","TIME"); //  NOT NULL, with TZ ?
        checkExpFails("current_time(-1)", "Argument to function 'CURRENT_TIME' must be a literal"); // i guess -s1 is an expression?
  //      checkExpFails("current_time('foo')","Validation Error: Can not apply 'CURRENT_TIME' to arguments of type 'CURRENT_TIME(<VARCHAR(3)>)'. " +
 //               "Supported form(s): 'CURRENT_TIME(<INTEGER>)'");

        // current_timestamp
        checkExp("CURRENT_TIMESTAMP(3)");
        checkExp("CURRENT_TIMESTAMP");                     //    fix sqlcontext later.
        checkExpFails("CURRENT_TIMESTAMP(1+2)","Argument to function 'CURRENT_TIMESTAMP' must be a literal") ;
        checkExpFails("CURRENT_TIMESTAMP()","Function 'CURRENT_TIMESTAMP' does not exist");
        checkType("CURRENT_TIMESTAMP","TIMESTAMP"); //  NOT NULL, with TZ ?
        checkExpFails("CURRENT_TIMESTAMP(-1)", "Argument to function 'CURRENT_TIMESTAMP' must be a literal"); // i guess -s1 is an expression?
  //      checkExpFails("CURRENT_TIMESTAMP('foo')","Validation Error: Can not apply 'CURRENT_TIMESTAMP' to arguments of type 'CURRENT_TIMESTAMP(<VARCHAR(3)>)'. " +
 //               "Supported form(s): 'CURRENT_TIMESTAMP(<INTEGER>)'");

       // Date literals
        checkExp("DATE '2004-12-01'");
        checkExp("TIME '12:01:01'");
        checkExp("TIMESTAMP '2004-12-01 12:01:01'");

        // REVIEW: Can't think of any date/time/ts literals that will parse, but not validate.

    }

    public void testInvalidFunction() {
        checkExpFails("foo()", ".*Function 'FOO' does not exist.*");
    }

}



// End SqlValidatorTestCase.java
