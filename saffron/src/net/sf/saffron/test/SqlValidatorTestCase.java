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
        SqlNode n=validator.validate(sqlNode);
        SaffronType actualType = validator.getValidatedNodeType(((SqlNodeList)((SqlCall)n).getOperands()[1]).get(0));
        String actual = actualType.toString();
        assertEquals(expected,actual);

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
                                "(?i)more than one column has alias 'c1'");
	}

	public void testMultipleDifferentAs() {
        check("select 1 as c1,2 as c2 from values(true)");
	}

    public void testTypes(){
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
        check("select n''=_iso_8859-6'abc' from values(true)");
        check("select N'f'<>'''' from values(true)");
    }

    public void testArthimeticOperators() {
        checkExp("pow(2,3)");
        checkExp("aBs(-2.3e-2)");
        checkExp("MOD(5             ,\t\f\r\n2)");
        checkExp("ln(5.43  )");
        checkExp("log(- -.2  )");
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


}



// End SqlValidatorTestCase.java