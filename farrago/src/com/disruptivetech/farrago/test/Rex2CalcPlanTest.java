/*
// $Id$
// Farrago is a relational database management system.
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
package com.disruptivetech.farrago.test;

import com.disruptivetech.farrago.calc.RexToCalcTranslator;

import java.io.*;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import junit.framework.*;

import net.sf.farrago.jdbc.engine.*;
import net.sf.farrago.query.*;
import net.sf.farrago.session.*;
import net.sf.farrago.test.*;

import openjava.mop.*;
import openjava.ptree.ClassDeclaration;
import openjava.ptree.MemberDeclarationList;
import openjava.ptree.ModifierList;

import org.eigenbase.oj.util.JavaRexBuilder;
import org.eigenbase.oj.util.OJUtil;
import org.eigenbase.rel.FilterRel;
import org.eigenbase.rel.ProjectRel;
import org.eigenbase.rel.RelNode;
import org.eigenbase.relopt.RelOptConnection;
import org.eigenbase.reltype.RelDataTypeFactory;
import org.eigenbase.reltype.RelDataTypeFactoryImpl;
import org.eigenbase.rex.RexNode;
import org.eigenbase.rex.RexTransformer;
import org.eigenbase.runtime.SyntheticObject;
import org.eigenbase.sql.fun.*;
import org.eigenbase.sql.SqlNode;
import org.eigenbase.sql.SqlOperatorTable;
import org.eigenbase.sql.SqlValidator;
import org.eigenbase.sql.parser.SqlParseException;
import org.eigenbase.sql.parser.SqlParser;
import org.eigenbase.sql2rel.SqlToRelConverter;
import org.eigenbase.util.SaffronProperties;
import org.eigenbase.util.Util;


/**
 * Validates that rex expressions get correctly translated to a correct
 * calculator program
 *
 * @author Wael Chatila
 * @since Feb 3, 2004
 * @version $Id$
 **/
public class Rex2CalcPlanTest extends FarragoTestCase
{
    //~ Static fields/initializers --------------------------------------------

    private static final String NL = System.getProperty("line.separator");
    private static TestContext testContext;

    //~ Constructors ----------------------------------------------------------

    public Rex2CalcPlanTest(String testName)
        throws Exception
    {
        super(testName);
    }

    //~ Methods ---------------------------------------------------------------

    protected void setUp()
        throws Exception
    {
        super.setUp();
        stmt.execute("set schema 'sales'");
        FarragoJdbcEngineConnection farragoConn =
            (FarragoJdbcEngineConnection) connection;
        TestContext testContext = getTestContext();
        testContext.stmtValidator =
            farragoConn.getSession().newStmtValidator();
        testContext.stmt =
            (FarragoPreparingStmt) farragoConn.getSession().newPreparingStmt(testContext.stmtValidator);
    }

    protected void tearDown()
        throws Exception
    {
        TestContext testContext = getTestContext();
        testContext.stmt.closeAllocation();
        testContext.stmt = null;
        testContext.stmtValidator.closeAllocation();
        testContext.stmtValidator = null;
        super.tearDown();
    }

    // implement TestCase
    public static Test suite()
    {
        return wrappedSuite(Rex2CalcPlanTest.class);
    }

    //--- Helper Functions ------------------------------------------------
    private void check(
        String sql,
        boolean nullSemanics,
        boolean shortCircuit)
    {
        boolean doComments = true;
        TestContext testContext = getTestContext();
        final SqlNode sqlQuery;
        try {
            sqlQuery = new SqlParser(sql).parseQuery();
        } catch (SqlParseException e) {
            throw new AssertionFailedError(e.toString());
        }
        RelDataTypeFactory typeFactory =
            testContext.stmt.getRelOptSchema().getTypeFactory();
        final SqlValidator validator =
            new SqlValidator(
                SqlStdOperatorTable.instance(),
                testContext.stmt,
                typeFactory);
        final JavaRexBuilder rexBuilder = new JavaRexBuilder(typeFactory);
        final SqlToRelConverter converter =
            new SqlToRelConverter(validator,
                testContext.stmt.getRelOptSchema(), testContext.env,
                testContext.stmt.getPlanner(),
                testContext.stmt, rexBuilder);
        RelNode rootRel = converter.convertQuery(sqlQuery);
        assertTrue(rootRel != null);

        ProjectRel project = (ProjectRel) rootRel;
        FilterRel filter = (FilterRel) project.getInput(0);
        RexNode condition = filter.condition;
        if (nullSemanics) {
            condition =
                rexBuilder.makeCall(
                    SqlStdOperatorTable.instance().isTrueOperator,
                    condition);
            condition =
                new RexTransformer(condition, rexBuilder)
                .tranformNullSemantics();
        }
        RexToCalcTranslator translator =
            new RexToCalcTranslator(rexBuilder,
                project.getChildExps(), condition);
        translator.setGenerateShortCircuit(shortCircuit);
        translator.setGenerateComments(doComments);
        String actual = translator.getProgram(null).trim();

        // dump the generated code
        try {
            Writer writer = openTestLog();
            PrintWriter printWriter = new PrintWriter(writer);
            printWriter.println(actual);
            printWriter.close();

            // and diff it against what we expect
            diffTestLog();
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    static TestContext getTestContext()
    {
        if (testContext == null) {
            testContext = new TestContext();
        }
        return testContext;
    }

    //~ Inner Classes ---------------------------------------------------------

    /**
     * Contains context shared between unit tests.
     *
     * <p>Lots of nasty stuff to set up the Openjava environment, should be
     * removed when we're not dependent upon Openjava.</p>
     */
    static class TestContext
    {
        private FarragoSessionStmtValidator stmtValidator;
        private FarragoPreparingStmt stmt;
        Environment env;
        private int executionCount;

        TestContext()
        {
            // Nasty OJ stuff
            env = OJSystem.env;

            String packageName = getTempPackageName();
            String className = getTempClassName();
            env = new FileEnvironment(env, packageName, className);
            ClassDeclaration decl =
                new ClassDeclaration(
                    new ModifierList(ModifierList.PUBLIC),
                    className,
                    null,
                    null,
                    new MemberDeclarationList());
            OJClass clazz = new OJClass(env, null, decl);
            env.record(
                clazz.getName(),
                clazz);
            env = new ClosedEnvironment(clazz.getEnvironment());
            OJUtil.threadDeclarers.set(clazz);
        }

        protected static String getClassRoot()
        {
            return SaffronProperties.instance().classDir.get(true);
        }

        protected String getTempClassName()
        {
            return "Dummy_"
                + Integer.toHexString(this.hashCode() + executionCount++);
        }

        protected static String getJavaRoot()
        {
            return SaffronProperties.instance().javaDir.get(getClassRoot());
        }

        protected String getTempPackageName()
        {
            return SaffronProperties.instance().packageName.get();
        }
    }

    //--- Tests ------------------------------------------------
    public void testSimplePassThroughFilter() {
        String sql="SELECT empno,empno FROM emps WHERE empno > 10";
        check(sql, false,false);
    }

    public void testSimplyEqualsFilter()
    {
        String sql="select empno from emps where empno=123";
        check(sql, true,false);
    }

    public void testSimplyEqualsFilterWithComments()
    {
        String sql="select empno from emps where name='Wael' and 123=0";
        check(sql, false,false);
    }

    public void testSimplyEqualsFilterShortCircuit()
    {
        String sql="select empno from emps where empno=123";
        check(sql, true,true);
    }

    public void testBooleanExpressions() {
        //AND has higher precedence than OR
        String sql="SELECT empno FROM emps WHERE true and not true or false and (not true and true)";
        check(sql,false,false);
    }

    public void testScalarExpression() {
        String sql="SELECT 2-2*2+2/2-2  FROM emps WHERE empno > 10";
        check(sql, true,false);
    }



    public void testMixedExpression() {
        String sql="SELECT name, 2*2  FROM emps WHERE name = 'Fred' AND  empno > 10";
        check(sql,true,false);
    }

    public void testNumbers() {
        String sql="SELECT " +
            "-(1+-2.*-3.e-1/-.4)>=+5, " +
            " 1e200 / 0.4" +
            "FROM emps WHERE empno > 10";
        check(sql, false,false);
    }

    public void testHexBitBinaryString() {
        String sql = "SELECT x'abc'=x'', b''=B'00111', X'0001'=x'FFeeDD' FROM emps WHERE empno > 10";
        check(sql, false,false);
    }

    public void testStringLiterals() {
        String sql= "SELECT n'aBc',_iso_8859-1'', 'abc' FROM emps WHERE empno > 10";
        check(sql, false,false);
    }

    public void testSimpleCompare() {
        String sql = "SELECT "+
            "1<>2" +
            ",1=2 is true is false is null is unknown"+
            ",true is not true "+
            ",true is not false"+
            ",true is not null "+
            ",true is not unknown"+
            " FROM emps WHERE empno > 10";
        check(sql, false,false);
    }

    public void testArithmeticOperators() {
        String sql = "SELECT POW(1.0,1.0), MOD(1,1), ABS(5000000000), ABS(1), " +
            "ABS(1.1), LN(1), LOG(1) FROM emps WHERE empno > 10";
        check(sql, false,false);
    }

    public void testFunctionInFunction() {
        String sql = "SELECT POW(3.0, ABS(2)+1) FROM emps WHERE empno > 10";
        check(sql, false,false);
    }

    // FIXME jvs 26-Jan-2005:  disabled because of calculator
    // assertion after I changed the type of string literals from
    // VARCHAR to CHAR (see dtbug 278)
    public void _testCaseExpressions() {
        String sql = "SELECT case 1+1 when 1 then 'wael' when 2 then 'waels clone' end" +
            ",case when 1=1 then 1+1+2 else 2+10 end" +
            " FROM emps WHERE empno > 10";
        check(sql, false,false);
    }

    public void testNullifExpression() {
        String sql = "SELECT nullif(1,2) "+
            " FROM emps WHERE empno > 10";
        check(sql, false,false);
    }

    public void testCoalesce() {
        String sql = "SELECT coalesce(1,2,3) FROM emps WHERE empno > 10";
        //CASE WHEN 1 IS NOT NULL THEN 1 ELSE (CASE WHEN 2 IS NOT NULL THEN 2 ELSE 3) END
        check(sql, false,false);
    }

    public void testCase1() {
        String sql = "SELECT CASE WHEN TRUE THEN 0 ELSE 1 END " +
            "FROM emps WHERE empno > 10";
        check(sql, false,false);
    }

    public void testCase2() {
        String sql1 = "SELECT CASE WHEN slacker then 2 when TRUE THEN 0 ELSE 1 END " +
            "FROM emps WHERE empno > 10";
        check(sql1, false,false);
    }

    public void testCase3() {
        String sql2= "select CASE 1 WHEN 1 THEN cast(null as integer) else 0 END " +
            "FROM emps WHERE empno > 10";
        check(sql2, false,false);
    }

    public void testCharEq() {
        checkCharOp("=", "EQ");
    }

    public void testCharNe() {
        checkCharOp("<>", "NE");
    }

    public void testCharGt() {
        checkCharOp(">", "GT");
    }

    public void testCharLt() {
        checkCharOp("<", "LT");
    }

    public void testCharGe() {
        checkCharOp(">=", "GE");
    }

    public void testCharLe() {
        checkCharOp("<=", "LE");
    }

    private void checkCharOp(final String op, final String instr) {
        String sql = "SELECT 'a' " + op +
            "'b' collate latin1$sv$1 FROM emps WHERE empno > 10";
        // FIXME jvs 3-Feb-2005:  disabled due to dtbug 280
        if (false) {
            check(sql, false,false);
        }
    }

    public void testBinaryGt() {
        checkBinaryOp(">", "GT");
    }

    private void checkBinaryOp(final String op, final String instr) {
        String sql = "SELECT x'ff' " + op +
            "x'01' FROM emps WHERE empno > 10";
        check(sql, false,false);
    }

    public void testStringFunctions() {
        String sql =
            "SELECT " +
            "char_length('a')," +
            "upper('a')," +
            "lower('a')," +
            "position('a' in 'a')," +
            "trim('a' from 'a')," +
            "overlay('a' placing 'a' from 1)," +
            "substring('a' from 1)," +
//                "substring(cast('a' as char(2)) from 1)," + todo uncomment this once cast char to varchar works
            "substring('a' from 1 for 10)," +
            "substring('a' from 'a' for '\\' )," +
            "'a'||'a'||'b'" +
            " FROM emps WHERE empno > 10";
        check(sql, false,false);
    }

    public void testPosition() {
        String sql =
            "SELECT " +
            "position('a' in  cast('a' as char(1)))," +
            "position('a' in  cast('ba' as char(2)))," +
            "position(cast('abc' as char(3)) in 'bbabc')" +
            " FROM emps WHERE empno > 10";
        check(sql, false,false);
    }

    public void testOverlay() {
        String sql =
            "SELECT " +
            "overlay('12' placing cast('abc' as char(3)) from 1)," +
            "overlay(cast('12' as char(3)) placing 'abc' from 1)" +
            " FROM emps WHERE empno > 10";
        check(sql, false,false);
    }

    public void testLikeAndSimilar() {
        String sql =
            "SELECT "+
            "'a' like 'b'" +
            ",'a' like 'b' escape 'c'" +
            ",'a' similar to 'a'" +
            ",'a' similar to 'a' escape 'c'" +
            " FROM emps WHERE empno > 10";
        check(sql, false,false);
    }

    public void testBetween1() {
        String sql1="select empno  from emps where empno between 40 and 60";
        check(sql1, false,false);
    }

    public void testBetween2() {
        String sql2="select empno  from emps where empno between asymmetric 40 and 60";
        check(sql2, false,false);
    }

    public void testBetween3() {
        String sql3 ="select empno  from emps where empno not between 40 and 60";
        check(sql3, false,false);
    }

    public void testBetween4() {
        String sql4 ="select empno  from emps where empno between symmetric 40 and 60";
        check(sql4, false,false);
    }

    public void testCastNull() {
        String sql =
            "SELECT " +
            "cast(null as varchar(1))" +
            ", cast(null as integer)" +
            " FROM emps WHERE empno > 10";
       check(sql, false,false);
    }

    public void testJdbcFunctionSyntax() {
        String sql =
            "SELECT " +
            "{fn log(1.0)}" +
            " FROM emps WHERE empno > 10";
        check(sql, false,false);
    }

    public void testMixingTypes() {
        String sql =
            "SELECT " +
            "1+1.0" +
            " FROM emps WHERE empno > 10";
        check(sql, false,false);
    }

    public void testCastCharTypesToNumbersAndBack() {
        String sql =
            "SELECT " +
            "cast(123 as varchar(3))" +
            ",cast(123 as char(3))" +
            ",cast(12.3 as varchar(3))" +
            ",cast(12.3 as char(3))" +
            ",cast('123' as bigint)" +
            ",cast('123' as tinyint)" +
            ",cast('123' as double)" +
            " FROM emps WHERE empno > 10";
        check(sql, false,false);
    }


}


// End Rex2CalcPlanTest.java
