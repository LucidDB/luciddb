/*
// $Id$
// Saffron preprocessor and data engine
// (C) Copyright 2004-2004 Disruptive Technologies, Inc.
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
import junit.framework.AssertionFailedError;
import net.sf.saffron.sql2rel.SqlToRelConverter;
import net.sf.saffron.core.*;
import net.sf.saffron.oj.OJTypeFactoryImpl;
import net.sf.saffron.oj.OJPlannerFactory;
import net.sf.saffron.oj.util.JavaRexBuilder;
import net.sf.saffron.oj.util.OJUtil;
import net.sf.saffron.opt.*;
import net.sf.saffron.sql.SqlNode;
import net.sf.saffron.sql.SqlValidator;
import net.sf.saffron.sql.SqlOperatorTable;
import net.sf.saffron.sql.parser.SqlParser;
import net.sf.saffron.sql.parser.ParseException;
import net.sf.saffron.rel.SaffronRel;
import net.sf.saffron.rel.FilterRel;
import net.sf.saffron.rel.ProjectRel;
import net.sf.saffron.util.Util;
import net.sf.saffron.util.SaffronProperties;
import net.sf.saffron.jdbc.SaffronJdbcConnection;
import net.sf.saffron.runtime.SyntheticObject;
import net.sf.saffron.rex.RexNode;
import net.sf.saffron.rex.RexTransformer;

import java.io.*;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import openjava.mop.*;
import openjava.ptree.util.ClassMap;
import openjava.ptree.*;

/**
 * Validates that rex expressions gets correctly translated to a correct calculator program
 *
 * @author wael
 * @since Feb 3, 2004
 * @version $Id$
 **/


public class Rex2CalcPlanTestCase extends TestCase
{
    private static final String NL = System.getProperty("line.separator");
    private static TestContext testContext;


    protected void setUp() throws Exception {
        super.setUp();
        // Create a type factory.
        SaffronTypeFactory typeFactory =
            SaffronTypeFactoryImpl.threadInstance();
        if (typeFactory == null) {
            typeFactory = new OJTypeFactoryImpl();
            typeFactory = new SaffronTypeFactoryImpl();
        }
        // And a planner factory.
        if (VolcanoPlannerFactory.threadInstance() == null) {
            VolcanoPlannerFactory.setThreadInstance(new OJPlannerFactory());
        }
    }

    //--- Helper Functions ------------------------------------------------
    private void check(String sql, String expectedProgram, boolean nullSemanics, boolean shortCircuit)
    {
        TestContext testContext = getTestContext();
        final SqlNode sqlQuery;
        try {
            sqlQuery = new SqlParser(sql).parseQuery();
        } catch (ParseException e) {
            throw new AssertionFailedError(e.toString());
        }
        final SqlValidator validator =
            new SqlValidator(
                SqlOperatorTable.instance(),
                testContext.seeker,
                testContext.connection.getSaffronSchema().getTypeFactory());
        final JavaRexBuilder rexBuilder = new JavaRexBuilder(testContext.connection.getSaffronSchema().getTypeFactory());
        final SqlToRelConverter converter = new SqlToRelConverter(
            validator,
            testContext.connection.getSaffronSchema(),
            testContext.env,
            testContext.connection,
            rexBuilder);
        SaffronRel rootRel = converter.convertQuery(sqlQuery);
        assertTrue(rootRel != null);

        ProjectRel project = (ProjectRel) rootRel;
        FilterRel filter = (FilterRel) project.getInput(0);
        RexNode condition = filter.condition;
        if (nullSemanics) {
            condition = rexBuilder.makeCall(SqlOperatorTable.instance().isTrueOperator, condition);
            condition = new RexTransformer(condition, rexBuilder).tranformNullSemantics();
        }
        CalcRelImplementor implmentor = new CalcRelImplementor(rexBuilder);
        CalcRelImplementor.Rex2CalcTranslator translator = implmentor.newTranslator(
                        project.getChildExps(),
                      condition);
        translator.setGenerateShortCircuit(shortCircuit);
        String actual = translator.getProgram(null).trim();
        String expected = expectedProgram.trim();
        if (!expected.equals(actual)) {
            String message = "expected:<" + expected + ">" + NL + "but was:<"
                + actual + ">";

            String fileName = "CalcImpl_"+this.getClass().toString();
            try {
                OutputStreamWriter o = new OutputStreamWriter(new FileOutputStream(fileName+"_actual"));
                o.write(actual,0,actual.length());
                o.close();
                o = new OutputStreamWriter(new FileOutputStream(fileName+"_expected"));
                o.write(expected,0,expected.length());
                o.close();
            } catch(IOException ignored) {}

            fail(message);
        }

    }

    static TestContext getTestContext()
    {
        if (testContext == null) {
            testContext = new TestContext();
        }
        return testContext;
    }

    /**
     * Contains context shared between unit tests.
     *
     * <p>Lots of nasty stuff to set up the Openjava environment, should be
     * removed when we're not dependent upon Openjava.</p>
     */
    static class TestContext
    {
        private final SqlValidator.CatalogReader seeker;
        private final Connection jdbcConnection;
        private final SaffronConnection connection;
        Environment env;
        private int executionCount;

        TestContext()
        {
            try {
                Class.forName("net.sf.saffron.jdbc.SaffronJdbcDriver");
            } catch (ClassNotFoundException e) {
                throw Util.newInternal(e, "Error loading JDBC driver");
            }
            try {
                jdbcConnection =
                    DriverManager.getConnection(
                        "jdbc:saffron:schema=sales.SalesInMemory");
            } catch (SQLException e) {
                throw Util.newInternal(e);
            }
            connection = ((SaffronJdbcConnection) jdbcConnection).saffronConnection;
            seeker = new SqlToRelConverter.SchemaCatalogReader(connection.getSaffronSchema(), false);
            // Nasty OJ stuff
            env = OJSystem.env;

            // compiler and class map must have same life-cycle, because
            // DynamicJava's compiler contains a class loader
            if (ClassMap.instance() == null) {
                ClassMap.setInstance(new ClassMap(SyntheticObject.class));
            }
            String packageName = getTempPackageName();
            String className = getTempClassName();
            env = new FileEnvironment(env,packageName,className);
            ClassDeclaration decl =
                new ClassDeclaration(
                    new ModifierList(ModifierList.PUBLIC),
                    className,
                    null,
                    null,
                    new MemberDeclarationList());
            OJClass clazz = new OJClass(env,null,decl);
            env.record(clazz.getName(),clazz);
            env = new ClosedEnvironment(clazz.getEnvironment());

            // Ensure that the thread has factories for types and planners. (We'd
            // rather that the client sets these.)
            SaffronTypeFactory typeFactory =
                SaffronTypeFactoryImpl.threadInstance();
            if (typeFactory == null) {
                typeFactory = new OJTypeFactoryImpl();
                SaffronTypeFactoryImpl.setThreadInstance(typeFactory);
            }
            if (VolcanoPlannerFactory.threadInstance() == null) {
                VolcanoPlannerFactory.setThreadInstance(new OJPlannerFactory());
            }

            OJUtil.threadDeclarers.set(clazz);

        }

        protected static String getClassRoot()
        {
            return SaffronProperties.instance().classDir.get(true);
        }

        protected String getTempClassName()
        {
            return
                "Dummy_" + Integer.toHexString(this.hashCode() + executionCount++);
        }

        protected static String getJavaRoot()
        {
            return SaffronProperties.instance().javaDir.get(
                getClassRoot());
        }

        protected String getTempPackageName()
        {
            return SaffronProperties.instance().packageName.get();
        }

    }
    //--- Tests ------------------------------------------------
    public void testSimplyEqualsFilter()
    {
        String sql="select \"empno\" from \"emps\" where \"empno\"=123";
        String prg =
                "O s4;" + NL +
                "I s4;" + NL +
                "L bo, bo, bo;" + NL +
                "S bo;" + NL +
                "C bo, bo, s8;" + NL +
                "V 1, 0, 123;" + NL +
                "T;" + NL +
                "ISNOTNULL L0, I0;" + NL +
                "EQ L1, I0, C2;" + NL +
                "AND L2, L0, L1;" + NL +
                "JMPT @7, L2;" + NL +
                "MOVE S0, C0;" + NL +
                "RETURN;" + NL +
                "MOVE S0, C1;" + NL +
                "MOVE O0, I0;" + NL +
                "RETURN;" + NL;
        check(sql, prg,true,false);
    }

    public void testSimplyEqualsFilterShortCircuit()
    {
        String sql="select \"empno\" from \"emps\" where \"empno\"=123";
        String prg =
                "O s4;" + NL +
                "I s4;" + NL +
                "L bo, bo, bo;" + NL +
                "S bo;" + NL +
                "C bo, bo, s8;" + NL +
                "V 1, 0, 123;" + NL +
                "T;" + NL +
                "ISNOTNULL L0, I0;" + NL +
                "JMPF @5, L0;" + NL +
                "EQ L1, I0, C2;" + NL +
                "MOVE L2, L1;" + NL +
                "JMP @6;" + NL +
                "MOVE L2, C1;" + NL +
                "JMPT @10, L2;" + NL +
                "MOVE S0, C0;" + NL +
                "RETURN;" + NL +
                "MOVE S0, C1;" + NL +
                "MOVE O0, I0;" + NL +
                "RETURN;" + NL;
        check(sql, prg,true,true);
    }

    public void testBooleanExpressions() {
        //AND has higher precedence than OR
        String sql="SELECT \"empno\" FROM \"emps\" WHERE true and not true or false and (not true and true)";
        String prg =
                "O s4;" + NL +
                "I s4;" + NL +
                "L bo, bo, bo, bo, bo;" + NL +
                "S bo;" + NL +
                "C bo, bo;" + NL +
                "V 1, 0;" + NL +
                "T;" + NL +
                "NOT L0, C0;" + NL +
                "AND L1, C0, L0;" + NL +
                "AND L2, L0, C0;" + NL +
                "AND L3, C1, L2;" + NL +
                "OR L4, L1, L3;" + NL +
                "JMPT @9, L4;" + NL +
                "MOVE S0, C0;" + NL +
                "RETURN;" + NL +
                "MOVE S0, C1;" + NL +
                "MOVE O0, I0;" + NL +
                "RETURN;" + NL;
        check(sql, prg,false,false);
    }

    public void testScalarExpression() {
        String sql="SELECT 2-2*2+2/2-2  FROM \"emps\" WHERE \"empno\" > 10";
        String prg =
                "O s8;" + NL +
                "I s4;" + NL +
                "L bo, bo, bo, s8, s8, s8, s8, s8;" + NL +
                "S bo;" + NL +
                "C bo, bo, s8, s8;" + NL +
                "V 1, 0, 10, 2;" + NL +
                "T;" + NL +
                "ISNOTNULL L0, I0;" + NL +
                "GT L1, I0, C2;" + NL +
                "AND L2, L0, L1;" + NL +
                "JMPT @7, L2;" + NL +
                "MOVE S0, C0;" + NL +
                "RETURN;" + NL +
                "MOVE S0, C1;" + NL +
                "MUL L3, C3, C3;" + NL +
                "SUB L4, C3, L3;" + NL +
                "DIV L5, C3, C3;" + NL +
                "ADD L6, L4, L5;" + NL +
                "SUB L7, L6, C3;" + NL +
                "MOVE O0, L7;" + NL +
                "RETURN;" + NL;
        check(sql, prg,true,false);
    }



    public void testMixedExpression() {
        String sql="SELECT \"name\", 2*2  FROM \"emps\" WHERE \"name\" = 'Fred' AND  \"empno\" > 10";
        String prg =
                "O vc,30, s8;" + NL +
                "I vc,30, s4;" + NL +
                "L bo, bo, s4, bo, bo, bo, bo, bo, s8;" + NL +
                "S bo;" + NL +
                "C bo, bo, vc,30, vc,30, s4, s8, s8;" + NL +
                "V 1, 0, 0x46726564, 0x49534F2D383835392D3124656E5F5553247072696D617279, 0, 10, 2;" + NL +
                "T;" + NL +
                "ISNOTNULL L0, I0;" + NL +
                "CALL 'strCmpA(L2, I0, C2, C3);" + NL +
                "EQ L1, L2, C4;" + NL +
                "AND L3, L0, L1;" + NL +
                "ISNOTNULL L4, I1;" + NL +
                "GT L5, I1, C5;" + NL +
                "AND L6, L4, L5;" + NL +
                "AND L7, L3, L6;" + NL +
                "JMPT @12, L7;" + NL +
                "MOVE S0, C0;" + NL +
                "RETURN;" + NL +
                "MOVE S0, C1;" + NL +
                "MUL L8, C6, C6;" + NL +
                "MOVE O0, I0;" + NL +
                "MOVE O1, L8;" + NL +
                "RETURN;" + NL;
        check(sql, prg,true,false);
    }

     public void testNumbers() {
        String sql="SELECT -(1+-2.*-3.e-1/-.4)>=+5 FROM \"emps\" WHERE \"empno\" > 10";
         String prg =
                 "O bo;" + NL +
                 "I s4;" + NL +
                 "L bo, s8, d, d, d, d, d, d, bo;" + NL +
                 "S bo;" + NL +
                 "C bo, bo, s8, s8, s8, d, d, s8;" + NL +
                 "V 1, 0, 10, 1, 2, 0.3, 0.4, 5;" + NL +
                 "T;" + NL +
                 "GT L0, I0, C2;" + NL +         // empno > 10
                 "JMPT @5, L0;" + NL +
                 "MOVE S0, C0;" + NL +
                 "RETURN;" + NL +
                 "MOVE S0, C1;" + NL +
                 "NEG L1, C4;" + NL +            // -2
                 "NEG L2, C5;" + NL +            // -0.3
                 "MUL L3, L1, L2;" + NL +        // -2 * -0.3
                 "NEG L4, C6;" + NL +            // -0.4
                 "DIV L5, L3, L4;" + NL +        // (-2 * 0.3) / 0.4
                 "ADD L6, C3, L5;" + NL +        // 1 + ((-2 * -0.3) / -0.4)
                 "NEG L7, L6;" + NL +
                 "GE L8, L7, C7;" + NL +         // x >= 5
                 "MOVE O0, L8;" + NL +
                 "RETURN;" + NL;
        check(sql, prg,false,false);
    }

    public void testHexBitBinaryString() {
        String sql = "SELECT x'abc'=x'', b''=B'00111', X'0001'=x'FFeeDD' FROM \"emps\" WHERE \"empno\" > 10";
        String prg =
                "O bo, bo, bo;" + NL +
                "I s4;" + NL +
                "L bo, bo, bo, bo;" + NL +
                "S bo;" + NL +
                "C bo, bo, s8, vb,30, vb,30, vb,30, vb,30, vb,30, vb,30;" + NL +
                "V 1, 0, 10, '0ABC', '', '', '07', '0001', 'FFEEDD';" + NL +
                "T;" + NL +
                "GT L0, I0, C2;" + NL +         // empno > 10
                "JMPT @5, L0;" + NL +
                "MOVE S0, C0;" + NL +
                "RETURN;" + NL +
                "MOVE S0, C1;" + NL +
                "EQ L1, C3, C4;" + NL +         // x'abc' = x''
                "EQ L2, C5, C6;" + NL +         // b'' = B'00111'
                "EQ L3, C7, C8;" + NL +         // x'0001' = x'ffeedd'
                "MOVE O0, L1;" + NL +
                "MOVE O1, L2;" + NL +
                "MOVE O2, L3;" + NL +
                "RETURN;" + NL;
        check(sql, prg,false,false);
    }

    public void testStringLiterals() {
        String sql="SELECT n'aBc',_iso_8859-1'', 'abc' FROM \"emps\" WHERE \"empno\" > 10";
        String prg =
                "O vc,30, vc,30, vc,30;" + NL +
                "I s4;" + NL +
                "L bo;" + NL +
                "S bo;" + NL +
                "C bo, bo, s8, vc,30, vc,30, vc,30;" + NL +
                "V 1, 0, 10, 0x614263, 0x, 0x616263;" + NL +
                "T;" + NL +
                "GT L0, I0, C2;" + NL +         // empno > 10
                "JMPT @5, L0;" + NL +
                "MOVE S0, C0;" + NL +
                "RETURN;" + NL +
                "MOVE S0, C1;" + NL +
                "MOVE O0, C3;" + NL +
                "MOVE O1, C4;" + NL +
                "MOVE O2, C5;" + NL +
                "RETURN;" + NL;
        check(sql, prg,false,false);
    }

    public void testSimpleCompare() {
        String sql = "SELECT "+
                "1<>2" +
                ",1=2 is true is false is null is unknown"+
                ",true is not true "+
                ",true is not false"+
                ",true is not null "+
                ",true is not unknown"+
                " FROM \"emps\" WHERE \"empno\" > 10";
        String prg =
                "O bo, bo, bo, bo, bo, bo;" + NL +
                "I s4;" + NL +
                "L bo, bo, bo, bo, bo, bo, bo, bo, bo, bo, bo, bo, bo, bo, bo, bo, bo, bo;" + NL +
                "S bo;" + NL +
                "C bo, bo, s8, s8, s8;" + NL +
                "V 1, 0, 10, 1, 2;" + NL +
                "T;" + NL +
                "GT L0, I0, C2;" + NL +      // empno > 10
                "JMPT @5, L0;" + NL +
                "MOVE S0, C0;" + NL +
                "RETURN;" + NL +
                "MOVE S0, C1;" + NL +
                "NE L1, C3, C4;" + NL +      // 1 != 2
                "EQ L2, C3, C4;" + NL +          // 1 = 2
                "ISNOTNULL L3, L2;" + NL +       // X IS TRUE
                "EQ L4, L2, C0;" + NL +
                "AND L5, L3, L4;" + NL +
                "ISNOTNULL L6, L5;" + NL +       // X IS FALSE
                "EQ L7, L5, C1;" + NL +
                "AND L8, L6, L7;" + NL +
                "ISNULL L9, L8;" + NL +          // X IS NULL
                "ISNULL L10, L9;" + NL +         // X IS UNKNOWN
                "ISNOTNULL L11, C0;" + NL +      // TRUE IS NOT TRUE
                "EQ L12, C0, C0;" + NL +         // TODO optimize expressions like this
                "AND L13, L11, L12;" + NL +
                "NOT L14, L13;" + NL +
                "EQ L15, C0, C1;" + NL +         // TODO optimize expressions like this
                "AND L16, L11, L15;" + NL +
                "NOT L17, L16;" + NL +
                "MOVE O0, L1;" + NL +
                "MOVE O1, L10;" + NL +
                "MOVE O2, L14;" + NL +
                "MOVE O3, L17;" + NL +
                "MOVE O4, L11;" + NL +
                "MOVE O5, L11;" + NL +
                "RETURN;" + NL;
        check(sql, prg,false,false);
    }

    public void testArithmeticOperators() {
        String sql = "SELECT POW(1,1), MOD(1,1), ABS(1), ABS(1.1), LN(1), LOG(1) FROM \"emps\" WHERE \"empno\" > 10";
        String prg =
                "O s8, s8, s8, d, d, d;" + NL +
                "I s4;" + NL +
                "L bo, s8, s8, s8, d, d, d;" + NL +
                "S bo;" + NL +
                "C bo, bo, s8, s8, d;" + NL +
                "V 1, 0, 10, 1, 1.1;" + NL +
                "T;" + NL +
                "GT L0, I0, C2;" + NL +
                "JMPT @5, L0;" + NL +
                "MOVE S0, C0;" + NL +
                "RETURN;" + NL +
                "MOVE S0, C1;" + NL +
                "CALL 'POW(L1, C3, C3);" + NL +
                "CALL 'MOD(L2, C3, C3);" + NL +
                "CALL 'ABS(L3, C3);" + NL +
                "CALL 'ABS(L4, C4);" + NL +
                "CALL 'LN(L5, C3);" + NL +
                "CALL 'LOG(L6, C3);" + NL +
                "MOVE O0, L1;" + NL +
                "MOVE O1, L2;" + NL +
                "MOVE O2, L3;" + NL +
                "MOVE O3, L4;" + NL +
                "MOVE O4, L5;" + NL +
                "MOVE O5, L6;" + NL +
                "RETURN;" + NL;
        check(sql, prg,false,false);
    }

    public void testFunctionInFunction() {
        String sql = "SELECT POW(3, ABS(2)+1) FROM \"emps\" WHERE \"empno\" > 10";
        String prg =
                "O s8;" + NL +
                "I s4;" + NL +
                "L bo, s8, s8, s8;" + NL +
                "S bo;" + NL +
                "C bo, bo, s8, s8, s8, s8;" + NL +
                "V 1, 0, 10, 3, 2, 1;" + NL +
                "T;" + NL +
                "GT L0, I0, C2;" + NL +
                "JMPT @5, L0;" + NL +
                "MOVE S0, C0;" + NL +
                "RETURN;" + NL +
                "MOVE S0, C1;" + NL +
                "CALL 'ABS(L1, C4);" + NL +
                "ADD L2, L1, C5;" + NL +
                "CALL 'POW(L3, C3, L2);" + NL +
                "MOVE O0, L3;" + NL +
                "RETURN;" + NL;
        check(sql, prg,false,false);
    }

    public void testCaseExpressions() {
        String sql = "SELECT case 1+1 when 1 then 'wael' when 2 then 'waels clone' end" +
                         ",case when 1=1 then 1+1+2 else 2+10 end" +
                " FROM \"emps\" WHERE \"empno\" > 10";
        String prg =
                "O vc,30, s8;" + NL +
                "I s4;" + NL +
                "L bo, vc,30, s8, bo, bo, s8, bo, s8, s8;" + NL +
                "S bo;" + NL +
                "C bo, bo, s8, s8, vc,30, s8, vc,30, vc,30;" + NL +
                "V 1, 0, 10, 1, 0x7761656C, 2, 0x7761656C7320636C6F6E65, ;" + NL +
                "T;" + NL +
                "GT L0, I0, C2;" + NL +         // empno > 10
                "JMPT @5, L0;" + NL +
                "MOVE S0, C0;" + NL +
                "RETURN;" + NL +
                "MOVE S0, C1;" + NL +
                "ADD L2, C3, C3;" + NL +        // 1 + 1
                "EQ L3, L2, C3;" + NL +         // WHEN 1 + 1 = 1
                "JMPF @10, L3;" + NL +
                "MOVE L1, C4;" + NL +           // THEN
                "JMP @15;" + NL +
                "EQ L4, L2, C5;" + NL +         // WHEN 1 + 1 = 2
                "JMPF @14, L4;" + NL +
                "MOVE L1, C6;" + NL +           // THEN
                "JMP @15;" + NL +
                "MOVE L1, C7;" + NL +           // ELSE NULL
                "EQ L6, C3, C3;" + NL +         // SECOND CASE 1 = 1
                "JMPF @20, L6;" + NL +
                "ADD L7, L2, C5;" + NL +        // 1 + 1 + 2
                "MOVE L5, L7;" + NL +
                "JMP @22;" + NL +
                "ADD L8, C5, C2;" + NL +        // ELSE 2 + 10
                "MOVE L5, L8;" + NL +
                "MOVE O0, L1;" + NL +
                "MOVE O1, L5;" + NL +
                "RETURN;" + NL;
        check(sql, prg,false,false);
    }

    public void testCoalesce() {
        String sql = "SELECT coalesce(1,2,3) FROM \"emps\" WHERE \"empno\" > 10";
        //CASE WHEN 1 IS NOT NULL THEN 1 ELSE (CASE WHEN 2 IS NOT NULL THEN 2 ELSE 3) END
        String prg =
                "O s8;" + NL +
                "I s4;" + NL +
                "L bo, s8, bo, s8, bo;" + NL +
                "S bo;" + NL +
                "C bo, bo, s8, s8, s8, s8;" + NL +
                "V 1, 0, 10, 1, 2, 3;" + NL +
                "T;" + NL +
                "GT L0, I0, C2;" + NL +
                "JMPT @5, L0;" + NL +
                "MOVE S0, C0;" + NL +
                "RETURN;" + NL +
                "MOVE S0, C1;" + NL +
                "ISNOTNULL L2, C3;" + NL +
                "JMPF @9, L2;" + NL +
                "MOVE L1, C3;" + NL +
                "JMP @15;" + NL +
                "ISNOTNULL L4, C4;" + NL +
                "JMPF @13, L4;" + NL +
                "MOVE L3, C4;" + NL +
                "JMP @14;" + NL +
                "MOVE L3, C5;" + NL +
                "MOVE L1, L3;" + NL +
                "MOVE O0, L1;" + NL +
                "RETURN;" + NL;
        check(sql, prg,false,false);
    }

    public void testStringEq() {
        checkStringOp("=", "EQ");
    }

    public void testStringNe() {
        checkStringOp("<>", "NE");
    }

    public void testStringGt() {
        checkStringOp(">", "GT");
    }

    public void testStringLt() {
        checkStringOp("<", "LT");
    }

    public void testStringGe() {
        checkStringOp(">=", "GE");
    }

    public void testStringLe() {
        checkStringOp("<=", "LE");
    }

    private void checkStringOp(final String op, final String instr) {
        String sql = "SELECT 'a' " + op +
                "'b' collate latin1$sv$1 FROM \"emps\" WHERE \"empno\" > 10";
        String prg =
                "O bo;" + NL +
                "I s4;" + NL +
                "L bo, bo, s4;" + NL +
                "S bo;" + NL +
                "C bo, bo, s8, vc,30, vc,30, vc,30, s4;" + NL +
                "V 1, 0, 10, 0x61, 0x62, 0x49534F2D383835392D312473762431, 0;" + NL +
                "T;" + NL +
                "GT L0, I0, C2;" + NL +
                "JMPT @5, L0;" + NL +
                "MOVE S0, C0;" + NL +
                "RETURN;" + NL +
                "MOVE S0, C1;" + NL +
                "CALL 'strCmpA(L2, C3, C4, C5);" + NL +
                instr + " L1, L2, C6;" + NL +
                "MOVE O0, L1;" + NL +
                "RETURN;" + NL;
        check(sql, prg,false,false);
    }

    public void testStringFunctions() {
        String sql=
                   "SELECT char_length('a'),upper('a'),lower('a'),position('a' in 'a'),trim('a' from 'a')," +
                   "overlay('a' placing 'a' from 1),substring('a' from 1)," +
                   "substring('a' from 1 for 10),substring('a' from 'a' for '\\' )" +
                   ", 'a'||'a'" +
                   " FROM \"emps\" WHERE \"empno\" > 10";
        String prg =
                "O s4, vc,30, vc,30, s4, vc,30, vc,30, vc,30, vc,30, vc,30, vc,30;" + NL +
                "I s4;" + NL +
                "L bo, s4, vc,30, vc,30, s4, vc,30, vc,30, vc,30, vc,30, vc,30, vc,30;" + NL +
                "S bo;" + NL +
                "C bo, bo, s8, vc,30, vc,30, s4, s8, vc,30;" + NL +
                "V 1, 0, 10, 0x61, 0x49534F2D383835392D31, 1, 1, 0x5C;" + NL +
                "T;" + NL +
                "GT L0, I0, C2;" + NL +
                "JMPT @5, L0;" + NL +
                "MOVE S0, C0;" + NL +
                "RETURN;" + NL +
                "MOVE S0, C1;" + NL +
                "CALL 'CHAR_LENGTH(L1, C3, C4);" + NL +
                "CALL 'strToUpperA(L2, C3);" + NL +
                "CALL 'strToLowerA(L3, C3);" + NL +
                "CALL 'POSITION(L4, C3, C3);" + NL +
                "CALL 'TRIM(L5, C3, C3, C5, C5);" + NL +
                "CALL 'OVERLAY(L6, C3, C3, C6);" + NL +
                "CALL 'SUBSTRING(L7, C3, C6);" + NL +
                "CALL 'SUBSTRING(L8, C3, C6, C2);" + NL +
                "CALL 'SUBSTRING(L9, C3, C3, C7);" + NL +
                "CALL 'CONCAT(L10, C3, C3);" + NL +
                "MOVE O0, L1;" + NL +
                "MOVE O1, L2;" + NL +
                "MOVE O2, L3;" + NL +
                "MOVE O3, L4;" + NL +
                "MOVE O4, L5;" + NL +
                "MOVE O5, L6;" + NL +
                "MOVE O6, L7;" + NL +
                "MOVE O7, L8;" + NL +
                "MOVE O8, L9;" + NL +
                "MOVE O9, L10;" + NL +
                "RETURN;" + NL;
        check(sql, prg,false,false);
    }
}

// End Rex2CalcPlanTestCase.java
