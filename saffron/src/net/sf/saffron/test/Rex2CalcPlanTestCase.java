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
    private static final String T = ";"+NL;
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
        String actual = translator.getProgram().trim();
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
            String classRoot =
                SaffronProperties.instance().getProperty(
                    SaffronProperties.PROPERTY_saffron_class_dir);
            if (classRoot == null) {
                throw Util.newInternal(
                    "Property " + SaffronProperties.PROPERTY_saffron_class_dir
                    + " must be set");
            }
            return classRoot;
        }

        protected String getTempClassName()
        {
            return
                "Dummy_" + Integer.toHexString(this.hashCode() + executionCount++);
        }

        protected static String getJavaRoot()
        {
            return SaffronProperties.instance().getProperty(
                SaffronProperties.PROPERTY_saffron_java_dir,
                getClassRoot());
        }

        protected String getTempPackageName()
        {
            return SaffronProperties.instance().getProperty(
                SaffronProperties.PROPERTY_saffron_package_name,
                SaffronProperties.PROPERTY_saffron_package_name_DEFAULT);
        }

    }
    //--- Tests ------------------------------------------------
    public void testSimplyEqualsFilter()
    {
        String sql="select \"empno\" from \"emps\" where \"empno\"=123";
        String prg=
                "output: s4[0]"+T+
                "input: s4[0]"+T+
                "literal: u1[0]=true,u1[1]=false,s8[2]=123"+T+
                "local: u1[0],u1[1],u1[2]"+T+
                "status: u1[0]"+T+
                //"values: 1,0,123"+T+
                "T"+T+
                "ISNOTNULL T0, I0"+T+
                "EQ T1, I0, L2"+T+
                "AND T2, T0, T1"+T+
                "JMPT 6, T2"+T+
                "MOVE S0, L0"+T+
                "RETURN"+T+
                "MOVE O0, I0"+T+
                "MOVE S0, L1"+T+
                "RETURN"+T;

        check(sql, prg,true,false);
    }

    public void testSimplyEqualsFilterShortCircuit()
    {
        String sql="select \"empno\" from \"emps\" where \"empno\"=123";
        String prg=
                "output: s4[0]"+T+
                "input: s4[0]"+T+
                "literal: u1[0]=true,u1[1]=false,s8[2]=123"+T+
                "local: u1[0],u1[1],u1[2]"+T+
                "status: u1[0]"+T+
                "T"+T+
                "ISNOTNULL T0, I0"+T+
                "JMPF 5, T0"+T+
                "EQ T1, I0, L2"+T+
                "MOVE T2, T1"+T+
                "JMP 6"+T+
                "MOVE T2, L1"+T+
                "JMPT 9, T2"+T+
                "MOVE S0, L0"+T+
                "RETURN"+T+
                "MOVE O0, I0"+T+
                "MOVE S0, L1"+T+
                "RETURN"+T;

        check(sql, prg,true,true);
    }

    public void testBooleanExpressions() {
        //AND has higher precedence than OR
        String sql="SELECT \"empno\" FROM \"emps\" WHERE true and not true or false and (not true and true)";
        String prg=
                "output: s4[0]"+T+
                "input: s4[0]"+T+
                "literal: u1[0]=true,u1[1]=false"+T+
                "local: u1[0],u1[1],u1[2],u1[3],u1[4]"+T+
                "status: u1[0]"+T+
                //"values: 1, 0
                "T"+T+
                "NOT T0, L0"+T+
                "AND T1, L0, T0"+T+
                "AND T2, T0, L0"+T+
                "AND T3, L1, T2"+T+
                "OR T4, T1, T3"+T+
                "JMPT 8, T4"+T+
                "MOVE S0, L0"+T+
                "RETURN"+T+
                "MOVE O0, I0"+T+
                "MOVE S0, L1"+T+
                "RETURN"+T;
        check(sql, prg,false,false);
    }

    public void testScalarExpression() {
        String sql="SELECT 2-2*2+2/2-2  FROM \"emps\" WHERE \"empno\" > 10";
        String prg=
                "output: s8[0]"+T+
                "input: s4[0]"+T+
                "literal: u1[0]=true,u1[1]=false,s8[2]=10,s8[3]=2"+T+
                "local: u1[0],u1[1],u1[2],s8[3],s8[4],s8[5],s8[6],s8[7]"+T+
                "status: u1[0]"+T+
                "T"+T+
                "ISNOTNULL T0, I0"+T+
                "GT T1, I0, L2"+T+
                "AND T2, T0, T1"+T+
                "JMPT 6, T2"+T+
                "MOVE S0, L0"+T+
                "RETURN"+T+
                "MUL T3, L3, L3"+T+
                "SUB T4, L3, T3"+T+
                "DIV T5, L3, L3"+T+
                "ADD T6, T4, T5"+T+
                "SUB T7, T6, L3"+T+
                "MOVE O0, T7"+T+
                "MOVE S0, L1"+T+
                "RETURN"+T;
        check(sql, prg,true,false);
    }



    public void testMixedExpression() {
        String sql="SELECT \"name\", 2*2  FROM \"emps\" WHERE \"name\" = 'Fred' AND  \"empno\" > 10";
        String prg=
                "output: vc[0],s8[1]"+T+
                "input: vc[0],s4[1]"+T+
                "literal: u1[0]=true,u1[1]=false,vc[2]='Fred',vc[3]='ISO-8859-1$en_US$primary',s4[4]=0,s8[5]=10,s8[6]=2"+T+
                "local: u1[0],u1[1],s4[2],u1[3],u1[4],u1[5],u1[6],u1[7],s8[8]"+T+
                "status: u1[0]"+T+
                "T"+T+
                "ISNOTNULL T0, I0"+T+
                "EXT T2, 'NLSCompare<vc, vc, vc>', I0, L2, L3"+T+
                "EQ T1, T2, L4"+T+
                "AND T3, T0, T1"+T+
                "ISNOTNULL T4, I1"+T+
                "GT T5, I1, L5"+T+
                "AND T6, T4, T5"+T+
                "AND T7, T3, T6"+T+
                "JMPT 11, T7"+T+
                "MOVE S0, L0"+T+
                "RETURN"+T+
                "MUL T8, L6, L6"+T+
                "MOVE O0, I0"+T+
                "MOVE O1, T8"+T+
                "MOVE S0, L1"+T+
                "RETURN"+T;
        check(sql, prg,true,false);
    }

     public void testNumbers() {
        String sql="SELECT -(1+-2.*-3.e-1/-.4)>=+5 FROM \"emps\" WHERE \"empno\" > 10";
        String prg=
                "output: u1[0]"+T+
                "input: s4[0]"+T+
                "literal: u1[0]=true,u1[1]=false,s8[2]=10,s8[3]=1,s8[4]=2,d[5]=0.3,d[6]=0.4,s8[7]=5"+T+
                "local: u1[0],s8[1],d[2],d[3],d[4],d[5],d[6],d[7],u1[8]"+T+
                "status: u1[0]"+T+
                "T"+T+
                "GT T0, I0, L2"+T+      //empno > 10
                "JMPT 4, T0"+T+
                "MOVE S0, L0"+T+
                "RETURN"+T+
                "NEG T1, L4"+T+         //-2
                "NEG T2, L5"+T+         //-0.3
                "MUL T3, T1, T2"+T+     //-2 * -0.3
                "NEG T4, L6"+T+         //-0.4
                "DIV T5, T3, T4"+T+     //(-2 * -0.3) / -0.4
                "ADD T6, L3, T5"+T+     //1+ ((-2 * -0.3) / -0.4)
                "NEG T7, T6"+T+
                "GE T8, T7, L7"+T+      // x >= 5
                "MOVE O0, T8"+T+
                "MOVE S0, L1"+T+
                "RETURN"+T;
        check(sql, prg,false,false);
    }

    public void testHexBitBinaryString() {
        String sql="SELECT x'abc'=x'', b''=B'00111', X'0001'=x'FFeeDD' FROM \"emps\" WHERE \"empno\" > 10";
        String prg=
                "output: u1[0],u1[1],u1[2]"+T+
                "input: s4[0]"+T+
                "literal: u1[0]=true,u1[1]=false,s8[2]=10,vb[3]='0ABC',vb[4]='',vb[5]='',vb[6]='07',"+
                         "vb[7]='0001',vb[8]='FFEEDD'"+T+
                "local: u1[0],u1[1],u1[2],u1[3]"+T+
                "status: u1[0]"+T+
                "T"+T+
                "GT T0, I0, L2"+T+      //empno > 10
                "JMPT 4, T0"+T+
                "MOVE S0, L0"+T+
                "RETURN"+T+
                "EQ T1, L3, L4"+T+         //x'abc'=x''
                "EQ T2, L5, L6"+T+         //b''=B'00111'
                "EQ T3, L7, L8"+T+         //x'0001'=x'ffeedd'
                "MOVE O0, T1"+T+
                "MOVE O1, T2"+T+
                "MOVE O2, T3"+T+
                "MOVE S0, L1"+T+
                "RETURN"+T;
        check(sql, prg,false,false);
    }

    public void testStringLiterals() {
        String sql="SELECT n'aBc',_iso_8859-1'', 'abc' FROM \"emps\" WHERE \"empno\" > 10";
        String prg=
                "output: vc[0],vc[1],vc[2]"+T+
                "input: s4[0]"+T+
                "literal: u1[0]=true,u1[1]=false,s8[2]=10,vc[3]='aBc',vc[4]='',vc[5]='abc'"+T+
                "local: u1[0]"+T+
                "status: u1[0]"+T+
                "T"+T+
                "GT T0, I0, L2"+T+      //empno > 10
                "JMPT 4, T0"+T+
                "MOVE S0, L0"+T+
                "RETURN"+T+
                "MOVE O0, L3"+T+
                "MOVE O1, L4"+T+
                "MOVE O2, L5"+T+
                "MOVE S0, L1"+T+
                "RETURN"+T;
        check(sql, prg,false,false);
    }

    public void testSimpleCompare() {
        String sql="SELECT "+
                    "1<>2" +
                    ",1=2 is true is false is null is unknown"+
                    ",true is not true "+
                    ",true is not false"+
                    ",true is not null "+
                    ",true is not unknown"+
                   " FROM \"emps\" WHERE \"empno\" > 10";
        String prg=
                "output: u1[0],u1[1],u1[2],u1[3],u1[4],u1[5]"+T+
                "input: s4[0]"+T+
                "literal: u1[0]=true,u1[1]=false,s8[2]=10,s8[3]=1,s8[4]=2"+T+
                "local: u1[0],u1[1],u1[2],u1[3],u1[4],u1[5],u1[6],u1[7],u1[8],u1[9],u1[10]," +
                       "u1[11],u1[12],u1[13],u1[14],u1[15],u1[16],u1[17]"+T+
                "status: u1[0]"+T+
                "T"+T+
                "GT T0, I0, L2"+T+      //empno > 10
                "JMPT 4, T0"+T+
                "MOVE S0, L0"+T+
                "RETURN"+T+
                "NE T1, L3, L4"+T+      //1<>2
                "EQ T2, L3, L4"+T+      //1=2

                "ISNOTNULL T3, T2"+T+   //X IS TRUE
                "EQ T4, T2, L0"+T+
                "AND T5, T3, T4"+T+

                "ISNOTNULL T6, T5"+T+   //X IS FALSE
                "EQ T7, T5, L1"+T+
                "AND T8, T6, T7"+T+

                "ISNULL T9, T8"+T+      //X IS NULL
                "ISNULL T10, T9"+T+     //X IS UNKNOWN

                "ISNOTNULL T11, L0"+T+   //TRUE IS NOT TRUE
                "EQ T12, L0, L0"+T+         //TODO optimize expressions like this
                "AND T13, T11, T12"+T+
                "NOT T14, T13"+T+

                "EQ T15, L0, L1"+T+         //TODO optimize expressions like this
                "AND T16, T11, T15"+T+
                "NOT T17, T16"+T+

                "MOVE O0, T1"+T+
                "MOVE O1, T10"+T+
                "MOVE O2, T14"+T+
                "MOVE O3, T17"+T+
                "MOVE O4, T11"+T+
                "MOVE O5, T11"+T+
                "MOVE S0, L1"+T+
                "RETURN"+T;
        check(sql, prg,false,false);
    }

    public void testArthimeticOperators() {
        String sql="SELECT POW(1,1), MOD(1,1), ABS(1), ABS(1.1), LN(1), LOG(1) FROM \"emps\" WHERE \"empno\" > 10";
        String prg=
                "output: s8[0],s8[1],s8[2],d[3],d[4],d[5]"+T+
                "input: s4[0]"+T+
                "literal: u1[0]=true,u1[1]=false,s8[2]=10,s8[3]=1,d[4]=1.1"+T+
                "local: u1[0],s8[1],s8[2],s8[3],d[4],d[5],d[6]"+T+
                "status: u1[0]"+T+
                "T"+T+
                "GT T0, I0, L2"+T+      //empno > 10
                "JMPT 4, T0"+T+
                "MOVE S0, L0"+T+
                "RETURN"+T+
                "EXT T1, 'POW<s8, s8>', L3, L3"+T+
                "EXT T2, 'MOD<s8, s8>', L3, L3"+T+
                "EXT T3, 'ABS<s8>', L3"+T+
                "EXT T4, 'ABS<d>', L4"+T+
                "EXT T5, 'LN<s8>', L3"+T+
                "EXT T6, 'LOG<s8>', L3"+T+
                "MOVE O0, T1"+T+
                "MOVE O1, T2"+T+
                "MOVE O2, T3"+T+
                "MOVE O3, T4"+T+
                "MOVE O4, T5"+T+
                "MOVE O5, T6"+T+
                "MOVE S0, L1"+T+
                "RETURN"+T;
        check(sql, prg,false,false);
    }

    public void testFunctionInFunction() {
        String sql="SELECT POW(3, ABS(2)+1) FROM \"emps\" WHERE \"empno\" > 10";
        String prg=
                "output: s8[0]"+T+
                "input: s4[0]"+T+
                "literal: u1[0]=true,u1[1]=false,s8[2]=10,s8[3]=3,s8[4]=2,s8[5]=1"+T+
                "local: u1[0],s8[1],s8[2],s8[3]"+T+
                "status: u1[0]"+T+
                "T"+T+
                "GT T0, I0, L2"+T+      //empno > 10
                "JMPT 4, T0"+T+
                "MOVE S0, L0"+T+
                "RETURN"+T+
                "EXT T1, 'ABS<s8>', L4"+T+
                "ADD T2, T1, L5"+T+
                "EXT T3, 'POW<s8, s8>', L3, T2"+T+
                "MOVE O0, T3"+T+
                "MOVE S0, L1"+T+
                "RETURN"+T;
        check(sql, prg,false,false);
    }

    public void testCaseExpressions() {
        String sql="SELECT case 1+1 when 1 then 'wael' when 2 then 'waels clone' end" +
                         ",case when 1=1 then 1+1+2 else 2+10 end" +
                " FROM \"emps\" WHERE \"empno\" > 10";
        String prg=
                "output: vc[0],s8[1]"+T+
                "input: s4[0]"+T+
                "literal: u1[0]=true,u1[1]=false,s8[2]=10,s8[3]=1,vc[4]='wael',s8[5]=2,vc[6]='waels clone',vc[7]"+T+
                "local: u1[0],vc[1],s8[2],u1[3],u1[4],s8[5],u1[6],s8[7],s8[8]"+T+
                "status: u1[0]"+T+
                "T"+T+
                "GT T0, I0, L2"+T+      //empno > 10
                "JMPT 4, T0"+T+
                "MOVE S0, L0"+T+
                "RETURN"+T+
                "ADD T2, L3, L3"+T+    //1+1
                "EQ T3, T2, L3"+T+     //when 1+1=1
                "JMPF 9, T3"+T+
                "MOVE T1, L4"+T+       //then
                "JMP 14"+T+
                "EQ T4, T2, L5"+T+     //when 1+1=2
                "JMPF 13, T4"+T+
                "MOVE T1, L6"+T+       //then
                "JMP 14"+T+
                "MOVE T1, L7"+T+
                "EQ T6, L3, L3"+T+     //SECOND CASE 1=1
                "JMPF 19, T6"+T+
                "ADD T7, T2, L5"+T+    //1+1+2
                "MOVE T5, T7"+T+
                "JMP 21"+T+
                "ADD T8, L5, L2"+T+   //else 2+10
                "MOVE T5, T8"+T+
                "MOVE O0, T1"+T+
                "MOVE O1, T5"+T+
                "MOVE S0, L1"+T+
                "RETURN"+T;
        check(sql, prg,false,false);
    }

    public void testCoalesce() {
        String sql="SELECT coalesce(1,2,3) FROM \"emps\" WHERE \"empno\" > 10";
        //CASE WHEN 1 IS NOT NULL THEN 1 ELSE (CASE WHEN 2 IS NOT NULL THEN 2 ELSE 3) END
        String prg=
                "output: s8[0]"+T+
                "input: s4[0]"+T+
                "literal: u1[0]=true,u1[1]=false,s8[2]=10,s8[3]=1,s8[4]=2,s8[5]=3"+T+
                "local: u1[0],s8[1],u1[2],s8[3],u1[4]"+T+
                "status: u1[0]"+T+
                "T"+T+
                "GT T0, I0, L2"+T+
                "JMPT 4, T0"+T+
                "MOVE S0, L0"+T+
                "RETURN"+T+
                "ISNOTNULL T2, L3"+T+
                "JMPF 8, T2"+T+
                "MOVE T1, L3"+T+
                "JMP 14"+T+
                "ISNOTNULL T4, L4"+T+
                "JMPF 12, T4"+T+
                "MOVE T3, L4"+T+
                "JMP 13"+T+
                "MOVE T3, L5"+T+
                "MOVE T1, T3"+T+
                "MOVE O0, T1"+T+
                "MOVE S0, L1"+T+
                "RETURN"+T;
        check(sql, prg,false,false);
    }

    public void testStringCompare() {
        String[] ops =      {"=" ,"<>",">" ,"<" ,">=","<="};
        String[] opsInstr = {"EQ","NE","GT","LT","GE","LE"};
        for (int i=0;i<ops.length;i++){
            String sql="SELECT 'a' "+ops[i]+"'b' collate latin1$sv$1 FROM \"emps\" WHERE \"empno\" > 10";
            String prg=
                    "output: u1[0]"+T+
                    "input: s4[0]"+T+
                    "literal: u1[0]=true,u1[1]=false,s8[2]=10,vc[3]='a',vc[4]='b',vc[5]='ISO-8859-1$sv$1',s4[6]=0"+T+
                    "local: u1[0],u1[1],s4[2]"+T+
                    "status: u1[0]"+T+
                    "T"+T+
                    "GT T0, I0, L2"+T+
                    "JMPT 4, T0"+T+
                    "MOVE S0, L0"+T+
                    "RETURN"+T+
                    "EXT T2, 'NLSCompare<vc, vc, vc>', L3, L4, L5"+T+
                    opsInstr[i]+" T1, T2, L6"+T+
                    "MOVE O0, T1"+T+
                    "MOVE S0, L1"+T+
                    "RETURN"+T;
            check(sql, prg,false,false);
        }
    }

    public void testStringFunctions() {
        String sql=
                   "SELECT char_length('a'),upper('a'),lower('a'),position('a' in 'a'),trim('a' from 'a')," +
                   "overlay('a' placing 'a' from 1),substring('a' from 1)," +
                   "substring('a' from 1 for 10),substring('a' from 'a' for '\\' )" +
                   ", 'a'||'a'" +
                   " FROM \"emps\" WHERE \"empno\" > 10";
        String prg=
                "output: s4[0],vc[1],vc[2],s4[3],vc[4],vc[5],vc[6],vc[7],vc[8],vc[9]"+T+
                "input: s4[0]"+T+
                "literal: u1[0]=true,u1[1]=false,s8[2]=10,vc[3]='a',vc[4]='ISO-8859-1',s4[5]=0,s4[6]=1,s8[7]=1,vc[8]='\\'"+T+
                "local: u1[0],s4[1],vc[2],vc[3],s4[4],vc[5],vc[6],vc[7],vc[8],vc[9],vc[10]"+T+
                "status: u1[0]"+T+
                "T"+T+
                "GT T0, I0, L2"+T+
                "JMPT 4, T0"+T+
                "MOVE S0, L0"+T+
                "RETURN"+T+
                "EXT T1, 'CHAR_LENGTH<vc, vc>', L3, L4"+T+
                "EXT T2, 'UPPER<vc, vc>', L3, L4"+T+
                "EXT T3, 'LOWER<vc, vc>', L3, L4"+T+
                "EXT T4, 'POSITION<vc, vc>', L3, L3"+T+
                "EXT T5, 'TRIM<vc, vc, s4, s4>', L3, L3, L5, L6"+T+
                "EXT T6, 'OVERLAY<vc, vc, s8>', L3, L3, L7"+T+
                "EXT T7, 'SUBSTRING<vc, s8>', L3, L7"+T+
                "EXT T8, 'SUBSTRING<vc, s8, s8>', L3, L7, L2"+T+
                "EXT T9, 'SUBSTRING<vc, vc, vc>', L3, L3, L8"+T+
                "EXT T10, 'CONCAT<vc, vc>', L3, L3"+T+
                "MOVE O0, T1"+T+
                "MOVE O1, T2"+T+
                "MOVE O2, T3"+T+
                "MOVE O3, T4"+T+
                "MOVE O4, T5"+T+
                "MOVE O5, T6"+T+
                "MOVE O6, T7"+T+
                "MOVE O7, T8"+T+
                "MOVE O8, T9"+T+
                "MOVE O9, T10"+T+
                "MOVE S0, L1"+T+
                "RETURN"+T;
        check(sql, prg,false,false);
    }
}

// End Rex2CalcPlanTestCase.java
