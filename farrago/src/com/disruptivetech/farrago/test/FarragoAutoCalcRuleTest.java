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

import com.disruptivetech.farrago.calc.CalcRexImplementorTableImpl;

import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;

import junit.extensions.TestSetup;
import junit.framework.Test;
import junit.framework.TestSuite;

import net.sf.farrago.defimpl.FarragoDefaultSession;
import net.sf.farrago.db.FarragoDbSessionFactory;
import net.sf.farrago.jdbc.engine.FarragoJdbcEngineConnection;
import net.sf.farrago.jdbc.engine.FarragoJdbcEngineDriver;
import net.sf.farrago.ojrex.FarragoOJRexImplementor;
import net.sf.farrago.ojrex.FarragoOJRexImplementorTable;
import net.sf.farrago.ojrex.FarragoRexToOJTranslator;
import net.sf.farrago.session.FarragoSession;
import net.sf.farrago.session.FarragoSessionFactory;
import net.sf.farrago.test.FarragoTestCase;
import net.sf.farrago.util.FarragoProperties;

import openjava.ptree.*;

import org.eigenbase.oj.rex.OJRexImplementor;
import org.eigenbase.oj.rex.OJRexImplementorTable;
import org.eigenbase.reltype.RelDataType;
import org.eigenbase.reltype.RelDataTypeFactory;
import org.eigenbase.rex.RexCall;
import org.eigenbase.rex.RexNode;
import org.eigenbase.sql.*;
import org.eigenbase.sql.type.UnknownParamInference;
import org.eigenbase.sql.type.ReturnTypeInference;
import org.eigenbase.sql.type.OperandsTypeChecking;
import org.eigenbase.sql.fun.SqlStdOperatorTable;

/**
 * FarragoAutoCalcRuleTest tests FarragoAutoCalcRule.  This class
 * duplicates some of FarragoTestCase's set up mechanism.  It does so
 * in order to register JPLUS(a Java-only duplicate of the SQL plus
 * operator) and CPLUS (a Fennel Calc-only analog) as SqlOperators.
 * Tests within the class use JPLUS and CPLUS in SQL statements to
 * guarantee expressions that can only be implemented by dividing
 * execution between the Java and Fennel calculators.
 *
 * @author Stephan Zuercher
 * @version $Id$
 */
public class FarragoAutoCalcRuleTest extends FarragoTestCase
{
    //~ Static fields/initializers --------------------------------------------

    private static SqlStdOperatorTable opTab;
    private static TestOJRexImplementorTable testOjRexImplementor;

    //~ Constructors ----------------------------------------------------------

    /**
     * Creates a new FarragoAutoCalcRuleTest object.
     *
     * @param testName .
     */
    public FarragoAutoCalcRuleTest(String testName)
        throws Exception
    {
        super(testName);
    }

    //~ Methods ---------------------------------------------------------------

    // implement TestCase
    public static Test suite()
    {
        return wrappedSuite(new TestSuite(FarragoAutoCalcRuleTest.class));
    }

    public static Test wrappedSuite(TestSuite suite)
    {
        TestSetup wrapper =
            new TestSetup(suite) {
                private String originalDriverClass;

                protected void setUp()
                    throws Exception
                {
                    originalDriverClass = staticAutoCalcSetUp();
                }

                protected void tearDown()
                    throws Exception
                {
                    staticAutoCalcTearDown(originalDriverClass);
                }
            };
        return wrapper;
    }

    public static String staticAutoCalcSetUp()
        throws Exception
    {
        // close any previous connection and create a special one
        FarragoTestCase.forceShutdown();
        
        opTab = new SqlStdOperatorTable();
        opTab.init();
        testOjRexImplementor = new TestOJRexImplementorTable(opTab);

        String originalDriverClass = 
            FarragoProperties.instance().testJdbcDriverClass.get();

        FarragoProperties.instance().testJdbcDriverClass.set(
            TestJdbcEngineDriver.class.getName());

        SqlFunction cppFunc =
            new SqlFunction("CPLUS", SqlKind.Function,
                ReturnTypeInference.useNullableBiggest,
                UnknownParamInference.useFirstKnown,
                OperandsTypeChecking.typeNullableNumericNumeric,
                SqlFunction.SqlFuncTypeName.Numeric);
        opTab.register(cppFunc);

        CalcRexImplementorTableImpl cImplTab =
            new CalcRexImplementorTableImpl(CalcRexImplementorTableImpl.std());
        CalcRexImplementorTableImpl.setThreadInstance(cImplTab);
        cImplTab.register(
            cppFunc,
            cImplTab.get(opTab.plusOperator));

        FarragoTestCase.staticSetUp();
        
        return originalDriverClass;
    }

    public static void staticAutoCalcTearDown(String originalDriverClass)
        throws Exception
    {
        // close our very special connection
        FarragoTestCase.staticTearDown();
        FarragoTestCase.forceShutdown();

        if (originalDriverClass != null) {
            FarragoProperties.instance().testJdbcDriverClass.set(
                originalDriverClass);
        } else {
            FarragoProperties.instance().remove(
                FarragoProperties.instance().testJdbcDriverClass.getPath());
        }
    }

    public void testFarragoAutoCalcRuleByDiff()
        throws Exception
    {
        // mask out source control Id
        addDiffMask("\\$Id.*\\$");
        runSqlLineTest("testcases/autoCalcRule.sql");
    }


    public void testDynamicParameterInConditional()
        throws SQLException
    {
        PreparedStatement stmt = connection.prepareStatement(
            "select name, cplus(1, jplus(2, cplus(3, 4))) from sales.emps where name like ?");
        try {
            stmt.setString(1, "F%");
             
            ResultSet rset = stmt.executeQuery();
            try {
                assertTrue(rset.next());
                assertEquals(
                    "Fred",
                    rset.getString(1));
                assertEquals(
                    1 + (2 + (3 + 4)),
                    rset.getInt(2));
                assertFalse(rset.next());
            } finally {
                rset.close();
            }
        } finally {
            stmt.close();
        }
    }


    public void testDynamicParameterInCall()
        throws SQLException
    {
        PreparedStatement stmt = connection.prepareStatement(
            "select cplus(1, jplus(100, cplus(50, ?))) from values(true)");
        try {
            stmt.setInt(1, 13);
             
            ResultSet rset = stmt.executeQuery();
            try {
                assertTrue(rset.next());
                assertEquals(
                    1 + (100 + (50 + 13)),
                    rset.getInt(1));
                assertFalse(rset.next());
            } finally {
                rset.close();
            }
        } finally {
            stmt.close();
        }
    }


    //~ Inner Classes ---------------------------------------------------------

    /**
     * TestOJRexImplementorTable extends FarragoOJRexImplementorTable
     * and adds the JPLUS operator.
     */
    private static class TestOJRexImplementorTable
        extends FarragoOJRexImplementorTable
    {
        public TestOJRexImplementorTable(SqlStdOperatorTable opTab)
        {
            super(opTab);
        }

        protected void initStandard(final SqlStdOperatorTable opTab)
        {
            super.initStandard(opTab);

            SqlFunction jplusFunc =
                new SqlFunction("JPLUS", SqlKind.Function,
                    ReturnTypeInference.useNullableBiggest,
                    UnknownParamInference.useFirstKnown,
                    OperandsTypeChecking.typeNullableNumericNumeric,
                    SqlFunction.SqlFuncTypeName.Numeric);
            opTab.register(jplusFunc);

            registerOperator(
                jplusFunc,
                get(opTab.plusOperator));

            SqlFunction jrowFunc =
                new SqlFunction("JROW", SqlKind.Function, null,
                    UnknownParamInference.useFirstKnown,
                    OperandsTypeChecking.typeNullableNumericNumeric,
                    SqlFunction.SqlFuncTypeName.Numeric)
                {
                    public RelDataType getType(
                        RelDataTypeFactory typeFactory,
                        RexNode [] args)
                    {
                        assert (args.length == 2);

                        RelDataType [] types =
                            new RelDataType [] {
                                args[0].getType(), args[1].getType()
                            };
                        String [] names = new String [] { "first", "second" };

                        return typeFactory.createProjectType(types, names);
                    }

                    public RelDataType inferType(
                        SqlValidator validator,
                        SqlValidator.Scope scope,
                        SqlCall call)
                    {
                        assert (call.getOperands().length == 2);

                        RelDataType [] types =
                            new RelDataType [] {
                                validator.getValidatedNodeType(call
                                        .getOperands()[0]),
                                validator.getValidatedNodeType(call
                                        .getOperands()[1])
                            };
                        String [] names = new String [] { "first", "second" };

                        return validator.typeFactory.createProjectType(types,
                            names);
                    }
                };
            opTab.register(jrowFunc);

            OJRexImplementor jrowFuncImpl =
                new FarragoOJRexImplementor() {
                    public Expression implementFarrago(
                        FarragoRexToOJTranslator trans,
                        RexCall call,
                        Expression [] operands)
                    {
                        // NOTE: this is untested and is probably
                        // never called during this test case.
                        RelDataType rowType = call.getType();

                        Variable rowVar = trans.createScratchVariable(rowType);

                        trans.addStatement(
                            new ExpressionStatement(
                                new AssignmentExpression(
                                    new ArrayAccess(
                                        rowVar,
                                        new Literal(Literal.INTEGER, "0")),
                                    AssignmentExpression.EQUALS,
                                    operands[0])));

                        trans.addStatement(
                            new ExpressionStatement(
                                new AssignmentExpression(
                                    new ArrayAccess(
                                        rowVar,
                                        new Literal(Literal.INTEGER, "1")),
                                    AssignmentExpression.EQUALS,
                                    operands[1])));

                        return rowVar;
                    }
                };

            registerOperator(jrowFunc, jrowFuncImpl);
        }
    }

    /**
     * TestDbSessionFactory extends FarragoDefaultSessionFactory and
     * returns a custom TestDbSession instance.
     */
    private static class TestDbSessionFactory
        extends FarragoDbSessionFactory
    {
        private OJRexImplementorTable ojRexImplementor;

        TestDbSessionFactory(OJRexImplementorTable ojRexImplementor)
        {
            this.ojRexImplementor = ojRexImplementor;
        }

        public FarragoSession newSession(
            String url,
            Properties info)
        {
            return new TestDbSession(url, info, this, ojRexImplementor);
        }
    }

    /**
     * TestDbSession extends FarragoDefaultSession to provide our custom
     * OJRexImplementor table in place of Farrago's normal
     * implementation.
     */
    private static class TestDbSession extends FarragoDefaultSession
    {
        private OJRexImplementorTable ojRexImplementor;

        public TestDbSession(
            String url,
            Properties info,
            FarragoSessionFactory factory,
            OJRexImplementorTable ojRexImplementor)
        {
            super(url, info, factory);

            this.ojRexImplementor = ojRexImplementor;
        }

        public SqlOperatorTable getSqlOperatorTable()
        {
            return opTab;
        }
        
        public OJRexImplementorTable getOJRexImplementorTable()
        {
            return ojRexImplementor;
        }
    }

    /**
     * TestJdbcEngineDriver extends FarragoJdbcEngineDriver and
     * provides our custom TestDbSessionFactory in place of Farrago's
     * normal implementation.
     */
    public static class TestJdbcEngineDriver extends FarragoJdbcEngineDriver
    {
        static {
            new TestJdbcEngineDriver().register();
        }

        public TestJdbcEngineDriver()
        {
        }

        public FarragoSessionFactory newSessionFactory()
        {
            return new TestDbSessionFactory(testOjRexImplementor);
        }

        public String getBaseUrl()
        {
            return "jdbc:farragotest:";
        }
    }
}
