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

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import junit.extensions.TestSetup;
import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

import net.sf.farrago.db.FarragoDbSession;
import net.sf.farrago.db.FarragoDbSessionFactory;
import net.sf.farrago.jdbc.engine.FarragoJdbcEngineConnection;
import net.sf.farrago.jdbc.engine.FarragoJdbcEngineDriver;
import net.sf.farrago.ojrex.FarragoOJRexImplementor;
import net.sf.farrago.ojrex.FarragoOJRexImplementorTable;
import net.sf.farrago.ojrex.FarragoRexToOJTranslator;
import net.sf.farrago.session.FarragoSession;
import net.sf.farrago.session.FarragoSessionFactory;

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
import org.eigenbase.sql.test.SqlTester;


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
public class FarragoAutoCalcRuleTest extends TestCase
{
    //~ Static fields/initializers --------------------------------------------

    private static FarragoJdbcEngineConnection farragoConnection;
    private static TestOJRexImplementorTable testOjRexImplementor;

    //~ Constructors ----------------------------------------------------------

    /**
     * Creates a new FarragoRexToOJTranslatorTest object.
     *
     * @param testName .
     */
    public FarragoAutoCalcRuleTest(String testName)
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
                protected void setUp()
                    throws Exception
                {
                    staticSetUp();
                }

                protected void tearDown()
                    throws Exception
                {
                    staticTearDown();
                }
            };
        return wrapper;
    }

    public static void staticSetUp()
        throws Exception
    {
        SqlStdOperatorTable opTab = SqlOperatorTable.std();
        testOjRexImplementor = new TestOJRexImplementorTable(opTab);

        SqlFunction cppFunc =
            new SqlFunction("CPLUS", SqlKind.Function,
                ReturnTypeInference.useNullableBiggest,
                UnknownParamInference.useFirstKnown,
                OperandsTypeChecking.typeNullableNumericNumeric,
                SqlFunction.SqlFuncTypeName.Numeric) {
                public void test(SqlTester tester)
                {
                }
            };

        opTab.register(cppFunc);

        CalcRexImplementorTableImpl cImplTab =
            new CalcRexImplementorTableImpl(CalcRexImplementorTableImpl.std());
        CalcRexImplementorTableImpl.setThreadInstance(cImplTab);
        cImplTab.register(
            cppFunc,
            cImplTab.get(opTab.plusOperator));

        FarragoJdbcEngineDriver driver = newJdbcEngineDriver();
        Connection connection =
            DriverManager.getConnection(driver.getUrlPrefix());
        farragoConnection = (FarragoJdbcEngineConnection) connection;
        connection.setAutoCommit(false);
    }

    protected static FarragoJdbcEngineDriver newJdbcEngineDriver()
        throws Exception
    {
        return new TestJdbcEngineDriver();
    }

    public static void staticTearDown()
        throws Exception
    {
        if (farragoConnection != null) {
            farragoConnection.rollback();
            farragoConnection.close();
            farragoConnection = null;
        }
    }

    public void testJavaPlus()
        throws SQLException
    {
        testExplain("select jplus(1, 1) from values(true)");
    }

    public void testCppPlus()
        throws SQLException
    {
        testExplain("select cplus(1, 1) from values(true)");
    }

    public void testAutoPlus()
        throws SQLException
    {
        testExplain("select cplus(1, 1), jplus(2, 2) from values(true)");
    }

    public void testAutoPlusReverse()
        throws SQLException
    {
        testExplain("select jplus(2, 2), cplus(1, 1) from values(true)");
    }

    public void testAutoPlusNested()
        throws SQLException
    {
        testExplain("select cplus(jplus(1, 1), 2) from values(true)");
    }

    public void testAutoPlusNestedReverse()
        throws SQLException
    {
        testExplain("select jplus(cplus(1, 1), 2) from values(true)");
    }

    public void testEmps()
        throws SQLException
    {
        testExplain(
            "select empno, cplus(jplus(deptno, empid), age), jplus(cplus(deptno, empid), age), age from sales.emps");
    }

    public void testAutoPlusTable()
        throws SQLException
    {
        testExplain("select * from sales.emps where jplus(deptno, 1) = 100");
    }

    public void testAutoPlusTableNested()
        throws SQLException
    {
        testExplain(
            "select * from sales.emps where jplus(cplus(deptno, 1), 2) = 100");
    }

    public void testAutoPlusTableNestedReverse()
        throws SQLException
    {
        testExplain(
            "select * from sales.emps where cplus(jplus(deptno, 1), 2) = 100");
    }

    public void testAutoPlusFull()
        throws SQLException
    {
        testExplain(
            "select cplus(jplus(deptno, 1), 2), jplus(cplus(deptno, 1), 2) from sales.emps where cplus(jplus(deptno, 1), 2) = 100 or jplus(cplus(deptno, 1), 2) = 100");
    }

    public void testAutoPlusFullJavaFirst()
        throws SQLException
    {
        testExplain(
            "select jplus(cplus(deptno, 1), 2), cplus(jplus(deptno, 1), 2) from sales.emps where cplus(jplus(deptno, 1), 2) = 100 or jplus(cplus(deptno, 1), 2) = 100");
    }

    public void testNonCallWhere()
        throws SQLException
    {
        testExplain(
            "select jplus(cplus(deptno, 1), 2), cplus(jplus(deptno, 1), 2) from sales.emps where slacker");
    }

    public void testTrailingReference()
        throws SQLException
    {
        // Test top-most level can be implemented in any calc and last
        // expression isn't a RexCall.  dtbug 210
        testExplain(
            "select deptno + jplus(cplus(deptno, 1), 2), empno from sales.emps");
    }

    public void testDynamicParameterInConditional()
        throws SQLException
    {
        PreparedStatement stmt =
            farragoConnection.prepareStatement(
                "select name, cplus(1, jplus(2, cplus(3, 4))) from sales.emps where name like ?");
        try {
            stmt.setString(1, "F%");

            ResultSet rset = stmt.executeQuery();
            try {
                assertTrue(rset.next());
                assertEquals(
                    "Fred",
                    rset.getString(1));
                assertFalse(rset.next());
            } finally {
                rset.close();
            }
        } finally {
            stmt.close();
        }
    }

    public void testFieldAccess()
        throws SQLException
    {
        // Equivalent to dtbug 210
        testExplain(
            "select cplus(t.r.\"second\", 1) from (select jrow(deptno, empno) as r from sales.emps) as t");
    }

    public void testFieldAccess2()
        throws SQLException
    {
        // Found a bug related to this expression while debugging dtbug 210.
        testExplain(
            "select t.r.\"second\" from (select jrow(deptno, cplus(empno, 1)) as r from sales.emps) as t");
    }

    // REVIEW: SZ: 7/14/2004: We should probably compare the results
    // to expected values, rather than just assuming that no
    // exceptions means it worked.  One idea would be to use the
    // .sql/.ref/.log mechanism of FarragoTestCase -- the only trick
    // is that we need to register JPLUS and CPLUS only for this test.
    private void testExplain(String query)
        throws SQLException
    {
        query = "explain plan for " + query;

        System.out.println(query + ":");
        Statement stmt = farragoConnection.createStatement();
        try {
            ResultSet rset = stmt.executeQuery(query);
            try {
                while (rset.next()) {
                    System.out.println(rset.getString(1));
                }
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
                    SqlFunction.SqlFuncTypeName.Numeric) {
                    public void test(SqlTester tester)
                    {
                    }
                };
            opTab.register(jplusFunc);

            registerOperator(
                jplusFunc,
                get(opTab.plusOperator));

            SqlFunction jrowFunc =
                new SqlFunction("JROW", SqlKind.Function, null,
                    UnknownParamInference.useFirstKnown,
                    OperandsTypeChecking.typeNullableNumericNumeric,
                    SqlFunction.SqlFuncTypeName.Numeric) {
                    public void test(SqlTester tester)
                    {
                    }

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
     * TestDbSessionFactory extends FarragoDbSessionFactory and
     * returns a custom TestDbSession instance.
     */
    private static class TestDbSessionFactory extends FarragoDbSessionFactory
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
     * TestDbSession extends FarragoDbSession to provide our custom
     * OJRexImplementor table in place of Farrago's normal
     * implementation.
     */
    private static class TestDbSession extends FarragoDbSession
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
    private static class TestJdbcEngineDriver extends FarragoJdbcEngineDriver
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
