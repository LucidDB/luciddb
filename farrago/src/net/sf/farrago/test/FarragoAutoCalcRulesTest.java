/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2009 The Eigenbase Project
// Copyright (C) 2002-2009 SQLstream, Inc.
// Copyright (C) 2009-2009 LucidEra, Inc.
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
package net.sf.farrago.test;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import java.util.*;

import junit.extensions.*;

import junit.framework.*;

import net.sf.farrago.db.*;
import net.sf.farrago.defimpl.*;
import net.sf.farrago.fennel.calc.*;
import net.sf.farrago.jdbc.engine.*;
import net.sf.farrago.ojrex.*;
import net.sf.farrago.query.*;
import net.sf.farrago.session.*;
import net.sf.farrago.util.*;

import openjava.ptree.*;

import org.eigenbase.oj.rex.*;
import org.eigenbase.reltype.*;
import org.eigenbase.rex.*;
import org.eigenbase.sql.*;
import org.eigenbase.sql.fun.*;
import org.eigenbase.sql.type.*;


/**
 * FarragoAutoCalcRuleTest tests FarragoAutoCalcRule. This class duplicates some
 * of FarragoTestCase's set up mechanism. It does so in order to register
 * JPLUS(a Java-only duplicate of the SQL plus operator) and CPLUS (a Fennel
 * Calc-only analog) as SqlOperators. Tests within the class use JPLUS and CPLUS
 * in SQL statements to guarantee expressions that can only be implemented by
 * dividing execution between the Java and Fennel calculators.
 *
 * @author Stephan Zuercher
 * @version $Id$
 */
public class FarragoAutoCalcRulesTest
    extends FarragoTestCase
{
    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a new FarragoAutoCalcRulesTest object.
     *
     * @param testName .
     */
    public FarragoAutoCalcRulesTest(String testName)
        throws Exception
    {
        super(testName);
    }

    //~ Methods ----------------------------------------------------------------

    // implement TestCase
    public static Test suite()
    {
        return wrappedSuite(new TestSuite(FarragoAutoCalcRulesTest.class));
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

        String originalDriverClass =
            FarragoProperties.instance().testJdbcDriverClass.get();

        FarragoProperties.instance().testJdbcDriverClass.set(
            TestJdbcEngineDriver.class.getName());

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
        // mask out source control Id and other data sections that are not
        // pertinent to the test being performed.
        setRefFileDiffMasks();

        runSqlLineTest(
            FarragoProperties.instance().expandProperties(
                "${FARRAGO_HOME}/testcases/autoCalcRule.sql"));
    }

    public void testSimple()
        throws SQLException
    {
        PreparedStatement stmt =
            connection.prepareStatement(
                "select cplus(1, 1) from (values (true))");
        stmt.close();
    }

    public void testDynamicParameterInConditional()
        throws SQLException
    {
        PreparedStatement stmt =
            connection.prepareStatement(
                "select name, cplus(1, jplus(2, cplus(3, 4))) from sales.emps where name like cast(? as varchar(128))");
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
        PreparedStatement stmt =
            connection.prepareStatement(
                "values cplus(1, jplus(100, cplus(50, cast(? as int))))");
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

    public static void registerTestJavaOps(
        FarragoOJRexImplementorTable implementorTable,
        final SqlStdOperatorTable opTab)
    {
        SqlFunction jplusFunc =
            new SqlFunction(
                "JPLUS",
                SqlKind.Function,
                SqlTypeStrategies.rtiLeastRestrictive,
                SqlTypeStrategies.otiFirstKnown,
                SqlTypeStrategies.otcNumericX2,
                SqlFunctionCategory.Numeric);
        opTab.register(jplusFunc);

        implementorTable.registerOperator(
            jplusFunc,
            implementorTable.get(SqlStdOperatorTable.plusOperator));

        SqlFunction jrowFunc =
            new SqlFunction(
                "JROW",
                SqlKind.Function,
                null,
                SqlTypeStrategies.otiFirstKnown,
                SqlTypeStrategies.otcNumericX2,
                SqlFunctionCategory.Numeric)
            {
                public RelDataType inferReturnType(
                    SqlOperatorBinding opBinding)
                {
                    assert (opBinding.getOperandCount() == 2);
                    String [] names = new String[] { "first", "second" };
                    return opBinding.getTypeFactory().createStructType(
                        opBinding.collectOperandTypes(),
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

        implementorTable.registerOperator(jrowFunc, jrowFuncImpl);
    }

    public static void registerTestCppOps(
        CalcRexImplementorTableImpl implementor,
        final SqlStdOperatorTable opTab)
    {
        SqlFunction cppFunc =
            new SqlFunction(
                "CPLUS",
                SqlKind.Function,
                SqlTypeStrategies.rtiLeastRestrictive,
                SqlTypeStrategies.otiFirstKnown,
                SqlTypeStrategies.otcNumericX2,
                SqlFunctionCategory.Numeric);
        opTab.register(cppFunc);

        implementor.register(
            cppFunc,
            implementor.get(SqlStdOperatorTable.plusOperator));
    }

    //~ Inner Classes ----------------------------------------------------------

    /**
     * TestDbSessionFactory extends FarragoDefaultSessionFactory and returns a
     * custom TestDbSession instance.
     */
    private static class TestDbSessionFactory
        extends FarragoDbSessionFactory
    {
        private final OJRexImplementorTable ojRexImplementor;
        private final CalcRexImplementorTable calcRexImplementor;

        TestDbSessionFactory(
            OJRexImplementorTable ojRexImplementor,
            CalcRexImplementorTable calcRexImplementor)
        {
            this.ojRexImplementor = ojRexImplementor;
            this.calcRexImplementor = calcRexImplementor;
        }

        public FarragoSession newSession(
            String url,
            Properties info)
        {
            FarragoSession session = new FarragoDbSession(url, info, this);

            // Constant reduction hides what we're trying to test for.
            session.setOptRuleDescExclusionFilter(
                FarragoReduceExpressionsRule.EXCLUSION_PATTERN);
            return session;
        }

        public FarragoSessionPersonality newSessionPersonality(
            FarragoSession session,
            FarragoSessionPersonality defaultPersonality)
        {
            return new TestDbSessionPersonality(
                (FarragoDbSession) session,
                ojRexImplementor,
                calcRexImplementor);
        }
    }

    /**
     * TestDbSession extends FarragoDefaultSessionPersonality to provide our
     * custom OJRexImplementor table in place of Farrago's normal
     * implementation.
     */
    private static class TestDbSessionPersonality
        extends FarragoDefaultSessionPersonality
    {
        private OJRexImplementorTable ojRexImplementor;
        private CalcRexImplementorTable calcRexImplementor;

        public TestDbSessionPersonality(
            FarragoDbSession session,
            OJRexImplementorTable ojRexImplementor,
            CalcRexImplementorTable calcRexImplementorTable)
        {
            super(session);
            this.ojRexImplementor = ojRexImplementor;
            this.calcRexImplementor = calcRexImplementorTable;
        }

        public FarragoSessionPlanner newPlanner(
            FarragoSessionPreparingStmt stmt,
            boolean init)
        {
            // This test needs Volcano.
            FarragoDefaultPlanner planner = new FarragoDefaultPlanner(stmt);
            if (init) {
                planner.init();
            }
            return planner;
        }

        public SqlOperatorTable getSqlOperatorTable(
            FarragoSessionPreparingStmt preparingStmt)
        {
            return TestJdbcEngineDriver.opTab;
        }

        public OJRexImplementorTable getOJRexImplementorTable(
            FarragoSessionPreparingStmt preparingStmt)
        {
            return ojRexImplementor;
        }

        public <C> C newComponentImpl(Class<C> componentInterface)
        {
            if (componentInterface == CalcRexImplementorTable.class) {
                return componentInterface.cast(calcRexImplementor);
            }

            return super.newComponentImpl(componentInterface);
        }
    }

    /**
     * TestJdbcEngineDriver extends FarragoJdbcEngineDriver and provides our
     * custom TestDbSessionFactory in place of Farrago's normal implementation.
     */
    public static class TestJdbcEngineDriver
        extends FarragoJdbcEngineDriver
    {
        static SqlStdOperatorTable opTab;
        static FarragoOJRexImplementorTable testOjRexImplementor;
        static CalcRexImplementorTableImpl testCalcRexImplementor;

        static {
            opTab = new SqlStdOperatorTable();
            opTab.init();
            testOjRexImplementor = new FarragoOJRexImplementorTable(opTab);
            registerTestJavaOps(testOjRexImplementor, opTab);

            testCalcRexImplementor =
                new CalcRexImplementorTableImpl(
                    CalcRexImplementorTableImpl.std());
            registerTestCppOps(testCalcRexImplementor, opTab);

            new TestJdbcEngineDriver().register();
        }

        public TestJdbcEngineDriver()
        {
        }

        public FarragoSessionFactory newSessionFactory()
        {
            return new TestDbSessionFactory(
                testOjRexImplementor,
                testCalcRexImplementor);
        }

        public String getBaseUrl()
        {
            return "jdbc:farragotest:";
        }
    }
}

// End FarragoAutoCalcRulesTest.java
