/*
// Farrago is a relational database management system.
// Copyright (C) 2003-2004 John V. Sichi.
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
package net.sf.farrago.test;

import java.io.*;
import java.sql.*;
import java.util.*;
import java.util.logging.*;
import java.util.regex.Pattern;

import junit.extensions.*;
import junit.framework.*;

import net.sf.farrago.catalog.*;
import net.sf.farrago.cwm.relational.*;
import net.sf.farrago.fem.config.*;
import net.sf.farrago.fem.med.*;
import net.sf.farrago.jdbc.engine.*;
import net.sf.farrago.trace.*;
import net.sf.farrago.util.*;
import net.sf.farrago.db.*;

import org.eigenbase.test.*;
import org.eigenbase.util.SaffronProperties;

import sqlline.SqlLine;


/**
 * FarragoTestCase is a common base for Farrago JUnit tests.  Subclasses must
 * implement the suite() method in order to get a database connection
 * correctly initialized.  See InnerTest for an example.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public abstract class FarragoTestCase extends DiffTestCase
{
    //~ Static fields/initializers --------------------------------------------

    /** Logger to use for test tracing. */
    protected static final Logger tracer = FarragoTrace.getTestTracer();

    /** JDBC connection to Farrago database. */
    protected static Connection connection;

    /** Repos for test object definitions. */
    protected static FarragoRepos repos;

    /**
     * Flag used to allow individual test methods to be called from
     * IntelliJ.
     */
    private static boolean individualTest;

    /**
     * Saved copy of Farrago configuration parameters.
     */
    private static SortedMap savedFarragoConfig;

    /**
     * Saved copy of Fennel configuration parameters.
     */
    private static SortedMap savedFennelConfig;

    private static Thread shutdownHook;

    //~ Instance fields -------------------------------------------------------

    /** ResultSet for processing queries. */
    protected ResultSet resultSet;

    /** PreparedStatement for processing queries. */
    protected PreparedStatement preparedStmt;

    /** Statement for processing queries. */
    protected Statement stmt;

    //~ Constructors ----------------------------------------------------------

    /**
     * Creates a new FarragoTestCase object.
     *
     * @param testName .
     *
     * @throws Exception .
     */
    protected FarragoTestCase(String testName)
        throws Exception
    {
        super(testName);
    }

    //~ Methods ---------------------------------------------------------------

    // override DiffTestCase
    protected File getTestlogRoot()
        throws Exception
    {
        return getTestlogRootStatic();
    }

    /**
     * Implementation for { @link DiffTestCase#getTestlogRoot } which
     * uses 'testlog' directory under Farrago home.
     *
     * @return the root under which testlogs should be written
     */
    public static File getTestlogRootStatic()
    {
        String homeDir = FarragoProperties.instance().homeDir.get(true);
        return new File(homeDir, "testlog");
    }
    
    /**
     * One-time setup routine.
     *
     * @throws Exception .
     */
    public static void staticSetUp()
        throws Exception
    {
        if (shutdownHook == null) {
            shutdownHook = new ShutdownThread();
            Runtime.getRuntime().addShutdownHook(shutdownHook);
        }
        if (connection == null) {
            FarragoJdbcEngineDriver driver = newJdbcEngineDriver();
            connection = DriverManager.getConnection(driver.getUrlPrefix());
            FarragoJdbcEngineConnection farragoConnection =
                (FarragoJdbcEngineConnection) connection;
            repos = farragoConnection.getSession().getRepos();
            connection.setAutoCommit(false);
            saveParameters();
        }

        runCleanup();
    }

    protected static void runCleanup()
        throws Exception
    {
        Cleanup cleanup = new Cleanup("cleanup");
        try {
            cleanup.setUp();
            cleanup.execute();
        } finally {
            // NOTE:  bypass staticTearDown
            cleanup.tearDownImpl();
        }
    }

    /**
     * One-time tear-down routine.
     *
     * @throws Exception .
     */
    public static void staticTearDown()
        throws Exception
    {
        if (repos != null) {
            restoreParameters();
        }
        if (connection != null) {
            connection.rollback();
        }
    }

    public static void forceShutdown() throws Exception
    {
        repos = null;
        if (connection != null) {
            connection.rollback();
            connection.close();
            connection = null;
        }
    }

    private static class ShutdownThread extends Thread
    {
        public void run()
        {
            try {
                forceShutdown();
            } catch (Exception ex) {
                // TODO:  trace
            }
        }
    }

    /**
     * Save system parameters.
     */
    protected static void saveParameters()
    {
        FarragoReposTxnContext reposTxn = new FarragoReposTxnContext(repos);
        reposTxn.beginReadTxn();
        savedFarragoConfig =
            JmiUtil.getAttributeValues(repos.getCurrentConfig());
        savedFennelConfig =
            JmiUtil.getAttributeValues(
                repos.getCurrentConfig().getFennelConfig());
        reposTxn.commit();
    }

    /**
     * Restore system parameters to state saved by saveParameters().
     */
    protected static void restoreParameters()
    {
        FarragoReposTxnContext reposTxn = new FarragoReposTxnContext(repos);
        reposTxn.beginWriteTxn();
        JmiUtil.setAttributeValues(
            repos.getCurrentConfig(),
            savedFarragoConfig);
        JmiUtil.setAttributeValues(
            repos.getCurrentConfig().getFennelConfig(),
            savedFennelConfig);
        reposTxn.commit();
    }

    // NOTE: Repos open/close is slow and causes sporadic problems when done
    // in quick succession, so only do it once for the entire test suite
    // instead of the Junit-recommended once per test case.

    /**
     * Generic implementation of suite() to be called by subclasses.
     *
     * @param clazz the subclass being wrapped
     *
     * @return a JUnit test suite which will take care of initialization of
     *         per-testcase members
     */
    public static Test wrappedSuite(Class clazz)
    {
        TestSuite suite = new TestSuite(clazz);
        return wrappedSuite(suite);
    }

    /**
     * Generic implementation of suite() to be called by subclasses.
     *
     * <p>If the {@link SaffronProperties#testEverything} property is false,
     * and the {@link SaffronProperties#testName} property is set, then returns
     * a suite containing only the tests whose names match.
     *
     * @param suite the suite being wrapped
     *
     * @return a JUnit test suite which will take care of initialization of
     *         per-testcase members
     */
    public static Test wrappedSuite(TestSuite suite)
    {
        // Filter out tests whose names match "saffron.test.Name".
        final SaffronProperties saffronProps = SaffronProperties.instance();
        if (!saffronProps.testEverything.get()) {
            final String testNamePattern = saffronProps.testName.get();
            if (testNamePattern != null) {
                Pattern testPattern = Pattern.compile(testNamePattern);
                suite = EigenbaseTestCase.copySuite(suite, testPattern);
            }
        }

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

    /**
     * .
     *
     * @return the number of rows in resultSet (which is consumed as a side
     * effect)
     *
     * @throws Exception .
     */
    protected int getResultSetCount()
        throws Exception
    {
        int n = 0;
        while (resultSet.next()) {
            ++n;
        }
        resultSet.close();
        return n;
    }

    // implement TestCase
    protected void setUp()
        throws Exception
    {
        if (connection == null) {
            assert (!individualTest) : "You forgot to implement suite()";
            individualTest = true;
            staticSetUp();
        }

        tracer.info("Entering test case "
            + getClass().getName() + "." + getName());
        super.setUp();
        stmt = connection.createStatement();

        // discard any cached query plans
        stmt.executeUpdate("alter system set \"codeCacheMaxBytes\" = min");
        stmt.executeUpdate("alter system set \"codeCacheMaxBytes\" = max");

        resultSet = null;
    }

    // implement TestCase
    protected void tearDown()
        throws Exception
    {
        tearDownImpl();
        if (individualTest) {
            staticTearDown();
        }
    }

    protected void tearDownImpl()
        throws Exception
    {
        try {
            if (resultSet != null) {
                resultSet.close();
                resultSet = null;
            }
            if (preparedStmt != null) {
                preparedStmt.close();
                preparedStmt = null;
            }
            if (stmt != null) {
                stmt.close();
                stmt = null;
            }
            if (connection != null) {
                connection.rollback();
            }
        } finally {
            tracer.info("Leaving test case "
                + getClass().getName() + "." + getName());
            if (tracer.isLoggable(Level.FINE)) {
                Runtime rt = Runtime.getRuntime();
                rt.gc();
                rt.gc();
                long heapSize = rt.totalMemory() - rt.freeMemory();
                tracer.fine("JVM heap size = " + heapSize);
            }
            super.tearDown();
        }
    }

    /**
     * Compare the first column of a result set against a String-valued
     * reference set, disregarding order entirely.
     *
     * @param refSet expected results
     *
     * @throws Exception .
     */
    protected void compareResultSet(Set refSet)
        throws Exception
    {
        Set actualSet = new HashSet();
        while (resultSet.next()) {
            String s = resultSet.getString(1);
            actualSet.add(s);
        }
        resultSet.close();
        assertEquals(refSet, actualSet);
    }

    /**
     * Compare the first column of a result set against a pattern. The result
     * set must return exactly one row.
     *
     * @param pattern Expected pattern
     */
    protected void compareResultSetWithPattern(Pattern pattern)
        throws Exception
    {
        if (!resultSet.next()) {
            fail("Query returned 0 rows, expected 1");
        }
        String actual = resultSet.getString(1);
        if (resultSet.next()) {
            fail("Query returned 2 or more rows, expected 1");
        }
        if (!pattern.matcher(actual).matches()) {
            fail("Query returned '" + actual + "', expected '"
                + pattern.pattern() + "'");
        }
    }

    /**
     * Compare the first column of a result set against a String-valued
     * reference set, taking order into account.
     *
     * @param refList expected results
     *
     * @throws Exception .
     */
    protected void compareResultList(List refList)
        throws Exception
    {
        List actualSet = new ArrayList();
        while (resultSet.next()) {
            String s = resultSet.getString(1);
            actualSet.add(s);
        }
        resultSet.close();
        assertEquals(refList, actualSet);
    }

    /**
     * Retrieve a new instance of the FarragoJdbcEngineDriver
     * configured for this test.
     *
     * @return an instance of FarragoJdbcEngineDriver (or a subclass)
     *
     * @throws Exception
     */
    protected static FarragoJdbcEngineDriver newJdbcEngineDriver()
        throws Exception
    {
        String driverName =
            FarragoProperties.instance().testJdbcDriverClass.get();
        if (driverName == null) {
            return new FarragoJdbcEngineDriver();
        }
        return (FarragoJdbcEngineDriver) Class.forName(driverName).newInstance();
    }

    protected void runSqlLineTest(String sqlFile)
        throws Exception
    {
        FarragoJdbcEngineDriver driver = newJdbcEngineDriver();
        assert (sqlFile.endsWith(".sql"));
        File sqlFileSansExt =
            new File(sqlFile.substring(0, sqlFile.length() - 4));
        String [] args =
            new String [] {
                "-u", driver.getUrlPrefix(), "-d",
                "net.sf.farrago.jdbc.engine.FarragoJdbcEngineDriver", "-n",
                "guest", "--force=true", "--silent=true",
                "--showWarnings=false", "--maxWidth=1024"
            };
        PrintStream savedOut = System.out;
        PrintStream savedErr = System.err;

        // read from the specified file
        FileInputStream inputStream = new FileInputStream(sqlFile.toString());

        // to make sure the connection is closed properly, append the
        // !quit command
        String quitCommand = "\n!quit\n";
        ByteArrayInputStream quitStream =
            new ByteArrayInputStream(quitCommand.getBytes());

        SequenceInputStream sequenceStream =
            new SequenceInputStream(inputStream, quitStream);
        try {
            OutputStream outputStream =
                openTestLogOutputStream(sqlFileSansExt);
            PrintStream printStream = new PrintStream(outputStream);
            System.setOut(printStream);
            System.setErr(printStream);

            // tell SqlLine not to exit (this boolean is active-low)
            System.setProperty("sqlline.system.exit", "true");
            SqlLine.mainWithInputRedirection(args, sequenceStream);
            printStream.flush();
            if (shouldDiff()) {
                diffTestLog();
            }
        } finally {
            System.setOut(savedOut);
            System.setErr(savedErr);
            inputStream.close();
        }
    }

    protected boolean shouldDiff()
    {
        return true;
    }

    //~ Inner Classes ---------------------------------------------------------

    /**
     * Helper for staticSetUp.
     */
    public static class Cleanup extends FarragoTestCase
    {
        public Cleanup(String name)
            throws Exception
        {
            super(name);
        }

        public void execute()
            throws Exception
        {
            restoreParameters();
            dropSchemas();
            dropDataWrappers();
        }

        private void dropSchemas()
            throws Exception
        {
            List list = new ArrayList();

            // NOTE:  don't use DatabaseMetaData.getSchemas since it doesn't
            // work when Fennel is disabled
            Iterator schemaIter =
                repos.getSelfAsCwmCatalog().getOwnedElement().iterator();
            while (schemaIter.hasNext()) {
                Object obj = schemaIter.next();
                if (!(obj instanceof CwmSchema)) {
                    continue;
                }
                CwmSchema schema = (CwmSchema) obj;
                if (schema.getName().equals("SALES")) {
                    continue;
                }
                list.add(schema.getName());
            }
            Iterator iter = list.iterator();
            while (iter.hasNext()) {
                String name = (String) iter.next();
                stmt.execute("drop schema \"" + name + "\" cascade");
            }
        }

        private void dropDataWrappers()
            throws Exception
        {
            List list = new ArrayList();
            Iterator iter =
                repos.medPackage.getFemDataWrapper().refAllOfClass().iterator();
            while (iter.hasNext()) {
                FemDataWrapper wrapper = (FemDataWrapper) iter.next();
                if (wrapper.getName().startsWith("SYS_")) {
                    continue;
                }
                list.add(wrapper.isForeign() ? "foreign" : "local");
                list.add(wrapper.getName());
            }
            iter = list.iterator();
            while (iter.hasNext()) {
                String wrapperType = (String) iter.next();
                String name = (String) iter.next();
                stmt.execute("drop " + wrapperType + " data wrapper \"" + name
                    + "\" cascade");
            }
        }
    }
}


// End FarragoTestCase.java
