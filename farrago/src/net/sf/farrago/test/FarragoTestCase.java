/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2005-2005 Disruptive Tech
// Copyright (C) 2005-2005 LucidEra, Inc.
// Portions Copyright (C) 2003-2005 John V. Sichi
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

import java.io.*;
import java.sql.*;
import java.util.*;
import java.util.logging.*;
import java.util.regex.Pattern;

import junit.extensions.*;
import junit.framework.*;

import net.sf.farrago.catalog.*;
import net.sf.farrago.cwm.relational.*;
import net.sf.farrago.fem.med.*;
import net.sf.farrago.fem.security.*;
import net.sf.farrago.jdbc.engine.*;
import net.sf.farrago.trace.*;
import net.sf.farrago.util.*;
import net.sf.farrago.db.*;
import net.sf.farrago.session.*;

import org.eigenbase.test.*;
import org.eigenbase.util.SaffronProperties;
import org.eigenbase.util.property.*;

import sqlline.SqlLine;


/**
 * FarragoTestCase is a common base for Farrago JUnit tests.  Subclasses must
 * implement the suite() method in order to get a database connection
 * correctly initialized.  See FarragoQueryTest for an example.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public abstract class FarragoTestCase extends ResultSetTestCase
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
    private static SortedMap<String,Object> savedFarragoConfig;

    /**
     * Saved copy of Fennel configuration parameters.
     */
    private static SortedMap<String,Object> savedFennelConfig;

    private static Thread shutdownHook;

    /**
     * Connection counter for distinguishing connections during debug.
     * @see #newConnection
     */
    private static int connCounter = 0;

    //~ Instance fields -------------------------------------------------------

    /** PreparedStatement for processing queries. */
    protected PreparedStatement preparedStmt;

    /** Statement for processing queries. */
    protected Statement stmt;

    /** An owner for any heavyweight allocations. */
    protected FarragoCompoundAllocation allocOwner;

    static 
    {
        // If required system properties aren't set yet, attempt to set them
        // based on environment variables.  This allows tests to work with less
        // fuss in IDE's such as Eclipse which make it difficult to set
        // properties globally.
        StringProperty homeDir =
            FarragoProperties.instance().homeDir;
        StringProperty traceConfigFile =
            FarragoProperties.instance().traceConfigFile;
        if (homeDir.get() == null) {
            String eigenHome = System.getenv("EIGEN_HOME");
            if (eigenHome != null) {
                homeDir.set(new File(eigenHome, "farrago").getAbsolutePath());
                if (traceConfigFile.get() == null) {
                    traceConfigFile.set(
                        new File(
                            new File(
                                homeDir.get(),
                                "trace"),
                            "FarragoTrace.properties").getAbsolutePath());
                }
            }
        }
    }

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
            connection = newConnection();
            repos = getSession().getRepos();
            saveParameters();
        } else {
            // cycle connections to discard any leftover session state; but do
            // it crab-wise so that repos doesn't get shut down in the middle
            Connection newConnection = newConnection();
            connection.close();
            connection = newConnection;
        }

        runCleanup();
    }

    protected static void runCleanup()
        throws Exception
    {
        // See CleanupFactory for an example of adding custom cleanup.
        Cleanup cleanup = CleanupFactory.getFactory().newCleanup("cleanup");
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
        if (!FarragoDbSingleton.isReferenced()) {
            // some kind of forced shutdown already happened; pop out
            return;
        }
        if (repos != null) {
            restoreParameters();
        }
        rollbackIfSupported();
    }

    /**
     * @return FarragoSession for this test
     */
    protected static FarragoSession getSession()
    {
        assert(connection != null);
        FarragoJdbcEngineConnection farragoConnection =
            (FarragoJdbcEngineConnection) connection;
        return farragoConnection.getSession();
    }

    private static void rollbackIfSupported()
        throws SQLException
    {
        if (connection == null) {
            return;
        }
        if (connection.getMetaData().supportsTransactions()) {
            connection.rollback();
        }
    }

    private static Connection newConnection()
        throws Exception
    {
        FarragoJdbcEngineDriver driver = newJdbcEngineDriver();
        // create sessionName with connection counter to help
        // distinguish connections during debugging
        String sessionName = ";sessionName=FarragoTestCase:" + ++connCounter;
        Connection newConnection =
            DriverManager.getConnection(
                driver.getUrlPrefix() +sessionName,
                FarragoCatalogInit.SA_USER_NAME,
                null);
        if (newConnection.getMetaData().supportsTransactions()) {
            newConnection.setAutoCommit(false);
        }
        return newConnection;
    }

    public static void forceShutdown() throws Exception
    {
        repos = null;
        rollbackIfSupported();
        if (connection != null) {
            connection.close();
            connection = null;
        }

        // This method shouldn't be called except by the "last one out".  So
        // if other sessions are still hanging around, that's bad; fail
        // tests so some unlucky person will have to track them down.
        // Hint:
        /*
        grep -n -F "`echo pinReference && echo disconnectSession`" \
        FarragoTrace.log | more
        */
        if (FarragoDbSingleton.isReferenced()) {
            String msg = "Leaked test sessions detected, aborting!";
            System.err.println(msg);
            tracer.severe(msg);
            Runtime.getRuntime().halt(1);
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
     * Saves system parameters.
     */
    protected static void saveParameters()
    {
        saveParameters(repos);
    }
    
    /**
     * Restores system parameters to state saved by saveParameters().
     */
    protected static void restoreParameters()
    {
        restoreParameters(repos);
    }
    
    protected static void saveParameters(FarragoRepos repos)
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

    protected static void restoreParameters(FarragoRepos repos)
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
    public static Test wrappedSuite(Class<? extends TestCase> clazz)
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

    // implement TestCase
    protected void setUp()
        throws Exception
    {
        allocOwner = new FarragoCompoundAllocation();
        
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
            rollbackIfSupported();
            allocOwner.closeAllocation();
            allocOwner = null;
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
     * Retrieves a new instance of the FarragoJdbcEngineDriver
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
        Class<?> clazz = Class.forName(driverName);
        return (FarragoJdbcEngineDriver) clazz.newInstance();
    }

    protected void runSqlLineTest(String sqlFile)
        throws Exception
    {
        FarragoJdbcEngineDriver driver = newJdbcEngineDriver();
        assert (sqlFile.endsWith(".sql"));
        File sqlFileSansExt =
            new File(sqlFile.substring(0, sqlFile.length() - 4));
        String driverName = driver.getClass().getName();
        String [] args =
            new String [] {
                "-u", driver.getUrlPrefix(), "-d",
                driverName, "-n",
                FarragoCatalogInit.SA_USER_NAME,
                "--force=true", "--silent=true",
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
     * CleanupFactory is a factory for Cleanup objects.  Test cases
     * overriding FarragoTestCase can provide custom test cleanup by
     * overriding CleanupFactory.  This is normally done only for
     * extension projects that have additional repository objects that
     * require special cleanup.  Most individual test cases can simply
     * use the setUp/tearDown facility built into JUnit. The alternate
     * CleanupFactory should override <code>newCleanup(String)</code>
     * to return an extension of Cleanup that provides custom cleanup.
     * The extension project test case should provide static helper
     * methods to wrap the Test objects returned by FarragoTestCase's
     * static <code>wrappedSuite</code> methods with code that sets
     * and resets the CleanupFactory.  In other words:
     *
     * <pre>
     * public abstract class ExtensionTestCase extends FarragoTestCase
     * {
     *     public static Test wrappedExtensionSuite(Class clazz)
     *     {
     *         TestSuite suite = new TestSuite(clazz);
     *         return wrappedExtensionSuite(suite);
     *     }
     *
     *     public static Test wrappedExtensionSuite(TestSuite suite)
     *     {
     *         Test farragoSuite = FarragoTestCase.wrappedSuite(suite);
     *       
     *         TestSetup wrapper = 
     *             new TestSetup(farragoSuite) {
     *                 protected void setUp() throws Exception
     *                 {
     *                     CleanupFactory.setFactory(
     *                         new ExtensionCleanupFactory());
     *                 }
     *       
     *                 protected void tearDown() throws Exception
     *                 {
     *                     CleanupFactory.resetFactory();
     *                 }
     *             };
     *         return wrapper;
     *     }
     * }
     * </pre>
     */
    public static class CleanupFactory
    {
        private static final CleanupFactory defaultFactory = 
        	new CleanupFactory();

        private static CleanupFactory factory = defaultFactory;

        public static synchronized void setFactory(CleanupFactory altFactory)
        {
            factory = altFactory;
        }

        public static synchronized void resetFactory()
        {
            factory = defaultFactory;
        }

        public static synchronized CleanupFactory getFactory()
        {
            return factory;
        }
        
        public Cleanup newCleanup(String name) throws Exception
        {
            return new Cleanup(name);
        }
    }

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

        protected FarragoRepos getRepos()
        {
            return repos;
        }
        
        protected Statement getStmt()
        {
            return stmt;
        }

        public void execute()
            throws Exception
        {
            restoreCleanupParameters();
            dropSchemas();
            dropDataWrappers();
            dropDataServers();
            dropAuthIds();
        }

        public void saveCleanupParameters()
        {
            if (getRepos() != null) {
                saveParameters(getRepos());
            }
        }
    
        public void restoreCleanupParameters()
        {
            if (getRepos() != null) {
                restoreParameters(getRepos());
            }
        }
        
        /**
         * Decides whether schema should be preserved as a global fixture.
         * Extension project test case can override this method to bless
         * additional schemas or use attributes other than the name to make
         * the determination.
         *
         * @param schema schema to check
         * @return true iff schema should be preserved as fixture
         */
        protected boolean isBlessedSchema(CwmSchema schema)
        {
            String name = schema.getName();
            return name.equals("SALES")
                || name.equals("SQLJ")
                || name.equals("INFORMATION_SCHEMA")
                || name.startsWith("SYS_");
        }

        /**
         * Decides whether server should be preserved as a global fixture.
         * Extension project test case can override this method to bless
         * additional servers or use attributes other than the name to make
         * the determination.
         *
         * @param server server to check
         * @return true iff schema should be preserved as fixture
         */
        protected boolean isBlessedServer(FemDataServer server)
        {
            String name = server.getName();
            return name.equals("HSQLDB_DEMO")
                || name.startsWith("SYS_");
        }

        private void dropSchemas()
            throws Exception
        {
            List<String> list = new ArrayList<String>();

            // NOTE:  don't use DatabaseMetaData.getSchemas since it doesn't
            // work when Fennel is disabled
            Iterator schemaIter =
                getRepos().getSelfAsCatalog().getOwnedElement().iterator();
            while (schemaIter.hasNext()) {
                Object obj = schemaIter.next();
                if (!(obj instanceof CwmSchema)) {
                    continue;
                }
                CwmSchema schema = (CwmSchema) obj;
                String schemaName = schema.getName();
                if (!isBlessedSchema(schema)) {
                    list.add(schemaName);
                }
            }
            for (String name : list) {
                getStmt().execute("drop schema \"" + name + "\" cascade");
            }
        }

        private void dropDataWrappers()
            throws Exception
        {
            List<String> list = new ArrayList<String>();
            for (FemDataWrapper wrapper :
                getRepos().allOfClass(FemDataWrapper.class)) {
                if (wrapper.getName().startsWith("SYS_")) {
                    continue;
                }
                list.add(wrapper.isForeign() ? "foreign" : "local");
                list.add(wrapper.getName());
            }
            Iterator<String> iter = list.iterator();
            while (iter.hasNext()) {
                String wrapperType = iter.next();
                String name = iter.next();
                getStmt().execute(
                    "drop " + wrapperType + " data wrapper \"" + name
                    + "\" cascade");
            }
        }

        // NOTE jvs 21-May-2006: Dropping data wrappers cascades to server, so
        // this isn't strictly necessary.  But it's convenient for test authors
        // so that they can use the prefab wrapper definitions and still have
        // servers dropped.
        private void dropDataServers()
            throws Exception
        {
            List<String> list = new ArrayList<String>();
            for (FemDataServer server :
                getRepos().allOfClass(FemDataServer.class)) {
                if (isBlessedServer(server)) {
                    continue;
                }
                list.add(server.getName());
            }
            for (String name : list) {
                getStmt().execute(
                    "drop server \"" + name
                        + "\" cascade");
            }
        }
        
        private void dropAuthIds()
            throws Exception
        {
            List<String> list = new ArrayList<String>();
            for (FemAuthId authId : getRepos().allOfType(FemAuthId.class)) {
                if (authId.getName().equals(
                    FarragoCatalogInit.SYSTEM_USER_NAME)
                    || authId.getName().equals(
                    FarragoCatalogInit.PUBLIC_ROLE_NAME)
                    || authId.getName().equals(
                    FarragoCatalogInit.SA_USER_NAME)) {
                    continue;
                }
                list.add(
                    ((authId instanceof FemRole) ? "ROLE" : "USER")
                        + " "
                        + authId.getName());
            }
            for (String name : list) {
                getStmt().execute("drop " + name);
            }
        }
    }
}


// End FarragoTestCase.java
