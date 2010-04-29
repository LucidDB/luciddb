/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2009 The Eigenbase Project
// Copyright (C) 2005-2009 SQLstream, Inc.
// Copyright (C) 2005-2009 LucidEra, Inc.
// Portions Copyright (C) 2003-2009 John V. Sichi
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
import java.util.regex.*;

import junit.extensions.*;

import junit.framework.*;

import net.sf.farrago.catalog.*;
import net.sf.farrago.cwm.core.*;
import net.sf.farrago.cwm.relational.*;
import net.sf.farrago.db.*;
import net.sf.farrago.fem.med.*;
import net.sf.farrago.fem.security.*;
import net.sf.farrago.jdbc.*;
import net.sf.farrago.jdbc.engine.*;
import net.sf.farrago.release.*;
import net.sf.farrago.session.*;
import net.sf.farrago.trace.*;
import net.sf.farrago.util.*;

import org.eigenbase.jmi.*;
import org.eigenbase.sql.*;
import org.eigenbase.sql.util.SqlBuilder;
import org.eigenbase.test.*;
import org.eigenbase.util.*;
import org.eigenbase.util.property.*;

import sqlline.SqlLine;


/**
 * FarragoTestCase is a common base for Farrago JUnit tests. Subclasses must
 * implement the suite() method in order to get a database connection correctly
 * initialized. See FarragoQueryTest for an example.
 *
 * <p>For SQL tests, FarragoTestCase writes the output from sqlline to a file
 * called <code>&lt;<i>testname</i>&gt;.log</code>, and compares that output
 * with a reference log file <code>&lt;<i>testname</i>&gt;.ref</code>.
 *
 * <p>It is also possible to have additional logfiles, for tests which generate
 * output to files. FarragoTestCase scans the input .sql file for lines of the
 * form
 *
 * <blockquote><code>##COMPARE &lt;file&gt;.log</code></blockquote>
 *
 * and for each such command, it compares file.log with file.ref.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public abstract class FarragoTestCase
    extends ResultSetTestCase
{
    //~ Static fields/initializers ---------------------------------------------

    /**
     * Logger to use for test tracing.
     */
    protected static final Logger tracer = FarragoTrace.getTestTracer();

    /**
     * JDBC connection to Farrago database.
     */
    protected static Connection connection;

    /**
     * Repos for test object definitions.
     */
    protected static FarragoRepos repos;

    /**
     * Flag used to allow individual test methods to be called from IntelliJ.
     */
    private static boolean individualTest;

    /**
     * Saved copy of Farrago configuration parameters.
     */
    private static SortedMap<String, Object> savedFarragoConfig;

    /**
     * Saved copy of Fennel configuration parameters.
     */
    private static SortedMap<String, Object> savedFennelConfig;

    private static Thread shutdownHook;

    /**
     * Connection counter for distinguishing connections during debug.
     *
     * @see #newConnection
     */
    private static int connCounter = 0;

    static {
        // If required system properties aren't set yet, attempt to set them
        // based on environment variables.  This allows tests to work with less
        // fuss in IDE's such as Eclipse which make it difficult to set
        // properties globally.
        StringProperty homeDir = FarragoProperties.instance().homeDir;
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

    //~ Instance fields --------------------------------------------------------

    /**
     * PreparedStatement for processing queries.
     */
    protected PreparedStatement preparedStmt;

    /**
     * Statement for processing queries.
     */
    protected Statement stmt;

    /**
     * An owner for any heavyweight allocations.
     */
    protected FarragoCompoundAllocation allocOwner;

    //~ Constructors -----------------------------------------------------------

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

    //~ Methods ----------------------------------------------------------------

    // override DiffTestCase
    protected File getTestlogRoot()
        throws Exception
    {
        return getTestlogRootStatic();
    }

    /**
     * Implementation for { @link DiffTestCase#getTestlogRoot } which uses
     * 'testlog' directory under Farrago home.
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
            if (connection instanceof FarragoJdbcEngineConnection) {
                repos = getSession().getRepos();
                saveParameters();
            }
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
        boolean endSession = false;
        try {
            cleanup.setUp();
            if (connection instanceof FarragoJdbcEngineConnection) {
                getSession().getRepos().beginReposSession();
                endSession = true;
            }
            cleanup.execute(); // let overrides see this call!
        } finally {
            if (endSession) {
                getSession().getRepos().endReposSession();
            }

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
        assert (connection != null);
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

    protected static Connection newConnection()
        throws Exception
    {
        FarragoAbstractJdbcDriver driver = newJdbcEngineDriver();
        String uri = getJdbcUri(driver);
        Properties props = new Properties();
        props.put("user", FarragoCatalogInit.SA_USER_NAME);
        props.put("password", "mumble");
        Connection newConnection = driver.connect(uri, props);
        if (newConnection.getMetaData().supportsTransactions()) {
            newConnection.setAutoCommit(false);
        }
        return newConnection;
    }

    public static void forceShutdown()
        throws Exception
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
        FarragoReposTxnContext reposTxn = repos.newTxnContext(true);
        try {
            reposTxn.beginReadTxn();
            savedFarragoConfig =
                JmiObjUtil.getAttributeValues(repos.getCurrentConfig());
            savedFennelConfig =
                JmiObjUtil.getAttributeValues(
                    repos.getCurrentConfig().getFennelConfig());

            // NOTE jvs 15-Mar-2007:  special case for these parameters
            // which doesn't take effect until restart anyway;
            // let the change be permanent (test must know what it is doing)
            savedFarragoConfig.remove("serverRmiRegistryPort");
            savedFarragoConfig.remove("serverSingleListenerPort");
            savedFarragoConfig.remove("connectionTimeoutMillis");
            savedFennelConfig.remove("resourceDir");
            savedFennelConfig.remove("deviceSchedulerType");
            savedFennelConfig.remove("freshmenPageQueuePercentage");
            savedFennelConfig.remove("pageHistoryQueuePercentage");
            savedFennelConfig.remove("prefetchPagesMax");
            savedFennelConfig.remove("prefetchThrottleRate");
            savedFennelConfig.remove("processorCacheBytes");
        } finally {
            reposTxn.commit();
        }
    }

    protected static void restoreParameters(FarragoRepos repos)
    {
        FarragoReposTxnContext reposTxn = repos.newTxnContext(true);
        try {
            reposTxn.beginWriteTxn();
            JmiObjUtil.setAttributeValues(
                repos.getCurrentConfig(),
                savedFarragoConfig);
            JmiObjUtil.setAttributeValues(
                repos.getCurrentConfig().getFennelConfig(),
                savedFennelConfig);
        } finally {
            reposTxn.commit();
        }
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
     * per-testcase members
     */
    public static Test wrappedSuite(Class<? extends TestCase> clazz)
    {
        TestSuite suite = new TestSuite(clazz);
        return wrappedSuite(suite);
    }

    /**
     * Generic implementation of suite() to be called by subclasses.
     *
     * <p>If the {@link SaffronProperties#testEverything} property is false, and
     * the {@link SaffronProperties#testName} property is set, then returns a
     * suite containing only the tests whose names match.
     *
     * @param suite the suite being wrapped
     *
     * @return a JUnit test suite which will take care of initialization of
     * per-testcase members
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

        tracer.info(
            "Entering test case "
            + getClass().getName() + "." + getName());
        super.setUp();
        stmt = connection.createStatement();
        if (connection instanceof FarragoJdbcEngineConnection) {
            // discard any cached query plans (can't call
            // sys_boot.mgmt.flush_code_cache because it may not exist yet,
            // plus it's slow)
            FarragoObjectCache codeCache =
                ((FarragoDbSession) getSession()).getDatabase().getCodeCache();
            long savedBytesMax = codeCache.getBytesMax();
            codeCache.setMaxBytes(0);
            codeCache.setMaxBytes(savedBytesMax);
        }

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
            tracer.info(
                "Leaving test case "
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
     * Retrieves a new instance of the FarragoJdbcEngineDriver configured for
     * this test.
     *
     * @return an instance of FarragoJdbcEngineDriver (or a subclass)
     *
     * @throws Exception
     */
    protected static FarragoAbstractJdbcDriver newJdbcEngineDriver()
        throws Exception
    {
        String driverName =
            FarragoProperties.instance().testJdbcDriverClass.get();
        if (driverName == null) {
            return new FarragoJdbcEngineDriver();
        }
        Class<?> clazz = Class.forName(driverName);
        return (FarragoAbstractJdbcDriver) clazz.newInstance();
    }

    protected static String getJdbcUri(FarragoAbstractJdbcDriver driver)
        throws Exception
    {
        final StringBuilder sb = new StringBuilder();
        sb.append(driver.getUrlPrefix());

        if (driver.acceptsUrlWithHostPort()) {
            // append host:port specification for client drivers
            // Default to "localhost", but could also get real hostname with
            // java.net.InetAddress.getLocalHost().getHostName().
            // Supplying RMI port allows client-driver tests to be run against
            // systems which use a non-default RMI port.
            sb.append("//localhost");
            int rmiRegistryPort =
                FarragoReleaseProperties.instance().jdbcUrlPortDefault.get();
            sb.append(":").append(rmiRegistryPort);
        }

        // create sessionName with connection counter to help
        // distinguish connections during debugging
        sb.append(";sessionName=FarragoTestCase:");
        sb.append(++connCounter);

        return sb.toString();
    }

    protected void runSqlLineTest(String sqlFile)
        throws Exception
    {
        runSqlLineTest(sqlFile, shouldDiff());
    }

    protected void runSqlLineTest(
        String sqlFile,
        boolean shouldDiff)
        throws Exception
    {
        tracer.finer("runSqlLineTest: Starting " + sqlFile);
        FarragoAbstractJdbcDriver driver = newJdbcEngineDriver();
        String uri = getJdbcUri(driver);
        assert (sqlFile.endsWith(".sql")) : "\"" + sqlFile
            + "\" does not end with .sql";
        File sqlFileSansExt =
            new File(sqlFile.substring(0, sqlFile.length() - 4));
        String driverName = driver.getClass().getName();
        String [] args =
            new String[] {
                "-u", uri, "-d",
                driverName, "-n",
                FarragoCatalogInit.SA_USER_NAME,
                "--force=true", "--silent=true",
                "--maxWidth=1024"
            };
        PrintStream savedOut = System.out;
        PrintStream savedErr = System.err;

        // get contents of file
        String sqlFileContents = fileContents(new File(sqlFile));

        // read from the specified file
        InputStream inputStream = new FileInputStream(sqlFile);

        // to make sure the connection is closed properly, append the
        // !quit command
        String quitCommand = "\n!quit\n";
        ByteArrayInputStream quitStream =
            new ByteArrayInputStream(quitCommand.getBytes());

        SequenceInputStream sequenceStream =
            new SequenceInputStream(inputStream, quitStream);
        try {
            OutputStream outputStream = openTestLogOutputStream(sqlFileSansExt);
            FilterOutputStream filterStream =
                new ReplacingOutputStream(
                    outputStream,
                    "(0: jdbc(:[^:>]+)+:(//.*:[0123456789]+)?|(\\. )*\\.?)>",
                    ">");
            PrintStream printStream = new PrintStream(filterStream);
            System.setOut(printStream);
            System.setErr(printStream);

            // tell SqlLine not to exit (this boolean is active-low)
            System.setProperty("sqlline.system.exit", "true");
            SqlLine.mainWithInputRedirection(args, sequenceStream);
            printStream.close();
            if (shouldDiff) {
                addDiffMask("\\$" + "Id: .*$");
                diffTestLog();

                // Execute any '##COMPARE <filename>' commands in the .sql file
                int k;
                while ((k = sqlFileContents.indexOf("##COMPARE ")) > 0) {
                    sqlFileContents = sqlFileContents.substring(k);
                    int n = sqlFileContents.indexOf(TestUtil.NL);
                    String logFile =
                        sqlFileContents.substring("##COMPARE ".length(), n);
                    if (!logFile.endsWith(".log")) {
                        throw new AssertionError(
                            "Filename argument to '##COMPARE' must end "
                            + "in '.log': " + logFile);
                    }
                    String refFile =
                        logFile.substring(
                            0,
                            logFile.length() - ".log".length()) + ".ref";
                    diffFile(new File(logFile), new File(refFile));
                }
            }
        } finally {
            System.setOut(savedOut);
            System.setErr(savedErr);
            inputStream.close();
        }
        tracer.finer("runSqlLineTest: Completed " + sqlFile);
    }

    protected boolean shouldDiff()
    {
        return true;
    }

    // override DiffTestCase
    protected void setRefFileDiffMasks()
    {
        super.setRefFileDiffMasks();

        // physical quantities such as row size may vary according to
        // architecture
        addDiffMask(".*Row size.*exceeds maximum.*");
    }

    //~ Inner Classes ----------------------------------------------------------

    private static class ShutdownThread
        extends Thread
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
     * CleanupFactory is a factory for Cleanup objects. Test cases overriding
     * FarragoTestCase can provide custom test cleanup by overriding
     * CleanupFactory. This is normally done only for extension projects that
     * have additional repository objects that require special cleanup. Most
     * individual test cases can simply use the setUp/tearDown facility built
     * into JUnit. The alternate CleanupFactory should override <code>
     * newCleanup(String)</code> to return an extension of Cleanup that provides
     * custom cleanup. The extension project test case should provide static
     * helper methods to wrap the Test objects returned by FarragoTestCase's
     * static <code>wrappedSuite</code> methods with code that sets and resets
     * the CleanupFactory. In other words:
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

        public Cleanup newCleanup(String name)
            throws Exception
        {
            return new Cleanup(name);
        }
    }

    /**
     * Helper for staticSetUp.
     */
    public static class Cleanup
        extends FarragoTestCase
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
            dropLabels();
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
         * additional schemas or use attributes other than the name to make the
         * determination.
         *
         * @param schema schema to check
         *
         * @return true iff schema should be preserved as fixture
         */
        protected boolean isBlessedSchema(CwmSchema schema)
        {
            String name = schema.getName();
            return isBlessedSchema(name);
        }

        protected boolean isBlessedSchema(String name)
        {
            tracer.finer("checking name: " + name);
            return name.equals("SALES")
                || name.equals("SQLJ")
                || name.equals("INFORMATION_SCHEMA")
                || name.startsWith("SYS_");
        }

        /**
         * Decides whether server should be preserved as a global fixture.
         * Extension project test case can override this method to bless
         * additional servers or use attributes other than the name to make the
         * determination.
         *
         * @param server server to check
         *
         * @return true iff schema should be preserved as fixture
         */
        protected boolean isBlessedServer(FemDataServer server)
        {
            return isBlessedServer(server.getName());
        }

        protected boolean isBlessedServer(String name)
        {
            tracer.finer("checking name: " + name);
            return name.equals("HSQLDB_DEMO")
                || name.startsWith("SYS_");
        }

        /**
         * Decides whether wrapper should be preserved as a global fixture.
         * Extension project test case can override this method to bless
         * additional schemas or use attributes other than the name to make the
         * determination.
         *
         * @param wrapper wrapper to check
         *
         * @return true iff wrapper should be preserved as fixture
         */
        protected boolean isBlessedWrapper(FemDataWrapper wrapper)
        {
            return isBlessedWrapper(wrapper.getName());
        }

        protected boolean isBlessedWrapper(String name)
        {
            tracer.finer("checking name: " + name);
            return name.startsWith("SYS_")
                || name.equals("SALESFORCE");
        }

        /**
         * Decides where a label should be preserved because it's a global
         * fixture or because it's a label alias. Label aliases are preserved
         * (temporarily) because they will be dropped, as needed, by the
         * cascaded drop of the parent label. Extension project test case can
         * override this method to bless additional labels or use attributes
         * other than the name to make the determination.
         *
         * @param label label to check
         *
         * @return true iff label should be preseved as fixture
         */
        protected boolean isBlessedLabel(FemLabel label)
        {
            if (label.getParentLabel() != null) {
                return true;
            }
            return isBlessedLabel(label.getName());
        }

        protected boolean isBlessedLabel(String name)
        {
            tracer.finer("checking name: " + name);
            return false;
        }

        /**
         * Decides whether authId should be preserved as a global fixture.
         * Extension project test case can override this method to bless
         * additional authIds or use attributes other than the name to make the
         * determination.
         *
         * @param authId authorization ID to check
         *
         * @return true iff authId should be preserved as fixture
         */
        protected boolean isBlessedAuthId(FemAuthId authId)
        {
            return isBlessedAuthId(authId.getName());
        }

        protected boolean isBlessedAuthId(String name)
        {
            tracer.finest("checking name: " + name);
            return name.equals(FarragoCatalogInit.SYSTEM_USER_NAME)
                || name.equals(FarragoCatalogInit.PUBLIC_ROLE_NAME)
                || name.equals(FarragoCatalogInit.SA_USER_NAME);
        }

        protected void dropSchemas()
            throws Exception
        {
            List<String> list = new ArrayList<String>();

            tracer.fine("Dropping Schemas.");

            // NOTE:  don't use DatabaseMetaData.getSchemas since it doesn't
            // work when Fennel is disabled.  Also note that the
            // repository is not available when using a Client JDBC driver.
            final FarragoRepos repos = getRepos();
            if (repos != null) {
                for (
                    CwmModelElement obj
                    : repos.getSelfAsCatalog().getOwnedElement())
                {
                    if (!(obj instanceof CwmSchema)) {
                        continue;
                    }
                    CwmSchema schema = (CwmSchema) obj;
                    String schemaName = schema.getName();
                    if (!isBlessedSchema(schema)) {
                        list.add(schemaName);
                    }
                }
            } else if (connection != null) {
                ResultSet schemas = connection.getMetaData().getSchemas();
                while (schemas.next()) {
                    // ignore schemas not in the default catalog
                    String catalog = schemas.getString(2);
                    if (catalog.startsWith("SYS_")) {
                        continue;
                    }
                    String name = schemas.getString(1);
                    if (!isBlessedSchema(name)) {
                        list.add(name);
                    }
                }
                schemas.close();
            }

            tracer.finer("Schema name list has " + list.size() + " entries");
            final SqlBuilder buf = new SqlBuilder(SqlDialect.EIGENBASE);
            for (String name : list) {
                String dropStmt =
                    buf.append("drop schema ")
                        .identifier(name)
                        .append(" cascade")
                        .getSqlAndClear();
                tracer.finer(dropStmt);
                try {
                    getStmt().execute(dropStmt);
                } catch (Exception e) {
                    tracer.log(Level.INFO, "could not drop schema " + name, e);
                }
            }
        }

        private void dropDataWrappers()
            throws Exception
        {
            tracer.fine("Dropping DataWrappers.");
            List<String> list = new ArrayList<String>();
            final FarragoRepos repos = getRepos();
            if (repos != null) {
                for (
                    FemDataWrapper wrapper
                    : repos.allOfClass(FemDataWrapper.class))
                {
                    if (isBlessedWrapper(wrapper)) {
                        continue;
                    }
                    list.add(wrapper.isForeign() ? "foreign" : "local");
                    list.add(wrapper.getName());
                }
            } else if (stmt != null) {
                if (stmt.execute(
                        "select \"name\",\"foreign\" from "
                        + "sys_fem.med.\"DataWrapper\""))
                {
                    ResultSet rset = stmt.getResultSet();
                    while (rset.next()) {
                        String name = rset.getString(1);
                        if (isBlessedWrapper(name)) {
                            continue;
                        }
                        String foreignFlag = rset.getString(2);
                        list.add(
                            foreignFlag.equals("true") ? "foreign" : "local");
                        list.add(name);
                    }
                }
            }

            tracer.finer(
                "Datawrapper name list has " + list.size() + " entries");
            Iterator<String> iter = list.iterator();
            final SqlBuilder buf = new SqlBuilder(SqlDialect.EIGENBASE);
            while (iter.hasNext()) {
                String wrapperType = iter.next();
                String name = iter.next();
                String sql =
                    buf.append("drop ")
                        .append(wrapperType)
                        .append(" data wrapper ")
                        .identifier(name)
                        .append(" cascade")
                        .getSqlAndClear();
                tracer.finer(sql);
                getStmt().execute(sql);
            }
        }

        // NOTE jvs 21-May-2006: Dropping data wrappers cascades to server, so
        // this isn't strictly necessary.  But it's convenient for test authors
        // so that they can use the prefab wrapper definitions and still have
        // servers dropped.
        private void dropDataServers()
            throws Exception
        {
            tracer.fine("Dropping DataServers.");
            List<String> list = new ArrayList<String>();
            final FarragoRepos repos = getRepos();
            if (repos != null) {
                for (
                    FemDataServer server
                    : repos.allOfClass(FemDataServer.class))
                {
                    if (isBlessedServer(server)) {
                        continue;
                    }
                    list.add(server.getName());
                }
            } else if (stmt != null) {
                if (stmt.execute(
                        "select \"name\" from sys_fem.med.\"DataServer\""))
                {
                    ResultSet rset = stmt.getResultSet();
                    while (rset.next()) {
                        String name = rset.getString(1);
                        if (isBlessedServer(name)) {
                            continue;
                        }
                        list.add(name);
                    }
                }
            }

            tracer.finer(
                "Dataserver name list has " + list.size() + " entries");
            final SqlBuilder buf = new SqlBuilder(SqlDialect.EIGENBASE);
            for (String name : list) {
                String sql =
                    buf.append("drop server ")
                        .identifier(name)
                        .append(" cascade")
                        .getSqlAndClear();
                tracer.finer(sql);
                getStmt().execute(sql);
            }
        }

        private void dropLabels()
            throws Exception
        {
            tracer.fine("Dropping Labels.");
            List<String> list = new ArrayList<String>();
            final FarragoRepos repos = getRepos();
            if (repos != null) {
                for (FemLabel label
                    : repos.allOfClass(FemLabel.class))
                {
                    if (isBlessedLabel(label)) {
                        continue;
                    }
                    list.add(label.getName());
                }
            } else if (stmt != null) {
                // Ignore label aliases, as they'll get dropped by the
                // cascaded drop of the base labels.
                if (stmt.execute(
                        "select \"name\" from sys_fem.med.\"Label\" "
                        + "where \"ParentLabel\" is null"))
                {
                    ResultSet rset = stmt.getResultSet();
                    while (rset.next()) {
                        String name = rset.getString(1);
                        if (isBlessedLabel(name)) {
                            continue;
                        }
                        list.add(name);
                    }
                }
            }

            tracer.finer("Label name list has " + list.size() + " entries");
            final SqlBuilder buf = new SqlBuilder(SqlDialect.EIGENBASE);
            for (String name : list) {
                String sql =
                    buf.append("drop label ")
                        .identifier(name)
                        .append(" cascade")
                        .getSqlAndClear();
                tracer.finer(sql);
                getStmt().execute(sql);
            }
        }

        private void dropAuthIds()
            throws Exception
        {
            tracer.fine("Dropping AuthIds.");
            List<String> list = new ArrayList<String>();
            final FarragoRepos repos = getRepos();
            final SqlBuilder buf = new SqlBuilder(SqlDialect.EIGENBASE);
            if (repos != null) {
                for (FemAuthId authId : repos.allOfType(FemAuthId.class)) {
                    if (isBlessedAuthId(authId)) {
                        continue;
                    }
                    list.add(
                        buf.append("drop ")
                            .append(authId instanceof FemRole ? "ROLE" : "USER")
                            .append(" ")
                            .identifier(authId.getName())
                            .getSqlAndClear());
                }
            } else if (stmt != null) {
                if (stmt.execute(
                        "select \"name\",\"mofClassName\" from "
                        + "sys_fem.\"Security\".\"AuthId\""))
                {
                    ResultSet rset = stmt.getResultSet();
                    while (rset.next()) {
                        String name = rset.getString(1);
                        String className = rset.getString(2);
                        if (isBlessedAuthId(name)) {
                            continue;
                        }
                        list.add(
                            buf.append("drop ")
                                .append(className + " " + name)
                                .getSqlAndClear());
                    }
                }
            }

            tracer.finer("AuthId name list has " + list.size() + " entries");
            for (String sql : list) {
                tracer.finer(sql);
                getStmt().execute(sql);
            }
        }
    }

    /**
     * Stream which applies regular expression replacement to its contents.
     *
     * <p>Lame implementation which buffers its input and applies replacement
     * only when {@link #close} is called.
     */
    protected static class ReplacingOutputStream
        extends FilterOutputStream
    {
        private final OutputStream outputStream;
        private final String seekPattern;
        private final String replace;

        public ReplacingOutputStream(
            OutputStream outputStream,
            String seekPattern,
            String replace)
        {
            super(new ByteArrayOutputStream());
            this.outputStream = outputStream;
            this.seekPattern = seekPattern;
            this.replace = replace;
        }

        public void close()
            throws IOException
        {
            super.close();
            final String s = ((ByteArrayOutputStream) this.out).toString();
            final String s2 = s.replaceAll(seekPattern, replace);
            outputStream.write(s2.getBytes());
            outputStream.close();
        }
    }
}

// End FarragoTestCase.java
