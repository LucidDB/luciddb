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

import junit.extensions.*;

import junit.framework.*;

import net.sf.farrago.catalog.*;
import net.sf.farrago.jdbc.engine.*;
import net.sf.farrago.util.*;
import net.sf.farrago.trace.*;
import net.sf.farrago.cwm.relational.*;
import net.sf.farrago.fem.med.*;

import net.sf.saffron.test.*;

import java.io.*;

import java.sql.*;

import java.util.*;
import java.util.logging.*;

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

    /** Catalog for test object definitions. */
    protected static FarragoCatalog catalog;

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
    protected FarragoTestCase(String testName) throws Exception
    {
        super(testName);
    }

    // override DiffTestCase
    protected File getSourceRoot() throws Exception
    {
        String homeDir = FarragoProperties.instance().homeDir.get(true);
        return new File(homeDir,"src");
    }

    // override DiffTestCase
    protected File getTestlogRoot() throws Exception
    {
        String homeDir = FarragoProperties.instance().homeDir.get(true);
        return new File(homeDir,"testlog");
    }

    //~ Methods ---------------------------------------------------------------

    /**
     * One-time setup routine.
     *
     * @throws Exception .
     */
    public static void staticSetUp() throws Exception
    {
        FarragoJdbcEngineDriver driver = newJdbcEngineDriver();
        connection = DriverManager.getConnection(
            driver.getUrlPrefix());
        FarragoJdbcEngineConnection farragoConnection =
            (FarragoJdbcEngineConnection) connection;
        catalog = farragoConnection.getSession().getCatalog();
        connection.setAutoCommit(false);
        runCleanup();
    }

    protected static void runCleanup() throws Exception
    {
        Cleanup cleanup = new Cleanup("cleanup");
        try {
            cleanup.setUp();
            cleanup.execute();
        } finally {
            cleanup.tearDown();
        }
    }

    /**
     * One-time tear-down routine.
     *
     * @throws Exception .
     */
    public static void staticTearDown() throws Exception
    {
        catalog = null;
        if (connection != null) {
            connection.rollback();
            connection.close();
            connection = null;
        }
    }

    // NOTE: Catalog open/close is slow and causes sporadic problems when done
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
     * @param suite the suite being wrapped
     *
     * @return a JUnit test suite which will take care of initialization of
     *         per-testcase members
     */
    public static Test wrappedSuite(TestSuite suite)
    {
        TestSetup wrapper =
            new TestSetup(suite) {
                protected void setUp() throws Exception
                {
                    staticSetUp();
                }

                protected void tearDown() throws Exception
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
    protected int getResultSetCount() throws Exception
    {
        int n = 0;
        while (resultSet.next()) {
            ++n;
        }
        resultSet.close();
        return n;
    }

    // implement TestCase
    protected void setUp() throws Exception
    {
        super.setUp();
        tracer.info("Entering test case " + getName());
        assert (connection != null) : "You forgot to implement suite()";
        stmt = connection.createStatement();
        resultSet = null;
    }

    // implement TestCase
    protected void tearDown() throws Exception
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
            connection.rollback();
        } finally {
            tracer.info("Leaving test case " + getName());
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
    protected void compareResultSet(Set refSet) throws Exception
    {
        Set actualSet = new HashSet();
        while (resultSet.next()) {
            String s = resultSet.getString(1);
            actualSet.add(s);
        }
        resultSet.close();
        assertEquals(refSet,actualSet);
    }

    /**
     * Compare the first column of a result set against a String-valued
     * reference set, taking order into account.
     *
     * @param refList expected results
     *
     * @throws Exception .
     */
    protected void compareResultList(List refList) throws Exception
    {
        List actualSet = new ArrayList();
        while (resultSet.next()) {
            String s = resultSet.getString(1);
            actualSet.add(s);
        }
        resultSet.close();
        assertEquals(refList,actualSet);
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
        return (FarragoJdbcEngineDriver)
            Class.forName(driverName).newInstance();
    }

    protected void runSqlLineTest(String sqlFile) throws Exception
    {
        FarragoJdbcEngineDriver driver = newJdbcEngineDriver();
        assert(sqlFile.endsWith(".sql"));
        File sqlFileSansExt = new File(
            sqlFile.substring(0,sqlFile.length() - 4));
        String [] args = new String [] {
            "-u",
            driver.getUrlPrefix(),
            "-d",
            "net.sf.farrago.jdbc.engine.FarragoJdbcEngineDriver",
            "-n",
            "guest",
            "--force=true",
            "--silent=true",
            "--showWarnings=false",
            "--maxWidth=1024"
        };
        PrintStream savedOut = System.out;
        PrintStream savedErr = System.err;

        // read from the specified file
        FileInputStream inputStream = new FileInputStream(sqlFile.toString());

        // to make sure the connection is closed properly, append the
        // !quit command
        String quitCommand = "\n!quit\n";
        ByteArrayInputStream quitStream = new ByteArrayInputStream(
            quitCommand.getBytes());

        SequenceInputStream sequenceStream = new SequenceInputStream(
            inputStream,quitStream);
        try {
            OutputStream outputStream = openTestLogOutputStream(sqlFileSansExt);
            PrintStream printStream = new PrintStream(outputStream);
            System.setOut(printStream);
            System.setErr(printStream);
            // tell SqlLine not to exit (this boolean is active-low)
            System.setProperty("sqlline.system.exit","true");
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

    // override DiffTestCase
    protected Writer openTestLog() throws Exception
    {
        File testClassDir = new File(
            getTestlogRoot(),
            ReflectUtil.getUnqualifiedClassName(getClass()));
        testClassDir.mkdirs();
        File testLogFile = new File(testClassDir,getName());
        return new OutputStreamWriter(openTestLogOutputStream(testLogFile));
    }

    protected boolean shouldDiff()
    {
        if (catalog.isFennelEnabled()) {
            return true;
        }
        return FarragoProperties.instance().testDiff.get();
    }
    
    //~ Inner Classes ---------------------------------------------------------

    /**
     * Helper for staticSetUp.
     */
    public static class Cleanup extends FarragoTestCase
    {
        public Cleanup(String name) throws Exception
        {
            super(name);
        }
        
        public void execute() throws Exception
        {
            dropSchemas();
            dropDataWrappers();
        }
        
        private void dropSchemas() throws Exception
        {
            List list = new ArrayList();
            // NOTE:  don't use DatabaseMetaData.getSchemas since it doesn't
            // work when Fennel is disabled
            Iterator schemaIter =
                catalog.getSelfAsCwmCatalog().getOwnedElement().iterator();
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
                stmt.execute(
                    "drop schema \"" + name + "\" cascade");
            }
        }

        private void dropDataWrappers() throws Exception
        {
            List list = new ArrayList();
            Iterator iter =
                catalog.medPackage.getFemDataWrapper()
                .refAllOfClass().iterator();
            while (iter.hasNext()) {
                FemDataWrapper wrapper = (FemDataWrapper) iter.next();
                if (wrapper.getName().startsWith("SYS_")) {
                    continue;
                }
                list.add(wrapper.getName());
            }
            iter = list.iterator();
            while (iter.hasNext()) {
                String name = (String) iter.next();
                stmt.execute(
                    "drop foreign data wrapper \"" + name + "\" cascade");
            }
        }
    }
    
}


// End FarragoTestCase.java
