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

import junit.framework.*;

import net.sf.farrago.db.*;
import net.sf.farrago.jdbc.*;
import net.sf.farrago.jdbc.client.*;
import net.sf.farrago.jdbc.engine.*;
import net.sf.farrago.resource.*;
import net.sf.farrago.server.*;

import org.eigenbase.util.*;
import org.eigenbase.util14.*;


/**
 * FarragoServerTest tests Farrago client/server connections via RmiJdbc. It
 * does not inherit from FarragoTestCase since that pulls in an embedded engine.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class FarragoServerTest
    extends TestCase
{
    //~ Static fields/initializers ---------------------------------------------

    static private final String stmtMismatch =
        "Returned statement does not match original";

    //~ Instance fields --------------------------------------------------------

    private FarragoAbstractServer server;

    //~ Constructors -----------------------------------------------------------

    /**
     * Initializes a new FarragoServerTest.
     *
     * @param testCaseName JUnit test case name
     */
    public FarragoServerTest(String testCaseName)
        throws Exception
    {
        super(testCaseName);
    }

    //~ Methods ----------------------------------------------------------------

    protected void setUp()
        throws Exception
    {
        super.setUp();
        FarragoTestCase.forceShutdown();
    }

    protected FarragoAbstractServer newServer()
    {
        return new FarragoRmiJdbcServer();
    }

    protected FarragoAbstractJdbcDriver newClientDriver()
    {
        return new FarragoJdbcClientDriver();
    }

    protected boolean isJRockit()
    {
        // See http://issues.eigenbase.org/browse/FRG-316

        String vmName = System.getProperty("java.vm.name");
        if (vmName == null) {
            return false;
        }
        return vmName.indexOf("JRockit") != -1;
    }

    public void testServer()
        throws Exception
    {
        if (isJRockit()) {
            return;
        }

        server = newServer();
        FarragoJdbcEngineDriver serverDriver = new FarragoJdbcEngineDriver();
        server.start(serverDriver);

        // NOTE: can't call DriverManager.getConnection here, because that
        // would deadlock
        FarragoAbstractJdbcDriver clientDriver = newClientDriver();

        // N.B. it is better practice to put the login credentials in the
        // Properties object rather than on the URL, but this is a convenient
        // test of the client driver's connect string processing.
        String uri = clientDriver.getUrlPrefix() + "localhost;user=sa";
        Connection connection =
            clientDriver.connect(
                uri,
                new Properties());
        boolean stopped;
        try {
            connection.createStatement().execute("set schema 'sales'");
            stopped = server.stopSoft();
            assertFalse(stopped);
        } finally {
            connection.close();
        }
        stopped = server.stopSoft();
        server = null;
        assertTrue(stopped);

        // NOTE jvs 27-Nov-2008: next two calls are coverage for LDB-190, to
        // make sure a shutdown call on an already-shutdown DB is treated as a
        // NOP.
        FarragoDbSingleton.shutdown();
        stopped = FarragoDbSingleton.shutdownConditional(0);
        assertTrue(stopped);
    }

    public void testKillServer()
        throws Exception
    {
        if (isJRockit()) {
            return;
        }

        server = newServer();
        FarragoJdbcEngineDriver serverDriver = new FarragoJdbcEngineDriver();
        server.start(serverDriver);
        FarragoAbstractJdbcDriver clientDriver = newClientDriver();

        // N.B. it is better practice to put the login credentials in the
        // Properties object rather than on the URL, but this is a convenient
        // test of the client driver's connect string processing.
        String uri = clientDriver.getUrlPrefix() + "localhost;user=sa";
        Connection connection =
            clientDriver.connect(
                uri,
                new Properties());
        connection.createStatement().execute("set schema 'sales'");
        killServer();
    }

    /**
     * Tests client driver connection URI parameters. The underlying
     * connect-string processing is tested by {@link
     * net.sf.farrago.test.jdbc.FarragoEngineDriverTest#testConnectStrings}
     * using the engine driver. This method adds tests of client connections and
     * parameter precedence with the client driver.
     */
    public void testConnectionParams()
        throws Throwable
    {
        if (isJRockit()) {
            return;
        }

        server = newServer();
        FarragoJdbcEngineDriver serverDriver = new FarragoJdbcEngineDriver();
        server.start(serverDriver);

        FarragoAbstractJdbcDriver clientDriver = newClientDriver();
        String uri = clientDriver.getUrlPrefix() + "localhost";

        // test a connection that fails without the params
        Properties empty = new Properties();
        Connection conn = null;
        try {
            conn = clientDriver.connect(uri, empty);
            fail("Farrago connect without user credentials");
        } catch (SQLException e) {
            FarragoJdbcTest.assertExceptionMatches(
                e,
                ".*Login failed.*");
        }
        Properties auth = new Properties();
        auth.setProperty("user", "sa");
        auth.setProperty("password", "");
        String loginUri = uri + ";" + ConnectStringParser.getParamString(auth);
        conn = clientDriver.connect(loginUri, empty);
        assertNotNull("null connection", conn);
        assertEquals(
            "empty props changed",
            0,
            empty.size());
        conn.close();

        // test that parameter precedence works
        Properties unauth = new Properties();
        unauth.setProperty("user", "unauthorized user");
        unauth.setProperty("password", "invalid password");

        // connect will fail unless loginUri attributes take precedence
        try {
            conn = clientDriver.connect(uri, unauth);
            fail("Farrago connect with bad user credentials");
        } catch (SQLException e) {
            FarragoJdbcTest.assertExceptionMatches(e, ".*Login failed.*");
        }
        conn = clientDriver.connect(loginUri, unauth);
        assertNotNull("null connection", conn);
        conn.close();

        boolean stopped = server.stopSoft();
        server = null;
        assertTrue(stopped);
    }

    /**
     * Tests that Paser/Validator exceptions contain original statement.
     *
     * @throws Throwable
     */
    public void testExceptionContents()
        throws Throwable
    {
        if (isJRockit()) {
            return;
        }

        server = newServer();
        FarragoJdbcEngineDriver serverDriver = new FarragoJdbcEngineDriver();
        server.start(serverDriver);

        // NOTE: can't call DriverManager.getConnection here, because that
        // would deadlock
        FarragoAbstractJdbcDriver clientDriver = newClientDriver();

        // N.B. it is better practice to put the login credentials in the
        // Properties object rather than on the URL, but this is a convenient
        // test of the client driver's connect string processing.
        String uri = clientDriver.getUrlPrefix() + "localhost;user=sa";
        Connection connection =
            clientDriver.connect(
                uri,
                new Properties());

        boolean recieved = true;
        String query = "create table sales.emps (col1 integer primary key)";
        try {
            connection.createStatement().execute(query);
            recieved = false;
        } catch (SQLException e) {
            String returned = FarragoJdbcUtil.findInputString(e);
            assertEquals(stmtMismatch, query, returned);
        }
        assertTrue("Expected DDL exception not received", recieved);

        query = "select wqe from sales.emps";
        try {
            connection.createStatement().execute(query);
            recieved = false;
        } catch (SQLException e) {
            String returned = FarragoJdbcUtil.findInputString(e);
            assertEquals(stmtMismatch, query, returned);
        }
        assertTrue("Expected validator exception not received", recieved);

        query = "select * frm sales.emps";
        try {
            connection.createStatement().execute(query);
            recieved = false;
        } catch (SQLException e) {
            String returned = FarragoJdbcUtil.findInputString(e);
            assertEquals(stmtMismatch, query, returned);
        }
        assertTrue("Expected parser exception not received", recieved);

        connection.close();
        boolean stopped = server.stopSoft();
        server = null;
        assertTrue(stopped);
    }

    /**
     * Tests error message when a 2nd server is started.
     */
    public void testTwoServers()
        throws Exception
    {
        if (isJRockit()) {
            return;
        }

        if (System.getProperty("os.name").startsWith("Windows")) {
            // TODO jvs 1-Nov-2006:  Get spawn working on Windows too.
            return;
        }
        server = newServer();
        FarragoJdbcEngineDriver serverDriver = new FarragoJdbcEngineDriver();
        server.start(serverDriver);

        // try to start another server with sqllineEngine script
        String [] cmd = { "./sqllineEngine" };
        StringReader appInput = new StringReader("!quit\n");
        StringWriter appOutput = new StringWriter();
        Logger logger = Logger.getAnonymousLogger();
        int status = Util.runApplication(cmd, logger, appInput, appOutput);
        StringBuffer buf = appOutput.getBuffer();
        if ((buf.length() > 0) && (logger != null)) { // dump command output

            // make output visible at default logging level if test failed
            Level level = (status == 0) ? Level.FINE : Level.INFO;
            logger.log(level, "***** command output *****");
            logger.log(
                level,
                buf.toString());
            logger.log(level, "***** end of output *****");
        }
        assertEquals("sqllineEngine status", 0, status);

        // look for expected error message in sqllineEngine's output
        String str = buf.toString();
        String err = FarragoResource.instance().CatalogFileLockFailed.str("");
        assertTrue("2nd server error message missing", str.indexOf(err) >= 0);

        appInput.close();
        appOutput.close();

        boolean stopped = server.stopSoft();
        server = null;
        assertTrue(stopped);
    }

    // implement TestCase
    protected void tearDown()
        throws Exception
    {
        if (server != null) {
            try {
                killServer();
            } catch (Throwable ex) {
                // NOTE:  swallow ex so it doesn't mask real exception
            }
        }
        super.tearDown();
    }

    private void killServer()
    {
        try {
            server.stopHard();
        } finally {
            server = null;
        }
    }
}

// End FarragoServerTest.java
