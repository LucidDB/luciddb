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

import java.sql.*;
import java.util.*;

import junit.framework.*;

import net.sf.farrago.jdbc.*;
import net.sf.farrago.jdbc.client.*;
import net.sf.farrago.jdbc.engine.*;
import net.sf.farrago.server.*;
import org.eigenbase.util14.ConnectStringParser;


/**
 * FarragoServerTest tests Farrago client/server connections via RmiJdbc.  It
 * does not inherit from FarragoTestCase since that pulls in an embedded
 * engine.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class FarragoServerTest extends TestCase
{
    //~ Instance fields -------------------------------------------------------
    
    private FarragoAbstractServer server;

    //~ Constructors ----------------------------------------------------------

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

    //~ Methods ---------------------------------------------------------------

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
    
    public void testServer()
        throws Exception
    {
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
        Connection connection = clientDriver.connect(uri, new Properties());
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
    }

    public void testKillServer()
        throws Exception
    {
        server = newServer();
        FarragoJdbcEngineDriver serverDriver = new FarragoJdbcEngineDriver();
        server.start(serverDriver);
        FarragoAbstractJdbcDriver clientDriver = newClientDriver();
        // N.B. it is better practice to put the login credentials in the
        // Properties object rather than on the URL, but this is a convenient
        // test of the client driver's connect string processing.
        String uri = clientDriver.getUrlPrefix() + "localhost;user=sa";
        Connection connection = clientDriver.connect(uri, new Properties());
        connection.createStatement().execute("set schema 'sales'");
        killServer();
    }

    /**
     * Tests client driver connection URI parameters.
     * The underlying connect-string processing is tested by
     * {@link FarragoJdbcTest#testConnectStrings} using the engine driver.
     * This method adds tests of client connections and
     * parameter precedence with the client driver.
     */
    public void testConnectionParams() throws Throwable
    {
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
                e, ".*Unknown user.*");
        }
        Properties auth = new Properties();
        auth.setProperty("user", "sa");
        auth.setProperty("password", "");
        String loginUri = uri +";" +ConnectStringParser.getParamString(auth);
        conn = clientDriver.connect(loginUri, empty);
        assertNotNull("null connection", conn);
        assertEquals("empty props changed", 0, empty.size());
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
            FarragoJdbcTest.assertExceptionMatches(e, ".*Unknown user.*");
        }
        conn = clientDriver.connect(loginUri, unauth);
        assertNotNull("null connection", conn);
        conn.close();

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
