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

import net.sf.farrago.jdbc.client.*;
import net.sf.farrago.jdbc.engine.*;
import net.sf.farrago.server.*;


/**
 * FarragoServerTest tests Farrago client/server connections.  It does
 * not inherit from FarragoTestCase since that pulls in an embedded engine.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class FarragoServerTest extends TestCase
{
    //~ Instance fields -------------------------------------------------------

    private FarragoServer server;

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
    
    public void testServer()
        throws Exception
    {
        server = new FarragoServer();
        FarragoJdbcEngineDriver serverDriver = new FarragoJdbcEngineDriver();
        server.start(serverDriver);

        // NOTE: can't call DriverManager.getConnection here, because that
        // would deadlock
        FarragoJdbcClientDriver clientDriver = new FarragoJdbcClientDriver();
        Connection connection =
            clientDriver.connect(
                clientDriver.getUrlPrefix() + "localhost",
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
    }

    public void testKillServer()
        throws Exception
    {
        server = new FarragoServer();
        FarragoJdbcEngineDriver serverDriver = new FarragoJdbcEngineDriver();
        server.start(serverDriver);
        FarragoJdbcClientDriver clientDriver = new FarragoJdbcClientDriver();
        Connection connection =
            clientDriver.connect(
                clientDriver.getUrlPrefix() + "localhost",
                new Properties());
        connection.createStatement().execute("set schema 'sales'");
        killServer();
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
