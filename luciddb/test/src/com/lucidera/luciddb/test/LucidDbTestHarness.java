/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2006-2006 LucidEra, Inc.
// Copyright (C) 2006-2006 The Eigenbase Project
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
package com.lucidera.luciddb.test;

import junit.framework.*;

import net.sf.farrago.db.*;
import net.sf.farrago.trace.*;
import net.sf.farrago.test.*;

import org.eigenbase.util.*;

import java.util.logging.*;

import java.sql.*;

/**
 * LucidDbTestHarness contains control methods for running LucidDB
 * from Blackhawk.  It's not a real test; it's only declared as a JUnit
 * TestCase because that's what Blackhawk wants in order for it
 * to be invocable from tinit.xml and tdone.xml (in luciddb/test/sql).
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class LucidDbTestHarness extends TestCase
{
    public static final Logger tracer =
        FarragoTrace.getClassTracer(LucidDbTestHarness.class);

    private static Connection connection;

    private static Statement stmt;

    private static int testDepth;

    private static boolean needCleanup;
    
    private static boolean haveSavedParameters;
    
    public LucidDbTestHarness(String testName) throws Exception
    {
        super(testName);
    }

    /**
     * Starts LucidDB running in engine mode and sets any necessary
     * parameters.  Called from SqlTest after initialization with
     * the correct properties.  REVIEW:  that initialization
     * should probably be refactored into here.
     *
     * @return connection to LucidDB
     */
    static Connection startupEngine(
        String urlPrefix, String username, String passwd)
        throws Exception
    {
        if (connection != null) {
            // Already started.  TODO:  if parameters don't match,
            // force restart.
            cleanupIfNeeded();
            return connection;
        }
        tracer.info("Starting LucidDB engine...");
        connection = DriverManager.getConnection(
            urlPrefix,
            username,
            passwd);
        stmt = connection.createStatement();
        if (!haveSavedParameters) {
            callProcedure("call sys_boot.sys_boot.save_test_parameters()");
            haveSavedParameters = true;
        }
        tracer.info("LucidDB engine started successfully");
        cleanupIfNeeded();
        return connection;
    }

    private static void cleanupIfNeeded()
        throws Exception
    {
        if (needCleanup) {
            tracer.info("Running cleanup...");
            callProcedure("call sys_boot.sys_boot.clean_test()");
            needCleanup = false;
        }
    }

    private static void callProcedure(String call)
        throws Exception
    {
        ResultSet resultSet = stmt.executeQuery(call);
        try {
            while (resultSet.next()) {
            }
        } finally {
            resultSet.close();
        }
    }

    /**
     * Called from tinit.xml.
     */
    public void testSuiteInit()
    {
        tracer.info("testSuiteInit");
        ++testDepth;
        needCleanup = true;
    }

    /**
     * Called from tdone.xml.
     */
    public void testSuiteDone()
    {
        tracer.info("testSuiteDone");
        --testDepth;
        if (testDepth == 0) {
            shutdownEngine();
        }
    }

    /**
     * Shuts down LucidDB when running in engine mode.
     */
    public void shutdownEngine()
    {
        if (!FarragoDbSingleton.isReferenced()) {
            tracer.info("LucidDB engine not running; nothing to do");
            return;
        }
        tracer.info("Shutting down LucidDB engine...");
        Util.squelchConnection(connection);
        connection = null;
        stmt = null;

        if (FarragoDbSingleton.isReferenced()) {
            // TODO jvs 26-Apr-2006:  FarragoTestCase is a lot less
            // permissive here.  We need to make sure this information
            // gets acted on.
            tracer.warning(
                "LucidDB engine has delinquent sessions; attempting to kill");
            FarragoDbSingleton.shutdown();
        }
        tracer.info("LucidDB engine shut down successfully");
    }
}

// End LucidDbTestHarness.java
