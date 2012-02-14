/*
// Licensed to DynamoBI Corporation (DynamoBI) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  DynamoBI licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at

//   http://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
*/
package com.lucidera.luciddb.test;

import junit.framework.*;

import net.sf.farrago.db.*;
import net.sf.farrago.trace.*;

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
    
    private static boolean noAutoStart;
    
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
    public static Connection startupEngine(
        String urlPrefix, String username, String passwd)
        throws Exception
    {
        if (noAutoStart) {
            return null;
        }
        
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
        stmt.executeUpdate(call);
    }

    /**
     * Called from tinit.xml.
     */
    public void testSuiteInit()
    {
        tracer.info("testSuiteInit");
        ++testDepth;
        needCleanup = true;
        noAutoStart = false;
    }
    
    /**
     * Called from tinitSingleTest.xml.  Use this if you want to execute a
     * single test within a LucidDB server instance.  LucidDB will start
     * when the test is initiated, rather than automatically, as is normally
     * the case when you want multiple tests to execute within the same
     * LucidDB server instance.  As a result, tests that use this
     * initialization won't execute automatic cleanup and restore of system
     * parameters.
     */
    public void testSuiteInitSingleTest()
    {
        tracer.info("testSuiteInitSingleTest");
        ++testDepth;
        noAutoStart = true;
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
     * called from tclose.xml
     */
    public void testCloseConnections()
    {
        tracer.info("testCloseConnections");
        Util.squelchConnection(connection);
        connection = null;
        stmt = null;

        // TODO: this should go away after LDB-164
        haveSavedParameters = false;

        tracer.info("All connections closed");
    }

    /**
     * called from tsetcf.xml
     */
    public void testSetCleanupFlag()
    {
        tracer.info("set LucidDbTestHarness needCleanup flag to true");
        needCleanup = true;
    }

    /** 
     * called from tunsetcf.xml
     */
    public void testUnsetCleanupFlag()
    {
        tracer.info("set LucidDbTestHarness needCleanup flag to false");
        needCleanup = false;
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
