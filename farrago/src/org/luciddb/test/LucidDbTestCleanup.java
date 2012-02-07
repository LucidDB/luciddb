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
package org.luciddb.test;

import java.sql.*;

import net.sf.farrago.catalog.*;
import net.sf.farrago.cwm.relational.*;
import net.sf.farrago.fem.med.*;
import net.sf.farrago.jdbc.engine.*;
import net.sf.farrago.test.*;

import org.eigenbase.enki.mdr.*;


/**
 * LucidDbTestCleanup takes care of cleaning up the catalog at the start of each
 * LucidDB test suite.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class LucidDbTestCleanup
    extends FarragoTestCase.Cleanup
{
    //~ Static fields/initializers ---------------------------------------------

    private static Thread shutdownHook;

    //~ Instance fields --------------------------------------------------------

    private final FarragoRepos ldbRepos;

    private final Statement ldbStmt;

    //~ Constructors -----------------------------------------------------------

    public LucidDbTestCleanup(Connection ldbConn)
        throws Exception
    {
        super("LucidDbCleanup");
        ldbStmt = ldbConn.createStatement();
        ldbRepos =
            ((FarragoJdbcEngineConnection) ldbConn).getSession().getRepos();
    }

    //~ Methods ----------------------------------------------------------------

    protected FarragoRepos getRepos()
    {
        return ldbRepos;
    }

    protected Statement getStmt()
    {
        return ldbStmt;
    }

    protected boolean isBlessedSchema(CwmSchema schema)
    {
        String name = schema.getName();
        return name.equals("SQLJ")
            || name.equals("APPLIB")
            || name.equals("INFORMATION_SCHEMA")
            || name.equals("SYSTEM")
            || name.startsWith("SYS_");
    }

    // override Cleanup
    protected boolean isBlessedWrapper(FemDataWrapper wrapper)
    {
        String name = wrapper.getName();
        return name.equals("ORACLE")
            || name.equals("SQL SERVER")
            || name.equals("FLAT FILE")
            || name.equals("LUCIDDB LOCAL")
            || name.equals("LUCIDDB REMOTE")
            || name.equals("SALESFORCE")
            || name.contains("NETSUITE")
            || super.isBlessedWrapper(wrapper);
    }

    public static void saveTestParameters()
        throws Exception
    {
        LucidDbTestCleanup cleanup = newCleanup();
        cleanup.saveCleanupParameters();
    }

    public static void cleanTest()
        throws Exception
    {
        if (shutdownHook == null) {
            // NOTE jvs 20-May-2006:  This thread doesn't actually
            // do anything; its only purpose is to prevent
            // this class from getting unloaded until the VM shuts down.
            shutdownHook = new ShutdownThread();
            Runtime.getRuntime().addShutdownHook(shutdownHook);
        }
        LucidDbTestCleanup cleanup = newCleanup();
        cleanup.getRepos().getEnkiMdrRepos().beginSession();
        try {
            cleanup.execute();
        } finally {
            cleanup.getRepos().getEnkiMdrRepos().endSession();
        }
    }

    public void execute()
        throws Exception
    {
        super.execute();
        ldbStmt.executeUpdate("alter system deallocate old");
    }

    private static LucidDbTestCleanup newCleanup()
        throws Exception
    {
        Connection conn =
            DriverManager.getConnection(
                "jdbc:default:connection");
        return new LucidDbTestCleanup(conn);
    }

    //~ Inner Classes ----------------------------------------------------------

    private static class ShutdownThread
        extends Thread
    {
        public void run()
        {
        }
    }
}

// End LucidDbTestCleanup.java
