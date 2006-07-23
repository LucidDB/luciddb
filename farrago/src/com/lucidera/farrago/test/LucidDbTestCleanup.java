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
package com.lucidera.farrago.test;

import java.sql.*;

import net.sf.farrago.catalog.*;
import net.sf.farrago.cwm.relational.*;
import net.sf.farrago.fem.med.*;
import net.sf.farrago.jdbc.engine.*;
import net.sf.farrago.test.*;


/**
 * LucidDbCleanup takes care of cleaning up the catalog at the start of each
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
        return
            name.equals("SQLJ")
            || name.equals("APPLIB")
            || name.equals("INFORMATION_SCHEMA")
            || name.equals("SYSTEM")
            || name.startsWith("SYS_");
    }

    // override Cleanup
    protected boolean isBlessedWrapper(FemDataWrapper wrapper)
    {
        String name = wrapper.getName();
        return
            name.equals("ORACLE")
            || name.equals("SQL SERVER")
            || name.equals("FLAT FILE")
            || name.equals("SALESFORCE")
            || name.equals("NETSUITE")
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
        cleanup.execute();
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
