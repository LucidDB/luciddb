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
        throws SQLException
    {
        if (connection != null) {
            // Already started.  TODO:  if parameters don't match,
            // force restart.
            return connection;
        }
        tracer.info("Starting LucidDB engine...");
        connection = DriverManager.getConnection(
            urlPrefix,
            username,
            passwd);
        tracer.info("LucidDB engine started successfully");
        return connection;
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
