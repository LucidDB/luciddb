/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2006-2006 The Eigenbase Project
// Copyright (C) 2006-2006 Disruptive Tech
// Copyright (C) 2006-2006 LucidEra, Inc.
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

import net.sf.farrago.jdbc.engine.*;


/**
 * This class is intended for use with a profiler.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class FarragoProfiler
{
    //~ Methods ----------------------------------------------------------------

    /**
     * Provides an entry point for profiling the SQL engine.
     *
     * @param args unused
     */
    public static void main(String [] args)
        throws Exception
    {
        // Trick to invoke FarragoTestCase's static initializer to get default
        // settings for environment variables.
        FarragoQueryTest unused = new FarragoQueryTest("unused");
        runTest();
    }

    private static void runTest()
        throws SQLException
    {
        FarragoJdbcEngineDriver driver = new FarragoJdbcEngineDriver();
        Properties info = new Properties();
        info.put("user", "sa");
        Connection connection =
            driver.connect(
                "jdbc:farrago:",
                info);

        Statement stmt = connection.createStatement();

        // disable stmt caching since we want to profile both
        // preparation and execution
        stmt.execute("alter system set \"codeCacheMaxBytes\"=min");

        // run query without profiling first in order to prime the system
        runQuery(stmt);

        // tell the profiler about this dummy entry point
        runProfiledQuery(stmt);

        connection.close();
    }

    private static void runProfiledQuery(Statement stmt)
        throws SQLException
    {
        runQuery(stmt);
    }

    private static void runQuery(Statement stmt)
        throws SQLException
    {
        ResultSet rs =
            stmt.executeQuery(
                "select count(*) from sys_boot.jdbc_metadata.columns_view");
        rs.next();
        int n = rs.getInt(1);
        rs.close();
        System.out.println("result = " + n);
    }
}

// End FarragoProfiler.java
