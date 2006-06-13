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

import net.sf.farrago.server.*;
import net.sf.farrago.util.*;

import com.lucidera.jdbc.*;
import com.lucidera.farrago.*;

import org.eigenbase.util.property.*;

import java.io.*;
import java.sql.*;
import java.util.*;

/**
 * This class is intended for use with a profiler.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class LucidDbProfiler
{
    public static void main(String [] args)
        throws SQLException
    {
        LucidDbDebugServer.initProperties();
        runTest();
    }

    private static void runTest()
        throws SQLException
    {
        LucidDbLocalDriver driver = new LucidDbLocalDriver();
        Properties info = new Properties();
        info.put("user", "sa");
        Connection connection = driver.connect(
            "jdbc:luciddb:",
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
        ResultSet rs = stmt.executeQuery(
            "select count(*) from sys_root.dba_columns");
        rs.next();
        int n = rs.getInt(1);
        rs.close();
        System.out.println("result = " + n);
    }
}

// End LucidDbProfiler.java
