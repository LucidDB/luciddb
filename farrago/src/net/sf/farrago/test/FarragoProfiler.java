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
