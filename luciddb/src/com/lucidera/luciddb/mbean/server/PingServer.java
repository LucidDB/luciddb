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
package com.lucidera.luciddb.mbean.server;

import java.sql.*;
import java.util.*;

import com.lucidera.luciddb.mbean.*;
import com.lucidera.luciddb.mbean.resource.*;
import org.eigenbase.util.*;

/**
 * MBean for getting the status of LucidDb by running a simple SQL query
 *
 * @author Sunny Choi
 * @version $Id$
 */
public class PingServer implements PingServerMBean
{
    Connection conn = null;
    String info = null;

    public static String STATUS_ALIVE = "ALIVE";
    public static String STATUS_DEAD = "DEAD";

    private ResultSet getResultSet() throws Exception
    {
        conn = MBeanUtil.getConnection(conn);
        Statement stmt = conn.createStatement();
        String sql = MBeanQueryObject.get().ValidationQuery.str();
        ResultSet rs = stmt.executeQuery(sql);
        return rs;
    }

    public String getCurrentStatus() throws Exception
    {
        try {
            getResultSet();
            info = null;
            return STATUS_ALIVE;
        } catch (Throwable ex) {
            info = ex.getMessage();
            return STATUS_DEAD;
        } finally {
            try {
                conn.close();
            } catch (Exception ex) {
                // do nothing
            }
        }
    }

    public String getInfo()
    {
        return info;
    }

}
// End PingServer.java
