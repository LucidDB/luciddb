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
package com.lucidera.luciddb.mbean.sysviews;

import java.sql.*;
import java.util.*;
import javax.management.openmbean.*;

import com.lucidera.luciddb.mbean.*;
import com.lucidera.luciddb.mbean.resource.*;
import org.eigenbase.util.*;

/**
 * MBean for LucidDb system views:
 *
 * DBA_FOREIGN_SERVERS
 * DBA_FOREIGN_SERVER_OPTIONS
 * DBA_FOREIGN_WRAPPERS
 * DBA_FOREIGN_WRAPPER_OPTIONS
 *
 * @author Sunny Choi
 * @version $Id$
 */
public class ForeignSources implements ForeignSourcesMBean
{
    Connection conn = null;

    private ResultSet getResultSet(String sql) throws Exception
    {
        conn = MBeanUtil.getConnection(conn);
        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery(sql);
        return rs;
    }

    public TabularData getForeignServers() throws Exception
    {
        try {
            return MBeanUtil.createTable(
                getResultSet(MBeanQueryObject.get().
                    ForeignServersQuery.str()));
        } finally {
            try {
                conn.close();
            } catch (Exception e) {
                // do nothing
            }
        }
    }

    public TabularData getForeignServerOptions() throws Exception
    {
        try {
            return MBeanUtil.createTable(
                getResultSet(MBeanQueryObject.get().
                    ForeignServerOptionsQuery.str()));
        } finally {
            try {
                conn.close();
            } catch (Exception ex) {
                // do nothing
            }
        }
    }

    public TabularData getForeignWrappers() throws Exception
    {
        try {
            return MBeanUtil.createTable(
                getResultSet(MBeanQueryObject.get().
                    ForeignWrappersQuery.str()));
        } finally {
            try {
                conn.close();
            } catch (Exception ex) {
                // do nothing
            }
        }
    }

    public TabularData getForeignWrapperOptions() throws Exception
    {
        try {
            return MBeanUtil.createTable(
                getResultSet(MBeanQueryObject.get().
                    ForeignWrapperOptionsQuery.str()));
        } finally {
            try {
                conn.close();
            } catch (Exception ex) {
                // do nothing
            }
        }
    }

    public String printForeignServers() throws Exception
    {
        ResultSet rs = getResultSet(MBeanQueryObject.get().
            ForeignServersQuery.str());
        try {
            return MBeanUtil.printView(rs);
        } finally {
            try {
                rs.close();
                conn.close();
            } catch (Exception ex) {
                // do nothing
            }
        }
    }

    public String printForeignServerOptions() throws Exception
    {
        ResultSet rs = getResultSet(MBeanQueryObject.get().
            ForeignServerOptionsQuery.str());
        try {
            return MBeanUtil.printView(rs);
        } finally {
            try {
                rs.close();
                conn.close();
            } catch (Exception ex) {
                // do nothing
            }
        }
    }

    public String printForeignWrappers() throws Exception
    {
        ResultSet rs = getResultSet(MBeanQueryObject.get().
            ForeignWrappersQuery.str());
        try {
            return MBeanUtil.printView(rs);
        } finally {
            try {
                rs.close();
                conn.close();
            } catch (Exception ex) {
                // do nothing
            }
        }
    }

    public String printForeignWrapperOptions() throws Exception
    {
        ResultSet rs = getResultSet(MBeanQueryObject.get().
            ForeignWrapperOptionsQuery.str());
        try {
            return MBeanUtil.printView(rs);
        } finally {
            try {
                rs.close();
                conn.close();
            } catch (Exception ex) {
                // do nothing
            }
        }
    }

}
// End ForeignSources.java
