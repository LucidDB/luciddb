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
import java.io.*;

import com.lucidera.luciddb.mbean.*;
import com.lucidera.luciddb.mbean.resource.*;
import org.eigenbase.util.*;

import net.sf.farrago.util.*;

/**
 * MBean for LucidDB storage management.
 *
 * @author John Sichi
 * @version $Id$
 */
public class StorageManagement implements StorageManagementMBean
{
    public static String FILE_GROW = "FILE_GROW";
    public static String FILE_KEEP = "FILE_KEEP";
    public static String ACCESS_ERROR = "ACCESS_ERROR";
    
    public String checkDatabaseGrowth(long thresholdInBytes)
        throws Exception
    {
        return checkFileGrowth(
            "DatabasePagesOccupiedHighWaterSinceInit",
            "db.dat",
            thresholdInBytes);
    }
    
    public String checkTempGrowth(long thresholdInBytes)
        throws Exception
    {
        return checkFileGrowth(
            "TempPagesOccupiedHighWaterSinceInit",
            "temp.dat",
            thresholdInBytes);
    }

    private String checkFileGrowth(
        String perfCounterName,
        String datFileName,
        long thresholdInBytes)
        throws Exception
    {
        String sql = MBeanQueryObject.get().PerfCounterQuery.str();
        Connection conn = null;
        PreparedStatement stmt = null;
        long pagesOccupied;
        try {
            conn = MBeanUtil.getConnection(conn);
            stmt = conn.prepareStatement(sql);
            stmt.setString(1, perfCounterName);
            ResultSet rs = stmt.executeQuery();
            if (!rs.next()) {
                return ACCESS_ERROR;
            }
            pagesOccupied = rs.getLong(1);
            // pump out last row from UDX
            rs.next();
        } finally {
            Util.squelchStmt(stmt);
            Util.squelchConnection(conn);
        }

        File file = new File(
            FarragoProperties.instance().getCatalogDir(), datFileName);
        long fileSizeInBytes = file.length();

        long bytesOccupied = pagesOccupied * 32768;

        if (fileSizeInBytes - bytesOccupied < thresholdInBytes) {
            return FILE_GROW;
        } else {
            return FILE_KEEP;
        }
    }
}

// End StorageManagement.java
