/*
// $Id$
// LucidDB is a DBMS optimized for business intelligence.
// Copyright (C) 2008-2008 LucidEra, Inc.
// Copyright (C) 2008-2008 The Eigenbase Project
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
