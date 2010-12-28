/*
// $Id$
// LucidDB is a DBMS optimized for business intelligence.
// Copyright (C) 2007-2007 LucidEra, Inc.
// Copyright (C) 2007-2007 The Eigenbase Project
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
// Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307  USA
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
