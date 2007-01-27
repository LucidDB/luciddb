/*
// $Id$
// (C) Copyright 2007-2007 LucidEra, Inc.
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
