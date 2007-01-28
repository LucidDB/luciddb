/*
// $Id$
// (C) Copyright 2007-2007 LucidEra, Inc.
*/
package com.lucidera.luciddb.mbean.sysviews;

import java.sql.*;
import java.util.*;
import javax.management.openmbean.*;

import com.lucidera.luciddb.mbean.*;
import com.lucidera.luciddb.mbean.resource.*;
import org.eigenbase.util.*;

/**
 * MBean for LucidDb system view DBA_SESSIONS
 *
 * @author Sunny Choi
 * @version $Id$
 */
public class Sessions implements SessionsMBean
{
    Connection conn = null;

    private ResultSet getResultSet() throws Exception
    {
        conn = MBeanUtil.getConnection(conn);
        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery(
            MBeanQueryObject.get().SessionsQuery.str());
        return rs;
    }

    public TabularData getSessions() throws Exception
    {
        try {
            return MBeanUtil.createTable(getResultSet());
        } finally {
            try {
                conn.close();
            } catch (Exception e) {
                // do nothing
            }
        }
    }

    public String printSessions() throws Exception
    {
        ResultSet rs = getResultSet();
        try {
            return MBeanUtil.printView(rs);
        } finally {
            try {
                rs.close();
                conn.close();
            } catch (Exception e) {
                // do nothing
            }
        }
    }

}
// End Sessions.java
