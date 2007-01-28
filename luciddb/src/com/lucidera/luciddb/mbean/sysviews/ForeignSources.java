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
