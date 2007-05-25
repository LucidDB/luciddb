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
