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
 * MBean for LucidDb system view DBA_SQL_STATEMENTS
 *
 * @author Sunny Choi
 * @version $Id$
 */
public class SqlStatements implements SqlStatementsMBean
{
    Connection conn = null;

    private ResultSet getResultSet() throws Exception
    {
        conn = MBeanUtil.getConnection(conn);
        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery(
            MBeanQueryObject.get().SqlStatementsQuery.str());
        return rs;
    }

    public TabularData getStatements() throws Exception
    {
        try {
            return MBeanUtil.createTable(getResultSet());
        } catch (Throwable ex) {
            ex.printStackTrace();
        } finally {
            try {
                conn.close();
            } catch (Exception e) {
                // do nothing
            }
        }
        return null;
    }

    public String printStatements() throws Exception
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

    public String getSqlStatements(String sessionId) throws Exception
    {
        ResultSet rs = null;
        PreparedStatement pstmt = null;

        try {
            conn = MBeanUtil.getConnection(conn);
            pstmt = conn.prepareStatement(
                MBeanQueryObject.get().SqlTextFromSessionIdQuery.str());
            pstmt.setString(1, sessionId);
            rs = pstmt.executeQuery();
            return MBeanUtil.printView(rs);
        } finally {
            try {
                rs.close();
                pstmt.close();
                conn.close();
            } catch (Exception e) {
                // do nothing
            }
        }
    }

    public String getDetailedSqlInfo(String stmtId) throws Exception
    {
        ResultSet rs = null;
        PreparedStatement pstmt = null;
        try {
            conn = MBeanUtil.getConnection(conn);
            pstmt = conn.prepareStatement(
                MBeanQueryObject.get().SqlStatementFromStatementIdQuery.str());
            pstmt.setString(1, stmtId);
            rs = pstmt.executeQuery();
            return MBeanUtil.printView(rs);
        } finally {
            try {
                pstmt.close();
                conn.close();
            } catch (Exception e) {
                // do nothing
            }
        }
    }

}
// End SqlStatements.java
