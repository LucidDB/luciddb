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
