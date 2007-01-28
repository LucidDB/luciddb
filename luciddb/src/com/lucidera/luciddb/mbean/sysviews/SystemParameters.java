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
 * MBean for LucidDb system view DBA_SYSTEM_PARAMETERS
 *
 * @author Sunny Choi
 * @version $Id$
 */
public class SystemParameters implements SystemParametersMBean
{
    Connection conn = null;

    private ResultSet getResultSet() throws Exception
    {
        conn = MBeanUtil.getConnection(conn);
        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery(
            MBeanQueryObject.get().SystemParametersQuery.str());
        return rs;
    }

    public TabularData getSystemParameters() throws Exception
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

    public String printSystemParameters() throws Exception
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

    public void alterSystem(String name, String value) throws Exception
    {
        PreparedStatement pstmt = null;
        try {
            conn = MBeanUtil.getConnection(conn);
            pstmt = conn.prepareStatement(
                MBeanQueryObject.get().AlterSystemQuery.str(name, value));
            pstmt.execute();
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
// End SystemParameters.java
