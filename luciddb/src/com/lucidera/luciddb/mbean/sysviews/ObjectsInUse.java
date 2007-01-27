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
 * MBean for LucidDb system view DBA_OBJECTS_IN_USE
 *
 * @author Sunny Choi
 * @version $Id$
 */
public class ObjectsInUse implements ObjectsInUseMBean
{
    Connection conn = null;

    private ResultSet getResultSet() throws Exception
    {
        conn = MBeanUtil.getConnection(conn);
        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery(
            MBeanQueryObject.get().ObjectsInUseJoinTablesQuery.str());
        return rs;
    }

    public TabularData getObjectsInUse() throws Exception
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

    public String printObjectsInUse() throws Exception
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
// End ObjectsInUse.java
