/*
// $Id$
// (C) Copyright 2007-2007 LucidEra, Inc.
*/
package com.lucidera.luciddb.mbean;

import java.sql.*;
import java.util.*;
import javax.management.openmbean.*;

import org.eigenbase.util.*;
import com.lucidera.jdbc.LucidDbRmiDriver;

/**
 * Utility class for LucidDb MBean classes
 *
 * @author Sunny Choi
 * @version $Id$
 */
public class MBeanUtil
{
    
    public static TabularData createTable(ResultSet rs) throws Exception
    {
        ResultSetMetaData rsMetaData = rs.getMetaData();
        int numCols = rsMetaData.getColumnCount();
        String[] headers = new String[numCols];
        OpenType[] allTypes = new OpenType[numCols];
        Vector[] values = new Vector[numCols];
        Object[] allValues = new Object[numCols];

        for (int i=0; i<numCols; i++) {
            headers[i] = rsMetaData.getColumnName(i+1);
            allTypes[i] = new ArrayType(1, SimpleType.STRING);
            values[i] = new Vector();
        }

        CompositeType ct = new CompositeType("column and values",
            "column and values", headers, headers, allTypes);
        TabularType tt = new TabularType(
            "column and values", "column with values", ct, headers);
        TabularData td = new TabularDataSupport(tt);

        while (rs.next()) {
            for (int i=0; i<numCols; i++) {
                values[i].add(rs.getString(i+1));
            }
        }

        for (int i=0; i<numCols; i++) {
            allValues[i] = (String[]) values[i].toArray(new String[0]);
        }

        CompositeData entry =
            new CompositeDataSupport(ct, headers, allValues);
        td.put(entry);
        return td;
    }

    public static String printView(ResultSet rs)
        throws Exception
    {
        ResultSetMetaData rsMetaData = rs.getMetaData();
        int numCols = rsMetaData.getColumnCount();
        String[] headers = new String[numCols];
        OpenType[] allTypes = new OpenType[numCols];
        Vector[] values = new Vector[numCols];
        Object[] allValues = new Object[numCols];

        String ret = "";

        for (int i=0; i<numCols; i++) {
            if (i == 0) {
                ret = rsMetaData.getColumnName(i+1);
            } else {
                ret = ret + "," + rsMetaData.getColumnName(i+1);
            }
        }
        ret = ret + "\n";

        while (rs.next()) {
            for (int i=0; i<numCols; i++) {
                if (i == 0) {
                    ret = ret + rs.getString(i+1);
                } else {
                    ret = ret + ", " + rs.getString(i+1);
                }
            }
            ret = ret + "\n";
        }

        return ret;
    }

    public static Connection getConnection(Connection c)
        throws Exception
    {
        Connection conn = c;
        if (c != null && !c.isClosed()) {
            return c;
        } else {
            Class clazz = Class.forName("com.lucidera.jdbc.LucidDbRmiDriver");
            LucidDbRmiDriver driver = (LucidDbRmiDriver) clazz.newInstance();
            String urlPrefix = driver.getUrlPrefix() + "localhost";
            c = DriverManager.getConnection(urlPrefix,"sa","");
        }
        return c;
    }

}
// End MBeanUtil.java
