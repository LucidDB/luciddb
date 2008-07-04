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
package com.lucidera.luciddb.mbean;

import java.sql.*;
import java.util.*;
import javax.management.openmbean.*;

import org.eigenbase.util.*;
import com.lucidera.jdbc.LucidDbLocalDriver;

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
            Class clazz = Class.forName("com.lucidera.jdbc.LucidDbLocalDriver");
            LucidDbLocalDriver driver = (LucidDbLocalDriver) clazz.newInstance();
            String urlPrefix = driver.getUrlPrefix();
            Properties props = new Properties();
            props.setProperty("user", "sa");
            props.setProperty("password", "");
            props.setProperty("requireExistingEngine", "true");
            c = DriverManager.getConnection(urlPrefix, props);
        }
        return c;
    }

}
// End MBeanUtil.java
