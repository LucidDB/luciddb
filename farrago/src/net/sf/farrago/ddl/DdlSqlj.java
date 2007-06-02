/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2007 The Eigenbase Project
// Copyright (C) 2005-2007 Disruptive Tech
// Copyright (C) 2005-2007 LucidEra, Inc.
// Portions Copyright (C) 2004-2007 John V. Sichi
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
// Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
*/
package net.sf.farrago.ddl;

import java.sql.*;

import org.eigenbase.util.*;


/**
 * DdlSqlj contains the system-defined implementations for the standard SQLJ
 * system procedures such as INSTALL_JAR.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public abstract class DdlSqlj
{
    //~ Methods ----------------------------------------------------------------

    /**
     * @sql.2003 Part 13 Section 11.1
     */
    public static void install_jar(
        String url,
        String jar,
        int deploy)
        throws SQLException
    {
        if (deploy != 0) {
            // TODO jvs 18-Jan-2005
            throw Util.needToImplement("deploy");
        }
        url = url.trim();
        jar = jar.trim();
        String sql =
            "CREATE JAR " + jar + " library '" + url
            + "' options(" + deploy + ")";
        executeSql(sql);
    }

    /**
     * @sql.2003 Part 13 Section 11.2
     */
    public static void replace_jar(
        String url,
        String jar)
        throws SQLException
    {
        url = url.trim();
        jar = jar.trim();

        // TODO jvs 18-Jan-2005
        throw Util.needToImplement("replace_jar");
    }

    /**
     * @sql.2003 Part 13 Section 11.3
     */
    public static void remove_jar(
        String jar,
        int undeploy)
        throws SQLException
    {
        jar = jar.trim();
        if (undeploy != 0) {
            // TODO jvs 18-Jan-2005
            throw Util.needToImplement("deploy");
        }
        String sql =
            "DROP JAR " + jar
            + " options (" + undeploy + ")" + " RESTRICT";
        executeSql(sql);
    }

    /**
     * @sql.2003 Part 13 Section 11.4
     */
    public static void alter_java_path(
        String jar,
        String path)
        throws SQLException
    {
        jar = jar.trim();
        path = path.trim();
        throw Util.needToImplement("alter_java_path");
    }

    private static void executeSql(
        String sql)
        throws SQLException
    {
        Connection conn =
            DriverManager.getConnection(
                "jdbc:default:connection");
        Statement stmt = conn.createStatement();
        stmt.executeUpdate(sql);

        // NOTE jvs 19-Jan-2005:  no need for cleanup; default connection
        // is cleaned up automatically.
    }
}

// End DdlSqlj.java
