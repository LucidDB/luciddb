/*
// $Id$
// LucidDB is a DBMS optimized for business intelligence.
// Copyright (C) 2006-2006 LucidEra, Inc.
// Copyright (C) 2006-2006 The Eigenbase Project
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

package com.lucidera.luciddb.applib.util;

import com.lucidera.luciddb.applib.resource.*;
import java.sql.*;

/**
 * GrantSelectForSchema UDP grants a user select privileges for all
 * tables and views in a specific schema.
 *
 * @author Oscar Gothberg
 * @version $Id$
 */

public abstract class GrantSelectForSchemaUdp {

    /**
     * @param schemaName name of schema to grant select privs for
     * @param userName username of user to get privileges
     */

    public static void execute(String schemaName, String userName) {
        
        Statement s1 = null, s2 = null;
        ResultSet rs;
        Connection conn = null;

        try {
            // set up a jdbc connection
            conn = DriverManager.getConnection("jdbc:default:connection");
            s1 = conn.createStatement();
            s2 = conn.createStatement();

            // retrieve list of tables
            rs = s1.executeQuery("select SCHEMA_NAME, TABLE_NAME from " 
                + "SYS_ROOT.DBA_TABLES where SCHEMA_NAME = '" + schemaName 
                + "'");

            // grant select for all tables
            while (rs.next()) {
                s2.executeUpdate("grant select on " + rs.getString(1) + "." 
                    + rs.getString(2) + " to " + userName);
            }

            // retrieve list of views
            rs = s1.executeQuery("select SCHEMA_NAME, VIEW_NAME from " 
                + "SYS_ROOT.DBA_VIEWS where SCHEMA_NAME = '" + schemaName 
                + "'");

            // grant select for all views
            while (rs.next()) {
                s2.executeUpdate("grant select on " + rs.getString(1) + "." 
                    + rs.getString(2) + " to " + userName);
            }

        } catch (SQLException e) {
            // SQLException in these statements should mean connection error
            throw ApplibResourceObject.get().DatabaseAccessError.ex(
                e.toString(), e);
        } finally {
            try {
                // cleanup
                if (s1 != null)
                    s1.close();
                if (s2 != null)
                    s2.close();
                if (conn != null)
                    conn.close();
            } catch (SQLException e) {
                // SQLException in these statements should mean connection error
                throw ApplibResourceObject.get().DatabaseAccessError.ex(
                    e.toString(), e);
            }
        }
    }
}
