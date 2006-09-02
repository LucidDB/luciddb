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

package com.lucidera.luciddb.applib.security;

import com.lucidera.luciddb.applib.resource.*;
import org.eigenbase.util.StackWriter;
import java.sql.*;
import java.io.*;

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

    public static void execute(String schemaName, String userName) throws SQLException {
        
        Statement s1 = null, s2 = null;
        PreparedStatement ps;
        ResultSet rs;
        Connection conn = null;
        StringWriter sw;
        StackWriter stackw;
        PrintWriter pw;

        // set up a jdbc connection
        conn = DriverManager.getConnection("jdbc:default:connection");
        
        // retrieve list of everything table-like in schema
        ps = conn.prepareStatement("select SCHEMA_NAME, TABLE_NAME " 
            + "from SYS_ROOT.DBA_TABLES where SCHEMA_NAME = ?");
        ps.setString(1, schemaName);
        rs = ps.executeQuery();
        
        // grant select for all of it
        while (rs.next()) {
            sw = new StringWriter();
            stackw = new StackWriter(sw, StackWriter.INDENT_SPACE4);
            pw = new PrintWriter(stackw);
            pw.print("grant select on ");
            StackWriter.printSqlIdentifier(pw, rs.getString(1));
            pw.print(".");
            StackWriter.printSqlIdentifier(pw, rs.getString(2));
            pw.print(" to ");
            StackWriter.printSqlIdentifier(pw, userName);
            pw.close();
            ps = conn.prepareStatement(sw.toString());
            rs = ps.executeQuery();
        }
    }
}
