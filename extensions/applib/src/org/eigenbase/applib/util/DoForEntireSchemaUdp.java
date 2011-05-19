/*
// $Id$
// Applib is a library of SQL-invocable routines for Eigenbase applications.
// Copyright (C) 2006 The Eigenbase Project
// Copyright (C) 2006 SQLstream, Inc.
// Copyright (C) 2006 DynamoBI Corporation
//
// This library is free software; you can redistribute it and/or modify it
// under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation; either version 2.1 of the License, or (at
// your option) any later version.
//
// This library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public
// License along with this library; if not, write to the Free Software
// Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA
*/
package org.eigenbase.applib.util;

import java.io.*;

import java.sql.*;

import org.eigenbase.applib.resource.*;
import org.eigenbase.util.*;


/**
 * DoForEntireSchemaUdp executes a sql statement for all tables, views, or
 * tables and views, in a specific schema.
 *
 * @author Oscar Gothberg
 * @version $Id$
 */

public abstract class DoForEntireSchemaUdp
{
    //~ Methods ----------------------------------------------------------------

    /**
     * @param sql sql statement with %TABLE_NAME% as wildcard
     * @param schemaName name of schema to execute statement for
     * @param objTypeStr type of objects to be processed. Can be "TABLES",
     * "VIEWS" or "TABLES_AND_VIEWS"
     */

    public static void execute(String sql, String schemaName, String objTypeStr)
        throws SQLException, ApplibException
    {
        PreparedStatement ps;
        Statement stmt;
        ResultSet rs;
        Connection conn = null;
        StringWriter sw;
        StackWriter stackw;
        PrintWriter pw;

        // set up a jdbc connection
        conn = DriverManager.getConnection("jdbc:default:connection");
        stmt = conn.createStatement();

        // make sure schema exists, print error otherwise (LER-2608)
        ps = conn.prepareStatement(
            "select SCHEMA_NAME from SYS_ROOT.DBA_TABLES "
            + "where SCHEMA_NAME = ?");
        ps.setString(1, schemaName);
        rs = ps.executeQuery();
        if (!rs.next()) {
            throw ApplibResource.instance().NoSuchSchema.ex(schemaName);
        }

        // retrieve list of wanted type of objects in schema
        if (objTypeStr.equals("TABLES")) {
            ps = conn.prepareStatement(
                "select SCHEMA_NAME, TABLE_NAME "
                + "from SYS_ROOT.DBA_TABLES "
                + "where SCHEMA_NAME = ? and TABLE_TYPE = 'LOCAL TABLE'");
        } else if (objTypeStr.equals("VIEWS")) {
            ps = conn.prepareStatement(
                "select SCHEMA_NAME, TABLE_NAME "
                + "from SYS_ROOT.DBA_TABLES "
                + "where SCHEMA_NAME = ? and TABLE_TYPE = 'LOCAL VIEW'");
        } else {
            ps = conn.prepareStatement(
                "select SCHEMA_NAME, TABLE_NAME "
                + "from SYS_ROOT.DBA_TABLES where SCHEMA_NAME = ?");
        }

        ps.setString(1, schemaName);
        rs = ps.executeQuery();

        // split the sql statement around token %TABLE_NAME%
        String [] parts = sql.split("%TABLE_NAME%");

        // execute sql statement for all tables and views
        while (rs.next()) {
            sw = new StringWriter();
            stackw = new StackWriter(sw, StackWriter.INDENT_SPACE4);
            pw = new PrintWriter(stackw);
            pw.print(parts[0]);
            StackWriter.printSqlIdentifier(pw, rs.getString(1));
            pw.print(".");
            StackWriter.printSqlIdentifier(pw, rs.getString(2));

            // don't choke on ArrayIndexOOB if %TABLE_NAME% was at the end
            if (parts.length > 1) {
                pw.print(parts[1]);
            }
            pw.close();

            // NOTE jvs 11-Oct-2006: I changed this to use executeUpdate so
            // that if it actually was a query, the user will get an error.
            // Oscar's old comment was "if anyone is interested in the
            // result set, we'll have to do a getResultSet.. thing
            // here. Right now I'm leaving it quiet."

            stmt.executeUpdate(sw.toString());
        }
    }
}

// End DoForEntireSchemaUdp.java
