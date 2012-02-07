/*
// Licensed to DynamoBI Corporation (DynamoBI) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  DynamoBI licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at

//   http://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
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
