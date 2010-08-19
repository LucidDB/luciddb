/*
// $Id$
// Applib is a library of SQL-invocable routines for Eigenbase applications.
// Copyright (C) 2009 The Eigenbase Project
// Copyright (C) 2009 SQLstream, Inc.
// Copyright (C) 2009 DynamoBI Corporation
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


/**
 * Executes a SQL statement if the evaluation SQL statement is empty.
 *
 * @param evalSql SQL to execute. evalSQL must return a resultSet. If rowcount >
 * 0 the execSql is then executed.
 * @param execSql SQL executed if evaluation is met. Test SQL: call
 * exec_sql_if_no_rows( 'SELECT * FROM SYS_ROOT.DBA_SCHEMAS WHERE SCHEMA_NAME
 * =''FRED''' ,'CREATE SCHEMA "FRED"' );
 *
 * @author Nicholas A. Goodman
 */
public abstract class ExecSqlIfNoRows
{
    //~ Methods ----------------------------------------------------------------

    public static void execute(String evalSQL, String execSQL)
        throws ApplibException, SQLException
    {
        PreparedStatement ps;
        Statement stmt;
        ResultSet rs;
        Connection conn = null;

        // set up a jdbc connection
        conn = DriverManager.getConnection("jdbc:default:connection");
        stmt = conn.createStatement();

        // Execute the SQL statement provided
        ps = conn.prepareStatement(evalSQL);
        rs = ps.executeQuery();

        // If no rows, return (eval is false)
        if (rs.next()) {
            return;
        }

        // Execute the SQL
        stmt.execute(execSQL);
    }
}

// End ExecSqlIfNoRows.java
