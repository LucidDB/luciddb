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
