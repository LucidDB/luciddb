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

import java.sql.*;

import java.util.logging.*;

import net.sf.farrago.catalog.*;
import net.sf.farrago.runtime.*;
import net.sf.farrago.trace.*;

import org.eigenbase.applib.resource.*;
import org.eigenbase.sql.*;


/**
 * [DDB-26] Create a UDP that creates a table from a generic cursor input, then
 * optionally loads the table from the input cursor.<br>
 *
 * @author Ray Zhang
 * @since Mar-18-2010
 */
public abstract class CreateTbFromSelectStmtUdp
{
    //~ Static fields/initializers ---------------------------------------------

    private static final Logger tracer =
        FarragoTrace.getClassTracer(CreateTbFromSelectStmtUdp.class);

    //~ Methods ----------------------------------------------------------------

    /**
     * @param sourceTbName
     * @param targetSchemaName
     * @param targetTableName
     * @param additionalColsInfo
     *
     * @throws Exception
     */
    public static void execute(
        String targetSchemaName,
        String targetTableName,
        String selectStmt,
        boolean shouldLoad)
        throws Exception
    {
        FarragoRepos repos = FarragoUdrRuntime.getSession().getRepos();
        String schema = targetSchemaName;
        String table = targetTableName;

        // validate the required input parameters.
        if (((table == null) || (table.trim().length() == 0))) {
            throw ApplibResource.instance().InputIsRequired.ex(
                "targetTableName");
        }
        if (((selectStmt == null) || (selectStmt.trim().length() == 0))) {
            throw ApplibResource.instance().InputIsRequired.ex("selectStmt");
        }

        ResultSet rs = null;
        PreparedStatement ps = null;
        Connection conn = null;

        try {
            // set up a jdbc connection
            conn = DriverManager.getConnection("jdbc:default:connection");

            // If schema is not specified, the udp will select the schema of the
            // connection calling.
            if (schema == null) {
                schema =
                    FarragoUdrRuntime.getSession().getSessionVariables()
                    .schemaName;
                // haven't called set schema before:
                if (schema == null) {
                  throw ApplibResource.instance().InputIsRequired.ex(
                      "targetSchemaName");
                }
            }

            // verify whehter the targe table is exsiting in specific schema.
            ps = conn.prepareStatement(
                "select count(1) from LOCALDB.SYS_ROOT.DBA_TABLES where SCHEMA_NAME=? and TABLE_NAME=?");
            ps.setString(1, schema);
            ps.setString(2, table);
            rs = ps.executeQuery();

            int row_cnt = 0;
            if (rs.next()) {
                row_cnt = rs.getInt(1);
            } else {
                throw ApplibResource.instance().EmptyInput.ex();
            }
            if (row_cnt > 0) {
                throw ApplibResource.instance().TableOrViewAlreadyExists.ex(
                    repos.getLocalizedObjectName(schema),
                    repos.getLocalizedObjectName(table));
            }

            // Create table structure based on select statement.
            StringBuilder ddl = new StringBuilder();
            ddl.append("create table ").append(
                SqlDialect.EIGENBASE.quoteIdentifier(schema)).append(".")
            .append(SqlDialect.EIGENBASE.quoteIdentifier(table)).append(" ( ");
            ps = conn.prepareStatement(selectStmt);
            ResultSetMetaData rsmd = ps.getMetaData();
            int colNum = rsmd.getColumnCount();
            int coltype;
            for (int i = 1; i <= colNum; i++) {
                ddl.append(
                    " " + rsmd.getColumnName(i) + " "
                    + rsmd.getColumnTypeName(i));

                coltype = rsmd.getColumnType(i);
                if ((coltype == Types.VARBINARY)
                    || (coltype == Types.BINARY)
                    || (coltype == Types.CHAR)
                    || (coltype == Types.VARCHAR))
                {
                    // data type needs precision information
                    // VARBINARY, BINARY, CHAR, VARCHAR
                    ddl.append("(" + rsmd.getPrecision(i) + ")");
                } else if (coltype == Types.DECIMAL) {
                    // DECIMAL needs precision and scale information
                    ddl.append(
                        "(" + rsmd.getPrecision(i) + ","
                        + rsmd.getScale(i) + ")");
                }
                ddl.append(",");
            }
            ddl.deleteCharAt((ddl.length() - 1));
            ddl.append(" )");
            tracer.info("create table statement: " + ddl.toString());
            ps = conn.prepareStatement(ddl.toString());
            ps.execute();

            // load data from source to target table if shouldLoad equals true
            if (shouldLoad) {
                StringBuilder insertstmt = new StringBuilder();
                insertstmt.append(
                    "insert into "
                    + SqlDialect.EIGENBASE.quoteIdentifier(schema)).append(".")
                .append(SqlDialect.EIGENBASE.quoteIdentifier(table)).append(" ")
                .append(selectStmt);
                tracer.info("insert statement: " + insertstmt.toString());
                ps = conn.prepareStatement(insertstmt.toString());
                ps.execute();
            }
        } finally {
            if (rs != null) {
                rs.close();
            }
            if (ps != null) {
                ps.close();
            }
            if (conn != null) {
                conn.close();
            }
        }
    }
}

// End CreateTbFromSelectStmtUdp.java
