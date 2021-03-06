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
import org.eigenbase.sql.parser.*;
import org.eigenbase.sql.type.*;


/**
 * Create a UDP that will deduce, and optionally execute the DDL for a table.
 *
 * @author Ray Zhang
 * @since Mar-16-2010
 */
public abstract class CreateTbFromSrcTbUdp
{
    //~ Static fields/initializers ---------------------------------------------

    private static final Logger tracer =
        FarragoTrace.getClassTracer(CreateTbFromSrcTbUdp.class);

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
        String sourceTbName,
        String targetSchemaName,
        String targetTableName,
        String additionalColsInfo)
        throws Exception
    {
        FarragoRepos repos = FarragoUdrRuntime.getSession().getRepos();
        tracer.info(
            "Input parameters: source table: [" + sourceTbName
            + "] target Schema: [" + targetSchemaName + "] target table: ["
            + targetTableName + "] additional cols: [" + additionalColsInfo
            + "]");

        //Input parameter check.
        if ((sourceTbName == null) || (sourceTbName.length() == 0)) {
            throw ApplibResource.instance().InputIsRequired.ex("sourceTbName");
        }
        if ((targetTableName == null) || (targetTableName.length() == 0)) {
            throw ApplibResource.instance().InputIsRequired.ex(
                "targetTableName");
        }

        ResultSet rs = null;
        PreparedStatement ps = null;
        Connection conn = null;
        try {
            // set up a jdbc connection
            conn = DriverManager.getConnection("jdbc:default:connection");

            if (targetSchemaName == null) {
                targetSchemaName =
                    FarragoUdrRuntime.getSession().getSessionVariables()
                    .schemaName;
            }

            // Check target table or view already exits.
            ps = conn.prepareStatement(
                "select count(1) from LOCALDB.SYS_ROOT.DBA_TABLES where SCHEMA_NAME=? and TABLE_NAME=?");
            ps.setString(1, targetSchemaName);
            ps.setString(2, targetTableName);
            rs = ps.executeQuery();

            int row_cnt = 0;
            if (rs.next()) {
                row_cnt = rs.getInt(1);
            } else {
                throw ApplibResource.instance().EmptyInput.ex();
            }
            if (row_cnt > 0) {
                throw ApplibResource.instance().TableOrViewAlreadyExists.ex(
                    repos.getLocalizedObjectName(targetSchemaName),
                    repos.getLocalizedObjectName(targetTableName));
            }

            SqlParser sqlParser = new SqlParser(sourceTbName);
            SqlIdentifier tableId = (SqlIdentifier) sqlParser.parseExpression();
            String [] ss = tableId.names;

            String catalog = "";
            String schema = "";
            String table = "";

            if (ss.length == 3) {
                catalog = ss[0];
                schema = ss[1];
                table = ss[2];
            } else if (ss.length == 2) {
                catalog =
                    FarragoUdrRuntime.getSession().getSessionVariables()
                    .catalogName;
                schema = ss[0];
                table = ss[1];
            } else if (ss.length == 1) {
                catalog =
                    FarragoUdrRuntime.getSession().getSessionVariables()
                    .catalogName;
                schema =
                    FarragoUdrRuntime.getSession().getSessionVariables()
                    .schemaName;
                table = ss[0];
            }

            StringBuilder ddl = new StringBuilder();
            ddl.append("create table ").append(
                SqlDialect.EIGENBASE.quoteIdentifier(targetSchemaName)).append(
                ".").append(
                SqlDialect.EIGENBASE.quoteIdentifier(targetTableName)).append(
                " ( ");
            StringBuilder select_stmt = new StringBuilder();

            // Get columns info of souce table from DBA_COLUMNS.
            select_stmt.append(
                "select COLUMN_NAME, DATATYPE, \"PRECISION\", DEC_DIGITS, IS_NULLABLE, REMARKS ")
            .append("from LOCALDB.SYS_ROOT.DBA_COLUMNS ").append(
                "where CATALOG_NAME=? and SCHEMA_NAME=? and TABLE_NAME =? ")
            .append("order by ORDINAL_POSITION");

            ps = conn.prepareStatement(select_stmt.toString());
            ps.setString(1, catalog);
            ps.setString(2, schema);
            ps.setString(3, table);
            rs = ps.executeQuery();

            while (rs.next()) {
                String column_name = rs.getString(1);
                String data_type = rs.getString(2);
                boolean is_nullable = rs.getBoolean(5);
                ddl.append(" " + column_name + " " + data_type);
                if (SqlTypeName.VARBINARY.getName().equals(data_type)
                    || SqlTypeName.BINARY.getName().equals(data_type)
                    || SqlTypeName.VARCHAR.getName().equals(data_type)
                    || SqlTypeName.CHAR.getName().equals(data_type))
                {
                    ddl.append("(" + rs.getInt(3) + ")");
                } else if (SqlTypeName.DECIMAL.getName().equals(data_type)) {
                    ddl.append("(" + rs.getInt(3) + "," + rs.getInt(4) + ")");
                }
                if (!is_nullable) {
                    ddl.append(" " + "not null");
                }
                ddl.append(",");
            }

            if (additionalColsInfo != null) {
                ddl.append(additionalColsInfo);
            } else {
                ddl.deleteCharAt((ddl.length() - 1));
            }

            ddl.append(" )");
            tracer.info("create table statement: " + ddl.toString());
            ps = conn.prepareStatement(ddl.toString());
            ps.execute();
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

// End CreateTbFromSrcTbUdp.java
