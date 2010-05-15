/*
 // LucidDB is a DBMS optimized for business intelligence.
 // Copyright (C) 2006-2007 LucidEra, Inc.
 // Copyright (C) 2006-2007 The Eigenbase Project
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

import java.sql.*;
import java.util.logging.*;

import org.eigenbase.sql.*;
import com.lucidera.luciddb.applib.resource.*;

import net.sf.farrago.catalog.*;
import net.sf.farrago.runtime.*;
import net.sf.farrago.trace.*;

/**
 * 
 * [DDB-26] Create a UDP that creates a table from a generic cursor input, then
 * optionally loads the table from the input cursor.<br>
 * 
 * @author Ray Zhang
 * @since Mar-18-2010
 */

public abstract class CreateTbFromSelectStmtUdp
{
    private static final Logger tracer = FarragoTrace.getClassTracer(CreateTbFromSelectStmtUdp.class);

    /**
     * 
     * @param sourceTbName
     * @param targetSchemaName
     * @param targetTableName
     * @param additionalColsInfo
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
        if ((table == null || table.trim().length() == 0)) {

            throw ApplibResourceObject.get().InputIsRequired.ex("targetTableName");
        }
        if ((selectStmt == null || selectStmt.trim().length() == 0)) {

            throw ApplibResourceObject.get().InputIsRequired.ex("selectStmt");
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
                schema = FarragoUdrRuntime.getSession().getSessionVariables().schemaName;
            }
            // verify whehter the targe table is exsiting in specific schema.
            ps = conn.prepareStatement("select count(1) from SYS_ROOT.DBA_TABLES where SCHEMA_NAME=? and TABLE_NAME=?");
            ps.setString(1, schema);
            ps.setString(2, table);
            rs = ps.executeQuery();

            int row_cnt = 0;
            if (rs.next()) {
                row_cnt = rs.getInt(1);
            } else {
                throw ApplibResourceObject.get().EmptyInput.ex();
            }
            if (row_cnt > 0) {
                throw ApplibResourceObject.get().TableOrViewAlreadyExists.ex(
                    repos.getLocalizedObjectName(schema),
                    repos.getLocalizedObjectName(table));
            }
            // Create table structure based on select statement.
            StringBuilder ddl = new StringBuilder();
            ddl.append("create table ")
                .append(SqlDialect.EIGENBASE.quoteIdentifier(schema))
                .append(".")
                .append(SqlDialect.EIGENBASE.quoteIdentifier(table))
                .append(" ( ");
            ps = conn.prepareStatement(selectStmt);
            ResultSetMetaData rsmd = ps.getMetaData();
            int colNum = rsmd.getColumnCount();
            int coltype;
            for (int i = 1; i <= colNum; i++) {
                ddl.append(" " + rsmd.getColumnName(i) + " "
                    + rsmd.getColumnTypeName(i));
                
                coltype = rsmd.getColumnType(i);
                if ((coltype == Types.VARBINARY) || (coltype == Types.BINARY) || (coltype == Types.CHAR)
                    || (coltype == Types.VARCHAR))
                {
                    // data type needs precision information
                    // VARBINARY, BINARY, CHAR, VARCHAR
                    ddl.append("(" + rsmd.getPrecision(i) + ")");
                } else if (coltype == Types.DECIMAL) {
                    // DECIMAL needs precision and scale information
                    ddl.append("(" + rsmd.getPrecision(i) + ","
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
                        + SqlDialect.EIGENBASE.quoteIdentifier(schema))
                    .append(".")
                    .append(SqlDialect.EIGENBASE.quoteIdentifier(table))
                    .append(" ")
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
