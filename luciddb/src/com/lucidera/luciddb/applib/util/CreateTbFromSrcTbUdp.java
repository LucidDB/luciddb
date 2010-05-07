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

import net.sf.farrago.catalog.*;
import net.sf.farrago.runtime.*;
import net.sf.farrago.trace.*;

import org.eigenbase.sql.*;
import org.eigenbase.sql.parser.*;
import com.lucidera.luciddb.applib.resource.*;

/**
 * 
 * Create a UDP that will deduce, and optionally execute the DDL for a table.
 * 
 * @author Ray Zhang
 * @since Mar-16-2010
 */

public abstract class CreateTbFromSrcTbUdp
{

    private static final Logger tracer = FarragoTrace.getClassTracer(CreateTbFromSrcTbUdp.class);
    private static final FarragoRepos repos = FarragoUdrRuntime.getSession()
        .getRepos();

    /**
     * 
     * @param sourceTbName
     * @param targetSchemaName
     * @param targetTableName
     * @param additionalColsInfo
     * @throws Exception
     */
    public static void execute(
        String sourceTbName,
        String targetSchemaName,
        String targetTableName,
        String additionalColsInfo)
        throws Exception
    {
        tracer.info("Input parameters: source table: [" + sourceTbName
            + "] target Schema: [" + targetSchemaName + "] target table: ["
            + targetTableName + "] additional cols: [" + additionalColsInfo
            + "]");
        //Input parameter check.      
        if (sourceTbName == null || sourceTbName.length() == 0) {

            throw ApplibResourceObject.get().InputIsRequired.ex("sourceTbName");
        }
        if (targetTableName == null || targetTableName.length() == 0) {

            throw ApplibResourceObject.get().InputIsRequired.ex("targetTableName");
        }

        ResultSet rs = null;
        PreparedStatement ps = null;
        Connection conn = null;
        try {
            // set up a jdbc connection
            conn = DriverManager.getConnection("jdbc:default:connection");

            if (targetSchemaName == null) {
                targetSchemaName = FarragoUdrRuntime.getSession()
                    .getSessionVariables().schemaName;
            }
            // Check target table or view already exits.
            ps = conn.prepareStatement("select count(1) from SYS_ROOT.DBA_TABLES where SCHEMA_NAME=? and TABLE_NAME=?");
            ps.setString(1, targetSchemaName);
            ps.setString(2, targetTableName);
            rs = ps.executeQuery();

            int row_cnt = 0;
            if (rs.next()) {
                row_cnt = rs.getInt(1);
            } else {
                throw ApplibResourceObject.get().EmptyInput.ex();
            }
            if (row_cnt > 0) {
                throw ApplibResourceObject.get().TableOrViewAlreadyExists.ex(
                    repos.getLocalizedObjectName(targetSchemaName),
                    repos.getLocalizedObjectName(targetTableName));
            }

            SqlParser sqlParser = new SqlParser(sourceTbName);
            SqlIdentifier tableId = (SqlIdentifier) sqlParser.parseExpression();
            String[] ss = tableId.names;

            if (ss.length == 1) {
                sourceTbName = SqlDialect.EIGENBASE.quoteIdentifier(FarragoUdrRuntime.getSession()
                    .getSessionVariables().schemaName)
                    + "." + sourceTbName;
            }

            StringBuilder select_stmt = new StringBuilder();
            // thru select statement, get table info. such as column name, column type, precision, scale, etc.
            select_stmt.append("select * from ").append(sourceTbName).append(
                " where 1 = 2");
            tracer.info("select statement: " + select_stmt.toString());
            ps = conn.prepareStatement(select_stmt.toString());
            ResultSetMetaData rsmd = ps.getMetaData();
            int colNum = rsmd.getColumnCount();
            StringBuilder ddl = new StringBuilder();

            ddl.append("create table ").append(
                SqlDialect.EIGENBASE.quoteIdentifier(targetSchemaName)).append(
                ".").append(
                SqlDialect.EIGENBASE.quoteIdentifier(targetTableName)).append(
                " ( ");

            int coltype;
            for (int i = 1; i <= colNum; i++) {
                ddl.append(" " + rsmd.getColumnLabel(i) + " "
                    + rsmd.getColumnTypeName(i));

                coltype = rsmd.getColumnType(i);
                if ((coltype == -3) || (coltype == -2) || (coltype == 1)
                    || (coltype == 12))
                {
                    // data type needs precision information
                    // VARBINARY, BINARY, CHAR, VARCHAR 
                    ddl.append("(" + rsmd.getPrecision(i) + ")");
                } else if (coltype == 3) {
                    // DECIMAL needs precision and scale information
                    ddl.append("(" + rsmd.getPrecision(i) + ","
                        + rsmd.getScale(i) + ")");
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
