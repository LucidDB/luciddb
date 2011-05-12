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

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;

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

    public static String INTEGER_TYPE = "INT";
    public static String CHAR_TYPE = "CHAR";
    public static String BINARY_TYPE = "BINARY";
    public static String DECIMAL_TYPE = "DEC";
    public static String NUMERIC_TYPE = "NUMERIC";

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

        String schema = targetSchemaName;
        String table = targetTableName;

        // validate the required input parameters.
        if ((table == null || table.trim().length() == 0)
            || (selectStmt == null || selectStmt.trim().length() == 0))
        {

            throw new Exception(
                "The input parameters target_table_name and select_statement are required. Please check them.");
        }

        ResultSet rs = null;
        PreparedStatement ps = null;
        Connection conn = null;
        StringBuffer sql = new StringBuffer();

        try {

            // set up a jdbc connection
            conn = DriverManager.getConnection("jdbc:default:connection");

            // If schema is not specified, the udp will select the schema of the connection calling.
            if (schema == null) {

                ps = conn.prepareStatement("select PARAM_VALUE from sys_root.user_session_parameters where param_name = 'schemaName'");
                rs = ps.executeQuery();

                while (rs.next()) {

                    schema = rs.getString(1);
                }

            }
            // verify whehter the targe table is exsiting in specific schema.
            ps = conn.prepareStatement("select count(1) from SYS_ROOT.DBA_TABLES where SCHEMA_NAME=? and TABLE_NAME=?");
            ps.setString(1, schema);
            ps.setString(2, table);
            rs = ps.executeQuery();

            int row_cnt = 0;
            while (rs.next()) {

                row_cnt = rs.getInt(1);
            }
            if (row_cnt > 0) {

                throw new Exception("The table[" + schema + "." + table
                    + "] is existing.");
            }

            //Create table structure based on select statement.
            String tmpSql = selectStmt;
            sql.append(selectStmt);
            if (selectStmt.toLowerCase().indexOf("where") == -1) {
                
                sql.append(" where 1=2");
                
            } else {

                sql.append(" and 1=2");
            }
            ps = conn.prepareStatement(tmpSql);
            rs = ps.executeQuery();

            sql = new StringBuffer();
            sql.append("Create Table \"" + schema + "\"" + ".\"" + table
                + "\" (");
            ResultSetMetaData meta = rs.getMetaData();
            int columnCount = meta.getColumnCount();

            for (int i = 0; i < columnCount; i++) {

                String columnName = meta.getColumnName(i + 1);
                String dataType = meta.getColumnTypeName(i + 1);
                int precision = meta.getPrecision(i + 1);
                boolean isNullAble = (meta.isNullable(i + 1) == 1) ? true
                    : false;
                String columnSql = createColumnSQL(
                    columnName,
                    dataType,
                    precision,
                    isNullAble);
                sql.append(columnSql).append(",");
            }

            sql.deleteCharAt((sql.length() - 1));
            sql.append(")");

            ps = conn.prepareStatement(sql.toString());
            ps.execute();
            // load data from source to target table if shouldLoad equals true
            if (shouldLoad) {
                sql = new StringBuffer();
                sql.append("insert into \"" + schema + "\".\"" + table + "\" "
                    + selectStmt);

                ps = conn.prepareStatement(sql.toString());
                ps.execute();
            }

        } catch (Exception ex) {

            String msg = "[ErrorInfo]: " + ex.getMessage() + "\n"
                + "[SQLDetail:] " + sql.toString();
            throw new Exception(msg);

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

    private static String createColumnSQL(
        String columnName,
        String type,
        int precision,
        boolean isNullable)
    {

        StringBuffer ret = new StringBuffer();
        String dataType = type.toUpperCase();

        if (dataType.indexOf(CHAR_TYPE) != -1) {

            ret.append(columnName + " " + type + "(" + precision + ")");

        } else if ((dataType.indexOf(DECIMAL_TYPE) != -1)
            || (dataType.indexOf(NUMERIC_TYPE) != -1))
        {

            ret.append(columnName + " " + type + "(" + precision + ")");

        } else if (dataType.indexOf(BINARY_TYPE) != -1) {

            ret.append(columnName + " " + type + "(" + precision + ")");

        } else {

            ret.append(columnName + " " + type);
        }

        if (!isNullable) {
            ret.append(" not null");
        }

        return ret.toString();

    }

}
