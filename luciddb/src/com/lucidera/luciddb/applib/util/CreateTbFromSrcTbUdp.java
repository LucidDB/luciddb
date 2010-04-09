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

/**
 * 
 * Create a UDP that will deduce, and optionally execute the DDL for a table.
 * 
 * @author Ray Zhang
 * @since Mar-16-2010
 */

public abstract class CreateTbFromSrcTbUdp
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
        String sourceTbName,
        String targetSchemaName,
        String targetTableName,
        String additionalColsInfo)
        throws Exception
    {

        ResultSet rs = null;
        PreparedStatement ps = null;
        Connection conn = null;

        StringBuffer createTbSql = new StringBuffer();

        try {

            // set up a jdbc connection
            conn = DriverManager.getConnection("jdbc:default:connection");

            if (targetSchemaName == null) {

                ps = conn.prepareStatement("select PARAM_VALUE from sys_root.user_session_parameters where param_name = 'schemaName'");
                rs = ps.executeQuery();

                while (rs.next()) {

                    targetSchemaName = rs.getString(1);
                }

                ps = conn.prepareStatement("select count(1) from SYS_ROOT.DBA_TABLES where SCHEMA_NAME=? and TABLE_NAME=?");
                ps.setString(1, targetSchemaName);
                ps.setString(2, targetTableName);
                rs = ps.executeQuery();

                int row_cnt = 0;
                while (rs.next()) {

                    row_cnt = rs.getInt(1);
                }
                if (row_cnt > 0) {

                    throw new Exception("The table[" + targetSchemaName + "."
                        + targetTableName + "] is existing.");
                }

            }

            StringBuffer sql = new StringBuffer();
            sql.append("Select COLUMN_NAME,DATATYPE,\"PRECISION\",IS_NULLABLE ")
                .append("From SYS_ROOT.DBA_COLUMNS ");
            String[] ss = sourceTbName.trim().replaceAll("\"", "").split("\\.");

            int size = ss.length;
            switch (size) {

            case 3:
                sql.append("Where CATALOG_NAME= '" + ss[0]
                    + "' And SCHEMA_NAME='" + ss[1] + "' And TABLE_NAME='"
                    + ss[2] + "'");
                break;
            case 2:
                sql.append("Where SCHEMA_NAME='" + ss[0] + "' And TABLE_NAME='"
                    + ss[1] + "'");
                break;
            case 1:

                String defaultSchema = "";
                ps = conn.prepareStatement("select PARAM_VALUE from sys_root.user_session_parameters where param_name = 'schemaName'");
                rs = ps.executeQuery();

                while (rs.next()) {

                    defaultSchema = rs.getString(1);
                }
                sql.append("Where SCHEMA_NAME='" + defaultSchema
                    + "' And TABLE_NAME='" + ss[0] + "'");
                break;
            default:
                throw new Exception("Source table or view[" + sourceTbName
                    + "] is invalid.");
            }
            sql.append("Order By ORDINAL_POSITION");
            ps = conn.prepareStatement(sql.toString());
            rs = ps.executeQuery();
            createTbSql.append("Create Table \"" + targetSchemaName + "\".\""
                + targetTableName + "\" ( ");

            int row_count = 0;
            while (rs.next()) {

                String columnName = rs.getString(1);
                String dataType = rs.getString(2);
                int precision = rs.getInt(3);
                boolean isNullable = rs.getBoolean(4);
                createTbSql.append(
                    createColumnSQL(columnName, dataType, precision, isNullable))
                    .append(",");
                row_count++;
            }
            if (row_count == 0) {

                throw new Exception("Source table or view[" + sourceTbName
                    + "] is not existing in database.");
            }
            if (additionalColsInfo != null) {

                createTbSql.append(additionalColsInfo);

            } else {

                createTbSql.deleteCharAt((createTbSql.length() - 1));
            }

            createTbSql.append(" )");

            ps = conn.prepareStatement(createTbSql.toString());
            ps.execute();

        } catch (Exception ex) {

            String msg = "[ErrorInfo]: " + ex.getMessage() + "\n"
                + "[Create Table SQL:] " + createTbSql.toString();
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
