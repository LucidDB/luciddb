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
package com.lucidera.luciddb.test.udr;

import java.sql.*;

/**
 * Create a table with the same column types and contents as an arbitrary
 * query.
 * 
 * The syntax is somewhat similar to Oracle's command
 *
 *   CREATE TABLE <schema>.<table> <options> AS <query>
 *
 * so, for example,
 *
 *   SELECT *
 *   FROM Create_Table_As_Select('sales','orders',
 *                            'SELECT * FROM Sales.orders')
 *
 * executes
 *
 *   CREATE TABLE sales.orders(orderid int, orderdate date, ...);
 *   INSERT INTO sales.orders SELECT * FROM Sales.orders;
 *
 * @author Ported from //bb/bb713/server/SQL/CreateTableAsSelect.java
 * @version $Id$
 */
public abstract class CreateTableAsSelectUdx
{
    public static void execute(
        String schema, String table, ResultSet inputSet, 
        PreparedStatement resultInserter)
        throws SQLException
    {
        ResultSetMetaData rsmd = inputSet.getMetaData();
        int colNum = rsmd.getColumnCount();
        StringBuilder sb = new StringBuilder();
        StringBuilder insertSql = new StringBuilder();
        
        // build query strings
        int coltype;
        sb.append("create table " + schema + "." + table + "(");
        insertSql.append("insert into " + schema + "." + table + 
            " values(");
        for (int i = 1; i <= colNum; i++) {
            sb.append(" " + rsmd.getColumnLabel(i) + " " + 
                rsmd.getColumnTypeName(i));
            
            coltype = rsmd.getColumnType(i);
            if ((coltype == -3) || (coltype == -2) || (coltype == 1) ||
                (coltype == 12))
            {
                // data type needs precision information
                // VARBINARY, BINARY, CHAR, VARCHAR 
                sb.append("(" + rsmd.getPrecision(i) + ")");
            } else if (coltype == 3) {
                // DECIMAL needs precision and scale information
                sb.append("(" + rsmd.getPrecision(i) + "," + 
                    rsmd.getScale(i) + ")");
            } 

            insertSql.append(" ?");
            if (i != colNum) {
                sb.append(",");
                insertSql.append(",");
            }
        }
        sb.append(")");
        insertSql.append(")");

        Connection conn = null;
        Statement stmt = null;
        PreparedStatement instmt = null;
        try {
            conn = DriverManager.getConnection("jdbc:default:connection");
            stmt = conn.createStatement();

            // create the table
            stmt.executeUpdate(sb.toString());
            stmt.close();

            // insert for newly created table
            instmt = conn.prepareStatement(insertSql.toString());

            while(inputSet.next()) {
                for (int x = 1; x <= colNum; x++) {
                    insertCol(instmt, x, inputSet);
                }
                
                if(instmt.executeUpdate() != 1) {
                    resultInserter.setString(
                        1, "incomplete insertion of rows");
                    resultInserter.executeUpdate();
                }
            }
        } catch (SQLException ex) {
            resultInserter.setString(1, ex.toString());
            resultInserter.executeUpdate();
        } finally {
            if (stmt!= null) {
                stmt.close();
            }
            if (instmt != null) {
                instmt.close();
            }
            if (conn != null) {
                conn.close();
            }
        }
    }

    private static void insertCol(
        PreparedStatement ri, int columnToInsert, ResultSet rs)
        throws SQLException
    {
        int columnType = rs.getMetaData().getColumnType(columnToInsert);

        // add 7 to column type so we don't have negative numbers
        columnType += 7;

        switch (columnType) {
        case 1: // TINYINT
        case 11: // INTEGER
        case 12: // SMALLINT
            ri.setInt(columnToInsert, rs.getInt(columnToInsert));
            break;
        case 2: // BIGINT
            ri.setLong(columnToInsert, rs.getLong(columnToInsert));
            break;
        case 4: // VARBINARY
        case 5: // BINARY
            ri.setBytes(columnToInsert, rs.getBytes(columnToInsert));
            break;
        case 8: // CHAR
        case 19: // VARCHAR
            ri.setString(columnToInsert, rs.getString(columnToInsert));
            break;
        case 10: // DECIMAL
        case 14: // REAL
            ri.setFloat(columnToInsert, rs.getFloat(columnToInsert));
            break;
        case 15: // DOUBLE
            ri.setDouble(columnToInsert, rs.getDouble(columnToInsert));
            break;
        case 23: // BOOLEAN
            ri.setBoolean(columnToInsert, rs.getBoolean(columnToInsert));
            break;
        case 98: // DATE
            ri.setDate(columnToInsert, rs.getDate(columnToInsert));
            break;
        case 99: // TIME
            ri.setTime(columnToInsert, rs.getTime(columnToInsert));
            break;
        case 100: // TIMESTAMP
            ri.setTimestamp(columnToInsert, rs.getTimestamp(columnToInsert));
            break;
        default:
            throw new SQLException(
                "CreateTableAsSelectUdx: invalid data type");
        }
    }

}

// End CreateTableAsSelectUdx.java
