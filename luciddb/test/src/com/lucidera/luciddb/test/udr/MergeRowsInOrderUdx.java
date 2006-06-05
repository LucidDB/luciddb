/*
// $Id$
// LucidDB is a DBMS optimized for business intelligence.
// Copyright (C) 2006-2006 LucidEra, Inc.
// Copyright (C) 2006-2006 The Eigenbase Project
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
package com.lucidera.luciddb.test.udr;

import java.sql.*;

/**
 * Test UDX that takes two tables with the same column types which are 
 * ordered on the same key and returns a table with rows from both tables 
 * ordered on the specified column (needs to be a string or int)
 *
 * @author Elizabeth Lin
 * @version $Id$
 */
public abstract class MergeRowsInOrderUdx
{
    public static void execute(
        ResultSet inputSetA, ResultSet inputSetB, int cmpCol,
        PreparedStatement resultInserter)
        throws SQLException
    {
        ResultSetMetaData rsmdA = inputSetA.getMetaData();
        ResultSetMetaData rsmdB = inputSetB.getMetaData();
        int n = rsmdA.getColumnCount();
        int i;
        boolean cmp;

        // verify that the tables have the same number of columns
        assert(n  == rsmdB.getColumnCount());

        // verify the table columns match and aren't autoincremented and setup
        for (i = 1; i <= n; i++) {
            assert(rsmdA.getColumnType(i) == rsmdB.getColumnType(i));
            assert(!rsmdA.isAutoIncrement(i) && !rsmdB.isAutoIncrement(i));
        }

        // verify compare column type is int or varchar or char
        int ct = rsmdA.getColumnType(cmpCol);
        assert ( (ct == 4) || (ct == 12) || (ct == 1) );

        inputSetA.next();
        inputSetB.next();

        while (!(inputSetA.isAfterLast() || inputSetB.isAfterLast())) {
            // compareCol returns true if inputSetA comes before inputSetB
            if (compareCol(inputSetA, inputSetB, cmpCol, ct))
            {
                for (i = 1; i <= n; i++) {
                    insertCol(resultInserter, i, inputSetA);
                }
                inputSetA.next();
            } else {
                for (int x = 1; i <= n; x++) {
                    insertCol(resultInserter, i, inputSetB);
                }
                inputSetB.next();
            }
            resultInserter.executeUpdate();
        }

        if (inputSetA.isAfterLast()) {
            while (inputSetB.next()) {
                for (i = 1; i <= n; i++) {
                    insertCol(resultInserter, i, inputSetB);
                }
                resultInserter.executeUpdate();
            }
        } else {
            while (inputSetA.next()) {
                for (i = 1; i <= n; i++) {
                    insertCol(resultInserter, i, inputSetA);
                }
                resultInserter.executeUpdate();
            }
        }

    }

    private static boolean compareCol(
        ResultSet rsA, ResultSet rsB, int col, int colType)
        throws SQLException
    {
        boolean result;
        switch(colType) {
        case 4: // integer
            result = (rsA.getInt(col) <= rsB.getInt(col));
            break;
        case 1: // char
        case 12: // varchar
            result = (rsA.getString(col).compareTo(rsB.getString(col)) <= 0);
            break;
        default: // should never get here
            throw(new SQLException(
                      "MergeRowsInOrderUdx: invalid compare column type"));
        }
        return result;
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
            throw new SQLException("MergeRowsInOrderUdx: invalid data type");
        }
    }
}

// End MergeRowsInOrderUdx.java
