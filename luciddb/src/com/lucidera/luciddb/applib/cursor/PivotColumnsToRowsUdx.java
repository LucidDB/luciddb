/*
// $Id$
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
package com.lucidera.luciddb.applib.cursor;

import java.sql.*;

/**
 * Pivots a table's columns to rows
 *
 * @author Elizabeth Lin
 * @version $Id$
 */
public abstract class PivotColumnsToRowsUdx
{
    /**
     * PivotColumnsToRows UDX takes a single row table with many columns and 
     * returns a table with the original column names and values pivoted into
     * the new columns param_name and param_value
     * 
     * @param inputSet input result set 
     * @param resultInserter prepared statment used to create output table
     */
    public static void execute(
        ResultSet inputSet, PreparedStatement resultInserter) 
        throws SQLException
    {
        // Validate ParameterMetaData requires two outputs
        assert(resultInserter.getParameterMetaData().getParameterCount() == 2);

        ResultSetMetaData rsmd = inputSet.getMetaData();
        int colNum = rsmd.getColumnCount();
        inputSet.next();
        for (int i = 1; i <= colNum; i++) {
            resultInserter.setString(1, rsmd.getColumnLabel(i));
            resultInserter.setString(2, inputSet.getString(i));
            resultInserter.executeUpdate();
        }
    }        
}

// End PivotColumnsToRowsUdx.java
