/*
// $Id$
// Applib is a library of SQL-invocable routines for Eigenbase applications.
// Copyright (C) 2006 The Eigenbase Project
// Copyright (C) 2006 SQLstream, Inc.
// Copyright (C) 2006 DynamoBI Corporation
//
// This library is free software; you can redistribute it and/or modify it
// under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation; either version 2.1 of the License, or (at
// your option) any later version.
//
// This library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public
// License along with this library; if not, write to the Free Software
// Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA
*/
package org.eigenbase.applib.cursor;

import java.sql.*;

import org.eigenbase.applib.resource.*;


/**
 * Pivots a table's columns to rows
 *
 * @author Elizabeth Lin
 * @version $Id$
 */
public abstract class PivotColumnsToRowsUdx
{
    //~ Methods ----------------------------------------------------------------

    /**
     * PivotColumnsToRows UDX takes a single row table with many columns and
     * returns a table with the original column names and values pivoted into
     * the new columns param_name and param_value
     *
     * @param inputSet input result set
     * @param resultInserter prepared statment used to create output table
     */
    public static void execute(
        ResultSet inputSet,
        PreparedStatement resultInserter)
        throws SQLException, ApplibException
    {
        // Validate ParameterMetaData requires two outputs
        assert (resultInserter.getParameterMetaData().getParameterCount() == 2);

        ResultSetMetaData rsmd = inputSet.getMetaData();
        int colNum = rsmd.getColumnCount();
        if (inputSet.next()) {
            for (int i = 1; i <= colNum; i++) {
                resultInserter.setString(1, rsmd.getColumnLabel(i));
                resultInserter.setString(2, inputSet.getString(i));
                resultInserter.executeUpdate();
            }
            if (inputSet.next()) {
                throw ApplibResource.instance().MoreThanOneRow.ex();
            }
        } else {
            throw ApplibResource.instance().EmptyInput.ex();
        }
    }
}

// End PivotColumnsToRowsUdx.java
