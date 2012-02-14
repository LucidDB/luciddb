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
