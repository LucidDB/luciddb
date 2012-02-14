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
package org.eigenbase.applib.test;

import java.sql.*;


/**
 * Takes in table and returns the column types
 *
 * @author Elizabeth Lin
 * @version $Id$
 */
public abstract class GetColumnTypesUdx
{
    //~ Methods ----------------------------------------------------------------

    public static void execute(
        ResultSet inputSet,
        PreparedStatement resultInserter)
        throws SQLException
    {
        ResultSetMetaData rsMetadata = inputSet.getMetaData();
        int n = rsMetadata.getColumnCount();

        for (int i = 1; i <= n; i++) {
            resultInserter.setString(1, rsMetadata.getColumnLabel(i));
            resultInserter.setInt(2, rsMetadata.getColumnType(i));
            resultInserter.setString(3, rsMetadata.getColumnTypeName(i));
            resultInserter.executeUpdate();
        }
    }

    public static void getColumnInfo(
        ResultSet inputSet,
        PreparedStatement resultInserter)
        throws SQLException
    {
        ResultSetMetaData rsMetadata = inputSet.getMetaData();
        int n = rsMetadata.getColumnCount();

        for (int i = 1; i <= n; i++) {
            resultInserter.setString(1, rsMetadata.getColumnName(i));
            resultInserter.setString(2, rsMetadata.getColumnTypeName(i));
            resultInserter.setInt(3, rsMetadata.getColumnDisplaySize(i));
            resultInserter.setInt(4, rsMetadata.getPrecision(i));
            resultInserter.setInt(5, rsMetadata.getScale(i));
            resultInserter.executeUpdate();
        }
        inputSet.close();
    }
}

// End GetColumnTypesUdx.java
