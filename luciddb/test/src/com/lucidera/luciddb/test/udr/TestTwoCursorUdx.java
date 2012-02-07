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
 * TODO:  Even the humblest classes deserve comments.
 *
 * @author Elizabeth Lin
 * @version $Id$
 */
public abstract class TestTwoCursorUdx
{
    public static void execute(
        ResultSet inputSetA, ResultSet inputSetB,
        PreparedStatement resultInserter)
        throws SQLException
    {
        ResultSetMetaData rsmdA = inputSetA.getMetaData();
        ResultSetMetaData rsmdB = inputSetB.getMetaData();
        int n = rsmdA.getColumnCount();
 
        // verify that the tables have the same number of columns
        assert(n  == rsmdB.getColumnCount());

        while (inputSetB.next()) {
            for (int i = 1; i <= n; i++) {
                resultInserter.setString(i, inputSetB.getString(i));
            }
            resultInserter.executeUpdate();
        }

        while (inputSetA.next()) {
            for (int i = 1; i <= n; i++) {
                resultInserter.setString(i, inputSetA.getString(i));
            }
            resultInserter.executeUpdate();
        }

    }
}

// End TestTwoCursorUdx.java
