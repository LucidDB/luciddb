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

package org.luciddb.pg2luciddb;

import java.util.*;
import java.io.*;
import java.sql.*;

public class pg_catalog_plugin
{
    // function to convert mofId to integer value:
    public static Integer mofIdToInteger(String input)
    {
        if (input.startsWith("j:"))
            input = input.substring(2);

        return Integer.parseInt(input, 16);
    }

    // get user by id:
    public static String getUserById(int id) throws SQLException 
    {
        String res = null;

        Connection conn = DriverManager.getConnection("jdbc:default:connection");
        PreparedStatement prep = conn.prepareStatement("SELECT USENAME FROM PG_CATALOG.PG_USER WHERE OID=?");
        prep.setInt(1, id);
        ResultSet rs = prep.executeQuery();
        if (rs.next()) 
        {
            res = rs.getString(1);
        }
        rs.close();
        return res;
    }

    // dummy procedure:
    public static void dummyProcedure(String input)
    {
    }
}

