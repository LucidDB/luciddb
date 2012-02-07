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
package com.yoyodyne;

import java.util.*;
import java.sql.*;

public class HtmlTokenizerUdx
{
    public static void execute(
        ResultSet lineInput,
        PreparedStatement resultInserter)
        throws Exception
    {
        while (lineInput.next()) {
            int lineNumber = lineInput.getInt(1);
            String line = lineInput.getString(2);
            StringTokenizer tokenizer = new StringTokenizer(
                line,
                "<>/-'\"{}(); \t\n\r\f");
            int tokenNumber = 0;
            while (tokenizer.hasMoreTokens()) {
                String token = tokenizer.nextToken();
                resultInserter.setInt(1, lineNumber);
                resultInserter.setInt(2, tokenNumber);
                resultInserter.setString(3, token);
                resultInserter.executeUpdate();
                ++tokenNumber;
            }
        }
    }
}

// End HtmlTokenizerUdx.java
