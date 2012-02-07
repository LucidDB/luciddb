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

import java.net.*;
import java.io.*;
import java.sql.*;

public class UrlTextFetchUdx
{
    public static void execute(
        String urlString,
        PreparedStatement resultInserter)
        throws Exception
    {
        URL url = new URL(urlString);
        InputStream inputStream = null;
        try {
            inputStream = url.openStream();
            InputStreamReader reader = new InputStreamReader(inputStream);
            LineNumberReader lineReader = new LineNumberReader(reader);
            for (;;) {
                String line = lineReader.readLine();
                if (line == null) {
                    return;
                }
                int lineNumber = lineReader.getLineNumber();
                resultInserter.setInt(1, lineNumber);
                resultInserter.setString(2, line);
                resultInserter.executeUpdate();
            }
        } finally {
            if (inputStream != null) {
                inputStream.close();
            }
        }
    }
}

// End UrlTextFetchUdx.java
