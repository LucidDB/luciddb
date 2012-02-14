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

import java.io.PrintWriter;
import java.io.StringWriter;
import java.sql.PreparedStatement;

public class SQLRunner
    extends Thread
{
    private String errorMsg;

    private PreparedStatement ps;

    public SQLRunner(PreparedStatement ps)
    {
        this.ps = ps;
    }

    public void run()
    {

        try {

            ps.execute();

        } catch (Exception ex) {

            StringWriter writer = new StringWriter();
            ex.printStackTrace(new PrintWriter(writer, true));
            errorMsg = writer.toString();
        }

    }

    public String getErrorMsg()
    {
        return errorMsg;
    }

    private void setErrorMsg(String errorMsg)
    {
        this.errorMsg = errorMsg;
    }
    
    
}
