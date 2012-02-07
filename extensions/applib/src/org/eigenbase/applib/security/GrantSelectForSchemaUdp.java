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
package org.eigenbase.applib.security;

import java.io.*;

import java.sql.*;

import org.eigenbase.applib.resource.*;
import org.eigenbase.applib.util.*;
import org.eigenbase.util.*;


/**
 * GrantSelectForSchema UDP grants a user select privileges for all tables and
 * views in a specific schema.
 *
 * @author Oscar Gothberg
 * @version $Id$
 */

public abstract class GrantSelectForSchemaUdp
{
    //~ Methods ----------------------------------------------------------------

    /**
     * @param schemaName name of schema to grant select privs for
     * @param userName username of user to get privileges
     */

    public static void execute(String schemaName, String userName)
        throws SQLException
    {
        StringWriter sw;
        StackWriter stackw;
        PrintWriter pw;

        // build statement, forward it to DoForEntireSchemaUdp
        sw = new StringWriter();
        stackw = new StackWriter(sw, StackWriter.INDENT_SPACE4);
        pw = new PrintWriter(stackw);
        pw.print("grant select on %TABLE_NAME% to ");
        StackWriter.printSqlIdentifier(pw, userName);
        pw.close();
        DoForEntireSchemaUdp.execute(
            sw.toString(),
            schemaName,
            "TABLES_AND_VIEWS");
    }
}

// End GrantSelectForSchemaUdp.java
