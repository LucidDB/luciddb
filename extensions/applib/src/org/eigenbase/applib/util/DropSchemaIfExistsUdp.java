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
package org.eigenbase.applib.util;

import java.io.*;

import java.sql.*;

import org.eigenbase.applib.resource.*;
import org.eigenbase.util.*;


/**
 * Tries to drop a schema. Won't return errors neither if successful nor
 * unsuccessful.
 *
 * @param schemaName name of schema to drop.
 * @param restrictOrCascade either "RESTRICT" or "CASCADE", supplied as a
 * parameter to the SQL DROP command.
 *
 * @author Oscar Gothberg
 * @version $Id$
 */
public abstract class DropSchemaIfExistsUdp
{
    //~ Methods ----------------------------------------------------------------

    public static void execute(String schemaName, String restrictOrCascade)
        throws ApplibException, SQLException
    {
        PreparedStatement ps;
        Statement stmt;
        ResultSet rs;
        Connection conn = null;
        StringWriter sw;
        StackWriter stackw;
        PrintWriter pw;

        // set up a jdbc connection
        conn = DriverManager.getConnection("jdbc:default:connection");
        stmt = conn.createStatement();

        // make sure restrictOrCascade actually says "RESTRICT" or "CASCADE"
        if (!restrictOrCascade.toUpperCase().equals("RESTRICT")
            && !restrictOrCascade.toUpperCase().equals("CASCADE"))
        {
            throw ApplibResource.instance()
            .ParameterMustBeEitherRestrictOrCascade.ex();
        }

        // make sure schema exists, exit otherwise
        ps = conn.prepareStatement(
            "select SCHEMA_NAME from SYS_ROOT.DBA_SCHEMAS where SCHEMA_NAME = ?");
        ps.setString(1, schemaName);
        rs = ps.executeQuery();
        if (!rs.next()) {
            return;
        }

        // try to drop the schema
        sw = new StringWriter();
        stackw = new StackWriter(sw, StackWriter.INDENT_SPACE4);
        pw = new PrintWriter(stackw);
        pw.print("drop schema ");
        StackWriter.printSqlIdentifier(pw, schemaName);
        pw.print(" " + restrictOrCascade);
        pw.close();
        String query = sw.toString();

        // REVIEW jvs 2-Jan-2007:  Why suppress in the RESTRICT case?

        try {
            stmt.executeUpdate(sw.toString());
        } catch (SQLException e) {
            if (restrictOrCascade.equals("RESTRICT")) {
                // suppress complaints about requiring 'CASCADE'
                return;
            } else {
                // if we did use 'CASCADE', it's something else
                throw e;
            }
        }
    }
}

// End DropSchemaIfExistsUdp.java
