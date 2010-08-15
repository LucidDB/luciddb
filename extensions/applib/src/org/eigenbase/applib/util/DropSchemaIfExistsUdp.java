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
