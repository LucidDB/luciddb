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
