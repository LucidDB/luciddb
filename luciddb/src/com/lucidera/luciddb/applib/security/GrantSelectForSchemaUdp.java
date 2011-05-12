/*
// $Id$
// LucidDB is a DBMS optimized for business intelligence.
// Copyright (C) 2006-2007 LucidEra, Inc.
// Copyright (C) 2006-2007 The Eigenbase Project
//
// This program is free software; you can redistribute it and/or modify it
// under the terms of the GNU General Public License as published by the Free
// Software Foundation; either version 2 of the License, or (at your option)
// any later version approved by The Eigenbase Project.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program; if not, write to the Free Software
// Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
*/

package com.lucidera.luciddb.applib.security;

import com.lucidera.luciddb.applib.resource.*;
import com.lucidera.luciddb.applib.util.DoForEntireSchemaUdp;
import org.eigenbase.util.StackWriter;
import java.sql.*;
import java.io.*;

/**
 * GrantSelectForSchema UDP grants a user select privileges for all
 * tables and views in a specific schema.
 *
 * @author Oscar Gothberg
 * @version $Id$
 */

public abstract class GrantSelectForSchemaUdp {

    /**
     * @param schemaName name of schema to grant select privs for
     * @param userName username of user to get privileges
     */

    public static void execute(String schemaName, String userName) throws SQLException {
        
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
        DoForEntireSchemaUdp.execute(sw.toString(), schemaName, "TABLES_AND_VIEWS");
    }
}
