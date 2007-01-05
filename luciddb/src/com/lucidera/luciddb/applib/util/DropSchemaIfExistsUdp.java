/*
// $Id$
// LucidDB is a DBMS optimized for business intelligence.
// Copyright (C) 2006-2006 LucidEra, Inc.
// Copyright (C) 2006-2006 The Eigenbase Project
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
package com.lucidera.luciddb.applib.util;

import com.lucidera.luciddb.applib.resource.*;
import org.eigenbase.util.StackWriter;
import java.sql.*;
import java.io.*;


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
public abstract class DropSchemaIfExistsUdp {
    
    public static void execute(String schemaName, String restrictOrCascade) 
        throws ApplibException, SQLException {
        
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
            && !restrictOrCascade.toUpperCase().equals("CASCADE")) {
            throw ApplibResourceObject.get().
                ParameterMustBeEitherRestrictOrCascade.ex();
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
