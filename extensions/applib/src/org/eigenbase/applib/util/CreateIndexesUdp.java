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

import java.sql.*;
import java.util.*;

import org.eigenbase.applib.resource.*;
import org.eigenbase.sql.*;
import org.eigenbase.sql.util.*;


/**
 * Meant to work in conjunction with ShowIndexCandidatesUdx,
 * this creates indexes on any passed columns with the name
 * of AUTOCREATED_colname.
 *
 * @author Kevin Secretan
 * @version $Id$
 */
public abstract class CreateIndexesUdp
{

    /**
     * Expects a given SQL query as a string that will return a table
     * containing a catalog, schema name, table name, and column name,
     * and will create an index on each column.
     * 
     * @param sql - SQL String will be executed and used to create indexes.
     */
    public static void execute(String sql)
        throws ApplibException
    {
        StringBuilder errors = new StringBuilder();
        try {
            Connection conn = 
                DriverManager.getConnection("jdbc:default:connection");
            Statement stmt = conn.createStatement();

            
            PreparedStatement ps = conn.prepareStatement(sql);
            ResultSet rs = ps.executeQuery();
            while (rs.next()) {
                int c = 0;
                String cat = rs.getString(++c);
                String schema = rs.getString(++c);
                String table = rs.getString(++c);
                String col = rs.getString(++c);

                SqlBuilder sb = new SqlBuilder(SqlDialect.EIGENBASE);
                sb.append("create index ");
                sb.identifier("AUTOCREATED_" + col);
                sb.append(" ON ");
                sb.identifier(cat, schema, table);
                sb.append("(");
                sb.identifier(col);
                sb.append(")");
                ps = conn.prepareStatement(sb.getSqlAndClear());

                try {
                    ps.execute();
                } catch (SQLException ex) {
                    errors.append("Could not create index on: " + col + "\n");
                }
            }
            conn.close();
        } catch (SQLException e) {
            throw ApplibResource.instance().DatabaseAccessError.ex(
                    e.toString(),
                    e);
        }
    }
}
