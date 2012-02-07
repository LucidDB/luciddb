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
