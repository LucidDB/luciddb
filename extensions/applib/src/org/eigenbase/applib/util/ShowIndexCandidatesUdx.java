/*
// $Id$
// Applib is a library of SQL-invocable routines for Eigenbase applications.
// Copyright (C) 2011 The Eigenbase Project
// Copyright (C) 2011 SQLstream, Inc.
// Copyright (C) 2011 DynamoBI Corporation
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

import net.sf.farrago.session.*;
import net.sf.farrago.runtime.*;

import org.eigenbase.sql.*;
import org.eigenbase.sql.util.*;
import org.eigenbase.applib.resource.*;


/**
 * Scans a given local table's columns to find potential columns that may
 * benefit from having an index, i.e. ones which have a sufficient ratio
 * of unique values.
 *
 * @author Kevin Secretan
 * @version $Id$
 */
public abstract class ShowIndexCandidatesUdx
{
    //~ Methods ----------------------------------------------------------------

    /**
     * Catalog-less alias of normal execute, default catalog
     * is the current session's catalog.
     * @param schema - schema to look in
     * @param table - table to scan columns of
     * @param threshold - percent threshold to allow/deny column candidates
     * @param resultInserter - handles output
     *
     * @see execute(String,String,String,int,PreparedStatement)
     */
    public static void execute(
            String schema,
            String table,
            int threshold,
            PreparedStatement resultInserter)
        throws ApplibException
    {
        FarragoSession session = FarragoUdrRuntime.getSession();
        String catalog = session.getSessionVariables().catalogName;
        execute(catalog, schema, table, threshold, resultInserter);
    }

    /**
     * Connects with the DB and estimates statistics for a given table,
     * then returns a list of columns that may be good potential candidates
     * for indexing.
     *
     * @param catalog - catalog to look in
     * @param schema - schema to look in
     * @param table - table to scan columns of
     * @param threshold - percent passed denies any columns whose ratio
     *                    of unique values in a column to the total number
     *                    of values in a column is not larger than threshold.
     * @param resultInserter - handles output
     */
    public static void execute(
            String catalog,
            String schema,
            String table,
            int threshold,
            PreparedStatement resultInserter)
        throws ApplibException
    {
        try {
            Connection conn =
                DriverManager.getConnection("jdbc:default:connection");
            Statement stmt = conn.createStatement();

            // get stats:
            SqlBuilder sb = new SqlBuilder(SqlDialect.EIGENBASE);
            sb.append("analyze table ");
            sb.identifier(catalog, schema, table);
            sb.append(" estimate statistics for all columns");
            PreparedStatement ps = conn.prepareStatement(sb.getSqlAndClear());
            ps.execute();

            // get candidate columns:
            sb.append(
                    "select st.column_name from sys_root.dba_column_stats st " +
                    "where st.column_name not in (" +
                        "select ic.column_name from " +
                        "sys_root.dba_unclustered_indexes ui " +
                        "inner join sys_boot.mgmt.dba_index_columns_internal " +
                        "ic ON ui.mof_id = ic.mof_id " +
                        "where ui.catalog_name=? and ui.schema_name=? and " +
                        "ui.table_name=?) " +
                    "and st.catalog_name=? and st.schema_name=? and " +
                    "st.table_name=? and " +
                    "100.0 * st.distinct_value_count / st.sample_size <=?");
            ps = conn.prepareStatement(sb.getSqlAndClear());
            int c = 0;
            for (int i = 0; i < 2; i++) {
                ps.setString(++c, catalog);
                ps.setString(++c, schema);
                ps.setString(++c, table);
            }
            ps.setInt(++c, threshold);

            ResultSet rs = ps.executeQuery();
            while (rs.next()) {
                int c1 = 0;
                resultInserter.setString(++c1, catalog);
                resultInserter.setString(++c1, schema);
                resultInserter.setString(++c1, table);
                resultInserter.setString(++c1, rs.getString(1));
                resultInserter.executeUpdate();
            }
        } catch (SQLException e) {
            throw ApplibResource.instance().DatabaseAccessError.ex(
                    e.toString(),
                    e);
        }
    }
}
