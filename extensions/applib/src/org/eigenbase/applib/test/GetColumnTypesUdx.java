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
package org.eigenbase.applib.test;

import java.sql.*;


/**
 * Takes in table and returns the column types
 *
 * @author Elizabeth Lin
 * @version $Id$
 */
public abstract class GetColumnTypesUdx
{
    //~ Methods ----------------------------------------------------------------

    public static void execute(
        ResultSet inputSet,
        PreparedStatement resultInserter)
        throws SQLException
    {
        ResultSetMetaData rsMetadata = inputSet.getMetaData();
        int n = rsMetadata.getColumnCount();

        for (int i = 1; i <= n; i++) {
            resultInserter.setString(1, rsMetadata.getColumnLabel(i));
            resultInserter.setInt(2, rsMetadata.getColumnType(i));
            resultInserter.setString(3, rsMetadata.getColumnTypeName(i));
            resultInserter.executeUpdate();
        }
    }

    public static void getColumnInfo(
        ResultSet inputSet,
        PreparedStatement resultInserter)
        throws SQLException
    {
        ResultSetMetaData rsMetadata = inputSet.getMetaData();
        int n = rsMetadata.getColumnCount();

        for (int i = 1; i <= n; i++) {
            resultInserter.setString(1, rsMetadata.getColumnName(i));
            resultInserter.setString(2, rsMetadata.getColumnTypeName(i));
            resultInserter.setInt(3, rsMetadata.getColumnDisplaySize(i));
            resultInserter.setInt(4, rsMetadata.getPrecision(i));
            resultInserter.setInt(5, rsMetadata.getScale(i));
            resultInserter.executeUpdate();
        }
        inputSet.close();
    }
}

// End GetColumnTypesUdx.java
