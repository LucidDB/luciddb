/*
// $Id$
// Applib is a library of SQL-invocable routines for Eigenbase applications.
// Copyright (C) 2008 The Eigenbase Project
// Copyright (C) 2008 SQLstream, Inc.
// Copyright (C) 2008 DynamoBI Corporation
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
package org.eigenbase.applib.cursor;

import java.sql.*;

import java.util.*;


/**
 * Retain only the first <n> rows.
 *
 * @author Joel Palmert
 * @version $Id$
 */
public class TopNUdx
{
    //~ Methods ----------------------------------------------------------------

    /**
     * Retain only the first <n> rows of the table.
     */
    public static void execute(
        ResultSet lineInput,
        int n,
        PreparedStatement resultInserter)
        throws SQLException
    {
        int columnCount = lineInput.getMetaData().getColumnCount();
        while ((n-- > 0) && lineInput.next()) {
            for (int i = 1; i <= columnCount; i++) {
                resultInserter.setObject(i, lineInput.getObject(i));
            }
            resultInserter.executeUpdate();
        }
    }
}

// End TopNUdx.java
