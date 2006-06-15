/*
// $Id$
// Farrago is an extensible data management system.
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
package com.lucidera.luciddb.test.udr;

import java.sql.*;

/**
 * Takes in table and returns the column types
 *
 * @author Elizabeth Lin
 * @version $Id$
 */
public abstract class GetColumnTypesUdx
{
    public static void execute(
        ResultSet inputSet, PreparedStatement resultInserter)
        throws SQLException
    {
        ResultSetMetaData rsMetadata = inputSet.getMetaData();
        int n = rsMetadata.getColumnCount();

        for (int i=1; i <= n; i++) {
            resultInserter.setString(1, rsMetadata.getColumnLabel(i));
            resultInserter.setInt(2, rsMetadata.getColumnType(i));
            resultInserter.setString(3, rsMetadata.getColumnTypeName(i));
            resultInserter.executeUpdate();
        }
    }
}

// End GetColumnTypesUdx.java
