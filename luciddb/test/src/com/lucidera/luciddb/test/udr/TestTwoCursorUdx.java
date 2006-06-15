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
 * TODO:  Even the humblest classes deserve comments.
 *
 * @author Elizabeth Lin
 * @version $Id$
 */
public abstract class TestTwoCursorUdx
{
    public static void execute(
        ResultSet inputSetA, ResultSet inputSetB,
        PreparedStatement resultInserter)
        throws SQLException
    {
        ResultSetMetaData rsmdA = inputSetA.getMetaData();
        ResultSetMetaData rsmdB = inputSetB.getMetaData();
        int n = rsmdA.getColumnCount();
 
        // verify that the tables have the same number of columns
        assert(n  == rsmdB.getColumnCount());

        while (inputSetB.next()) {
            for (int i = 1; i <= n; i++) {
                resultInserter.setString(i, inputSetB.getString(i));
            }
            resultInserter.executeUpdate();
        }

        while (inputSetA.next()) {
            for (int i = 1; i <= n; i++) {
                resultInserter.setString(i, inputSetA.getString(i));
            }
            resultInserter.executeUpdate();
        }

    }
}

// End TestTwoCursorUdx.java
