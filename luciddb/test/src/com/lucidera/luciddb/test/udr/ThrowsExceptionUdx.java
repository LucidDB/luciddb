/*
// $Id$
// Farrago is an extensible data management system.
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
package com.lucidera.luciddb.test.udr;

import java.sql.*;

/**
 * Throws an exception after processing 2 rows
 *
 * @author Elizabeth Lin
 * @version $Id$
 */
public abstract class ThrowsExceptionUdx
{
    public static void execute(
        ResultSet inputSet, PreparedStatement resultInserter)
        throws SQLException
    {
        int i = 1;
        while(inputSet.next()) {
            if (i++ == 3) {
                throw new SQLException("hey, hey, it's an exception");
            }
            resultInserter.setString(1, inputSet.getString(1));
            resultInserter.executeUpdate();
        }
    }
        
}

// End ThrowsExceptionUdx.java
