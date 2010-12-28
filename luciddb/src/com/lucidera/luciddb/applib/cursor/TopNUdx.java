/*
// $Id$
// LucidDB is a DBMS optimized for business intelligence.
// Copyright (C) 2008-2008 LucidEra, Inc.
// Copyright (C) 2008-2008 The Eigenbase Project
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
package com.lucidera.luciddb.applib.cursor;

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
        while (n-->0 && lineInput.next()) {
            for(int i=1; i<=columnCount; i++) {
                resultInserter.setObject(i, lineInput.getObject(i));
            }
            resultInserter.executeUpdate();
        }
    }
}
