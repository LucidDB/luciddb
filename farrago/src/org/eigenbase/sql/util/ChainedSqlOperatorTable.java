/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2004-2005 Disruptive Tech
// Copyright (C) 2005-2005 Red Square, Inc.
// Portions Copyright (C) 2004-2005 John V. Sichi
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
package org.eigenbase.sql.util;

import org.eigenbase.sql.*;

import java.util.*;

/**
 * ChainedSqlOperatorTable implements the {@link SqlOperatorTable} interface by
 * chaining together any number of underlying operator table instances.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class ChainedSqlOperatorTable implements SqlOperatorTable
{
    private final List tableList;

    /**
     * Creates a new empty table.
     */
    public ChainedSqlOperatorTable()
    {
        tableList = new ArrayList();
    }

    /**
     * Adds an underlying table.  The order in which tables are added
     * is significant; tables added earlier have higher lookup precedence.
     *
     * @param table table to add
     */
    public void add(SqlOperatorTable table)
    {
        tableList.add(table);
    }

    // implement SqlOperatorTable
    public List lookupOperatorOverloads(SqlIdentifier opName, SqlSyntax syntax)
    {
        List list = new ArrayList();
        for (int i = 0; i < tableList.size(); ++i) {
            SqlOperatorTable table = (SqlOperatorTable) tableList.get(i);
            list.addAll(table.lookupOperatorOverloads(opName, syntax));
        }
        return list;
    }
    
    // implement SqlOperatorTable
    public List getOperatorList()
    {
        List list = new ArrayList();
        for (int i = 0; i < tableList.size(); ++i) {
            SqlOperatorTable table = (SqlOperatorTable) tableList.get(i);
            list.addAll(table.getOperatorList());
        }
        return list;
    }
}

// End ChainedSqlOperatorTable.java
