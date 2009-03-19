/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005-2007 The Eigenbase Project
// Copyright (C) 2004-2007 SQLstream, Inc.
// Copyright (C) 2005-2007 LucidEra, Inc.
// Portions Copyright (C) 2004-2007 John V. Sichi
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

import java.util.*;

import org.eigenbase.sql.*;


/**
 * ChainedSqlOperatorTable implements the {@link SqlOperatorTable} interface by
 * chaining together any number of underlying operator table instances.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class ChainedSqlOperatorTable
    implements SqlOperatorTable
{
    //~ Instance fields --------------------------------------------------------

    private final List<SqlOperatorTable> tableList;

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a new empty table.
     */
    public ChainedSqlOperatorTable()
    {
        tableList = new ArrayList<SqlOperatorTable>();
    }

    //~ Methods ----------------------------------------------------------------

    /**
     * Adds an underlying table. The order in which tables are added is
     * significant; tables added earlier have higher lookup precedence. A table
     * is not added if it is already on the list.
     *
     * @param table table to add
     */
    public void add(SqlOperatorTable table)
    {
        if (!tableList.contains(table)) {
            tableList.add(table);
        }
    }

    // implement SqlOperatorTable
    public List<SqlOperator> lookupOperatorOverloads(
        SqlIdentifier opName,
        SqlFunctionCategory category,
        SqlSyntax syntax)
    {
        List<SqlOperator> list = new ArrayList<SqlOperator>();
        for (int i = 0; i < tableList.size(); ++i) {
            SqlOperatorTable table = tableList.get(i);
            list.addAll(
                table.lookupOperatorOverloads(opName, category, syntax));
        }
        return list;
    }

    // implement SqlOperatorTable
    public List<SqlOperator> getOperatorList()
    {
        List<SqlOperator> list = new ArrayList<SqlOperator>();
        for (SqlOperatorTable table : tableList) {
            list.addAll(table.getOperatorList());
        }
        return list;
    }
}

// End ChainedSqlOperatorTable.java
