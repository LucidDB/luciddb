/*
// $Id$
// Package org.eigenbase is a class library of database components.
// Copyright (C) 2002-2004 Disruptive Tech
// Copyright (C) 2003-2004 John V. Sichi
//
// This program is free software; you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation; either version 2 of the License, or
// (at your option) any later version.
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

package org.eigenbase.runtime;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;


/**
 * When a relational expression obeys the {@link
 * org.eigenbase.relopt.CallingConvention#RESULT_SET result set calling
 * convention}, and does not explicitly specify a row type, the results are
 * object of type <code>Row</code>.
 */
public class Row
{
    //~ Instance fields -------------------------------------------------------

    ResultSet resultSet;
    Object [] values;

    //~ Constructors ----------------------------------------------------------

    public Row(ResultSet resultSet)
        throws SQLException
    {
        this.resultSet = resultSet;
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        final int count = resultSetMetaData.getColumnCount();
        this.values = new Object[count];
        for (int i = 0; i < values.length; i++) {
            values[i] = resultSet.getObject(i + 1);
        }
    }

    //~ Methods ---------------------------------------------------------------

    /**
     * Returns the value of a given column, similar to {@link
     * ResultSet#getObject(int)}.
     *
     * @param columnIndex the first column is 1, the second is 2, ...
     *
     * @return a <code>java.lang.Object</code> holding the column value
     */
    public Object getObject(int columnIndex)
    {
        return values[columnIndex - 1];
    }

    /**
     * Returns the result set that this row belongs to.
     */
    public ResultSet getResultSet()
    {
        return resultSet;
    }
}


// End Row.java
