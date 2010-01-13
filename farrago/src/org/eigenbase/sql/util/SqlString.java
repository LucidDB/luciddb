/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2009-2009 The Eigenbase Project
// Copyright (C) 2009-2009 SQLstream, Inc.
// Copyright (C) 2009-2009 LucidEra, Inc.
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

import org.eigenbase.sql.SqlDialect;

/**
 * String that represents a kocher SQL statement, expression, or fragment.
 *
 * <p>A SqlString just contains a regular Java string, but the SqlWtring wrapper
 * indicates that the string has been created carefully guarding against all SQL
 * dialect and injection issues.
 *
 * <p>The easiest way to do build a SqlString is to use a {@link SqlBuilder}.
 *
 * @version $Id$
 * @author jhyde
 */
public class SqlString
{
    private final String s;
    private SqlDialect dialect;

    /**
     * Creates a SqlString.
     *
     * @param s Contents of string
     */
    public SqlString(SqlDialect dialect, String s)
    {
        this.dialect = dialect;
        this.s = s;
        assert s != null;
        assert dialect != null;
    }

    @Override
    public int hashCode()
    {
        return s.hashCode();
    }

    @Override
    public boolean equals(Object obj)
    {
        return obj instanceof SqlString
           && s.equals(((SqlString) obj).s);
    }

    /**
     * {@inheritDoc}
     *
     * <p>Returns the SQL string.
     *
     * @see #getSql()
     * @return SQL string
     */
    @Override
    public String toString()
    {
        return s;
    }

    /**
     * Returns the SQL string.
     * @return SQL string
     */
    public String getSql()
    {
        return s;
    }

    /**
     * Returns the dialect.
     */
    public SqlDialect getDialect()
    {
        return dialect;
    }
}

// End SqlString.java
