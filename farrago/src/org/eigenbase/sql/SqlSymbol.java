/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2002-2005 Disruptive Tech
// Copyright (C) 2005-2005 Red Square, Inc.
// Portions Copyright (C) 2003-2005 John V. Sichi
//
// This program is free software; you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation; either version 2 of the License, or
// (at your option) any later Eigenbase-approved version.
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

package org.eigenbase.sql;

import org.eigenbase.sql.SqlLiteral;
import org.eigenbase.sql.parser.SqlParserPos;
import org.eigenbase.sql.type.SqlTypeName;
import org.eigenbase.util.Util;


/**
 * Represents a value of an enumerated type which is exposed as a SQL keyword.
 *
 * <p>Consider the TRIM function, whose syntax is
 * <code>TRIM([LEADING | TRAILING | BOTH] char FROM string)</code>. You can
 * represent the values LEADING, TRAILING and BOTH as values of an enumeration,
 * and you can also associate a literal value for when these values need to
 * appear in a parse tree.</p>
 *
 * <p>Derived classes should generally follow the pattern set by
 * {@link org.eigenbase.sql.fun.SqlTrimFunction.Flag}:<ul>
 * <li>a <code>static final</code> member for each value;</li>
 * <li>a <code>static final</code> member 'EnumeratedValues enumeration';</li>
 * <li><code>static</code> lookup methods 'get(String)' and 'get(int)'.</li>
 * </ul>
 *
 * @author jhyde
 * @since May 28, 2004
 * @version $Id$
 **/
public class SqlSymbol extends SqlLiteral
{
    //~ Instance fields -------------------------------------------------------

    public final String name;

    //~ Constructors ----------------------------------------------------------

    public SqlSymbol(
        String name,
        SqlParserPos pos)
    {
        super(name, SqlTypeName.Symbol, pos);
        this.name = name;
    }

    //~ Methods ---------------------------------------------------------------

    public String getDescription()
    {
        return null;
    }

    public String getName()
    {
        return name;
    }

    /**
     * Returns the value's name.
     */
    public String toString()
    {
        return name;
    }

    public Error unexpected()
    {
        return Util.newInternal("Value " + name + " of class " + getClass()
            + " unexpected here");
    }

    public void unparse(
        SqlWriter writer,
        int leftPrec,
        int rightPrec)
    {
        writer.print(name.toUpperCase());
    }
}


// End SqlSymbol.java
