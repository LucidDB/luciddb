/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005-2007 The Eigenbase Project
// Copyright (C) 2004-2007 Disruptive Tech
// Copyright (C) 2005-2007 LucidEra, Inc.
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
package org.eigenbase.sql;

import java.util.*;

import org.eigenbase.reltype.*;
import org.eigenbase.sql.parser.*;
import org.eigenbase.sql.type.*;


/**
 * A SQL literal representing a DATE value, such as <code>DATE
 * '2004-10-22'</code>.
 *
 * <p>Create values using {@link SqlLiteral#createDate}.
 *
 * @author jhyde
 * @version $Id$
 */
public class SqlDateLiteral
    extends SqlAbstractDateTimeLiteral
{
    //~ Constructors -----------------------------------------------------------

    SqlDateLiteral(Calendar d, SqlParserPos pos)
    {
        super(d, false, SqlTypeName.DATE, 0, SqlParserUtil.DateFormatStr, pos);
    }

    SqlDateLiteral(Calendar d, String format, SqlParserPos pos)
    {
        super(d, false, SqlTypeName.DATE, 0, format, pos);
    }

    //~ Methods ----------------------------------------------------------------

    public SqlNode clone(SqlParserPos pos)
    {
        return new SqlDateLiteral((Calendar) value, pos);
    }

    public String toString()
    {
        return "DATE '" + toFormattedString() + "'";
    }

    /**
     * Returns e.g. '1969-07-21'.
     */
    public String toFormattedString()
    {
        return getDate().toString(formatString);
    }

    public RelDataType createSqlType(RelDataTypeFactory typeFactory)
    {
        return typeFactory.createSqlType(getTypeName());
    }

    public void unparse(
        SqlWriter writer,
        int leftPrec,
        int rightPrec)
    {
        if (writer.getDialect().isSqlServer()) {
            writer.literal("'" + this.toFormattedString() + "'");
        } else {
            writer.literal(this.toString());
        }
    }
}

// End SqlDateLiteral.java
