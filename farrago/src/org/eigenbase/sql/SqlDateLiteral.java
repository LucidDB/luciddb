/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2004-2005 Disruptive Tech
// Copyright (C) 2005-2005 LucidEra, Inc.
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

import org.eigenbase.reltype.RelDataType;
import org.eigenbase.reltype.RelDataTypeFactory;
import org.eigenbase.sql.parser.SqlParserPos;
import org.eigenbase.sql.parser.SqlParserUtil;
import org.eigenbase.sql.type.SqlTypeName;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;

/**
 * A SQL literal representing a DATE value,
 * such as <code>DATE '2004-10-22'</code>.
 *
 * <p>Create values using {@link SqlLiteral#createDate}.
 *
 * @version $Id$
 * @author jhyde
 */
public class SqlDateLiteral extends SqlAbstractDateTimeLiteral
 {

    SqlDateLiteral(Calendar d, SqlParserPos pos)
    {
        super(d, false, SqlTypeName.Date, 0, SqlParserUtil.DateFormatStr, pos);
    }

    SqlDateLiteral(Calendar d, String format, SqlParserPos pos)
    {
        super(d, false, SqlTypeName.Date, 0, format, pos);
    }

    /**
     * Constructs a new dateformat object for the given string.
     * Note that DateFormat objects aren't thread-safe.
     * @param dfString
     * @return date format object
     */
    static DateFormat getDateFormat(String dfString)
    {
        SimpleDateFormat df = new SimpleDateFormat(dfString);
        df.setLenient(false);
        return df;
    }

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
        return getDateFormat(formatString).format(getDate());
    }

    public RelDataType createSqlType(RelDataTypeFactory typeFactory)
    {
        return typeFactory.createSqlType(getTypeName());
    }
}

// End SqlDateLiteral.java
