/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005-2007 The Eigenbase Project
// Copyright (C) 2004-2007 SQLstream, Inc.
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

import org.eigenbase.sql.parser.*;
import org.eigenbase.sql.type.*;


/**
 * A SQL literal representing a TIME value, for example <code>TIME
 * '14:33:44.567'</code>.
 *
 * <p>Create values using {@link SqlLiteral#createTime}.
 */
public class SqlTimeLiteral
    extends SqlAbstractDateTimeLiteral
{
    //~ Constructors -----------------------------------------------------------

    SqlTimeLiteral(
        Calendar t,
        int precision,
        boolean hasTZ,
        SqlParserPos pos)
    {
        super(
            t,
            hasTZ,
            SqlTypeName.TIME,
            precision,
            SqlParserUtil.TimeFormatStr,
            pos);
    }

    SqlTimeLiteral(
        Calendar t,
        int precision,
        boolean hasTZ,
        String format,
        SqlParserPos pos)
    {
        super(t, hasTZ, SqlTypeName.TIME, precision, format, pos);
    }

    //~ Methods ----------------------------------------------------------------

    public SqlNode clone(SqlParserPos pos)
    {
        return new SqlTimeLiteral(
            (Calendar) value,
            precision,
            hasTimeZone,
            formatString,
            pos);
    }

    public String toString()
    {
        return "TIME '" + toFormattedString() + "'";
    }

    /**
     * Returns e.g. '03:05:67.456'.
     */
    public String toFormattedString()
    {
        String result = getTime().toString(formatString);
        final Calendar cal = getCal();
        if (precision > 0) {
            assert (precision <= 3);

            // get the millisecond count.  millisecond => at most 3 digits.
            String digits = Long.toString(cal.getTimeInMillis());
            result =
                result + "."
                + digits.substring(
                    digits.length() - 3,
                    digits.length() - 3 + precision);
        } else {
            assert (0 == cal.get(Calendar.MILLISECOND));
        }
        return result;
    }
}

// End SqlTimeLiteral.java
