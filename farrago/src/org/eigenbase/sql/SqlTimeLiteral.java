/*
// $Id$
// Package org.eigenbase is a class library of database components.
// Copyright (C) 2004-2004 Disruptive Tech
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
package org.eigenbase.sql;

import org.eigenbase.sql.parser.SqlParserPos;
import org.eigenbase.sql.parser.SqlParserUtil;
import org.eigenbase.sql.type.SqlTypeName;

import java.sql.Time;
import java.text.SimpleDateFormat;
import java.util.Calendar;

/**
 * A SQL literal representing a TIME value, for example
 * <code>TIME '14:33:44.567'</code>.
 *
 * <p>Create values using {@link SqlLiteral#createTime}.
 **/
public class SqlTimeLiteral extends SqlAbstractDateTimeLiteral
{
    SqlTimeLiteral(
        Calendar t,
        int precision,
        boolean hasTZ,
        SqlParserPos pos)
    {
        super(t, hasTZ, SqlTypeName.Time, precision, SqlParserUtil.TimeFormatStr,
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
        String result =
            new SimpleDateFormat(formatString).format(getTime());
        final Calendar cal = getCal();
        if (precision > 0) {
            assert (precision <= 3);

            // get the millisecond count.  millisecond => at most 3 digits.
            String digits = Long.toString(cal.getTimeInMillis());
            result =
                result + "."
                + digits.substring(digits.length() - 3,
                    digits.length() - 3 + precision);
        } else {
            assert (0 == cal.get(Calendar.MILLISECOND));
        }
        return result;
    }
}

// End SqlTimeLiteral.java
