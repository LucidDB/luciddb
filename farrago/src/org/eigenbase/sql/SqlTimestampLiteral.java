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

import org.eigenbase.sql.parser.SqlParserPos;
import org.eigenbase.sql.parser.SqlParserUtil;
import org.eigenbase.sql.type.SqlTypeName;

import java.util.Calendar;
import java.sql.Timestamp;
import java.sql.Time;
import java.text.SimpleDateFormat;

/**
 * A SQL literal representing a TIMESTAMP value, for example
 * <code>TIMESTAMP '1969-07-21 03:15 GMT'</code>.
 *
 * <p>Create values using {@link SqlLiteral#createTimestamp}.
 */
public class SqlTimestampLiteral extends SqlAbstractDateTimeLiteral
{
    public SqlTimestampLiteral(
        Calendar cal,
        int precision,
        boolean hasTimeZone,
        SqlParserPos pos)
    {
        super(cal, hasTimeZone, SqlTypeName.Timestamp, precision,
            SqlParserUtil.TimestampFormatStr, pos);
    }

    /**
     * Converts this literal to a {@link java.sql.Timestamp} object.
     */
    public Timestamp getTimestamp()
    {
        return new Timestamp(getCal().getTimeInMillis());
    }

    /**
     * Converts this literal to a {@link java.sql.Time} object.
     */
    public Time getTime() {
        long millis = getCal().getTimeInMillis();
        int tzOffset =
            Calendar.getInstance().getTimeZone().getOffset(millis);
        return new Time(millis - tzOffset);
    }

    public String toString()
    {
        return "TIMESTAMP '" + toFormattedString() + "'";
    }

    /**
     * Returns e.g. '03:05:67.456'.
     */
    public String toFormattedString() {
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

// End SqlTimestampLiteral.java
