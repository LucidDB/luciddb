/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2004-2005 Disruptive Tech
// Copyright (C) 2005-2005 Red Square, Inc.
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
import org.eigenbase.sql.type.SqlTypeName;

import java.util.Calendar;
import java.util.TimeZone;
import java.sql.Date;
import java.sql.Time;

/**
 * A SQL literal representing a DATE, TIME or TIMESTAMP value.
 *
 * <p>Examples:<ul>
 * <li>DATE '2004-10-22'</li>
 * <li>TIME '14:33:44.567'</li>
 * <li><code>TIMESTAMP '1969-07-21 03:15 GMT'</code></li>
 * </ul>
 **/
abstract class SqlAbstractDateTimeLiteral extends SqlLiteral {
    protected final boolean hasTimeZone;
    protected final String formatString;
    public final int precision;

    protected SqlAbstractDateTimeLiteral(
        Calendar d,
        boolean tz,
        SqlTypeName typeName,
        int precision,
        String formatString,
        SqlParserPos pos)
    {
        super(d, typeName, pos);
        this.hasTimeZone = tz;
        this.precision = precision;
        this.formatString = formatString;
    }

    public int getPrec()
    {
        return precision;
    }

    public String toValue()
    {
        return Long.toString(getCal().getTimeInMillis());
    }

    public Calendar getCal()
    {
        return (Calendar) value;
    }

    /**
     * Returns timezone component of this literal.  Technically, a sql date
     * doesn't come with a tz, but time and ts inherit this, and the calendar
     * object has one, so it seems harmless.
     * @return timezone
     */
    public TimeZone getTimeZone()
    {
        assert hasTimeZone : "Attempt to get timezone on Literal date: "
        + getCal() + ", which has no timezone";
        return getCal().getTimeZone();
    }

    /**
     * Returns e.g. <code>DATE '1969-07-21'</code>.
     */
    public abstract String toString();

    /**
     * Returns e.g. <code>1969-07-21</code>.
     */
    public abstract String toFormattedString();

    public RelDataType createSqlType(RelDataTypeFactory typeFactory)
    {
        return typeFactory.createSqlType(typeName, precision);
    }

    public void unparse(
        SqlWriter writer,
        int leftPrec,
        int rightPrec)
    {
        writer.print(this.toString());
    }

    /**
     * Converts this literal to a {@link java.sql.Date} object.
     */
    protected Date getDate()
    {
        return new Date(getCal().getTimeInMillis()
            - Calendar.getInstance().getTimeZone().getRawOffset());
    }

    /**
     * Converts this literal to a {@link java.sql.Time} object.
     */
    protected Time getTime()
    {
        long millis = getCal().getTimeInMillis();
        int tzOffset =
            Calendar.getInstance().getTimeZone().getOffset(millis);
        return new Time(millis - tzOffset);
    }

}

// End SqlAbstractDateTimeLiteral.java
