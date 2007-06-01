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

import java.util.*;

import org.eigenbase.reltype.*;
import org.eigenbase.sql.parser.*;
import org.eigenbase.sql.type.*;
import org.eigenbase.util14.*;


/**
 * A SQL literal representing a DATE, TIME or TIMESTAMP value.
 *
 * <p>Examples:
 *
 * <ul>
 * <li>DATE '2004-10-22'</li>
 * <li>TIME '14:33:44.567'</li>
 * <li><code>TIMESTAMP '1969-07-21 03:15 GMT'</code></li>
 * </ul>
 */
abstract class SqlAbstractDateTimeLiteral
    extends SqlLiteral
{
    //~ Instance fields --------------------------------------------------------

    protected final boolean hasTimeZone;
    protected final String formatString;
    protected final int precision;

    //~ Constructors -----------------------------------------------------------

    /**
     * Constructs a datetime literal based on a Calendar. If the literal is to
     * represent a Timestamp, the Calendar is expected to follow java.sql
     * semantics. If the Calendar is to represent a Time or Date, the Calendar
     * is expected to follow {@link ZonelessTime} and {@link ZonelessDate}
     * semantics.
     */
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

    //~ Methods ----------------------------------------------------------------

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
     * Returns timezone component of this literal. Technically, a sql date
     * doesn't come with a tz, but time and ts inherit this, and the calendar
     * object has one, so it seems harmless.
     *
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
        return typeFactory.createSqlType(
            getTypeName(),
            getPrec());
    }

    public void unparse(
        SqlWriter writer,
        int leftPrec,
        int rightPrec)
    {
        writer.literal(this.toString());
    }

    /**
     * Converts this literal to a {@link ZonelessDate} object.
     */
    protected ZonelessDate getDate()
    {
        ZonelessDate zd = new ZonelessDate();
        zd.setZonelessTime(getCal().getTimeInMillis());
        return zd;
    }

    /**
     * Converts this literal to a {@link ZonelessTime} object.
     */
    protected ZonelessTime getTime()
    {
        ZonelessTime zt = new ZonelessTime();
        zt.setZonelessTime(getCal().getTimeInMillis());
        return zt;
    }

    /**
     * Converts this literal to a {@link ZonelessTimestamp} object.
     */
    protected ZonelessTimestamp getTimestamp()
    {
        ZonelessTimestamp zt = new ZonelessTimestamp();
        zt.setZonelessTime(getCal().getTimeInMillis());
        return zt;
    }
}

// End SqlAbstractDateTimeLiteral.java
