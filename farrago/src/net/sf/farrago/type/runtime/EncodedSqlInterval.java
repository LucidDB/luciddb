/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2006-2009 The Eigenbase Project
// Copyright (C) 2006-2009 SQLstream, Inc.
// Copyright (C) 2006-2009 LucidEra, Inc.
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
package net.sf.farrago.type.runtime;

import java.text.*;
import java.util.Calendar;

import org.eigenbase.sql.*;
import org.eigenbase.sql.SqlIntervalQualifier.TimeUnit;
import org.eigenbase.util.*;


/**
 * Runtime type for interval values.
 *
 * <p>TODO:<ul>
 * <li>Need to include precision.
 * <li>Need to support casting from string, exact numerics
 * <li>Both of above would be easier if we just get the SqlIntervalQualifier
 * </ul>
 *
 * @author angel
 * @version $Id$
 * @since Aug 20, 2006
 */
public abstract class EncodedSqlInterval
    implements AssignableValue
{
    //~ Static fields/initializers ---------------------------------------------

    // used for code generation
    public static final String GET_START_UNIT_METHOD_NAME = "getStartUnit";
    public static final String GET_END_UNIT_METHOD_NAME = "getEndUnit";

    // Beware: NumberFormat classes are not guaranteed threadsafe.
    // Must use synchronization blocks around these
    protected static final NumberFormat NF2 = new DecimalFormat("00");
    protected static final NumberFormat NF3 = new DecimalFormat("000");

    //~ Instance fields --------------------------------------------------------

    private boolean isNull;
    public long value;

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a runtime object
     */
    public EncodedSqlInterval()
    {
    }

    //~ Methods ----------------------------------------------------------------

    protected abstract void parse(String date);

    /**
     * @return long.class
     */
    public static Class getPrimitiveClass()
    {
        return long.class;
    }

    // implement NullableValue
    public boolean isNull()
    {
        return isNull;
    }

    // implement NullableValue
    public void setNull(boolean b)
    {
        isNull = b;
    }

    // implement AssignableValue
    public void assignFrom(Object obj)
    {
        if (obj == null) {
            setNull(true);
            return;
        }
        setNull(false);
        if (obj instanceof Number) {
            setNull(false);
            assignFrom(((Number) obj).longValue());
        } else if (obj instanceof DataValue) {
            DataValue dataValue = (DataValue) obj;
            assignFrom(dataValue.getNullableData());
        } else if (obj instanceof String) {
            String intervalStr = (String) obj;
            parse(intervalStr);
        } else {
            throw Util.newInternal(
                "Cannot assign class " + obj.getClass() + " to interval");
        }
    }

    // implement AssignableValue
    public void assignFrom(long v)
    {
        setNull(false);
        value = v;
    }

    // implement DataValue
    public Object getNullableData()
    {
        if (isNull()) {
            return null;
        }

        // TODO: What should be the correct type to return?
        // Returns String representation of interval
        return toString();
    }

    // Implemented by code generation
    protected abstract SqlIntervalQualifier.TimeUnit getStartUnit();

    protected abstract SqlIntervalQualifier.TimeUnit getEndUnit();

    /**
     * Rounds this interval value down to a unit of time expressed using its
     * ordinal. Called by generated code.
     *
     * @param timeUnitOrdinal Ordinal of {@link
     * org.eigenbase.sql.SqlIntervalQualifier.TimeUnit} value
     */
    public void floor(int timeUnitOrdinal)
    {
        floor(SqlIntervalQualifier.TimeUnit.getValue(timeUnitOrdinal));
    }

    protected void floor(TimeUnit timeUnit)
    {
        throw Util.unexpected(timeUnit);
    }

    /**
     * Rounds this interval value up to a unit of time expressed using its
     * ordinal. Called by generated code.
     *
     * @param timeUnitOrdinal Ordinal of {@link
     * org.eigenbase.sql.SqlIntervalQualifier.TimeUnit} value
     */
    public void ceil(int timeUnitOrdinal)
    {
        ceil(SqlIntervalQualifier.TimeUnit.getValue(timeUnitOrdinal));
    }

    protected void ceil(TimeUnit timeUnit)
    {
        throw Util.unexpected(timeUnit);
    }

    private static void appendHours(StringBuffer buf, long hours)
    {
        buf.append(' ');
        buf.append(NF2.format(hours));
    }

    private static void appendMinutes(StringBuffer buf, long minutes)
    {
        buf.append(':');
        buf.append(NF2.format(minutes));
    }

    private static void appendSeconds(
        StringBuffer buf,
        long seconds,
        long fractions)
    {
        buf.append(':');
        buf.append(NF2.format(seconds));

        if (fractions > 0) {
            buf.append('.');
            buf.append(NF3.format(fractions));
        }
    }

    //~ Inner Classes ----------------------------------------------------------

    public abstract static class EncodedSqlIntervalYM
        extends EncodedSqlInterval
    {
        public static final long MONTHS_PER_YEAR = 12;

        // implement AssignableValue
        public void assignFrom(Object obj)
        {
            if (obj instanceof EncodedSqlIntervalYM) {
                EncodedSqlInterval interval = (EncodedSqlInterval) obj;
                setNull(interval.isNull());
                value = interval.value;
            } else {
                super.assignFrom(obj);
            }
        }

        protected void parse(String interval)
        {
            throw Util.needToImplement(
                "Conversion from string to month year interval");
        }

        public String toString()
        {
            long v = value;
            StringBuffer strbuf = new StringBuffer();
            char sign = '+';
            if (v < 0) {
                v = -v;
                sign = '-';
            }
            long years = v / MONTHS_PER_YEAR;
            long months = v % MONTHS_PER_YEAR;

            SqlIntervalQualifier.TimeUnit startUnit = getStartUnit();
            SqlIntervalQualifier.TimeUnit endUnit = getEndUnit();

            strbuf.append(sign);

            // Number formatter is not guaranteed threadsafe (though
            // Sun's implementation actually is).  Therefore, be cautious
            // and synchronize

            if (startUnit == SqlIntervalQualifier.TimeUnit.Year) {
                strbuf.append(years);
                if (endUnit == SqlIntervalQualifier.TimeUnit.Month) {
                    strbuf.append('-');
                    synchronized (NF2) {
                        strbuf.append(NF2.format(months));
                    }
                }
            } else {
                strbuf.append(months);
            }

            return strbuf.toString();
        }
    }

    public abstract static class EncodedSqlIntervalDT
        extends EncodedSqlInterval
    {
        public static final long MS_PER_SECOND = 1000;
        public static final long MS_PER_MINUTE = 60 * MS_PER_SECOND;
        public static final long MS_PER_HOUR = 60 * MS_PER_MINUTE;
        public static final long MS_PER_DAY = 24 * MS_PER_HOUR;

        // implement AssignableValue
        public void assignFrom(Object obj)
        {
            if (obj instanceof EncodedSqlIntervalDT) {
                EncodedSqlInterval interval = (EncodedSqlInterval) obj;
                setNull(interval.isNull());
                value = interval.value;
            } else {
                super.assignFrom(obj);
            }
        }

        /**
         * Rounds this interval value down to a unit of time. All smaller units
         * of time are zeroed also.
         *
         * <p>For example, <code>floor(MINUTE)</code> applied to <code>INTERVAL
         * '7 11:23:45' DAY TO SECOND</code> returns <code>INTERVAL '7 11:23:00'
         * DAY TO SECOND</code>.
         *
         * @param timeUnit Time unit
         */
        public void floor(SqlIntervalQualifier.TimeUnit timeUnit)
        {
            if (value < 0) {
                value = -value;
                ceil(timeUnit);
                value = -value;
                return;
            }
            switch (timeUnit) {
            case Day:
                value = value / MS_PER_DAY * MS_PER_DAY;
                break;
            case Hour:
                value = value / MS_PER_HOUR * MS_PER_HOUR;
                break;
            case Minute:
               value = value / MS_PER_MINUTE * MS_PER_MINUTE;
               break;
            case Second:
                value = value / MS_PER_SECOND * MS_PER_SECOND;
                break;
            default:
                throw Util.unexpected(timeUnit);
            }
        }

        /**
         * Rounds this interval value up to a unit of time. All smaller units of
         * time are zeroed also.
         *
         * <p>For example, <code>floor(MINUTE)</code> applied to <code>INTERVAL
         * '7 11:23:45' DAY TO SECOND</code> returns <code>INTERVAL '7 11:24:00'
         * DAY TO SECOND</code>.
         *
         * @param timeUnit Time unit
         */
        public void ceil(SqlIntervalQualifier.TimeUnit timeUnit)
        {
            if (value < 0) {
                value = -value;
                floor(timeUnit);
                value = -value;
                return;
            }
            switch (timeUnit) {
            case Day:
                value = (value + (MS_PER_DAY - 1)) / MS_PER_DAY * MS_PER_DAY;
                break;
            case Hour:
                value = (value + (MS_PER_HOUR - 1)) / MS_PER_HOUR * MS_PER_HOUR;
                break;
            case Minute:
               value =
                   (value + (MS_PER_MINUTE - 1)) / MS_PER_MINUTE
                   * MS_PER_MINUTE;
               break;
            case Second:
                value =
                    (value + (MS_PER_SECOND - 1)) / MS_PER_SECOND
                    * MS_PER_SECOND;
                break;
            default:
                throw Util.unexpected(timeUnit);
            }
        }

        protected void parse(String interval)
        {
            throw Util.needToImplement(
                "Conversion from string to day time interval");
        }

        public String toString()
        {
            long v = value;
            StringBuffer strbuf = new StringBuffer();
            char sign = '+';
            if (v < 0) {
                v = -v;
                sign = '-';
            }
            long days = v / MS_PER_DAY;
            v = v % MS_PER_DAY;
            long hours = v / MS_PER_HOUR;
            v = v % MS_PER_HOUR;
            long minutes = v / MS_PER_MINUTE;
            v = v % MS_PER_MINUTE;
            long seconds = v / MS_PER_SECOND;
            v = v % MS_PER_SECOND;
            long fractions = v;

            SqlIntervalQualifier.TimeUnit startUnit = getStartUnit();
            SqlIntervalQualifier.TimeUnit endUnit = getEndUnit();
            strbuf.append(sign);

            // Number formatter is not guaranteed threadsafe (though
            // Sun's implementation actually is).  Therefore, be cautious
            // and synchronize.
            //
            // Also, don't enforce precision format on leading fields
            synchronized (NF2) {
                if (startUnit == SqlIntervalQualifier.TimeUnit.Day) {
                    strbuf.append(days);

                    if ((endUnit != null)
                        && (endUnit != SqlIntervalQualifier.TimeUnit.Day))
                    {
                        appendHours(strbuf, hours);
                        if (endUnit != SqlIntervalQualifier.TimeUnit.Hour) {
                            appendMinutes(strbuf, minutes);
                            if (endUnit
                                != SqlIntervalQualifier.TimeUnit.Minute)
                            {
                                appendSeconds(strbuf, seconds, fractions);
                            }
                        }
                    }
                } else if (
                    getStartUnit()
                    == SqlIntervalQualifier.TimeUnit.Hour)
                {
                    strbuf.append(hours);

                    if ((endUnit != null)
                        && (endUnit != SqlIntervalQualifier.TimeUnit.Hour))
                    {
                        appendMinutes(strbuf, minutes);
                        if (endUnit != SqlIntervalQualifier.TimeUnit.Minute) {
                            appendSeconds(strbuf, seconds, fractions);
                        }
                    }
                } else if (
                    getStartUnit()
                    == SqlIntervalQualifier.TimeUnit.Minute)
                {
                    strbuf.append(minutes);

                    if ((endUnit != null)
                        && (endUnit != SqlIntervalQualifier.TimeUnit.Minute))
                    {
                        appendSeconds(strbuf, seconds, fractions);
                    }
                } else if (
                    getStartUnit()
                    == SqlIntervalQualifier.TimeUnit.Second)
                {
                    strbuf.append(seconds);

                    if (fractions > 0) {
                        strbuf.append('.');
                        strbuf.append(NF3.format(fractions));
                    }
                }
            } //synchronized(NF2)

            return strbuf.toString();
        }
    }
}

// End EncodedSqlInterval.java
