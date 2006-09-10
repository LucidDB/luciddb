/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2006-2006 The Eigenbase Project
// Copyright (C) 2006-2006 Disruptive Tech
// Copyright (C) 2006-2006 LucidEra, Inc.
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

import org.eigenbase.util.Util;

import java.text.NumberFormat;
import java.text.DecimalFormat;

/**
 * Runtime type for interval values.
 *
 * TODO: Need to include start, end time unit and precision
 * TODO: Need to support casting from string, exact numerics
 *
 * @author angel
 * @version $Id$
 * @since Aug 20, 2006
 */
public abstract class EncodedSqlInterval
    implements AssignableValue
{
    //~ Static fields/initializers ---------------------------------------------
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
            Util.permAssert(false,
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

    public static class EncodedSqlIntervalYM extends EncodedSqlInterval
    {
        public static long MONTHS_PER_YEAR = 12;

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
            throw Util.needToImplement("Conversion from string to month year interval");
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
            long years = v/MONTHS_PER_YEAR;
            long months = v % MONTHS_PER_YEAR;

            strbuf.append(sign);
            strbuf.append(NF2.format(years));
            strbuf.append('-');
            strbuf.append(NF2.format(months));
            return strbuf.toString();
        }
    }

    public static class EncodedSqlIntervalDT extends EncodedSqlInterval
    {
        public static long MS_PER_SECOND = 1000;
        public static long MS_PER_MINUTE = 60*MS_PER_SECOND;
        public static long MS_PER_HOUR = 60*MS_PER_MINUTE;
        public static long MS_PER_DAY = 24*MS_PER_HOUR;

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

        protected void parse(String interval)
        {
            throw Util.needToImplement("Conversion from string to day time interval");
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
            long days = v/MS_PER_DAY;
            v = v % MS_PER_DAY;
            long hours = v/MS_PER_HOUR;
            v = v % MS_PER_HOUR;
            long minutes = v/MS_PER_MINUTE;
            v = v % MS_PER_MINUTE;
            long seconds = v/MS_PER_SECOND;
            v = v % MS_PER_SECOND;

            strbuf.append(sign);
            if (days > 0) {
                strbuf.append(NF2.format(days));
                strbuf.append(' ');
            }
            strbuf.append(NF2.format(hours));
            strbuf.append(':');
            strbuf.append(NF2.format(minutes));
            strbuf.append(':');
            strbuf.append(NF2.format(seconds));
            if (v > 0) {
                strbuf.append('.');
                strbuf.append(NF3.format(v));
            }
            return strbuf.toString();
        }
    }

}

// End EncodedSqlInterval.java
