/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2006-2007 The Eigenbase Project
// Copyright (C) 2006-2007 Disruptive Tech
// Copyright (C) 2006-2007 LucidEra, Inc.
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
package net.sf.farrago.syslib;

import java.sql.*;

import java.text.*;

import net.sf.farrago.resource.*;
import net.sf.farrago.runtime.*;

import org.eigenbase.util14.*;


/**
 * Moved over from luciddb applib datetime package for general use. Date
 * conversion, based on standard Java libraries
 *
 * @author Elizabeth Lin
 * @version $Id$
 */
public abstract class FarragoConvertDatetimeUDR
{
    //~ Enums ------------------------------------------------------------------

    protected enum Type
    {
        UDR, DIRECT_DATE, DIRECT_TIME, DIRECT_TIMESTAMP;
    }

    //~ Methods ----------------------------------------------------------------

    public static Date char_to_date(String format, String dateString)
    {
        if ((format == null) || (dateString == null)) {
            return null;
        }
        return new Date(charToDateHelper(format, dateString));
    }

    public static Time char_to_time(String format, String timeString)
    {
        if ((format == null) || (timeString == null)) {
            return null;
        }
        return new Time(charToDateHelper(format, timeString));
    }

    public static Timestamp char_to_timestamp(
        String format,
        String timestampString)
    {
        if ((format == null) || (timestampString == null)) {
            return null;
        }
        return new Timestamp(charToDateHelper(format, timestampString));
    }

    public static String date_to_char(String format, Date d)
    {
        return date_to_char(format, d, false);
    }

    public static String time_to_char(String format, Time t)
    {
        return time_to_char(format, t, false);
    }

    public static String timestamp_to_char(String format, Timestamp ts)
    {
        return timestamp_to_char(format, ts, false);
    }

    protected static String date_to_char(
        String format,
        Date d,
        boolean directCall)
    {
        DateFormat df;

        if ((format == null) || (d == null)) {
            return null;
        }
        if (directCall) {
            df = getDateFormat(format, Type.DIRECT_DATE);
        } else {
            df = getDateFormat(format, Type.UDR);
        }
        return df.format(d);
    }

    protected static String time_to_char(
        String format,
        Time t,
        boolean directCall)
    {
        DateFormat df;

        if ((format == null) || (t == null)) {
            return null;
        }
        if (directCall) {
            df = getDateFormat(format, Type.DIRECT_TIME);
        } else {
            df = getDateFormat(format, Type.UDR);
        }
        return df.format(t);
    }

    protected static String timestamp_to_char(
        String format,
        Timestamp ts,
        boolean directCall)
    {
        DateFormat df;

        if ((format == null) || (ts == null)) {
            return null;
        }
        if (directCall) {
            df = getDateFormat(format, Type.DIRECT_TIMESTAMP);
        } else {
            df = getDateFormat(format, Type.UDR);
        }
        return df.format(ts);
    }

    /**
     * Converts a string to a standard Java date, expressed in milliseconds
     */
    private static long charToDateHelper(String format, String s)
    {
        DateFormat df = getDateFormat(format, Type.UDR);
        long ret;
        try {
            ret = df.parse(s).getTime();
        } catch (ParseException ex) {
            throw FarragoResource.instance().InvalidDateString.ex(
                format,
                s);
        }
        return ret;
    }

    /**
     * Gets a date formatter, caching it in the Farrago runtime context
     */
    private static DateFormat getDateFormat(String format, Type caller)
    {
        if (caller != Type.UDR) {
            DatetimeFormatHelper dfh =
                (DatetimeFormatHelper) FarragoUdrRuntime.getContext();
            if (dfh == null) {
                dfh = new DatetimeFormatHelper();
                FarragoUdrRuntime.setContext(dfh);
            }

            if (!dfh.isSet(caller)) {
                dfh.setFormat(caller, format);
            }
            return dfh.getFormat(caller);
        } else {
            SimpleDateFormat sdf =
                (SimpleDateFormat) FarragoUdrRuntime.getContext();
            if (sdf == null) {
                sdf = DateTimeUtil.newDateFormat(format);
                FarragoUdrRuntime.setContext(sdf);
            }
            return sdf;
        }
    }

    //~ Inner Classes ----------------------------------------------------------

    private static class DatetimeFormatHelper
    {
        private SimpleDateFormat datefmt;
        private SimpleDateFormat timefmt;
        private SimpleDateFormat timestampfmt;

        protected DatetimeFormatHelper()
        {
            datefmt = timefmt = timestampfmt = null;
        }

        protected void setFormat(Type type, String format)
        {
            switch (type) {
            case DIRECT_DATE:
                datefmt = DateTimeUtil.newDateFormat(format);
                break;
            case DIRECT_TIME:
                timefmt = DateTimeUtil.newDateFormat(format);
                break;
            case DIRECT_TIMESTAMP:
                timestampfmt = DateTimeUtil.newDateFormat(format);
                break;
            default:
                throw FarragoResource.instance().InvalidConvertDatetimeCaller
                .ex(type.name());
            }
        }

        protected SimpleDateFormat getFormat(Type type)
        {
            switch (type) {
            case DIRECT_DATE:
                return datefmt;
            case DIRECT_TIME:
                return timefmt;
            case DIRECT_TIMESTAMP:
                return timestampfmt;
            default:
                throw FarragoResource.instance().InvalidConvertDatetimeCaller
                .ex(type.name());
            }
        }

        protected boolean isSet(Type type)
        {
            switch (type) {
            case DIRECT_DATE:
                return (datefmt != null);
            case DIRECT_TIME:
                return (timefmt != null);
            case DIRECT_TIMESTAMP:
                return (timestampfmt != null);
            default:
                throw FarragoResource.instance().InvalidConvertDatetimeCaller
                .ex(type.name());
            }
        }
    }
}

// End FarragoConvertDatetimeUDR.java
