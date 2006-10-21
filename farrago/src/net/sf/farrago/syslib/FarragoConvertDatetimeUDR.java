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
package net.sf.farrago.syslib;

import net.sf.farrago.runtime.*;
import net.sf.farrago.resource.*;

import java.sql.*;
import java.text.*;
import java.util.TimeZone;

/**
 * Moved over from luciddb applib datetime package for general use. 
 * Date conversion, based on standard Java libraries
 *
 * @author 
 * @version $Id$
 */
public abstract class FarragoConvertDatetimeUDR
{

    //~ Static fields/initializers -----------------------------------------

    protected static final int UDR = 0;
    protected static final int DIRECT_DATE = 1;
    protected static final int DIRECT_TIME = 2;
    protected static final int DIRECT_TIMESTAMP = 3;

    //~ Methods -----------------------------------------------------------

    public static Date char_to_date(String format, String dateString)
    {
        return new Date(charToDateHelper(format, dateString));
    }

    public static Time char_to_time(String format, String timeString)
    {
        return new Time(charToDateHelper(format, timeString));
    }

    public static Timestamp char_to_timestamp(
        String format, String timestampString)
    {
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
        String format, Date d, boolean directCall)
    {
        DateFormat df;

        if (directCall) {
            df = getDateFormat(format, DIRECT_DATE);
        } else {
            df = getDateFormat(format, UDR);
        }
        return df.format(d);
    }

    protected static String time_to_char(
        String format, Time t, boolean directCall)
    {
        DateFormat df;

        if (directCall) {
            df = getDateFormat(format, DIRECT_TIME);
        } else {
            df = getDateFormat(format, UDR);
        }
        return df.format(t);
    }

    protected static String timestamp_to_char(
        String format, Timestamp ts, boolean directCall)
    {
        DateFormat df;

        if (directCall) {
            df = getDateFormat(format, DIRECT_TIMESTAMP);
        } else {
            df = getDateFormat(format, UDR);
        }
        return df.format(ts);
    }

    /**
     * Converts a string to a standard Java date, expressed in milliseconds
     */
    private static long charToDateHelper(String format, String s)
    {
        DateFormat df = getDateFormat(format, UDR);
        long ret;
        try {
            ret = df.parse(s).getTime();
        } catch (ParseException ex) {
            throw FarragoResource.instance().InvalidDateString.ex(
                format, s);
        }
        return ret;
    }

    /**
     * Gets a date formatter, caching it in the Farrago runtime context
     */
    private static DateFormat getDateFormat(String format, int caller)
    {
        if (caller != UDR) {
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
                sdf = new SimpleDateFormat(format);
                FarragoUdrRuntime.setContext(sdf);
            }
            return sdf;
        }
    }


    //~ Inner classes  ---------------------------------------------------
    private static class DatetimeFormatHelper
    {
        private SimpleDateFormat datefmt;
        private SimpleDateFormat timefmt;
        private SimpleDateFormat timestampfmt;

        protected DatetimeFormatHelper()
        {
            datefmt = timefmt = timestampfmt = null;
        }
    
        protected void setFormat(int type, String format) {
            switch (type) {
            case FarragoConvertDatetimeUDR.DIRECT_DATE:
                datefmt = new SimpleDateFormat(format);
                break;
            case FarragoConvertDatetimeUDR.DIRECT_TIME:
                timefmt = new SimpleDateFormat(format);
                break;
            case FarragoConvertDatetimeUDR.DIRECT_TIMESTAMP:
                timestampfmt = new SimpleDateFormat(format);
                break;
            default:
                throw FarragoResource.instance(
                    ).InvalidConvertDatetimeCaller.ex(String.valueOf(type));
            }
        }
    
        protected SimpleDateFormat getFormat(int type) {
            switch (type) {
            case FarragoConvertDatetimeUDR.DIRECT_DATE:
                return datefmt;
            case FarragoConvertDatetimeUDR.DIRECT_TIME:
                return timefmt;
            case FarragoConvertDatetimeUDR.DIRECT_TIMESTAMP:
                return timestampfmt;
            default:
                throw FarragoResource.instance(
                    ).InvalidConvertDatetimeCaller.ex(String.valueOf(type));
            }
        }
    
        protected boolean isSet(int type) {
            switch (type) {
            case FarragoConvertDatetimeUDR.DIRECT_DATE:
                return (datefmt != null);
            case FarragoConvertDatetimeUDR.DIRECT_TIME:
                return (timefmt != null);
            case FarragoConvertDatetimeUDR.DIRECT_TIMESTAMP:
                return (timestampfmt != null);
            default:
                throw FarragoResource.instance(
                    ).InvalidConvertDatetimeCaller.ex(String.valueOf(type));
            }
        }
        
    }

}

// End FarragoConvertDatetimeUDR.java
