/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2005 LucidEra, Inc.
// Copyright (C) 2005-2005 The Eigenbase Project
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
package com.lucidera.luciddb.applib.datetime;

import com.lucidera.luciddb.applib.resource.*;

import net.sf.farrago.runtime.*;
import net.sf.farrago.syslib.*;

import java.sql.*;
import java.text.*;
import java.util.TimeZone;

/**
 * Date conversion, based on standard Java libraries
 *
 * @author John Pham
 * @version $Id$
 */
public class StdConvertDateUdf
{
    public static Date char_to_date(String format, String dateString)
    {
        return FarragoConvertDatetimeUDR.char_to_date(format, dateString);
    }

    public static Time char_to_time(String format, String timeString)
    {
        return FarragoConvertDatetimeUDR.char_to_time(format, timeString);
    }

    public static Timestamp char_to_timestamp(
        String format, String timestampString)
    {
        return FarragoConvertDatetimeUDR.char_to_timestamp(
            format, timestampString);
    }

    public static String date_to_char(String format, Date d)
    {
        return FarragoConvertDatetimeUDR.date_to_char(format, d);
    }

    public static String time_to_char(String format, Time t)
    {
        return FarragoConvertDatetimeUDR.time_to_char(format, t);
    }

    public static String timestamp_to_char(String format, Timestamp ts)
    {
        return FarragoConvertDatetimeUDR.timestamp_to_char(format, ts);
    }
}

// End StdConvertDateUdf.java
