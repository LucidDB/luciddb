/*
// $Id$
// Applib is a library of SQL-invocable routines for Eigenbase applications.
// Copyright (C) 2005 The Eigenbase Project
// Copyright (C) 2005 SQLstream, Inc.
// Copyright (C) 2005 DynamoBI Corporation
//
// This library is free software; you can redistribute it and/or modify it
// under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation; either version 2.1 of the License, or (at
// your option) any later version.
//
// This library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public
// License along with this library; if not, write to the Free Software
// Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA
*/
package org.eigenbase.applib.datetime;

import java.sql.*;

import java.text.*;

import net.sf.farrago.runtime.*;
import net.sf.farrago.syslib.*;

import org.eigenbase.applib.resource.*;


/**
 * Date conversion, based on standard Java libraries
 *
 * @author John Pham
 * @version $Id$
 */
public class StdConvertDateUdf
{
    //~ Methods ----------------------------------------------------------------

    public static Date char_to_date(String format, String dateString)
    {
        return FarragoConvertDatetimeUDR.char_to_date(format, dateString);
    }

    public static Time char_to_time(String format, String timeString)
    {
        return FarragoConvertDatetimeUDR.char_to_time(format, timeString);
    }

    public static Timestamp char_to_timestamp(
        String format,
        String timestampString)
    {
        return FarragoConvertDatetimeUDR.char_to_timestamp(
            format,
            timestampString);
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
