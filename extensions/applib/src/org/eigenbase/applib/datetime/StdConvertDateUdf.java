/*
// Licensed to DynamoBI Corporation (DynamoBI) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  DynamoBI licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at

//   http://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
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
