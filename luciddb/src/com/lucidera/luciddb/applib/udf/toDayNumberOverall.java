/*
// $Id$
// LucidDB is a DBMS optimized for business intelligence.
// Copyright (C) 2006-2006 LucidEra, Inc.
// Copyright (C) 2006-2006 The Eigenbase Project
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
package com.lucidera.luciddb.applib;

import java.sql.*;
import java.text.*;
import java.util.*;

/**
 * Convert a date to an absolute day number.  Useful for having integer date
 * key that will work with the TimeDimension Java table.
 *
 * Ported from //bb/bb713/server/SQL/toDayNumberOverall.java
 */
public class toDayNumberOverall
{
    // Convert date, of local time zone, to equivalent date in GMT time
    private static DateFormat localFormatter;
    private static DateFormat utcFormatter;
    private static java.util.Date convertToGmt( java.util.Date localDate ) 
        throws SQLException
    {
        ApplibResource res = ApplibResourceObject.get();

        if (localFormatter == null) {
            localFormatter = new SimpleDateFormat(res.LocalDateFormat.str());
            utcFormatter = new SimpleDateFormat(res.UtcDateFormat.str());
            utcFormatter.setTimeZone( TimeZone.getTimeZone("UTC") );
        }

        String dateString = localFormatter.format(localDate);
        java.util.Date gmtDate;
        try {
            gmtDate = utcFormatter.parse(dateString);
        } catch (ParseException e) {
            throw new SQLException(res.DayNumOverallParseError.str());
        }
        return gmtDate;
    }

    /**
     * @param dtIn Date to convert
     * @return Absolute day number
     * @exception SQLException
     */
    public static int FunctionExecute( java.sql.Date dtIn ) throws SQLException
    {
        java.util.Date dt = convertToGmt((java.util.Date)dtIn);
        return( (int) ( dt.getTime() / 1000 / 60 / 60 / 24 ) );
    }

    /**
     * @param tsIn Timestamp to convert
     * @return Absolute day number
     * @exception SQLException
     */
    public static int FunctionExecute( Timestamp tsIn ) throws SQLException
    {
        java.util.Date ts = convertToGmt(tsIn);
        return( (int) ( ts.getTime() / 1000 / 60 / 60 / 24 ) );
    }

}

// End toDayNumberOverall.java
