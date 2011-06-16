/*
// $Id$
// Applib is a library of SQL-invocable routines for Eigenbase applications.
// Copyright (C) 2011 The Eigenbase Project
// Copyright (C) 2011 SQLstream, Inc.
// Copyright (C) 2011 DynamoBI Corporation
// Portions Copyright (C) 2011 Jeremy Lemaire
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
package org.eigenbase.applib.contrib;

import org.eigenbase.applib.resource.*;

import java.sql.Timestamp;
import java.util.Calendar;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author  Jeremy Lemaire
 * @date    Apr 15, 2011
 * @version $Id$
 */
public class TimestampUtilUdf
{
    private static final String HOURLY = "HOURLY";
    private static final String DAILY = "DAILY";
    private static final String WEEKLY = "WEEKLY";
    private static final String MONTHLY = "MONTHLY";
    private static final String YEARLY = "YEARLY";

    private static final String SECOND = "SECOND";
    private static final String MINUTE = "MINUTE";
    private static final String HOUR = "HOUR";
    private static final String DAY = "DAY";
    private static final String DOW = "DOW";
    private static final String WEEK = "WEEK";
    private static final String MONTH = "MONTH";
    private static final String YEAR = "YEAR";
    private static final String HOW = "HOW";  // hour of week (for day-parting)

    /**
     * Truncate the specified field of a java.sql.Timestamp
     *
     * @param Timestamp timestamp
     * @param String period
     * @return Timestamp
     */
    public static Timestamp truncateTimestamp(
            Timestamp timestamp,
            String period )
    {
        Timestamp ts = null;

        if (period!=null) {
            Calendar cal = Calendar.getInstance();
            cal.setTimeInMillis( timestamp.getTime() );

            if ( period.equalsIgnoreCase( HOURLY ) ) {
                cal.set( Calendar.MILLISECOND, 0 );
                cal.set( Calendar.SECOND, 0 );
                cal.set( Calendar.MINUTE, 0 );
            } else if ( period.equalsIgnoreCase( DAILY ) ) {
                cal.set( Calendar.MILLISECOND, 0 );
                cal.set( Calendar.SECOND, 0 );
                cal.set( Calendar.MINUTE, 0 );
                cal.set( Calendar.HOUR_OF_DAY, 0 );
            } else if ( period.equalsIgnoreCase( WEEKLY ) ) {
                cal.set( Calendar.MILLISECOND, 0 );
                cal.set( Calendar.SECOND, 0 );
                cal.set( Calendar.MINUTE, 0 );
                cal.set( Calendar.HOUR_OF_DAY, 0 );
                cal.set( Calendar.DAY_OF_WEEK, Calendar.MONDAY );
            } else if ( period.equalsIgnoreCase( MONTHLY ) ) {
                cal.set( Calendar.MILLISECOND, 0 );
                cal.set( Calendar.SECOND, 0 );
                cal.set( Calendar.MINUTE, 0 );
                cal.set( Calendar.HOUR_OF_DAY, 0 );
                cal.set( Calendar.DAY_OF_MONTH, 1 );
            } else if ( period.equalsIgnoreCase( YEARLY ) ) {
                cal.set( Calendar.MILLISECOND, 0 );
                cal.set( Calendar.SECOND, 0 );
                cal.set( Calendar.MINUTE, 0 );
                cal.set( Calendar.HOUR_OF_DAY, 0 );
                cal.set( Calendar.DAY_OF_MONTH, 1 );
                cal.set( Calendar.MONTH, Calendar.JANUARY );
            }

            ts = new Timestamp( cal.getTimeInMillis() );
        }

        return (ts!=null) ? ts : timestamp ;
    }

    /**
     * Extract the specified field from a java.sql.Timestamp
     *
     * @param Timestamp timestamp
     * @param String field
     * @return int
     * @throws ApplibException
     */
    public static int extractTimestamp( Timestamp timestamp, String field )
        throws ApplibException
    {
        if ( field != null ) {
            Calendar cal = Calendar.getInstance();
            cal.setTimeInMillis( timestamp.getTime() );

            if ( field.equalsIgnoreCase( SECOND ) ) {
                return cal.get( Calendar.SECOND );
            } else if ( field.equalsIgnoreCase( MINUTE ) ) {
                return cal.get( Calendar.MINUTE );
            } else if ( field.equalsIgnoreCase( HOUR ) ) {
                return cal.get( Calendar.HOUR_OF_DAY );
            } else if ( field.equalsIgnoreCase( DAY ) ) {
                return cal.get( Calendar.DAY_OF_MONTH );
            } else if ( field.equalsIgnoreCase( DOW ) ) {
                return cal.get( Calendar.DAY_OF_WEEK );
            } else if ( field.equalsIgnoreCase( WEEK ) ) {
                return cal.get( Calendar.WEEK_OF_YEAR );
            } else if ( field.equalsIgnoreCase( MONTH ) ) {
                return cal.get( Calendar.MONTH );
            } else if ( field.equalsIgnoreCase( YEAR ) ) {
                return cal.get( Calendar.YEAR );
            } else if ( field.equalsIgnoreCase( HOW ) ) {
                return cal.get(Calendar.HOUR_OF_DAY) + (24 * (cal.get(
                                Calendar.DAY_OF_WEEK) - 1));
            }
        }

        throw ApplibResource.instance().InputIsRequired.ex("field");
    }

    /**
     * Adjust java.sql.Timestamp for the specified
     * timezone change or gmt offset (using one overrides the other).
     *
     * @param timestamp - base timestamp
     * @param timezone - plus/minus hours:minutes to adjust
     * @param gmtOffset - plus/minus hours to adjust
     * @return Timestamp
     */
    public static Timestamp adjustTimestamp(
            Timestamp timestamp,
            String timezone,
            int gmtOffset )
    {
        Timestamp ts = null;

        if ( timezone != null ) {
            Calendar cal = Calendar.getInstance();
            cal.setTimeInMillis( timestamp.getTime() );

            Pattern p = Pattern.compile(
                    "([\\+\\-]?)([\\d]{1,2})[:]?([\\d]{1,2})?");
            Matcher m = p.matcher(timezone);

            if (m.find()) {
                String hh = m.group(2);

                String sign = m.group(1);
                int hours = Integer.valueOf(hh);
                int minutes = 0;

                if (m.group(3) != null) {
                    String mm = m.group(3);
                    minutes = Integer.valueOf(mm);
                }

                gmtOffset = (hours * 60 + minutes);

                if (sign.compareTo("-") == 0) {
                    gmtOffset *= (-1);
                }
            } else {
                // The gmtOffset was passed in
                // To support half-hour offsets, convert to minutes.

                gmtOffset *= 60;
            }

            cal.add(Calendar.MINUTE, gmtOffset);

            ts = new Timestamp( cal.getTimeInMillis() );
        }

        return (ts != null) ? ts : timestamp;
    }
}

