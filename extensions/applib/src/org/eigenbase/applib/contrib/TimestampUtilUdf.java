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

