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

import java.util.Calendar;
import java.util.GregorianCalendar;
import java.sql.SQLException;
import java.sql.Types;
import java.sql.Date;

/**
 * Internal helper class for Time Dimension UDX
 * Ported from //bb/bb713/server/Java/Broadbase/TimeDimensionInternal.java
 *
 * @author Elizabeth Lin
 * @version $Id$
 */
public class TimeDimensionInternal extends GregorianCalendar
{
    private int startYear;
    private int	startMonth;
    private int	startDate;
    private int	numDays;
    private Date firstOfWeekDate;
    private Date currentDate;

    public TimeDimensionInternal()
    {
        return;
    }

    public TimeDimensionInternal( int startYear, int startMonth, int startDate,
        int endYear, int endMonth, int endDate ) throws SQLException
    {
        // construct superclass
        super( startYear, startMonth-1, startDate );

        ApplibResource res = ApplibResourceObject.get();

        if ((startMonth < 0) || (startDate < 0)) {
            throw new IllegalArgumentException(
                res.TimeDimInvalidStartDate.ex());
        }

        if ((endMonth < 0) || (endDate < 0)) {
            throw new IllegalArgumentException(res.TimeDimInvalidEndDate.ex());
        }

        long start = getTimeInMillis();
        set( endYear, endMonth-1, endDate );
        long end = getTimeInMillis();

        if( start > end ) {
            throw new IllegalArgumentException(
                res.TimeDimStartDayMustPrecedeEndDay.ex());
        }

        this.startYear = startYear;
        this.startMonth = startMonth-1;
        this.startDate = startDate;
        this.numDays = (int) ( ( end - start ) / 1000 / 60 / 60 / 24 );
    }

    public static String getDayOfWeek( int day )
    {
        switch ( day )
        {
        case 0: return "Sunday";
        case 1: return "Monday";
        case 2: return "Tuesday";
        case 3: return "Wednesday";
        case 4: return "Thursday";
        case 5: return "Friday";
        case 6: return "Saturday"; 
        default: throw new IllegalArgumentException(
            ApplibResourceObject.get().TimeDimInvalidDayOfWeek.ex()); 
        }
    }

    public static String getMonthOfYear( int month )
    {
        switch ( month )
        {
        case 0: return "January";
        case 1: return "February";
        case 2: return "March";
        case 3: return "April";
        case 4: return "May";
        case 5: return "June";
        case 6: return "July";
        case 7: return "August";
        case 8: return "September";
        case 9: return "October";
        case 10: return "November";
        case 11: return "December";
        default: throw new IllegalArgumentException(
            ApplibResourceObject.get().TimeDimInvalidMonthOfYear.ex());
        }
    }

    public void Start() throws SQLException
    {
        set( this.startYear, this.startMonth, this.startDate );
        complete();

        int daysPastFirst = get( Calendar.DAY_OF_WEEK ) - getFirstDayOfWeek();
        if( daysPastFirst < 0 ) {
            daysPastFirst += 7;
        }
        long firstOfWeek = getTimeInMillis() 
            - daysPastFirst * 1000 * 60 * 60 * 24;
        this.firstOfWeekDate = new Date( firstOfWeek );
        this.currentDate = new Date(getTimeInMillis());
    }

    public Date getFirstDayOfWeekDate() {
        return this.firstOfWeekDate;
    }

    public int getAndUpdateFirstDayOfWeek() {
        int dayOfWeek = get(Calendar.DAY_OF_WEEK);

        if (getFirstDayOfWeek() == dayOfWeek) {
            this.firstOfWeekDate.setTime(getTimeInMillis());
        }

        return dayOfWeek;
    }

    public int getNumDays() {
        return this.numDays;
    }

    public int getYear() {
        return get(Calendar.YEAR);
    }

    public int getJulianDay() {
        int ret = (int) (getTimeInMillis() / 1000 / 60 / 60 / 24);
        if (getYear() < 1970) {
            ret--;
        }
        return ret;
    }

    public int getMonth() {
        return get(Calendar.MONTH) + 1;
    }

    public int getDayOfMonth() {
        return get(Calendar.DAY_OF_MONTH);
    }

    public int getDayOfYear() {
        return get(Calendar.DAY_OF_YEAR);
    }

    public int getWeek() {
        return get(Calendar.WEEK_OF_YEAR);
    }

    public Date getDate() {
        this.currentDate.setTime(getTimeInMillis());
        return currentDate;
    }

    public void increment() {
        add(Calendar.DATE, 1);
        complete();
    }
}

// End TimeDimensionInternal.java
