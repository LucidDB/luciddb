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
package com.lucidera.luciddb.applib.datetime;

import java.util.Calendar;
import java.util.GregorianCalendar;
import java.sql.SQLException;
import java.sql.Types;
import java.sql.Date;
import java.lang.Math;
import com.lucidera.luciddb.applib.resource.*;

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
    private int fiscalYearStartMonth;
    private int quarterStartWeek; // week in year where current quarter starts
    private int fiscalQuarterStartWeek; // ditto, fiscal year
    private int fiscalYearStartWeek;

    private Date currentDate;
    private Date firstOfWeekDate;
    private Date lastOfWeekDate;
    private Date firstOfMonthDate;
    private Date lastOfMonthDate;
    private Date firstOfQuarterDate;
    private Date lastOfQuarterDate;
    private Date firstOfYearDate;
    private Date lastOfYearDate;

    private Date firstOfFiscalQuarterDate;
    private Date lastOfFiscalQuarterDate;
    private Date firstOfFiscalYearDate;
    private Date lastOfFiscalYearDate;

    final int millisInADay = 1000*60*60*24;


    public TimeDimensionInternal()
    {
        return;
    }

    public TimeDimensionInternal( int startYear, int startMonth, int startDate,
        int endYear, int endMonth, int endDate, int fiscalYearStartMonth ) throws ApplibException
    {
        // construct superclass
        super( startYear, startMonth-1, startDate );

        ApplibResource res = ApplibResourceObject.get();

        if ((startMonth < 0) || (startDate < 0)) {
            throw res.TimeDimInvalidStartDate.ex();
        }

        if ((endMonth < 0) || (endDate < 0)) {
            throw res.TimeDimInvalidEndDate.ex();
        }

        long start = getTimeInMillis();
        set( endYear, endMonth-1, endDate );
        long end = getTimeInMillis();

        if( start > end ) {
            throw res.TimeDimStartDayMustPrecedeEndDay.ex();
        }

        this.startYear = startYear;
        this.startMonth = startMonth-1;
        this.fiscalYearStartMonth = fiscalYearStartMonth - 1;
        this.startDate = startDate;
        this.numDays = (int) Math.round((double)( end - start ) / millisInADay );
    }

    public static String getDayOfWeek( int day ) throws ApplibException
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
        default: throw ApplibResourceObject.get().TimeDimInvalidDayOfWeek.ex();
        }
    }

    public static String getMonthOfYear( int month ) throws ApplibException
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
        default: 
            throw ApplibResourceObject.get().TimeDimInvalidMonthOfYear.ex();
        }
    }

    public void Start()
    {

        // set first and last day of month
        set(this.startYear, this.startMonth, getActualMinimum(Calendar.DAY_OF_MONTH));
        complete();
        this.firstOfMonthDate = new Date(getTimeInMillis());
        set(this.startYear, this.startMonth, getActualMaximum(Calendar.DAY_OF_MONTH));
        this.lastOfMonthDate = new Date(getTimeInMillis());

        // set first and last day of quarter
        add(Calendar.MONTH, 2 - this.startMonth % 3);
        this.lastOfQuarterDate = new Date(getTimeInMillis());
        add(Calendar.MONTH, -3);
        add(Calendar.DATE, 1);
        this.firstOfQuarterDate = new Date(getTimeInMillis());
        this.quarterStartWeek = get(Calendar.WEEK_OF_YEAR);

        // set first and last day of year
        set(Calendar.MONTH, getActualMinimum(Calendar.MONTH));
        this.firstOfYearDate = new Date(getTimeInMillis());
        add(Calendar.YEAR, 1);        
        add(Calendar.DATE, -1);
        this.lastOfYearDate = new Date(getTimeInMillis());

        // set first and last day of fiscal quarter
        set( this.startYear, this.startMonth, getActualMinimum(Calendar.DAY_OF_MONTH) );
        int fMth = (this.startMonth - this.fiscalYearStartMonth + 12) % 12;
        add(Calendar.MONTH, - fMth % 3);
        this.firstOfFiscalQuarterDate = new Date(getTimeInMillis());
        this.fiscalQuarterStartWeek = get(Calendar.WEEK_OF_YEAR);
        add(Calendar.MONTH, 3);
        add(Calendar.DATE, -1);
        this.lastOfFiscalQuarterDate = new Date(getTimeInMillis());

        // set first and last day of fiscal year
        set( this.startYear, this.startMonth, getActualMinimum(Calendar.DAY_OF_MONTH) );
        set(Calendar.MONTH, this.fiscalYearStartMonth);
        if (this.startMonth < this.fiscalYearStartMonth) {
            add(Calendar.YEAR, -1);
        }
        this.firstOfFiscalYearDate = new Date(getTimeInMillis());
        this.fiscalYearStartWeek = get(Calendar.WEEK_OF_YEAR);
        add(Calendar.YEAR, 1);
        this.lastOfFiscalYearDate = new Date(getTimeInMillis() - millisInADay);

        set( this.startYear, this.startMonth, this.startDate );

        int daysPastFirst = get( Calendar.DAY_OF_WEEK ) - getFirstDayOfWeek();
        if( daysPastFirst < 0 ) {
            daysPastFirst += 7;
        }
        long firstOfWeek = getTimeInMillis() - daysPastFirst * millisInADay;
        this.firstOfWeekDate = new Date( firstOfWeek );
        this.lastOfWeekDate = new Date(
            firstOfWeek + 6*millisInADay);

        this.currentDate = new Date(getTimeInMillis());
    }

    public Date getFirstDayOfWeekDate() {
        return this.firstOfWeekDate;
    }

    public Date getLastDayOfWeekDate() {
        return this.lastOfWeekDate;
    }

    public Date getFirstDayOfMonthDate() {
        return this.firstOfMonthDate;
    }

    public Date getLastDayOfMonthDate() {
        return this.lastOfMonthDate;
    }

    public Date getFirstDayOfQuarterDate() {
        return this.firstOfQuarterDate;
    }

    public Date getLastDayOfQuarterDate() {
        return this.lastOfQuarterDate;
    }

    public Date getFirstDayOfYearDate() {
        return this.firstOfYearDate;
    }

    public Date getLastDayOfYearDate() {
        return this.lastOfYearDate;
    }

    public int getDayOfWeek() {
        return get (Calendar.DAY_OF_WEEK);
    }

    public int getNumDays() {
        return this.numDays;
    }

    public int getYear() {
        return get(Calendar.YEAR);
    }

    public int getJulianDay() {
        int ret = (int) (getTimeInMillis() / millisInADay);
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

    public int getWeekOfQuarter() {
        return get(Calendar.WEEK_OF_YEAR) - this.quarterStartWeek + 1;
    }

    public int getWeekOfMonth() {
        return get(Calendar.WEEK_OF_MONTH);
    }

    public int getWeek() {
        return get(Calendar.WEEK_OF_YEAR);
    }

    public Date getDate() {
        this.currentDate.setTime(getTimeInMillis());
        return currentDate;
    }

    //~---------- fiscal dates ----------
    // this helper function assumes startDate < currentDate
    // and returns week number of current date with respect to startDate
    private int WeekFrom(Date startDate) {
        long startTime = startDate.getTime();
        // this rounding is for day light saving time in the US
        long days = Math.round((double)(getTimeInMillis() - startTime) / millisInADay);
        return (int)(1 +  days / 7 + ((get(Calendar.DAY_OF_WEEK) <= (days % 7)) ? 1 : 0));
    }

    public int getWeekOfFiscalMonth() {
        return WeekFrom(this.firstOfMonthDate);
    }

    public int getFiscalMonth() {
        return (get(Calendar.MONTH) - this.fiscalYearStartMonth + 12) % 12 + 1;
    }

    public int getWeekOfFiscalQuarter() {
        return WeekFrom(this.firstOfFiscalQuarterDate);
    }

    public int getFiscalQuarter() {
        return (this.getFiscalMonth() - 1) / 3 + 1;
    }

    public int getWeekOfFiscalYear() {
        return WeekFrom(this.firstOfFiscalYearDate);
    }

    public Date getFirstDayOfFiscalQuarterDate() {
        return this.firstOfFiscalQuarterDate;
    }

    public Date getLastDayOfFiscalQuarterDate() {
        return this.lastOfFiscalQuarterDate;
    }

    public Date getFirstDayOfFiscalYearDate() {
        return this.firstOfFiscalYearDate;
    }

    public Date getLastDayOfFiscalYearDate() {
        return this.lastOfFiscalYearDate;
    }

    public void increment() {
        add(Calendar.DATE, 1);
        complete();

        long currentTime = getTimeInMillis();

        // update first/last day of week/month/quarter/year
        if (getFirstDayOfWeek() == get(Calendar.DAY_OF_WEEK)) {
            this.firstOfWeekDate.setTime(currentTime);
            this.lastOfWeekDate.setTime(currentTime + 6*millisInADay);
        }
        if (get(Calendar.DAY_OF_MONTH) == 1) {
            int month = get(Calendar.MONTH);
            if ((month % 3) == 0) {
                if (month == 0) {
                    this.firstOfYearDate.setTime(currentTime);
                    add(Calendar.YEAR, 1);
                    this.lastOfYearDate.setTime(getTimeInMillis() - millisInADay);
                    add(Calendar.YEAR, -1);
                }
                this.firstOfQuarterDate.setTime(currentTime);
                this.quarterStartWeek = get(Calendar.WEEK_OF_YEAR);
                add(Calendar.MONTH, 3);
                this.lastOfQuarterDate.setTime(getTimeInMillis() - millisInADay);
                add(Calendar.MONTH, -3);
            }

            int fMonth = (month - this.fiscalYearStartMonth + 12) % 12;
            if ((fMonth % 3) == 0) {
                if (fMonth == 0) {
                    this.firstOfFiscalYearDate.setTime(currentTime);
                    this.fiscalYearStartWeek = get(Calendar.WEEK_OF_YEAR);
                    add(Calendar.YEAR, 1);
                    this.lastOfFiscalYearDate.setTime(getTimeInMillis() - millisInADay);
                    add(Calendar.YEAR, -1);
                }
                this.firstOfFiscalQuarterDate.setTime(currentTime);
                this.fiscalQuarterStartWeek = get(Calendar.WEEK_OF_YEAR);
                add(Calendar.MONTH, 3);
                this.lastOfFiscalQuarterDate.setTime(getTimeInMillis() - millisInADay);
                add(Calendar.MONTH, -3);
            }

            this.firstOfMonthDate.setTime(getTimeInMillis());
            add(Calendar.MONTH, 1);
            this.lastOfMonthDate.setTime(getTimeInMillis() - millisInADay);
            add(Calendar.MONTH, -1);
        }
    }
}

// End TimeDimensionInternal.java
