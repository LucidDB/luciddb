/*
// $Id$
// LucidDB is a DBMS optimized for business intelligence.
// Copyright (C) 2006-2007 LucidEra, Inc.
// Copyright (C) 2006-2007 The Eigenbase Project
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

    // day number in year when the current quarter starts
    private int quarterStartDay; 
    // day number in year when the current fiscal quarter starts
    private int fiscalQuarterStartDay;
    // day number in year when the current fiscal year starts
    private int fiscalYearStartDay;

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
    private Date firstOfFiscalWeekDate;
    private Date lastOfFiscalWeekDate;

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

        if ((startMonth < 0) || (startDate < 0) || (startMonth > 12)
            || (startDate > 31)) 
        {
            throw res.TimeDimInvalidStartDate.ex();
        }
        if ((endMonth < 0) || (endDate < 0) || (endMonth > 12) 
            || (endDate > 31)) 
        {
            throw res.TimeDimInvalidEndDate.ex();
        }
        if ((fiscalYearStartMonth < 0) || (fiscalYearStartMonth > 12)) {
            throw res.TimeDimInvalidFiscalStartMonth.ex();
        }


        long start = getTimeInMillis();
        complete();
        this.startMonth = get(Calendar.MONTH);
        this.startYear = get(Calendar.YEAR);
        this.startDate = get(Calendar.DATE);

        set( endYear, endMonth-1, endDate );
        complete();
        long end = getTimeInMillis();

        if( start > end ) {
            throw res.TimeDimStartDayMustPrecedeEndDay.ex();
        }

        this.fiscalYearStartMonth = fiscalYearStartMonth - 1;
        this.numDays = (int) Math.round((double)( end - start ) / millisInADay );
        // set back to known state 
        set( this.startYear, this.startMonth, this.startDate);
        complete();
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
        // set last date of month
        set(this.startYear, this.startMonth, 
            getActualMaximum(Calendar.DAY_OF_MONTH));
        complete();
        this.lastOfMonthDate = new Date(getTimeInMillis());

        // set first date of month
        set(this.startYear, this.startMonth, 
            getActualMinimum(Calendar.DAY_OF_MONTH));
        complete();
        this.firstOfMonthDate = new Date(getTimeInMillis());

        // set last date of quarter
        add(Calendar.MONTH, 2 - (this.startMonth % 3));
        set(Calendar.DAY_OF_MONTH, getActualMaximum(Calendar.DAY_OF_MONTH));
        complete();
        this.lastOfQuarterDate = new Date(getTimeInMillis());

        // set first date, week, day of quarter
        // set to minimum day first, so we won't get something unexpected from
        // rolling back 2 months
        set(Calendar.DAY_OF_MONTH, getActualMinimum(Calendar.DAY_OF_MONTH));
        complete();
        add(Calendar.MONTH, -2);
        set(Calendar.DAY_OF_MONTH, getActualMinimum(Calendar.DAY_OF_MONTH));
        complete();
        this.firstOfQuarterDate = new Date(getTimeInMillis());
        this.quarterStartDay = get(Calendar.DAY_OF_YEAR);

        // set first date of year
        set(Calendar.MONTH, getActualMinimum(Calendar.MONTH));
        set(Calendar.DAY_OF_MONTH, getActualMinimum(Calendar.DAY_OF_MONTH));
        complete();
        this.firstOfYearDate = new Date(getTimeInMillis());

        // set last date of year
        set(Calendar.DAY_OF_YEAR, getActualMaximum(Calendar.DAY_OF_YEAR));
//         add(Calendar.YEAR, 1);        
//         add(Calendar.DATE, -1);
        complete();
        this.lastOfYearDate = new Date(getTimeInMillis());

        // set first date, week, day of fiscal quarter
        set( this.startYear, this.startMonth, this.startDate);
        complete();
        // set to minimum day first
        set(Calendar.DAY_OF_MONTH, getActualMinimum(Calendar.DAY_OF_MONTH));
        complete();
        int fMth = (this.startMonth - this.fiscalYearStartMonth + 12) % 12;
        add(Calendar.MONTH, - (fMth % 3));
        set(Calendar.DAY_OF_MONTH, getActualMinimum(Calendar.DAY_OF_MONTH));
        complete();
        this.firstOfFiscalQuarterDate = new Date(getTimeInMillis());
        this.fiscalQuarterStartDay = get(Calendar.DAY_OF_YEAR);

        // set last date of fiscal quarter
        add(Calendar.MONTH, 2);
        set(Calendar.DAY_OF_MONTH, getActualMaximum(Calendar.DAY_OF_MONTH));
        complete();
//        add(Calendar.DATE, -1);
        this.lastOfFiscalQuarterDate = new Date(getTimeInMillis());

        // set first date, week, day of fiscal year
        set(this.startYear, this.startMonth, this.startDate);
        complete();
        set(Calendar.MONTH, this.fiscalYearStartMonth);
        complete();
        set(Calendar.DAY_OF_MONTH, getActualMinimum(Calendar.DAY_OF_MONTH));
        complete();
        if (this.startMonth < this.fiscalYearStartMonth) {
            add(Calendar.YEAR, -1);
        }
        this.firstOfFiscalYearDate = new Date(getTimeInMillis());
        this.fiscalYearStartDay = get(Calendar.DAY_OF_YEAR);

        // set last date of fiscal year
        add(Calendar.MONTH, 11);
        set(Calendar.DAY_OF_MONTH, getActualMaximum(Calendar.DAY_OF_MONTH));
        complete();
//         add(Calendar.YEAR, 1);
//         add(Calendar.DATE, -1);
        this.lastOfFiscalYearDate = new Date(getTimeInMillis());

        // set calendar back to start date
        set( this.startYear, this.startMonth, this.startDate );
        complete();

        // set first/last dates for week 
        int daysPastFirst = get( Calendar.DAY_OF_WEEK ) - getFirstDayOfWeek();
        if( daysPastFirst < 0 ) {
            daysPastFirst += 7;
        }
        long firstOfWeek = getTimeInMillis() - daysPastFirst * millisInADay;
        this.firstOfWeekDate = new Date( firstOfWeek );
        if (this.firstOfWeekDate.before(this.firstOfYearDate)) {
            this.firstOfWeekDate.setTime(this.firstOfYearDate.getTime());
        }
        this.lastOfWeekDate = new Date(firstOfWeek + 6*millisInADay);
        if (this.lastOfWeekDate.after(this.lastOfYearDate)) {
            this.lastOfWeekDate.setTime(this.lastOfYearDate.getTime());
        }

        // set first/last fiscal dates of week
        this.firstOfFiscalWeekDate = new Date( firstOfWeek );
        if (this.firstOfFiscalWeekDate.before(this.firstOfFiscalYearDate)) {
            this.firstOfFiscalWeekDate.setTime(
                this.firstOfFiscalYearDate.getTime());
        }
        this.lastOfFiscalWeekDate = new Date( firstOfWeek + 6*millisInADay);
        if (this.lastOfFiscalWeekDate.after(this.lastOfFiscalYearDate)) {
            this.lastOfFiscalWeekDate.setTime(
                this.firstOfFiscalYearDate.getTime());
        }

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

    public int getDayOfQuarter() {
        return get(Calendar.DAY_OF_YEAR) - this.quarterStartDay + 1;
    }

    public int getDayOfYear() {
        return get(Calendar.DAY_OF_YEAR);
    }

    public int getWeekOfQuarter() {
        return WeekFrom(this.firstOfQuarterDate);
    }

    public int getWeekOfMonth() {
        return get(Calendar.WEEK_OF_MONTH);
    }

    public int getWeek() {
        return WeekFrom(this.firstOfYearDate);
    }

    public Date getDate() {
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

    public int getDayOfFiscalQuarter() {
        int doy = get(Calendar.DAY_OF_YEAR);
        int dofq;
        if (doy >= this.fiscalQuarterStartDay) {
            dofq = doy - this.fiscalQuarterStartDay + 1;
        } else {
            long tempTime = getTimeInMillis();
            add(Calendar.YEAR, -1);
            dofq = getActualMaximum(Calendar.DAY_OF_YEAR) - 
                this.fiscalQuarterStartDay + doy + 1;
            setTimeInMillis(tempTime);
            complete();
        }
        return dofq;
    }

    public int getWeekOfFiscalQuarter() {
        return WeekFrom(this.firstOfFiscalQuarterDate);
    }

    public int getFiscalQuarter() {
        return (this.getFiscalMonth() - 1) / 3 + 1;
    }

    public int getDayOfFiscalYear() {
        int doy = get(Calendar.DAY_OF_YEAR);
        int dofy;
        if (doy >= this.fiscalYearStartDay) {
            dofy = doy - this.fiscalYearStartDay + 1;
        } else {
            long tempTime = getTimeInMillis();
            add(Calendar.YEAR, -1);
            dofy = getActualMaximum(Calendar.DAY_OF_YEAR) -
                this.fiscalYearStartDay + doy + 1;
            setTimeInMillis(tempTime);
            complete();
        }
        return dofy;
    }

    public int getWeekOfFiscalYear() {
        return WeekFrom(this.firstOfFiscalYearDate);
    }

    public int getFiscalYear() {
        // The fiscal year is referred to by the date in which it ends.
        // For example, if a company's fiscal year ends October 31, 2006, then
        // everything between November 1, 2005 and October 31, 2006 would be
        // referred to as FY 2006
        if ((this.fiscalYearStartMonth == Calendar.JANUARY) ||
            (get(Calendar.MONTH) < this.fiscalYearStartMonth))
        {
            return get(Calendar.YEAR);
        } else {
            return get(Calendar.YEAR) + 1;
        }
    }

    public Date getFirstDayOfFiscalWeekDate() {
        return this.firstOfFiscalWeekDate;
    }

    public Date getLastDayOfFiscalWeekDate() {
        return this.lastOfFiscalWeekDate;
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
        currentDate.setTime(currentTime);
        boolean isFirstDayOfYear = false;
        boolean isFiscalFirstDayOfYear = false;

        // update first/last day of month/quarter/year
        if (get(Calendar.DAY_OF_MONTH) == 1) {
            int month = get(Calendar.MONTH);

            this.firstOfMonthDate.setTime(getTimeInMillis());
//            add(Calendar.MONTH, 1);
            set(Calendar.DAY_OF_MONTH,
                getActualMaximum(Calendar.DAY_OF_MONTH));
            complete();
            this.lastOfMonthDate.setTime(getTimeInMillis());
//            add(Calendar.MONTH, -1);
            setTimeInMillis(currentTime);
            complete();

            if ((month % 3) == 0) {
                if (month == 0) {
                    isFirstDayOfYear = true;
                    this.firstOfYearDate.setTime(currentTime);
//                    add(Calendar.YEAR, 1);
                    set(Calendar.DAY_OF_YEAR,
                        getActualMaximum(Calendar.DAY_OF_YEAR));
                    complete();
                    this.lastOfYearDate.setTime(getTimeInMillis());
//                    add(Calendar.YEAR, -1);
                    setTimeInMillis(currentTime);
                    complete();
                }
                this.firstOfQuarterDate.setTime(currentTime);
                this.quarterStartDay = get(Calendar.DAY_OF_YEAR);
                add(Calendar.MONTH, 2);
                set(Calendar.DAY_OF_MONTH, 
                    getActualMaximum(Calendar.DAY_OF_MONTH));
                complete();
                this.lastOfQuarterDate.setTime(getTimeInMillis());
//                add(Calendar.MONTH, -3);
                setTimeInMillis(currentTime);
                complete();
            }

            int fMonth = (month - this.fiscalYearStartMonth + 12) % 12;
            if ((fMonth % 3) == 0) {
                if (fMonth == 0) {
                    isFiscalFirstDayOfYear = true;
                    this.firstOfFiscalYearDate.setTime(currentTime);
                    this.fiscalYearStartDay = get(Calendar.DAY_OF_YEAR);
                    add(Calendar.YEAR, 1);
                    this.lastOfFiscalYearDate.setTime(getTimeInMillis() - millisInADay);
//                    add(Calendar.YEAR, -1);
                    setTimeInMillis(currentTime);
                    complete();
                    
                }
                this.firstOfFiscalQuarterDate.setTime(currentTime);
                this.fiscalQuarterStartDay = get(Calendar.DAY_OF_YEAR);
//                add(Calendar.MONTH, 3);
                add(Calendar.MONTH, 2);
                set(Calendar.DAY_OF_MONTH, 
                    getActualMaximum(Calendar.DAY_OF_MONTH));
                complete();
                this.lastOfFiscalQuarterDate.setTime(getTimeInMillis());
//                add(Calendar.MONTH, -3);
                setTimeInMillis(currentTime);
                complete();
            }
        }
        // update first/last day of week
        int currentDayOfWeek = get(Calendar.DAY_OF_WEEK);
        if (isFirstDayOfYear) {
            this.firstOfWeekDate.setTime(currentTime);
            int lastDayOfWeek = (((getFirstDayOfWeek()-1)+6)%7)+1;
            if (lastDayOfWeek >= currentDayOfWeek) {
                this.lastOfWeekDate.setTime(currentTime + 
                    (lastDayOfWeek - currentDayOfWeek)*millisInADay);
            } else {
                this.lastOfWeekDate.setTime(currentTime +
                    (lastDayOfWeek - currentDayOfWeek + 7)*millisInADay);
            }
        } else if (getFirstDayOfWeek() == currentDayOfWeek) {
            this.firstOfWeekDate.setTime(currentTime);
            this.lastOfWeekDate.setTime(currentTime + 6*millisInADay);
            // if last day of week is into the new year, set it to the last 
            // day of the year
            if (this.lastOfWeekDate.after(this.lastOfYearDate)) {
                this.lastOfWeekDate.setTime(this.lastOfYearDate.getTime());
            }
        }

        // update first/last fiscal day of week
        if (isFiscalFirstDayOfYear) {
            this.firstOfFiscalWeekDate.setTime(currentTime);
            int lastDayOfWeek = (((getFirstDayOfWeek()-1)+6)%7)+1;
            if (lastDayOfWeek >= currentDayOfWeek) {
                this.lastOfFiscalWeekDate.setTime(currentTime +
                    (lastDayOfWeek - currentDayOfWeek)*millisInADay);
            } else {
                this.lastOfFiscalWeekDate.setTime(currentTime +
                    (lastDayOfWeek - currentDayOfWeek + 7)*millisInADay);
            }
        } else if (getFirstDayOfWeek() == currentDayOfWeek) {
            this.firstOfFiscalWeekDate.setTime(currentTime);
            this.lastOfFiscalWeekDate.setTime(currentTime + 6*millisInADay);
            if (this.lastOfFiscalWeekDate.after(this.lastOfFiscalYearDate)) {
                this.lastOfFiscalWeekDate.setTime(
                    this.lastOfFiscalYearDate.getTime());
            }
        }
    }
}

// End TimeDimensionInternal.java
