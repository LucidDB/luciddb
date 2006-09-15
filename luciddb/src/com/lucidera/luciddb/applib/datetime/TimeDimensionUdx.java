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

import java.sql.*;

/**
 * Time Dimension UDX
 * Ported from //bb/bb713/server/SQL/TimeDimension.java &
 * //bb/bb713/server/Java/Broadbase/TimeDimensionInternal.java
 *
 * @author Elizabeth Lin
 * @version $Id$
 */
public abstract class TimeDimensionUdx
{
    // original time dimension for backwards compatibility
    public static void execute(
        int startYear, int startMonth, int startDay, int endYear, int endMonth,
        int endDay, PreparedStatement resultInserter) 
        throws SQLException
    {
        TimeDimensionInternal tdi = new TimeDimensionInternal(
            startYear, startMonth, startDay, endYear, endMonth, endDay, 1);
        tdi.Start();

        for (int rowCount = 0; rowCount <= tdi.getNumDays(); rowCount++) {
            // Year
            int year = tdi.getYear();
            
            // Days since the epoch (January 1, 1970)
            int julianDay = tdi.getJulianDay();

            // Month number (1=January)
            int month = tdi.getMonth();

            // Months since the epoch (January 1, 1970)
            int julianMonth = (year - 1970) * 12 + month;

            // Day of month
            int date = tdi.getDayOfMonth();

            // Calendar quarter
            int quarter = (month - 1) / 3 + 1;

            // Beginning of year
            int dayInYear = tdi.getDayOfYear();

            // Week number in month
            int weekNumInMonth = tdi.getWeekOfMonth();

            // Week number in year
            int week = tdi.getWeek();

            // Day of week
            int dayOfWeek = tdi.getDayOfWeek();

            String isWeekend;
            if ((dayOfWeek == 1) || (dayOfWeek == 7)) {
                isWeekend = "Y";
            } else {
                isWeekend = "N";
            }

            // insert data into row
            int column = 0;
            // TIME_KEY_SEQ
            resultInserter.setInt(++column, rowCount+1);

            //~---------- calendar columns ----------

            // TODO: Julian date here? TIME_KEY 
            resultInserter.setDate(++column, tdi.getDate());

            // DAY_OF_WEEK
            resultInserter.setString(
                ++column, TimeDimensionInternal.getDayOfWeek(dayOfWeek - 1));

            // WEEKEND
            resultInserter.setString(++column, isWeekend);

            // DAY_NUMBER_IN_WEEK
            resultInserter.setInt(++column, dayOfWeek);

            // DAY_NUMBER_IN_MONTH
            resultInserter.setInt(++column, date);

            // DAY_NUMBER_IN_YEAR
            resultInserter.setInt(++column, dayInYear);

            // DAY_NUMBER_OVERALL
            resultInserter.setInt(++column, julianDay);//THIS IS IT (uh, what?)

            // WEEK_NUMBER_IN_YEAR
            resultInserter.setInt(++column, week);

            // WEEK_NUMBER_OVERALL
            resultInserter.setInt(++column, 1 + julianDay / 7);

            // MONTH_NAME
            resultInserter.setString(++column, 
                TimeDimensionInternal.getMonthOfYear(month - 1));

            // MONTH_NUMBER_IN_YEAR
            resultInserter.setInt(++column, month);

            // MONTH_NUMBER_OVERALL
            resultInserter.setInt(++column, julianMonth);

            // QUARTER
            resultInserter.setInt(++column, quarter);

            // YEAR
            resultInserter.setInt(++column, year);

            // CALENDAR_QUARTER
            resultInserter.setString(++column, 
                CalendarQuarterUdf.getCalendarQuarter(quarter, year));

            // FIRST_DAY_OF_WEEK
            resultInserter.setDate(++column, tdi.getFirstDayOfWeekDate());

            // update row
            resultInserter.executeUpdate();

            // increment date
            tdi.increment();
        }
    }

    public static void execute(
        int startYear, int startMonth, int startDay, int endYear, int endMonth,
        int endDay, int fiscalYearStartMonth, PreparedStatement resultInserter) 
        throws SQLException
    {
        TimeDimensionInternal tdi = new TimeDimensionInternal(
            startYear, startMonth, startDay, endYear, endMonth, endDay, fiscalYearStartMonth);
        tdi.Start();

        for (int rowCount = 0; rowCount <= tdi.getNumDays(); rowCount++) {
            // Year
            int year = tdi.getYear();
            
            // Days since the epoch (January 1, 1970)
            int julianDay = tdi.getJulianDay();

            // Month number (1=January)
            int month = tdi.getMonth();

            // Months since the epoch (January 1, 1970)
            int julianMonth = (year - 1970) * 12 + month;

            // Day of month
            int date = tdi.getDayOfMonth();

            // Calendar quarter
            int quarter = (month - 1) / 3 + 1;

            // Beginning of year
            int dayInYear = tdi.getDayOfYear();

            // Week number in month
            int weekNumInMonth = tdi.getWeekOfMonth();

            // Week number in year
            int week = tdi.getWeek();

            // Day of week
            int dayOfWeek = tdi.getDayOfWeek();

            String isWeekend;
            if ((dayOfWeek == 1) || (dayOfWeek == 7)) {
                isWeekend = "Y";
            } else {
                isWeekend = "N";
            }

            // insert data into row
            int column = 0;
            // TIME_KEY_SEQ
            resultInserter.setInt(++column, rowCount+1);

            //~---------- calendar columns ----------

            // TODO: Julian date here? TIME_KEY 
            resultInserter.setDate(++column, tdi.getDate());

            // DAY_OF_WEEK
            resultInserter.setString(
                ++column, TimeDimensionInternal.getDayOfWeek(dayOfWeek - 1));

            // WEEKEND
            resultInserter.setString(++column, isWeekend);

            // DAY_NUMBER_IN_WEEK
            resultInserter.setInt(++column, dayOfWeek);

            // DAY_NUMBER_IN_MONTH
            resultInserter.setInt(++column, date);

            // DAY_NUMBER_IN_YEAR
            resultInserter.setInt(++column, dayInYear);

            // DAY_NUMBER_OVERALL
            resultInserter.setInt(++column, julianDay);//THIS IS IT (uh, what?)

            // DAYS_FROM_JULIAN = DAY_NUMBER_OVERALL + 2440588
            resultInserter.setInt(++column, julianDay + 2440588);

            // WEEK_NUMBER_IN_MONTH
            resultInserter.setInt(++column, weekNumInMonth);

            // WEEK_NUMBER_IN_QUARTER
            resultInserter.setInt(++column, tdi.getWeekOfQuarter());

            // WEEK_NUMBER_IN_YEAR
            resultInserter.setInt(++column, week);

            // WEEK_NUMBER_OVERALL
            resultInserter.setInt(++column, 1 + julianDay / 7);

            // MONTH_NAME
            resultInserter.setString(++column, 
                TimeDimensionInternal.getMonthOfYear(month - 1));

            // MONTH_NUMBER_IN_QUARTER
            resultInserter.setInt(++column, (month - 1) % 3 + 1);
 
            // MONTH_NUMBER_IN_YEAR
            resultInserter.setInt(++column, month);

            // MONTH_NUMBER_OVERALL
            resultInserter.setInt(++column, julianMonth);

            // QUARTER
            resultInserter.setInt(++column, quarter);

            // YEAR
            resultInserter.setInt(++column, year);

            // CALENDAR_QUARTER
            resultInserter.setString(++column, 
                CalendarQuarterUdf.getCalendarQuarter(quarter, year));

            // WEEK_START_DATE
            resultInserter.setDate(++column, tdi.getFirstDayOfWeekDate());

            // WEEK_END_DATE
            resultInserter.setDate(++column, tdi.getLastDayOfWeekDate());

            // MONTH_START_DATE
            resultInserter.setDate(++column, tdi.getFirstDayOfMonthDate());

            // MONTH_END_DATE
            resultInserter.setDate(++column, tdi.getLastDayOfMonthDate());

            // QUARTER_START_DATE
            resultInserter.setDate(++column, tdi.getFirstDayOfQuarterDate());

            // QUARTER_END_DATE
            resultInserter.setDate(++column, tdi.getLastDayOfQuarterDate());

            // YEAR_START_DATE
            resultInserter.setDate(++column, tdi.getFirstDayOfYearDate());

            // YEAR_END_DATE
            resultInserter.setDate(++column, tdi.getLastDayOfYearDate());

            //~---------- fiscal columns ----------
          
            // FISCAL_WEEK_START_DATE
            resultInserter.setDate(++column, tdi.getFirstDayOfWeekDate());

            // FISCAL_WEEK_END_DATE
            resultInserter.setDate(++column, tdi.getLastDayOfWeekDate());

            // FISCAL_WEEK_NUMBER_IN_MONTH
            resultInserter.setInt(++column, tdi.getWeekOfFiscalMonth());

            // FISCAL_WEEK_NUMBER_IN_QUARTER
            resultInserter.setInt(++column, tdi.getWeekOfFiscalQuarter());

            // FISCAL_WEEK_NUMBER_IN_YEAR
            resultInserter.setInt(++column, tdi.getWeekOfFiscalYear());

            // FISCAL_MONTH_START_DATE
            resultInserter.setDate(++column, tdi.getFirstDayOfMonthDate());
 
            // FISCAL_MONTH_END_DATE
            resultInserter.setDate(++column, tdi.getLastDayOfMonthDate());

            // FISCAL_MONTH_NUMBER_IN_QUARTER
            resultInserter.setInt(++column, (tdi.getFiscalMonth() - 1) % 3 + 1);
            
            // FISCAL_MONTH_NUMBER_IN_YEAR
            resultInserter.setInt(++column, tdi.getFiscalMonth());

            // FISCAL_QUARTER_START_DATE
            resultInserter.setDate(++column, tdi.getFirstDayOfFiscalQuarterDate());

            // FISCAL_QUARTER_END_DATE
            resultInserter.setDate(++column, tdi.getLastDayOfFiscalQuarterDate());
           
            // FISCAL_QUARTER_NUMBER_IN_YEAR
            resultInserter.setInt(++column, tdi.getFiscalQuarter());

            // FISCAL_YEAR_START_DATE
            resultInserter.setDate(++column, tdi.getFirstDayOfFiscalYearDate());

            // FISCAL_YEAR_END_DATE
            resultInserter.setDate(++column, tdi.getLastDayOfFiscalYearDate());

            // update row
            resultInserter.executeUpdate();

            // increment date
            tdi.increment();
        }
    }

}

// End TimeDimensionUdx.java
