/*
// $Id$
// Package org.eigenbase is a class library of database components.
// Copyright (C) 2004-2004 Disruptive Tech
//
// This program is free software; you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation; either version 2 of the License, or
// (at your option) any later version.
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
package org.eigenbase.sql;

import org.eigenbase.sql.type.SqlTypeName;
import org.eigenbase.sql.parser.ParserPosition;
import org.eigenbase.util.EnumeratedValues;

/**
 * A SQL literal representing a time interval.
 *
 * <p>Examples:<ul>
 * <li>INTERVAL '1' SECOND</li>
 * <li>INTERVAL '1:00:05.345' HOUR</li>
 * <li>INTERVAL '3:4' YEAR TO MONTH</li>
 * </ul>
 *
 * <p>YEAR/MONTH intervals are not implemented yet.</p>
 *
 * <p>The interval string, such as '1:00:05.345', is not parsed yet.</p>
 */
public class SqlIntervalLiteral extends SqlLiteral
{
    protected SqlIntervalLiteral(
            String value, TimeUnit timeUnit,
            SqlTypeName sqlTypeName, ParserPosition pos) {
        super(new DayTimeInterval(timeUnit, value), sqlTypeName, pos);
    }

    public void unparse(
            SqlWriter writer,
            int leftPrec,
            int rightPrec) {
        DayTimeInterval dayTimeInterval = (DayTimeInterval) value;
        writer.print("INTERVAL '");
        writer.print(dayTimeInterval.value);
        writer.print("' ");
        writer.print(dayTimeInterval.timeUnit.name.toUpperCase());
    }

    /**
     * Enumeration of time units used to construct an interval.
     */
    public static class TimeUnit extends EnumeratedValues.BasicValue {
        private TimeUnit(String name, int ordinal) {
            super(name, ordinal, null);
        }

        public static final TimeUnit Year = new TimeUnit("Year", 0);
        public static final TimeUnit Month = new TimeUnit("Month", 1);
        public static final TimeUnit Day = new TimeUnit("Day", 2);
        public static final TimeUnit Hour = new TimeUnit("Hour", 3);
        public static final TimeUnit Minute = new TimeUnit("Minute", 4);
        public static final TimeUnit Second = new TimeUnit("Second", 5);
        public static final EnumeratedValues enumeration =
                new EnumeratedValues(new TimeUnit[]{
                    Year, Month, Day, Hour, Minute, Second,
                });
    }

    /**
     * A day-time interval value.
     */
    static class DayTimeInterval {
        private final TimeUnit timeUnit;
        private final String value;

        DayTimeInterval(TimeUnit timeUnit, String value) {
            this.timeUnit = timeUnit;
            this.value = value;
        }
    }
}

// End SqlIntervalLiteral.java
