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
            int[] values, SqlIntervalQualifier intervalQualifier,
            SqlTypeName sqlTypeName, ParserPosition pos) {
        super(new IntervalValue(intervalQualifier, values), sqlTypeName, pos);
    }

    public void unparse(
            SqlWriter writer,
            int leftPrec,
            int rightPrec) {
        IntervalValue interval = (IntervalValue) value;
        writer.print("INTERVAL '");
        writer.print(value.toString());
        writer.print("' ");
        writer.print(interval.intervalQualifier.toString());
    }

    /**
     * A Interval value.
     */
    static class IntervalValue {
        private final SqlIntervalQualifier intervalQualifier;
        private final int[] values;

        IntervalValue(SqlIntervalQualifier intervalQualifier, int[] values) {
            this.intervalQualifier = intervalQualifier;
            this.values = values;
        }

        public SqlIntervalQualifier getIntervalQualifier() {
            return intervalQualifier;
        }

        public int[] getValues() {
            return values;
        }

        public String toString() {
            StringBuffer ret = new StringBuffer();
            if (-1 == values[0]) {
                ret.append('-');
            }
            ret.append(String.valueOf(values[1]));
            if (intervalQualifier.isYearMonth() && 3==values.length) {
                ret.append("-");
                ret.append(String.valueOf(values[2]));
            } else if (values.length > 2) {
                SqlIntervalQualifier.TimeUnit start =
                    intervalQualifier.getStartUnit();
                SqlIntervalQualifier.TimeUnit end =
                    intervalQualifier.getEndUnit();

                if (SqlIntervalQualifier.TimeUnit.Day.equals(
                    intervalQualifier.getStartUnit())) {
                    ret.append(" ");
                } else if
                    (!SqlIntervalQualifier.TimeUnit.Second.equals(start)) {
                    ret.append(":");
                }

                if (null == end) {
                    end = start;
                }

                for (int i = 2; i < values.length; i++) {
                    if (SqlIntervalQualifier.TimeUnit.Second.equals(end) &&
                        ((end.ordinal-start.ordinal)<(i-1))) {
                            ret.append(".");
                    } else if (i >= 3) {
                        ret.append(":");
                    }
                    ret.append(String.valueOf(values[i]));
                }
            }

            return ret.toString();
        }
    }
}

// End SqlIntervalLiteral.java
