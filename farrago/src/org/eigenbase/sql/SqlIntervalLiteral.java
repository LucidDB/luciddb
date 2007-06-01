/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2004-2005 Disruptive Tech
// Copyright (C) 2005-2005 LucidEra, Inc.
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
package org.eigenbase.sql;

import org.eigenbase.sql.parser.*;
import org.eigenbase.sql.type.*;
import org.eigenbase.util.*;


/**
 * A SQL literal representing a time interval.
 *
 * <p>Examples:
 *
 * <ul>
 * <li>INTERVAL '1' SECOND</li>
 * <li>INTERVAL '1:00:05.345' HOUR</li>
 * <li>INTERVAL '3:4' YEAR TO MONTH</li>
 * </ul>
 *
 * <p>YEAR/MONTH intervals are not implemented yet.</p>
 *
 * <p>The interval string, such as '1:00:05.345', is not parsed yet.</p>
 *
 * @author jhyde
 * @version $Id$
 */
public class SqlIntervalLiteral
    extends SqlLiteral
{
    //~ Constructors -----------------------------------------------------------

    protected SqlIntervalLiteral(
        int sign,
        String intervalStr,
        SqlIntervalQualifier intervalQualifier,
        SqlTypeName sqlTypeName,
        SqlParserPos pos)
    {
        this(
            new IntervalValue(intervalQualifier, sign, intervalStr),
            sqlTypeName,
            pos);
    }

    private SqlIntervalLiteral(
        IntervalValue intervalValue,
        SqlTypeName sqlTypeName,
        SqlParserPos pos)
    {
        super(
            intervalValue,
            sqlTypeName,
            pos);
    }

    //~ Methods ----------------------------------------------------------------

    public SqlNode clone(SqlParserPos pos)
    {
        return new SqlIntervalLiteral(
            (IntervalValue) value,
            getTypeName(),
            pos);
    }

    public void unparse(
        SqlWriter writer,
        int leftPrec,
        int rightPrec)
    {
        IntervalValue interval = (IntervalValue) value;
        writer.keyword("INTERVAL");
        if (interval.getSign() == -1) {
            writer.print("-");
        }
        writer.literal("'" + value.toString() + "'");
        writer.keyword(interval.intervalQualifier.toString());
    }

    //~ Inner Classes ----------------------------------------------------------

    /**
     * A Interval value.
     */
    public static class IntervalValue
    {
        private final SqlIntervalQualifier intervalQualifier;
        private final String intervalStr;
        private final int sign;

        /**
         * Creates an interval value.
         *
         * @param intervalQualifier Interval qualifier
         * @param sign Sign (+1 or -1)
         * @param intervalStr
         */
        IntervalValue(
            SqlIntervalQualifier intervalQualifier,
            int sign,
            String intervalStr)
        {
            assert (sign == -1) || (sign == 1);
            assert intervalQualifier != null;
            assert intervalStr != null;
            this.intervalQualifier = intervalQualifier;
            this.sign = sign;
            this.intervalStr = intervalStr;
        }

        public boolean equals(Object obj)
        {
            if (!(obj instanceof IntervalValue)) {
                return false;
            }
            IntervalValue that = (IntervalValue) obj;
            return this.intervalStr.equals(that.intervalStr)
                && (this.sign == that.sign)
                && this.intervalQualifier.equalsDeep(
                    that.intervalQualifier,
                    false);
        }

        public int hashCode()
        {
            int h = Util.hash(sign, intervalStr);
            int i = Util.hash(h, intervalQualifier);
            return i;
        }

        public SqlIntervalQualifier getIntervalQualifier()
        {
            return intervalQualifier;
        }

        public String getIntervalLiteral()
        {
            return intervalStr;
        }

        public int getSign()
        {
            return sign;
        }

        public String toString()
        {
            return intervalStr;
        }
    }
}

// End SqlIntervalLiteral.java
