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
import org.eigenbase.sql.util.*;
import org.eigenbase.sql.validate.*;
import org.eigenbase.util.*;


/**
 * Represents an INTERVAL qualifier.
 *
 * <p>INTERVAL qualifier is defined as follows:
 *
 * <blockquote><code>
 * <pre>
 *
 * &lt;interval qualifier&gt; ::=
 *               &lt;start field&gt; TO &lt;end field&gt;
 *               | &lt;single datetime field&gt;
 * &lt;start field&gt; ::=
 *               &lt;non-second primary datetime field&gt;
 *               [ &lt;left paren&gt; &lt;interval leading field precision&gt; &lt;right paren&gt; ]
 * &lt;end field&gt; ::=
 *               &lt;non-second primary datetime field&gt;
 *               | SECOND [ &lt;left paren&gt; &lt;interval fractional seconds precision&gt; &lt;right paren&gt; ]
 * &lt;single datetime field&gt; ::=
 *               &lt;non-second primary datetime field&gt;
 *               [ &lt;left paren&gt; &lt;interval leading field precision&gt; &lt;right paren&gt; ]
 *               | SECOND [ &lt;left paren&gt; &lt;interval leading field precision&gt;
 *               [ &lt;comma&gt; &lt;interval fractional seconds precision&gt; ] &lt;right paren&gt; ]
 * &lt;primary datetime field&gt; ::=
 *              &lt;non-second primary datetime field&gt;
 *              | SECOND
 * &lt;non-second primary datetime field&gt; ::= YEAR | MONTH | DAY | HOUR | MINUTE
 * &lt;interval fractional seconds precision&gt; ::= &lt;unsigned integer&gt;
 * &lt;interval leading field precision&gt; ::= &lt;unsigned integer&gt;
 *
 * </pre>
 * </code></blockquote>
 *
 * <p>Examples include:
 *
 * <ul>
 * <li><code>INTERVAL '1:23:45.678' HOUR TO SECOND</code></li>
 * <li><code>INTERVAL '1 2:3:4' DAY TO SECOND</code></li>
 * <li><code>INTERVAL '1 2:3:4' DAY(4) TO SECOND(4)</code></li>
 * </ul>
 *
 * An instance of this class is immutable.
 *
 * @author Wael Chatila
 * @version $Id$
 * @since Oct 31, 2004
 */
public class SqlIntervalQualifier
    extends SqlNode
{

    //~ Instance fields --------------------------------------------------------

    private final int startPrecision;
    private final TimeUnit startUnit;
    private final TimeUnit endUnit;
    private final int fractionalSecondPrecision;

    //~ Constructors -----------------------------------------------------------

    public SqlIntervalQualifier(
        TimeUnit startUnit,
        int startPrecision,
        TimeUnit endUnit,
        int fractionalSecondPrecision,
        SqlParserPos pos)
    {
        super(pos);
        assert null != startUnit;
        this.startPrecision = startPrecision;
        this.startUnit = startUnit;
        this.endUnit = endUnit;
        this.fractionalSecondPrecision = fractionalSecondPrecision;
    }

    //~ Methods ----------------------------------------------------------------

    public void validate(
        SqlValidator validator,
        SqlValidatorScope scope)
    {
        validator.validateIntervalQualifier(this);
    }

    public <R> R accept(SqlVisitor<R> visitor)
    {
        return visitor.visit(this);
    }

    public boolean equalsDeep(SqlNode node, boolean fail)
    {
        final String thisString = this.toString();
        final String thatString = node.toString();
        if (!thisString.equals(thatString)) {
            assert !fail : this + "!=" + node;
            return false;
        }
        return true;
    }

    public int getStartPrecision()
    {
        return startPrecision;
    }

    public int getFractionalSecondPrecision()
    {
        return fractionalSecondPrecision;
    }

    public TimeUnit getStartUnit()
    {
        return startUnit;
    }

    public TimeUnit getEndUnit()
    {
        return endUnit;
    }

    public SqlNode clone(SqlParserPos pos)
    {
        return
            new SqlIntervalQualifier(
                startUnit,
                startPrecision,
                endUnit,
                fractionalSecondPrecision,
                pos);
    }

    public void unparse(
        SqlWriter writer,
        int leftPrec,
        int rightPrec)
    {
        final String start = startUnit.getName().toUpperCase();
        if (startUnit.equals(TimeUnit.Second)) {
            if (0 != fractionalSecondPrecision) {
                final SqlWriter.Frame frame = writer.startFunCall(start);
                writer.print(startPrecision);
                writer.sep(",", true);
                writer.print(fractionalSecondPrecision);
                writer.endList(frame);
            } else if (0 != startPrecision) {
                final SqlWriter.Frame frame = writer.startFunCall(start);
                writer.print(startPrecision);
                writer.endList(frame);
            } else {
                writer.keyword(start);
            }
        } else {
            if (0 != startPrecision) {
                final SqlWriter.Frame frame = writer.startFunCall(start);
                writer.print(startPrecision);
                writer.endList(frame);
            } else {
                writer.keyword(start);
            }

            if (null != endUnit) {
                writer.keyword("TO");
                final String end = endUnit.getName().toUpperCase();
                if (0 != fractionalSecondPrecision) {
                    final SqlWriter.Frame frame = writer.startFunCall(end);
                    writer.print(fractionalSecondPrecision);
                    writer.endList(frame);
                } else {
                    writer.keyword(end);
                }
            } else if (0 != fractionalSecondPrecision) {
                final SqlWriter.Frame frame = writer.startList("(", ")");
                writer.print(fractionalSecondPrecision);
                writer.endList(frame);
            }
        }
    }

    public boolean isYearMonth()
    {
        return
            TimeUnit.Year.equals(startUnit)
            || TimeUnit.Month.equals(startUnit);
    }

    public static long getConversion(TimeUnit unit)
    {
        long val = 0;
        switch (unit.getOrdinal()) {
            case SqlIntervalQualifier.TimeUnit.Day_ordinal:
                val = 24 * 3600000;  // number of millis
                break;
            case SqlIntervalQualifier.TimeUnit.Hour_ordinal:
                val = 3600000;  // number of millis
                break;
            case SqlIntervalQualifier.TimeUnit.Minute_ordinal:
                val = 60000;  // number of millis
                break;
            case SqlIntervalQualifier.TimeUnit.Second_ordinal:
                val = 1000;  // number of millis
                break;
            case SqlIntervalQualifier.TimeUnit.Year_ordinal:
                val = 12; // number of months
                break;
            case SqlIntervalQualifier.TimeUnit.Month_ordinal:
                val = 1;  // number of months
                break;
            default:
                assert false : "invalid interval qualifier";
                break;
        }
        return val;
    }

    //~ Inner Classes ----------------------------------------------------------

    /**
     * Enumeration of time units used to construct an interval.
     */
    public static class TimeUnit
        extends EnumeratedValues.BasicValue
    {
        public static final String GET_VALUE_METHOD_NAME = "getValue";

        public static final int Year_ordinal = 0;
        public static final TimeUnit Year = new TimeUnit("Year", Year_ordinal);
        public static final int Month_ordinal = 1;
        public static final TimeUnit Month =
            new TimeUnit("Month", Month_ordinal);
        public static final int Day_ordinal = 2;
        public static final TimeUnit Day = new TimeUnit("Day", Day_ordinal);
        public static final int Hour_ordinal = 3;
        public static final TimeUnit Hour = new TimeUnit("Hour", Hour_ordinal);
        public static final int Minute_ordinal = 4;
        public static final TimeUnit Minute =
            new TimeUnit("Minute", Minute_ordinal);
        public static final int Second_ordinal = 5;
        public static final TimeUnit Second =
            new TimeUnit("Second", Second_ordinal);
        public static final EnumeratedValues enumeration =
            new EnumeratedValues(
                new TimeUnit[] {
                    Year, Month, Day, Hour, Minute, Second,
                });

        /**
         * Returns the TimeUnit associated with an ordinal. The value 
         * returned is null if the ordinal is not a member of the 
         * TimeUnit enumeration.
         */
        public static TimeUnit getValue(int ordinal)
        {
            return (TimeUnit) enumeration.getValue(ordinal);
        }

        private TimeUnit(String name, int ordinal)
        {
            super(name, ordinal, null);
        }
    }
}

// End SqlIntervalQualifier.java
