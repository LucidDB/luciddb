/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2004-2005 Disruptive Tech
// Copyright (C) 2005-2005 Red Square, Inc.
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

import org.eigenbase.util.EnumeratedValues;
import org.eigenbase.sql.parser.SqlParserPos;
import org.eigenbase.sql.util.SqlVisitor;
import org.eigenbase.sql.validate.SqlValidatorScope;
import org.eigenbase.sql.validate.SqlValidator;

/**
 * Represents an INTERVAL qualifier.
 *
 * <p>INTERVAL qualifier is defined as follows:
 * <blockquote><code><pre>
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
 * </pre></code></blockquote>
 *
 * <p>Examples include:<ul>
 * <li><code>INTERVAL '1:23:45.678' HOUR TO SECOND</code></li>
 * <li><code>INTERVAL '1 2:3:4' DAY TO SECOND</code></li>
 * <li><code>INTERVAL '1 2:3:4' DAY(4) TO SECOND(4)</code></li>
 * </ul>
 *
 * An instance of this class is immutable.
 *
 * @author Wael Chatila
 * @since Oct 31, 2004
 * @version $Id$
 */
public class SqlIntervalQualifier extends SqlNode
{
    private final int         startPrecision;
    private final TimeUnit    startUnit;
    private final TimeUnit    endUnit;
    private final int         fractionalSecondPrecision;

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

    public void validate(
        SqlValidator validator,
        SqlValidatorScope scope)
    {
        validator.validateIntervalQualifier(this);
    }

    public void accept(SqlVisitor visitor)
    {
        visitor.visit(this);
    }

    public boolean equalsDeep(SqlNode node)
    {
        return this.toString().equals(node.toString());
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

    public String toString()
    {
        StringBuffer ret = new StringBuffer();
        ret.append(startUnit.name.toUpperCase());
        if (startUnit.equals(TimeUnit.Second)) {
            if (0 != fractionalSecondPrecision) {
                ret.append("(");
                ret.append(startPrecision);
                ret.append(", ");
                ret.append(fractionalSecondPrecision);
                ret.append(")");
            } else if (0 != startPrecision) {
                ret.append("(");
                ret.append(startPrecision);
                ret.append(")");
            }
        } else {

            if (0 != startPrecision) {
                ret.append("(");
                ret.append(startPrecision);
                ret.append(")");
            }

            if (null != endUnit) {
                ret.append(" TO ");
                ret.append(endUnit.name.toUpperCase());
            }

            if (0 != fractionalSecondPrecision) {
                ret.append("(");
                ret.append(fractionalSecondPrecision);
                ret.append(")");
            }
        }
        return ret.toString();
    }

    public void unparse(
        SqlWriter writer,
        int leftPrec,
        int rightPrec)
    {
        writer.print(this.toString());
    }

    public boolean isYearMonth()
    {
        return TimeUnit.Year.equals(startUnit) ||
               TimeUnit.Month.equals(startUnit);
    }

    //~ INNER CLASS --------------------------

    /**
     * Enumeration of time units used to construct an interval.
     */
    public static class TimeUnit extends EnumeratedValues.BasicValue
    {
        private TimeUnit(String name, int ordinal) {
            super(name, ordinal, null);
        }

        public static final int Year_ordinal = 0;
        public static final TimeUnit Year =
            new TimeUnit("Year", Year_ordinal);
        public static final int Month_ordinal = 1;
        public static final TimeUnit Month =
            new TimeUnit("Month", Month_ordinal);
        public static final int Day_ordinal = 2;
        public static final TimeUnit Day =
            new TimeUnit("Day", Day_ordinal);
        public static final int Hour_ordinal = 3;
        public static final TimeUnit Hour =
            new TimeUnit("Hour", Hour_ordinal);
        public static final int Minute_ordinal = 4;
        public static final TimeUnit Minute =
            new TimeUnit("Minute", Minute_ordinal);
        public static final int Second_ordinal = 5;
        public static final TimeUnit Second =
            new TimeUnit("Second", Second_ordinal);
        public static final EnumeratedValues enumeration =
                new EnumeratedValues(new TimeUnit[]{
                    Year, Month, Day, Hour, Minute, Second,
                });
    }
}

// End SqlIntervalQualifier.java

