package org.eigenbase.sql;

import org.eigenbase.util.EnumeratedValues;

/**
 * Represents an INTERVAL qualifier.
 * INTERVAL qualifier is defined as
 * <blockquote><code>
 *
 * &lt;interval qualifier&gt; ::=
 *                 &lt;start field&gt; TO &lt;end field&gt;
 *                 | &lt;single datetime field&gt;
 * &lt;start field&gt; ::=
 *                 &lt;non-second primary datetime field&gt;
 *                 [ &lt;left paren&gt; &lt;interval leading field precision&gt; &lt;right paren&gt; ]
 * &lt;end field&gt; ::=
 *                 &lt;non-second primary datetime field&gt;
 *                 | SECOND [ &lt;left paren&gt; &lt;interval fractional seconds precision&gt; &lt;right paren&gt; ]
 * &lt;single datetime field&gt; ::=
 *                 &lt;non-second primary datetime field&gt;
 *                 [ &lt;left paren&gt; &lt;interval leading field precision&gt; &lt;right paren&gt; ]
 *                 | SECOND [ &lt;left paren&gt; &lt;interval leading field precision&gt;
 *                 [ &lt;comma&gt; &lt;interval fractional seconds precision&gt; ] &lt;right paren&gt; ]
 * &lt;primary datetime field&gt; ::=
 *                &lt;non-second primary datetime field&gt;
 *                 | SECOND
 * &lt;non-second primary datetime field&gt; ::= YEAR | MONTH | DAY | HOUR | MINUTE
 * &lt;interval fractional seconds precision&gt; ::= &lt;unsigned integer&gt;
 * &lt;interval leading field precision&gt; ::= &lt;unsigned integer&gt;
 *
 * </code></blockquote>
 *
 * An instance of this class is immutable.
 *
 * @author Wael Chatila
 * @since Oct 31, 2004
 * @version $Id$
 */
public class SqlIntervalQualifier implements Cloneable {

    private int         startPrecision;
    private TimeUnit    startUnit;
    private TimeUnit    endUnit;
    private int         fractionalSecondPrecision;

    public SqlIntervalQualifier(
        TimeUnit startUnit,
        int startPrecision,
        TimeUnit endUnit,
        int fractionalSecondPrecision) {
        assert(null!=startUnit);
        this.startPrecision = startPrecision;
        this.startUnit = startUnit;
        this.endUnit = endUnit;
        this.fractionalSecondPrecision = fractionalSecondPrecision;
    }

    public int getStartPrecision() {
        return startPrecision;
    }

    public int getFractionalSecondPrecision() {
        return fractionalSecondPrecision;
    }

    public TimeUnit getStartUnit() {
        return startUnit;
    }

    public TimeUnit getEndUnit() {
        return endUnit;
    }

    public String toString() {
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

    public boolean isYearMonth() {
        return TimeUnit.Year.equals(startUnit) ||
               TimeUnit.Month.equals(startUnit);
    }

    //~ INNER CLASS --------------------------

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



}


