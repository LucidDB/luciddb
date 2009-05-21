/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005-2009 The Eigenbase Project
// Copyright (C) 2004-2009 SQLstream, Inc.
// Copyright (C) 2005-2009 LucidEra, Inc.
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

import java.util.regex.*;

import org.eigenbase.sql.parser.*;
import org.eigenbase.sql.type.*;
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
    //~ Static fields/initializers ---------------------------------------------

    private static final int USE_DEFAULT_PRECISION = -1;

    //~ Enums ------------------------------------------------------------------

    /**
     * Enumeration of time units used to construct an interval.
     */
    public enum TimeUnit
        implements SqlLiteral.SqlSymbol
    {
        Year(true, 12 /* months */), Month(true, 1 /* months */),
        Day(false, 86400000 /* millis = 24 * 3600000 */),
        Hour(false, 3600000 /* millis */), Minute(false, 60000 /* millis */),
        Second(false, 1000 /* millis */);

        public final boolean yearMonth;
        public final long multiplier;
        private static final TimeUnit [] CachedValues = values();

        private TimeUnit(boolean yearMonth, long multiplier)
        {
            this.yearMonth = yearMonth;
            this.multiplier = multiplier;
        }

        /**
         * Returns the TimeUnit associated with an ordinal. The value returned
         * is null if the ordinal is not a member of the TimeUnit enumeration.
         */
        public static TimeUnit getValue(int ordinal)
        {
            return ((ordinal < 0) || (ordinal >= CachedValues.length)) ? null
                : CachedValues[ordinal];
        }

        public static final String GET_VALUE_METHOD_NAME = "getValue";
    }

    //~ Instance fields --------------------------------------------------------

    private final int startPrecision;
    private final TimeUnit startUnit;
    private final TimeUnit endUnit;
    private final int fractionalSecondPrecision;

    private final boolean useDefaultStartPrecision;
    private final boolean useDefaultFractionalSecondPrecision;

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

        this.startUnit = startUnit;
        this.endUnit = endUnit;

        // if unspecified, start precision = 2
        if (startPrecision == USE_DEFAULT_PRECISION) {
            useDefaultStartPrecision = true;
            if (this.isYearMonth()) {
                this.startPrecision =
                    SqlTypeName.INTERVAL_YEAR_MONTH.getDefaultPrecision();
            } else {
                this.startPrecision =
                    SqlTypeName.INTERVAL_DAY_TIME.getDefaultPrecision();
            }
        } else {
            useDefaultStartPrecision = false;
            this.startPrecision = startPrecision;
        }

        // unspecified fractional second precision = 6
        if (fractionalSecondPrecision == USE_DEFAULT_PRECISION) {
            useDefaultFractionalSecondPrecision = true;
            if (this.isYearMonth()) {
                this.fractionalSecondPrecision =
                    SqlTypeName.INTERVAL_YEAR_MONTH.getDefaultScale();
            } else {
                this.fractionalSecondPrecision =
                    SqlTypeName.INTERVAL_DAY_TIME.getDefaultScale();
            }
        } else {
            useDefaultFractionalSecondPrecision = false;
            this.fractionalSecondPrecision = fractionalSecondPrecision;
        }
    }

    public SqlIntervalQualifier(
        TimeUnit startUnit,
        TimeUnit endUnit,
        SqlParserPos pos)
    {
        this(
            startUnit,
            USE_DEFAULT_PRECISION,
            endUnit,
            USE_DEFAULT_PRECISION,
            pos);
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

    public static int getDefaultPrecisionId()
    {
        return USE_DEFAULT_PRECISION;
    }

    public int getStartPrecision()
    {
        return startPrecision;
    }

    public int getStartPrecisionPreservingDefault()
    {
        if (useDefaultStartPrecision) {
            return USE_DEFAULT_PRECISION;
        } else {
            return startPrecision;
        }
    }

    public static int combineStartPrecisionPreservingDefault(
        SqlIntervalQualifier qual1,
        SqlIntervalQualifier qual2)
    {
        if (qual1.getStartPrecision()
            > qual2.getStartPrecision())
        {
            // qual1 is more precise, but if it has the default indicator
            // set, we need to return that indicator so result will also
            // use default
            return (qual1.getStartPrecisionPreservingDefault());
        } else if (qual1.getStartPrecision()
            < qual2.getStartPrecision())
        {
            // qual2 is more precise, but if it has the default indicator
            // set, we need to return that indicator so result will also
            // use default
            return (qual2.getStartPrecisionPreservingDefault());
        } else {
            // they are equal.  return default if both are default,
            // otherwise return exact precision
            if (qual1.useDefaultStartPrecision
                && qual2.useDefaultStartPrecision)
            {
                return qual1.getStartPrecisionPreservingDefault();
            } else {
                return qual1.getStartPrecision();
            }
        }
    }

    public int getFractionalSecondPrecision()
    {
        return fractionalSecondPrecision;
    }

    public int getFractionalSecondPrecisionPreservingDefault()
    {
        if (useDefaultFractionalSecondPrecision) {
            return USE_DEFAULT_PRECISION;
        } else {
            return startPrecision;
        }
    }

    public static int combineFractionalSecondPrecisionPreservingDefault(
        SqlIntervalQualifier qual1,
        SqlIntervalQualifier qual2)
    {
        if (qual1.getFractionalSecondPrecision()
            > qual2.getFractionalSecondPrecision())
        {
            // qual1 is more precise, but if it has the default indicator
            // set, we need to return that indicator so result will also
            // use default
            return (qual1.getFractionalSecondPrecisionPreservingDefault());
        } else if (
            qual1.getFractionalSecondPrecision()
            < qual2.getFractionalSecondPrecision())
        {
            // qual2 is more precise, but if it has the default indicator
            // set, we need to return that indicator so result will also
            // use default
            return (qual2.getFractionalSecondPrecisionPreservingDefault());
        } else {
            // they are equal.  return default if both are default,
            // otherwise return exact precision
            if (qual1.useDefaultFractionalSecondPrecision
                && qual2.useDefaultFractionalSecondPrecision)
            {
                return qual1.getFractionalSecondPrecisionPreservingDefault();
            } else {
                return qual1.getFractionalSecondPrecision();
            }
        }
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
        return new SqlIntervalQualifier(
            startUnit,
            useDefaultStartPrecision ? USE_DEFAULT_PRECISION
                : startPrecision,
            endUnit,
            useDefaultFractionalSecondPrecision ? USE_DEFAULT_PRECISION
                : fractionalSecondPrecision,
            pos);
    }

    public void unparse(
        SqlWriter writer,
        int leftPrec,
        int rightPrec)
    {
        final String start = startUnit.name().toUpperCase();
        if (startUnit.equals(TimeUnit.Second)) {
            if (!useDefaultFractionalSecondPrecision) {
                final SqlWriter.Frame frame = writer.startFunCall(start);
                writer.print(startPrecision);
                writer.sep(",", true);
                writer.print(fractionalSecondPrecision);
                writer.endList(frame);
            } else if (!useDefaultStartPrecision) {
                final SqlWriter.Frame frame = writer.startFunCall(start);
                writer.print(startPrecision);
                writer.endList(frame);
            } else {
                writer.keyword(start);
            }
        } else {
            if (!useDefaultStartPrecision) {
                final SqlWriter.Frame frame = writer.startFunCall(start);
                writer.print(startPrecision);
                writer.endList(frame);
            } else {
                writer.keyword(start);
            }

            if (null != endUnit) {
                writer.keyword("TO");
                final String end = endUnit.name().toUpperCase();
                if ((TimeUnit.Second.equals(endUnit))
                    && (!useDefaultFractionalSecondPrecision))
                {
                    final SqlWriter.Frame frame = writer.startFunCall(end);
                    writer.print(fractionalSecondPrecision);
                    writer.endList(frame);
                } else {
                    writer.keyword(end);
                }
            } else if (
                (TimeUnit.Second.equals(startUnit))
                && (!useDefaultFractionalSecondPrecision))
            {
                final SqlWriter.Frame frame = writer.startList("(", ")");
                writer.print(fractionalSecondPrecision);
                writer.endList(frame);
            }
        }
    }

    public boolean isYearMonth()
    {
        return TimeUnit.Year.equals(startUnit)
            || TimeUnit.Month.equals(startUnit);
    }

    private int getIntervalSign(String value)
    {
        int sign = 1; // positive until proven otherwise

        if (!Util.isNullOrEmpty(value)) {
            if ('-' == value.charAt(0)) {
                sign = -1; // Negative
            }
        }

        return (sign);
    }

    private String stripLeadingSign(String value)
    {
        String unsignedValue = value;

        if (!Util.isNullOrEmpty(value)) {
            if (('-' == value.charAt(0)) || ('+' == value.charAt(0))) {
                unsignedValue = value.substring(1);
            }
        }

        return (unsignedValue);
    }

    private boolean isLeadFieldInRange(int field, TimeUnit unit)
    {
        // we should never get handed a negative field value
        assert (field >= 0);

        // Leading fields are only restricted by startPrecision, which
        // has already been checked for using pattern matching.
        // Therefore, always return true
        return (true);
    }

    private boolean isFractionalSecondFieldInRange(int field)
    {
        // we should never get handed a negative field value
        assert (field >= 0);

        // Fractional second fields are only restricted by precision, which
        // has already been checked for using pattern matching.
        // Therefore, always return true
        return (true);
    }

    private boolean isSecondaryFieldInRange(int field, TimeUnit unit)
    {
        boolean retval = false;

        // we should never get handed a negative field value
        assert (field >= 0);

        // YEAR and DAY can never be secondary units,
        // nor can unit be null
        assert (unit != null);
        assert (unit != TimeUnit.Year);
        assert (unit != TimeUnit.Day);

        // Secondary field limits, as per section 4.6.3 of SQL2003 spec
        if (((TimeUnit.Month == unit) && (field <= 11))
            || ((TimeUnit.Hour == unit) && (field <= 23))
            || ((TimeUnit.Minute == unit) && (field <= 59))
            || ((TimeUnit.Second == unit) && (field <= 59)))
        {
            retval = true;
        }

        return (retval);
    }

    private int normalizeSecondFraction(String secondFracStr)
    {
        // Decimal value can be more than 3 digits. So just get
        // the millisecond part.
        int ret = (int) (Float.parseFloat("0." + secondFracStr) * 1000);
        return (ret);
    }

    private int [] fillIntervalValueArray(
        int sign,
        int year,
        int month)
    {
        int [] ret = new int[3];

        ret[0] = sign;
        ret[1] = year;
        ret[2] = month;

        return (ret);
    }

    private int [] fillIntervalValueArray(
        int sign,
        int day,
        int hour,
        int minute,
        int second,
        int secondFrac)
    {
        int [] ret = new int[6];

        ret[0] = sign;
        ret[1] = day;
        ret[2] = hour;
        ret[3] = minute;
        ret[4] = second;
        ret[5] = secondFrac;

        return (ret);
    }

    /**
     * Validates an INTERVAL literal against a YEAR interval qualifier.
     *
     * @return null if the interval value is illegal.
     */
    private int [] evaluateIntervalLiteralAsYear(
        int sign,
        String value)
    {
        int year;

        // validate as YEAR(startPrecision), e.g. 'YY'
        String intervalPattern = "(\\d{1," + startPrecision + "})";

        Matcher m = Pattern.compile(intervalPattern).matcher(value);
        if (m.matches()) {
            // Break out  field values
            try {
                year = Integer.parseInt(m.group(1));
            } catch (NumberFormatException e) {
                return null;
            }

            // Validate individual fields
            if (!isLeadFieldInRange(year, TimeUnit.Year)) {
                return null;
            }

            // package values up for return
            return (fillIntervalValueArray(sign, year, 0));
        } else {
            return null;
        }
    }

    /**
     * Validates an INTERVAL literal against a YEAR TO MONTH interval qualifier.
     *
     * @return null if the interval value is illegal.
     */
    private int [] evaluateIntervalLiteralAsYearToMonth(
        int sign,
        String value)
    {
        int year, month;

        // validate as YEAR(startPrecision) TO MONTH, e.g. 'YY-DD'
        String intervalPattern = "(\\d{1," + startPrecision + "})-(\\d{1,2})";

        Matcher m = Pattern.compile(intervalPattern).matcher(value);
        if (m.matches()) {
            // Break out  field values
            try {
                year = Integer.parseInt(m.group(1));
                month = Integer.parseInt(m.group(2));
            } catch (NumberFormatException e) {
                return null;
            }

            // Validate individual fields
            if (!(isLeadFieldInRange(year, TimeUnit.Year))
                || !(isSecondaryFieldInRange(month, TimeUnit.Month)))
            {
                return null;
            }

            // package values up for return
            return (fillIntervalValueArray(sign, year, month));
        } else {
            return null;
        }
    }

    /**
     * Validates an INTERVAL literal against a MONTH interval qualifier.
     *
     * @return null if the interval value is illegal.
     */
    private int [] evaluateIntervalLiteralAsMonth(
        int sign,
        String value)
    {
        int month;

        // validate as MONTH(startPrecision), e.g. 'MM'
        String intervalPattern = "(\\d{1," + startPrecision + "})";

        Matcher m = Pattern.compile(intervalPattern).matcher(value);
        if (m.matches()) {
            // Break out  field values
            try {
                month = Integer.parseInt(m.group(1));
            } catch (NumberFormatException e) {
                return null;
            }

            // Validate individual fields
            if (!isLeadFieldInRange(month, TimeUnit.Month)) {
                return null;
            }

            // package values up for return
            return (fillIntervalValueArray(sign, 0, month));
        } else {
            return null;
        }
    }

    /**
     * Validates an INTERVAL literal against a DAY interval qualifier.
     *
     * @return null if the interval value is illegal.
     */
    private int [] evaluateIntervalLiteralAsDay(
        int sign,
        String value)
    {
        int day;

        // validate as DAY(startPrecision), e.g. 'DD'
        String intervalPattern = "(\\d{1," + startPrecision + "})";

        Matcher m = Pattern.compile(intervalPattern).matcher(value);
        if (m.matches()) {
            // Break out  field values
            try {
                day = Integer.parseInt(m.group(1));
            } catch (NumberFormatException e) {
                return null;
            }

            // Validate individual fields
            if (!isLeadFieldInRange(day, TimeUnit.Day)) {
                return null;
            }

            // package values up for return
            return (fillIntervalValueArray(sign, day, 0, 0, 0, 0));
        } else {
            return null;
        }
    }

    /**
     * Validates an INTERVAL literal against a DAY TO HOUR interval qualifier.
     *
     * @return null if the interval value is illegal.
     */
    private int [] evaluateIntervalLiteralAsDayToHour(
        int sign,
        String value)
    {
        int day, hour;

        // validate as DAY(startPrecision) TO HOUR, e.g. 'DD HH'
        String intervalPattern = "(\\d{1," + startPrecision + "}) (\\d{1,2})";

        Matcher m = Pattern.compile(intervalPattern).matcher(value);
        if (m.matches()) {
            // Break out  field values
            try {
                day = Integer.parseInt(m.group(1));
                hour = Integer.parseInt(m.group(2));
            } catch (NumberFormatException e) {
                return null;
            }

            // Validate individual fields
            if (!(isLeadFieldInRange(day, TimeUnit.Day))
                || !(isSecondaryFieldInRange(hour, TimeUnit.Hour)))
            {
                return null;
            }

            // package values up for return
            return (fillIntervalValueArray(sign, day, hour, 0, 0, 0));
        } else {
            return null;
        }
    }

    /**
     * Validates an INTERVAL literal against a DAY TO MINUTE interval qualifier.
     *
     * @return null if the interval value is illegal.
     */
    private int [] evaluateIntervalLiteralAsDayToMinute(
        int sign,
        String value)
    {
        int day, hour, minute;

        // validate as DAY(startPrecision) TO MINUTE, e.g. 'DD HH:MM'
        String intervalPattern =
            "(\\d{1," + startPrecision + "}) (\\d{1,2}):(\\d{1,2})";

        Matcher m = Pattern.compile(intervalPattern).matcher(value);
        if (m.matches()) {
            // Break out  field values
            try {
                day = Integer.parseInt(m.group(1));
                hour = Integer.parseInt(m.group(2));
                minute = Integer.parseInt(m.group(3));
            } catch (NumberFormatException e) {
                return null;
            }

            // Validate individual fields
            if (!(isLeadFieldInRange(day, TimeUnit.Day))
                || !(isSecondaryFieldInRange(hour, TimeUnit.Hour))
                || !(isSecondaryFieldInRange(minute, TimeUnit.Minute)))
            {
                return null;
            }

            // package values up for return
            return (fillIntervalValueArray(sign, day, hour, minute, 0, 0));
        } else {
            return null;
        }
    }

    /**
     * Validates an INTERVAL literal against a DAY TO SECOND interval qualifier.
     *
     * @return null if the interval value is illegal.
     */
    private int [] evaluateIntervalLiteralAsDayToSecond(
        int sign,
        String value)
    {
        int day, hour, minute, second, secondFrac;
        boolean hasFractionalSecond;

        // validate as DAY(startPrecision) TO MINUTE,
        // e.g. 'DD HH:MM:SS' or 'DD HH:MM:SS.SSS'
        // Note: must check two patterns, since fractional second is optional
        String intervalPatternWithFracSec =
            "(\\d{1," + startPrecision + "})"
            + " (\\d{1,2}):(\\d{1,2}):(\\d{1,2})\\.(\\d{1,"
            + fractionalSecondPrecision + "})";
        String intervalPatternWithoutFracSec =
            "(\\d{1," + startPrecision + "})"
            + " (\\d{1,2}):(\\d{1,2}):(\\d{1,2})";

        Matcher m = Pattern.compile(intervalPatternWithFracSec).matcher(value);
        if (m.matches()) {
            hasFractionalSecond = true;
        } else {
            m = Pattern.compile(intervalPatternWithoutFracSec).matcher(value);
            hasFractionalSecond = false;
        }

        if (m.matches()) {
            // Break out  field values
            try {
                day = Integer.parseInt(m.group(1));
                hour = Integer.parseInt(m.group(2));
                minute = Integer.parseInt(m.group(3));
                second = Integer.parseInt(m.group(4));
            } catch (NumberFormatException e) {
                return null;
            }

            if (hasFractionalSecond) {
                secondFrac = normalizeSecondFraction(m.group(5));
            } else {
                secondFrac = 0;
            }

            // Validate individual fields
            if (!(isLeadFieldInRange(day, TimeUnit.Day))
                || !(isSecondaryFieldInRange(hour, TimeUnit.Hour))
                || !(isSecondaryFieldInRange(minute, TimeUnit.Minute))
                || !(isSecondaryFieldInRange(second, TimeUnit.Second))
                || !(isFractionalSecondFieldInRange(secondFrac)))
            {
                return null;
            }

            // package values up for return
            return (fillIntervalValueArray(
                sign,
                day,
                hour,
                minute,
                second,
                secondFrac));
        } else {
            return null;
        }
    }

    /**
     * Validates an INTERVAL literal against an HOUR interval qualifier.
     *
     * @return null if the interval value is illegal.
     */
    private int [] evaluateIntervalLiteralAsHour(
        int sign,
        String value)
    {
        int hour;

        // validate as HOUR(startPrecision), e.g. 'HH'
        String intervalPattern = "(\\d{1," + startPrecision + "})";

        Matcher m = Pattern.compile(intervalPattern).matcher(value);
        if (m.matches()) {
            // Break out  field values
            try {
                hour = Integer.parseInt(m.group(1));
            } catch (NumberFormatException e) {
                return null;
            }

            // Validate individual fields
            if (!isLeadFieldInRange(hour, TimeUnit.Hour)) {
                return null;
            }

            // package values up for return
            return (fillIntervalValueArray(sign, 0, hour, 0, 0, 0));
        } else {
            return null;
        }
    }

    /**
     * Validates an INTERVAL literal against an HOUR TO MINUTE interval
     * qualifier.
     *
     * @return null if the interval value is illegal.
     */
    private int [] evaluateIntervalLiteralAsHourToMinute(
        int sign,
        String value)
    {
        int hour, minute;

        // validate as HOUR(startPrecision) TO MINUTE, e.g. 'HH:MM'
        String intervalPattern = "(\\d{1," + startPrecision + "}):(\\d{1,2})";

        Matcher m = Pattern.compile(intervalPattern).matcher(value);
        if (m.matches()) {
            // Break out  field values
            try {
                hour = Integer.parseInt(m.group(1));
                minute = Integer.parseInt(m.group(2));
            } catch (NumberFormatException e) {
                return null;
            }

            // Validate individual fields
            if (!(isLeadFieldInRange(hour, TimeUnit.Hour))
                || !(isSecondaryFieldInRange(minute, TimeUnit.Minute)))
            {
                return null;
            }

            // package values up for return
            return (fillIntervalValueArray(sign, 0, hour, minute, 0, 0));
        } else {
            return null;
        }
    }

    /**
     * Validates an INTERVAL literal against an HOUR TO SECOND interval
     * qualifier.
     *
     * @return null if the interval value is illegal.
     */
    private int [] evaluateIntervalLiteralAsHourToSecond(
        int sign,
        String value)
    {
        int hour, minute, second, secondFrac;
        boolean hasFractionalSecond;

        // validate as HOUR(startPrecision) TO SECOND,
        // e.g. 'HH:MM:SS' or 'HH:MM:SS.SSS'
        // Note: must check two patterns, since fractional second is optional
        String intervalPatternWithFracSec =
            "(\\d{1," + startPrecision + "}):(\\d{1,2}):(\\d{1,2})\\.(\\d{1,"
            + fractionalSecondPrecision + "})";
        String intervalPatternWithoutFracSec =
            "(\\d{1," + startPrecision + "}):(\\d{1,2}):(\\d{1,2})";

        Matcher m = Pattern.compile(intervalPatternWithFracSec).matcher(value);
        if (m.matches()) {
            hasFractionalSecond = true;
        } else {
            m = Pattern.compile(intervalPatternWithoutFracSec).matcher(value);
            hasFractionalSecond = false;
        }

        if (m.matches()) {
            // Break out  field values
            try {
                hour = Integer.parseInt(m.group(1));
                minute = Integer.parseInt(m.group(2));
                second = Integer.parseInt(m.group(3));
            } catch (NumberFormatException e) {
                return null;
            }

            if (hasFractionalSecond) {
                secondFrac = normalizeSecondFraction(m.group(4));
            } else {
                secondFrac = 0;
            }

            // Validate individual fields
            if (!(isLeadFieldInRange(hour, TimeUnit.Hour))
                || !(isSecondaryFieldInRange(minute, TimeUnit.Minute))
                || !(isSecondaryFieldInRange(second, TimeUnit.Second))
                || !(isFractionalSecondFieldInRange(secondFrac)))
            {
                return null;
            }

            // package values up for return
            return (fillIntervalValueArray(
                sign,
                0,
                hour,
                minute,
                second,
                secondFrac));
        } else {
            return null;
        }
    }

    /**
     * Validates an INTERVAL literal against an MINUTE interval qualifier.
     *
     * @return null if the interval value is illegal.
     */
    private int [] evaluateIntervalLiteralAsMinute(
        int sign,
        String value)
    {
        int minute;

        // validate as MINUTE(startPrecision), e.g. 'MM'
        String intervalPattern = "(\\d{1," + startPrecision + "})";

        Matcher m = Pattern.compile(intervalPattern).matcher(value);
        if (m.matches()) {
            // Break out  field values
            try {
                minute = Integer.parseInt(m.group(1));
            } catch (NumberFormatException e) {
                return null;
            }

            // Validate individual fields
            if (!isLeadFieldInRange(minute, TimeUnit.Minute)) {
                return null;
            }

            // package values up for return
            return (fillIntervalValueArray(sign, 0, 0, minute, 0, 0));
        } else {
            return null;
        }
    }

    /**
     * Validates an INTERVAL literal against an MINUTE TO SECOND interval
     * qualifier.
     *
     * @return null if the interval value is illegal.
     */
    private int [] evaluateIntervalLiteralAsMinuteToSecond(
        int sign,
        String value)
    {
        int minute, second, secondFrac;
        boolean hasFractionalSecond;

        // validate as MINUTE(startPrecision) TO SECOND,
        // e.g. 'MM:SS' or 'MM:SS.SSS'
        // Note: must check two patterns, since fractional second is optional
        String intervalPatternWithFracSec =
            "(\\d{1," + startPrecision + "}):(\\d{1,2})\\.(\\d{1,"
            + fractionalSecondPrecision + "})";
        String intervalPatternWithoutFracSec =
            "(\\d{1," + startPrecision + "}):(\\d{1,2})";

        Matcher m = Pattern.compile(intervalPatternWithFracSec).matcher(value);
        if (m.matches()) {
            hasFractionalSecond = true;
        } else {
            m = Pattern.compile(intervalPatternWithoutFracSec).matcher(value);
            hasFractionalSecond = false;
        }

        if (m.matches()) {
            // Break out  field values
            try {
                minute = Integer.parseInt(m.group(1));
                second = Integer.parseInt(m.group(2));
            } catch (NumberFormatException e) {
                return null;
            }

            if (hasFractionalSecond) {
                secondFrac = normalizeSecondFraction(m.group(3));
            } else {
                secondFrac = 0;
            }

            // Validate individual fields
            if (!(isLeadFieldInRange(minute, TimeUnit.Minute))
                || !(isSecondaryFieldInRange(second, TimeUnit.Second))
                || !(isFractionalSecondFieldInRange(secondFrac)))
            {
                return null;
            }

            // package values up for return
            return (fillIntervalValueArray(
                sign,
                0,
                0,
                minute,
                second,
                secondFrac));
        } else {
            return null;
        }
    }

    /**
     * Validates an INTERVAL literal against an SECOND interval qualifier.
     *
     * @return null if the interval value is illegal.
     */
    private int [] evaluateIntervalLiteralAsSecond(
        int sign,
        String value)
    {
        int second, secondFrac;
        boolean hasFractionalSecond;

        // validate as SECOND(startPrecision, fractionalSecondPrecision)
        // e.g. 'SS' or 'SS.SSS'
        // Note: must check two patterns, since fractional second is optional
        String intervalPatternWithFracSec =
            "(\\d{1," + startPrecision + "})\\.(\\d{1,"
            + fractionalSecondPrecision + "})";
        String intervalPatternWithoutFracSec =
            "(\\d{1," + startPrecision + "})";

        Matcher m = Pattern.compile(intervalPatternWithFracSec).matcher(value);
        if (m.matches()) {
            hasFractionalSecond = true;
        } else {
            m = Pattern.compile(intervalPatternWithoutFracSec).matcher(value);
            hasFractionalSecond = false;
        }

        if (m.matches()) {
            // Break out  field values
            try {
                second = Integer.parseInt(m.group(1));
            } catch (NumberFormatException e) {
                return null;
            }

            if (hasFractionalSecond) {
                secondFrac = normalizeSecondFraction(m.group(2));
            } else {
                secondFrac = 0;
            }

            // Validate individual fields
            if (!(isLeadFieldInRange(second, TimeUnit.Second))
                || !(isFractionalSecondFieldInRange(secondFrac)))
            {
                return null;
            }

            // package values up for return
            return (fillIntervalValueArray(sign, 0, 0, 0, second, secondFrac));
        } else {
            return null;
        }
    }

    /**
     * Validates an INTERVAL literal according to the rules specified by the
     * interval qualifier. The assumption is made that the interval qualfier has
     * been validated prior to calling this method. Evaluating against an
     * invalid qualifier could lead to strange results.
     *
     * @return null if the interval value is illegal.
     */
    public int [] evaluateIntervalLiteral(
        String value)
    {
        // we should have passed the validator
        int [] ret = null;

        // First strip off any leading whitespace
        value = value.trim();

        // check if the sign was explicitly specified.  Record
        // the explicit or implicit sign, and strip it off to
        // simplify pattern matching later.
        int sign = getIntervalSign(value);
        value = stripLeadingSign(value);

        // If we have an empty or null literal at this point,
        // it's illegal.  Complain and bail out.
        if (Util.isNullOrEmpty(value)) {
            return null;
        }

        // Validate remaining string according to the pattern
        // that corresponds to the start and end units as
        // well as explicit or implicit precision and range.
        if (TimeUnit.Year.equals(startUnit)
            && (null == endUnit))
        {
            // YEAR
            ret = evaluateIntervalLiteralAsYear(sign, value);
        }
        if (TimeUnit.Year.equals(startUnit)
            && TimeUnit.Month.equals(endUnit))
        {
            // YEAR TO MONTH
            ret = evaluateIntervalLiteralAsYearToMonth(sign, value);
        } else if (TimeUnit.Month.equals(startUnit)
            && (null == endUnit))
        {
            // MONTH
            ret = evaluateIntervalLiteralAsMonth(sign, value);
        } else if (TimeUnit.Day.equals(startUnit)
            && (null == endUnit))
        {
            // DAY
            ret = evaluateIntervalLiteralAsDay(sign, value);
        } else if (
            TimeUnit.Day.equals(startUnit)
            && TimeUnit.Hour.equals(endUnit))
        {
            // DAY TO HOUR
            ret = evaluateIntervalLiteralAsDayToHour(sign, value);
        } else if (
            TimeUnit.Day.equals(startUnit)
            && TimeUnit.Minute.equals(endUnit))
        {
            // DAY TO MINUTE
            ret = evaluateIntervalLiteralAsDayToMinute(sign, value);
        } else if (
            TimeUnit.Day.equals(startUnit)
            && TimeUnit.Second.equals(endUnit))
        {
            // DAY TO SECOND
            ret = evaluateIntervalLiteralAsDayToSecond(sign, value);
        } else if (TimeUnit.Hour.equals(startUnit)
            && (null == endUnit))
        {
            // HOUR
            ret = evaluateIntervalLiteralAsHour(sign, value);
        } else if (
            TimeUnit.Hour.equals(startUnit)
            && TimeUnit.Minute.equals(endUnit))
        {
            // HOUR TO MINUTE
            ret = evaluateIntervalLiteralAsHourToMinute(sign, value);
        } else if (
            TimeUnit.Hour.equals(startUnit)
            && TimeUnit.Second.equals(endUnit))
        {
            // HOUR TO SECOND
            ret = evaluateIntervalLiteralAsHourToSecond(sign, value);
        } else if (TimeUnit.Minute.equals(startUnit)
            && (null == endUnit))
        {
            // MINUTE
            ret = evaluateIntervalLiteralAsMinute(sign, value);
        } else if (
            TimeUnit.Minute.equals(startUnit)
            && TimeUnit.Second.equals(endUnit))
        {
            // MINUTE TO SECOND
            ret = evaluateIntervalLiteralAsMinuteToSecond(sign, value);
        } else if (TimeUnit.Second.equals(startUnit)
            && (null == endUnit))
        {
            // SECOND
            ret = evaluateIntervalLiteralAsSecond(sign, value);
        }

        return ret;
    }

    public static long getConversion(TimeUnit unit)
    {
        long val = 0;
        switch (unit) {
        case Day:
            val = 24 * 3600000; // number of millis
            break;
        case Hour:
            val = 3600000; // number of millis
            break;
        case Minute:
            val = 60000; // number of millis
            break;
        case Second:
            val = 1000; // number of millis
            break;
        case Year:
            val = 12; // number of months
            break;
        case Month:
            val = 1; // number of months
            break;
        default:
            assert false : "invalid interval qualifier";
            break;
        }
        return val;
    }
}

// End SqlIntervalQualifier.java
