/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2002-2005 Disruptive Tech
// Copyright (C) 2005-2005 LucidEra, Inc.
// Portions Copyright (C) 2003-2005 John V. Sichi
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
package org.eigenbase.sql.type;

import java.io.*;

import java.sql.*;
import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Calendar;
import java.util.TimeZone;

import org.eigenbase.util.*;
import org.eigenbase.sql.SqlLiteral;
import org.eigenbase.sql.parser.SqlParserPos;


/**
 * Enumeration of the type names which can be used to construct a SQL type.
 * Rationale for this class's existence (instead of just using the standard
 * java.sql.Type ordinals):
 *
 * <ul>
 * <li>java.sql.Type does not include all SQL2003 datatypes
 * <li>SqlTypeName provides a type-safe enumeration
 * <li>SqlTypeName provides a place to hang extra information such as whether
 * the type carries precision and scale
 * </ul>
 *
 * @author jhyde
 * @version $Id$
 * @since Nov 24, 2003
 */
public class SqlTypeName
    extends EnumeratedValues.SerializableValue
{

    //~ Static fields/initializers ---------------------------------------------

    public static final SqlTypeName [] EMPTY_ARRAY = new SqlTypeName[0];

    // Flags indicating precision/scale combinations
    private static final int PrecNoScaleNo = 1;
    private static final int PrecYesScaleNo = 2;
    private static final int PrecYesScaleYes = 4;

    private static SqlTypeName [] jdbcTypeToName;
    public static final int MIN_JDBC_TYPE = Types.BIT;
    public static final int MAX_JDBC_TYPE = Types.REF;

    public static final int MAX_DATETIME_PRECISION = 3;
    public static final int MAX_NUMERIC_PRECISION = 19;
    public static final int MAX_NUMERIC_SCALE = 19;
    public static final int MAX_CHAR_LENGTH = 65536;
    public static final int MAX_BINARY_LENGTH = 65536;

    // SQL Type Definitions ------------------
    public static final int Boolean_ordinal = 0;
    public static final SqlTypeName Boolean =
        new SqlTypeName("BOOLEAN", Boolean_ordinal, PrecNoScaleNo);
    public static final int Tinyint_ordinal = 1;
    public static final SqlTypeName Tinyint =
        new SqlTypeName("TINYINT", Tinyint_ordinal, PrecNoScaleNo);
    public static final int Smallint_ordinal = 2;
    public static final SqlTypeName Smallint =
        new SqlTypeName("SMALLINT", Smallint_ordinal, PrecNoScaleNo);
    public static final int Integer_ordinal = 3;
    public static final SqlTypeName Integer =
        new SqlTypeName("INTEGER", Integer_ordinal, PrecNoScaleNo);
    public static final int Bigint_ordinal = 4;
    public static final SqlTypeName Bigint =
        new SqlTypeName("BIGINT", Bigint_ordinal, PrecNoScaleNo);
    public static final int Decimal_ordinal = 5;
    public static final SqlTypeName Decimal =
        new SqlTypeName("DECIMAL",
            Decimal_ordinal,
            PrecNoScaleNo | PrecYesScaleNo | PrecYesScaleYes);
    public static final int Float_ordinal = 6;
    public static final SqlTypeName Float =
        new SqlTypeName("FLOAT", Float_ordinal, PrecNoScaleNo);
    public static final int Real_ordinal = 7;
    public static final SqlTypeName Real =
        new SqlTypeName("REAL", Real_ordinal, PrecNoScaleNo);
    public static final int Double_ordinal = 8;
    public static final SqlTypeName Double =
        new SqlTypeName("DOUBLE", Double_ordinal, PrecNoScaleNo);
    public static final int Date_ordinal = 9;
    public static final SqlTypeName Date =
        new SqlTypeName("DATE", Date_ordinal, PrecNoScaleNo);
    public static final int Time_ordinal = 10;
    public static final SqlTypeName Time =
        new SqlTypeName("TIME", Time_ordinal, PrecNoScaleNo | PrecYesScaleNo);
    public static final int Timestamp_ordinal = 11;
    public static final SqlTypeName Timestamp =
        new SqlTypeName("TIMESTAMP",
            Timestamp_ordinal,
            PrecNoScaleNo | PrecYesScaleNo);
    public static final int IntervalYearMonth_ordinal = 12;
    public static final SqlTypeName IntervalYearMonth =
        new SqlTypeName("IntervalYearMonth",
            IntervalYearMonth_ordinal,
            PrecNoScaleNo);
    public static final int IntervalDayTime_ordinal = 13;
    public static final SqlTypeName IntervalDayTime =
        new SqlTypeName("IntervalDayTime",
            IntervalDayTime_ordinal,
            PrecNoScaleNo);
    public static final int Char_ordinal = 14;
    public static final SqlTypeName Char =
        new SqlTypeName("CHAR", Char_ordinal, PrecNoScaleNo | PrecYesScaleNo);
    public static final int Varchar_ordinal = 15;
    public static final SqlTypeName Varchar =
        new SqlTypeName("VARCHAR",
            Varchar_ordinal,
            PrecNoScaleNo | PrecYesScaleNo);
    public static final int Binary_ordinal = 16;
    public static final SqlTypeName Binary =
        new SqlTypeName("BINARY",
            Binary_ordinal,
            PrecNoScaleNo | PrecYesScaleNo);
    public static final int Varbinary_ordinal = 17;
    public static final SqlTypeName Varbinary =
        new SqlTypeName("VARBINARY",
            Varbinary_ordinal,
            PrecNoScaleNo | PrecYesScaleNo);
    public static final int Null_ordinal = 18;
    public static final SqlTypeName Null =
        new SqlTypeName("NULL", Null_ordinal, PrecNoScaleNo);
    public static final int Any_ordinal = 19;
    public static final SqlTypeName Any =
        new SqlTypeName("ANY", Any_ordinal, PrecNoScaleNo);
    public static final int Symbol_ordinal = 20;
    public static final SqlTypeName Symbol =
        new SqlTypeName("SYMBOL", Symbol_ordinal, PrecNoScaleNo);
    public static final int Multiset_ordinal = 21;
    public static final SqlTypeName Multiset =
        new SqlTypeName("MULTISET", Multiset_ordinal, PrecNoScaleNo);
    public static final int Distinct_ordinal = 22;
    public static final SqlTypeName Distinct =
        new SqlTypeName("DISTINCT", Distinct_ordinal, PrecNoScaleNo);
    public static final int Structured_ordinal = 23;
    public static final SqlTypeName Structured =
        new SqlTypeName("STRUCTURED", Structured_ordinal, PrecNoScaleNo);
    public static final int Row_ordinal = 24;
    public static final SqlTypeName Row =
        new SqlTypeName("ROW", Row_ordinal, PrecNoScaleNo);
    public static final int Cursor_ordinal = 25;
    public static final SqlTypeName Cursor =
        new SqlTypeName("CURSOR", Cursor_ordinal, PrecNoScaleNo);
    public static final int ColumnList_ordinal = 26;
    public static final SqlTypeName ColumnList =
        new SqlTypeName("COLUMN_LIST", ColumnList_ordinal, PrecNoScaleNo);

    /**
     * Array of all allowable {@link SqlTypeName} values.
     */
    public static final SqlTypeName [] allTypes =
        new SqlTypeName[] {
            Boolean, Integer, Varchar, Date, Time, Timestamp, Null, Decimal,
            Any, Char, Binary, Varbinary, Tinyint, Smallint, Bigint, Real,
            Double, Symbol, IntervalYearMonth, IntervalDayTime,
            Float, Multiset, Distinct, Structured, Row, Cursor, ColumnList
        };

    // categorizations used by SqlTypeFamily definitions

    public static final SqlTypeName [] booleanTypes = {
            Boolean
        };

    public static final SqlTypeName [] binaryTypes = {
            Binary, Varbinary
        };

    public static final SqlTypeName [] intTypes =
        {
            Tinyint, Smallint, Integer, Bigint
        };

    public static final SqlTypeName [] exactTypes =
        combine(
            intTypes,
            new SqlTypeName[] { Decimal });

    public static final SqlTypeName [] approxTypes = {
            Float, Real, Double
        };

    public static final SqlTypeName [] numericTypes =
        combine(exactTypes, approxTypes);

    public static final SqlTypeName [] fractionalTypes =
        combine(
            approxTypes,
            new SqlTypeName[] { Decimal });

    public static final SqlTypeName [] charTypes = {
            Char, Varchar
        };

    public static final SqlTypeName [] stringTypes =
        combine(charTypes, binaryTypes);

    public static final SqlTypeName [] datetimeTypes =
        {
            Date, Time, Timestamp
        };

    public static final SqlTypeName [] timeIntervalTypes =
        {
            IntervalDayTime, IntervalYearMonth
        };

    public static final SqlTypeName [] multisetTypes = {
            Multiset
        };

    public static final SqlTypeName [] cursorTypes = {
            Cursor
        };
    
    public static final SqlTypeName [] columnListTypes = {
            ColumnList
        };

    /**
     * Enumeration of all allowable {@link SqlTypeName} values.
     */
    public static final EnumeratedValues enumeration =
        new EnumeratedValues(allTypes);

    static {
        // This squanders some memory since MAX_JDBC_TYPE == 2006!
        jdbcTypeToName = new SqlTypeName[(1 + MAX_JDBC_TYPE) - MIN_JDBC_TYPE];

        setNameForJdbcType(Types.TINYINT, Tinyint);
        setNameForJdbcType(Types.SMALLINT, Smallint);
        setNameForJdbcType(Types.BIGINT, Bigint);
        setNameForJdbcType(Types.INTEGER, Integer);
        setNameForJdbcType(Types.NUMERIC, Decimal); // REVIEW
        setNameForJdbcType(Types.DECIMAL, Decimal);

        setNameForJdbcType(Types.FLOAT, Float);
        setNameForJdbcType(Types.REAL, Real);
        setNameForJdbcType(Types.DOUBLE, Double);

        setNameForJdbcType(Types.CHAR, Char);
        setNameForJdbcType(Types.VARCHAR, Varchar);

        // TODO
        // setNameForJdbcType(Types.LONGVARCHAR, Longvarchar);
        // setNameForJdbcType(Types.CLOB, Clob);
        // setNameForJdbcType(Types.LONGVARBINARY, Longvarbinary);
        // setNameForJdbcType(Types.BLOB, Blob);

        setNameForJdbcType(Types.BINARY, Binary);
        setNameForJdbcType(Types.VARBINARY, Varbinary);

        setNameForJdbcType(Types.DATE, Date);
        setNameForJdbcType(Types.TIME, Time);
        setNameForJdbcType(Types.TIMESTAMP, Timestamp);
        setNameForJdbcType(Types.BIT, Boolean);
        setNameForJdbcType(Types.BOOLEAN, Boolean);

        // TODO
        // setNameForJdbcType(Types.DISTINCT, Distinct);
        // setNameForJdbcType(Types.STRUCT, Structured);
    }

    //~ Instance fields --------------------------------------------------------

    /**
     * Bitwise-or of flags indicating allowable precision/scale combinations.
     */
    private final int signatures;
    private static final BigDecimal TWO = new BigDecimal(2);

    //~ Constructors -----------------------------------------------------------

    private SqlTypeName(
        String name,
        int ordinal,
        int signatures)
    {
        super(name, ordinal, null);
        this.signatures = signatures;
    }

    //~ Methods ----------------------------------------------------------------

    /**
     * Looks up a type name from its ordinal.
     */
    public static SqlTypeName get(int ordinal)
    {
        return (SqlTypeName) enumeration.getValue(ordinal);
    }

    /**
     * Looks up a type name from its name.
     */
    public static SqlTypeName get(String name)
    {
        if (enumeration.containsName(name)) {
            return (SqlTypeName) enumeration.getValue(name);
        } else {
            return null;
        }
    }

    /**
     * Returns true if <code>name</code> is defined in {@link
     * SqlTypeName#enumeration}; otherwise, it returns false.
     *
     * @param name
     */
    public static boolean containsName(String name)
    {
        return enumeration.containsName(name);
    }

    public boolean allowsNoPrecNoScale()
    {
        return (signatures & PrecNoScaleNo) != 0;
    }

    public boolean allowsPrecNoScale()
    {
        return (signatures & PrecYesScaleNo) != 0;
    }

    public boolean allowsPrec()
    {
        return allowsPrecScale(true, true)
            || allowsPrecScale(true, false);
    }

    public boolean allowsScale()
    {
        return allowsPrecScale(true, true);
    }

    /**
     * Returns whether this type can be specified with a given combination of
     * precision and scale. For example,
     *
     * <ul>
     * <li><code>Varchar.allowsPrecScale(true, false)</code> returns <code>
     * true</code>, because the VARCHAR type allows a precision parameter, as in
     * <code>VARCHAR(10)</code>.</li>
     * <li><code>Varchar.allowsPrecScale(true, true)</code> returns <code>
     * true</code>, because the VARCHAR type does not allow a precision and a
     * scale parameter, as in <code>VARCHAR(10, 4)</code>.</li>
     * <li><code>allowsPrecScale(false, true)</code> returns <code>false</code>
     * for every type.</li>
     * </ul>
     *
     * @param precision Whether the precision/length field is part of the type
     * specification
     * @param scale Whether the scale field is part of the type specification
     *
     * @return Whether this combination of precision/scale is valid
     */
    public boolean allowsPrecScale(
        boolean precision,
        boolean scale)
    {
        int mask =
            precision ? (scale ? PrecYesScaleYes : PrecYesScaleNo)
            : (scale ? 0 : PrecNoScaleNo);
        return (signatures & mask) != 0;
    }

    /**
     * Returns true if not of a "pure" standard sql type. "Inpure" types are
     * {@link #Any}, {@link #Null} and {@link #Symbol}
     */
    public boolean isSpecial()
    {
        switch (getOrdinal()) {
        case Any_ordinal:
        case Null_ordinal:
        case Symbol_ordinal:
            return true;
        }

        return false;
    }

    /**
     * @return the ordinal from {@link java.sql.Types} corresponding to this
     * SqlTypeName
     */
    public int getJdbcOrdinal()
    {
        switch (getOrdinal()) {
        case Boolean_ordinal:
            return Types.BOOLEAN;
        case Tinyint_ordinal:
            return Types.TINYINT;
        case Smallint_ordinal:
            return Types.SMALLINT;
        case Integer_ordinal:
            return Types.INTEGER;
        case Bigint_ordinal:
            return Types.BIGINT;
        case Decimal_ordinal:
            return Types.DECIMAL;
        case Float_ordinal:
            return Types.FLOAT;
        case Real_ordinal:
            return Types.REAL;
        case Double_ordinal:
            return Types.DOUBLE;
        case Date_ordinal:
            return Types.DATE;
        case Time_ordinal:
            return Types.TIME;
        case Timestamp_ordinal:
            return Types.TIMESTAMP;
        case Char_ordinal:
            return Types.CHAR;
        case Varchar_ordinal:
            return Types.VARCHAR;
        case Binary_ordinal:
            return Types.BINARY;
        case Varbinary_ordinal:
            return Types.VARBINARY;
        case Null_ordinal:
            return Types.NULL;
        case Multiset_ordinal:
            return Types.ARRAY;
        case Distinct_ordinal:
            return Types.DISTINCT;
        case Row_ordinal:
        case Structured_ordinal:
            return Types.STRUCT;
        case Cursor_ordinal:
            return Types.OTHER + 1;
        case ColumnList_ordinal:
            return Types.OTHER + 2;
        default:
            return Types.OTHER;
        }
    }

    private static SqlTypeName [] combine(SqlTypeName [] array0,
        SqlTypeName [] array1)
    {
        SqlTypeName [] ret = new SqlTypeName[array0.length + array1.length];
        System.arraycopy(array0, 0, ret, 0, array0.length);
        System.arraycopy(array1, 0, ret, array0.length, array1.length);
        return ret;
    }

    /**
     * @return default precision for this type if supported, otherwise -1 if
     * precision is either unsupported or must be specified explicitly
     */
    public int getDefaultPrecision()
    {
        switch (getOrdinal()) {
        case Char_ordinal:
        case Binary_ordinal:
        case Varchar_ordinal:
        case Varbinary_ordinal:
            return 1;
        case Time_ordinal:
            return 0;
        case Timestamp_ordinal:

            // TODO jvs 26-July-2004:  should be 6 for microseconds,
            // but we can't support that yet
            return 0;
        case Decimal_ordinal:
            return MAX_NUMERIC_PRECISION;
        default:
            return -1;
        }
    }

    /**
     * @return default scale for this type if supported, otherwise -1 if scale
     * is either unsupported or must be specified explicitly
     */
    public int getDefaultScale()
    {
        switch (getOrdinal()) {
        case Decimal_ordinal:
            return 0;
        default:
            return -1;
        }
    }

    /**
     * Gets the SqlTypeFamily containing this SqlTypeName.
     *
     * @return containing family, or null for none
     */
    public SqlTypeFamily getFamily()
    {
        return SqlTypeFamily.getFamilyForSqlType(this);
    }

    /**
     * Gets the SqlTypeName corresponding to a JDBC type.
     *
     * @param jdbcType the JDBC type of interest
     *
     * @return corresponding SqlTypeName
     */
    public static SqlTypeName getNameForJdbcType(int jdbcType)
    {
        return jdbcTypeToName[jdbcType - MIN_JDBC_TYPE];
    }

    private static void setNameForJdbcType(
        int jdbcType,
        SqlTypeName name)
    {
        jdbcTypeToName[jdbcType - MIN_JDBC_TYPE] = name;
    }

    /**
     * Retrieves a matching SqlTypeName instance based on the <code>
     * _ordinal</code> deserialized by {@link
     * EnumeratedValues.SerializableValue#readObject}. Current instance is the
     * candidate object deserialized from the ObjectInputStream. It is
     * incomplete, cannot be used as-is, and this method must return a valid
     * replacement.
     *
     * @return replacement instance that matches <code>_ordinal</code>
     *
     * @throws java.io.ObjectStreamException
     */
    protected Object readResolve()
        throws ObjectStreamException
    {
        return SqlTypeName.get(_ordinal);
    }

    /**
     * Returns the limit of this datatype.
     *
     * For example,
     * <table border="1">
     * <tr>
     * <th>Datatype</th>
     * <th>sign</th><th>limit</th><th>beyond</th><th>precision</th><th>scale</th>
     * <th>Returns</th>
     * </tr>
     * <tr>
     * <td>Integer</th>
     * <td>true</td><td>true</td><td>false</td><td>-1</td><td>-1</td>
     * <td>2147483647 (2 ^ 31 -1 = MAXINT)</td>
     * </tr>
     * <tr>
     * <td>Integer</th>
     * <td>true</td><td>true</td><td>true</td><td>-1</td><td>-1</td>
     * <td>2147483648 (2 ^ 31 = MAXINT + 1)</td>
     * </tr>
     * <tr>
     * <td>Integer</th>
     * <td>false</td><td>true</td><td>false</td><td>-1</td><td>-1</td>
     * <td>-2147483648 (-2 ^ 31 = MININT)</td>
     * </tr>
     * <tr>
     * <td>Boolean</th>
     * <td>true</td><td>true</td><td>false</td><td>-1</td><td>-1</td>
     * <td>TRUE</td>
     * </tr>
     * <tr>
     * <td>Varchar</th>
     * <td>true</td><td>true</td><td>false</td><td>10</td><td>-1</td>
     * <td>'ZZZZZZZZZZ'</td>
     * </tr>
     * </table>
     *
     * @param sign If true, returns upper limit, otherwise lower limit
     * @param limit If true, returns value at or near to overflow; otherwise
     *   value at or near to underflow
     * @param beyond If true, returns the value just beyond the limit,
     *   otherwise the value at the limit
     * @param precision Precision, or -1 if not applicable
     * @param scale Scale, or -1 if not applicable
     * @return Limit value
     */
    public Object getLimit(
        boolean sign, Limit limit, boolean beyond, int precision, int scale)
    {
        assert allowsPrecScale(precision != -1, scale != -1) : this;
        if (limit == Limit.ZERO) {
            if (beyond) {
                return null;
            }
            sign = true;
        }
        Calendar calendar;

        switch (this.getOrdinal()) {
        case Boolean_ordinal:
            switch (limit) {
            case ZERO:
                return false;
            case UNDERFLOW:
                return null;
            case OVERFLOW:
                if (beyond || !sign) {
                    return null;
                } else {
                    return true;
                }
            default:
                throw Util.unexpected(limit);
            }

        case Tinyint_ordinal:
            return getNumericLimit(2, 8, sign, limit, beyond);

        case Smallint_ordinal:
            return getNumericLimit(2, 16, sign, limit, beyond);

        case Integer_ordinal:
            return getNumericLimit(2, 32, sign, limit, beyond);

        case Bigint_ordinal:
            return getNumericLimit(2, 64, sign, limit, beyond);

        case Decimal_ordinal:
            BigDecimal decimal =
                getNumericLimit(10, precision, sign, limit, beyond);
            if (decimal == null) {
                return null;
            }

            // Decimal values must fit into 64 bits. So, the maximum value of
            // a DECIMAL(19, 0) is 2^63 - 1, not 10^19 - 1.
            switch (limit) {
            case OVERFLOW:
                final BigDecimal other =
                    (BigDecimal) Bigint.getLimit(sign, limit, beyond, -1, -1);
                if (decimal.compareTo(other) == (sign ? 1 : -1)) {
                    decimal = other;
                }
            }

            // Apply scale.
            if (scale == 0) {
                ;
            } else if (scale > 0) {
                decimal = decimal.divide(BigDecimal.TEN.pow(scale));
            } else {
                decimal = decimal.multiply(BigDecimal.TEN.pow(-scale));
            }
            return decimal;

        case Char_ordinal:
        case Varchar_ordinal:
            if (!sign) {
                return null; // this type does not have negative values
            }
            StringBuilder buf = new StringBuilder();
            switch (limit) {
            case ZERO:
                break;
            case UNDERFLOW:
                if (beyond) {
                    // There is no value between the empty string and the
                    // smallest non-empty string.
                    return null;
                }
                buf.append("a");
                break;
            case OVERFLOW:
                for (int i = 0; i < precision; ++i) {
                    buf.append("Z");
                }
                if (beyond) {
                    buf.append("Z");
                }
                break;
            }
            return buf.toString();

        case Binary_ordinal:
        case Varbinary_ordinal:
            if (!sign) {
                return null; // this type does not have negative values
            }
            byte[] bytes;
            switch (limit) {
            case ZERO:
                bytes = new byte[0];
                break;
            case UNDERFLOW:
                if (beyond) {
                    // There is no value between the empty string and the
                    // smallest value.
                    return null;
                }
                bytes = new byte[] {0x00};
                break;
            case OVERFLOW:
                bytes = new byte[precision + (beyond ? 1 : 0)];
                Arrays.fill(bytes, (byte) 0xff);
                break;
            default:
                throw Util.unexpected(limit);
            }
            return bytes;

        case Date_ordinal:
            calendar = Calendar.getInstance();
            switch (limit) {
            case ZERO:
                // The epoch.
                calendar.set(Calendar.YEAR, 1970);
                calendar.set(Calendar.MONTH, 0);
                calendar.set(Calendar.DAY_OF_MONTH, 1);
                break;
            case UNDERFLOW:
                return null;
            case OVERFLOW:
                if (beyond) {
                    // It is impossible to represent an invalid year as a date
                    // literal. SQL dates are represented as 'yyyy-mm-dd', and
                    // 1 <= yyyy <= 9999 is valid. There is no year 0: the year
                    // before 1AD is 1BC, so SimpleDateFormat renders the day
                    // before 0001-01-01 (AD) as 0001-12-31 (BC), which looks
                    // like a valid date.
                    return null;
                }
                // "SQL:2003 6.1 <data type> Access Rules 6" says that year is
                // between 1 and 9999, and days/months are the valid Gregorian
                // calendar values for these years.
                if (sign) {
                    calendar.set(Calendar.YEAR, 9999);
                    calendar.set(Calendar.MONTH, 11);
                    calendar.set(Calendar.DAY_OF_MONTH, 31);
                } else {
                    calendar.set(Calendar.YEAR, 1);
                    calendar.set(Calendar.MONTH, 0);
                    calendar.set(Calendar.DAY_OF_MONTH, 1);
                }
                break;
            }
            calendar.set(Calendar.HOUR_OF_DAY, 0);
            calendar.set(Calendar.MINUTE, 0);
            calendar.set(Calendar.SECOND, 0);
            return calendar;

        case Time_ordinal:
            if (!sign) {
                return null; // this type does not have negative values
            }
            if (beyond) {
                return null; // invalid values are impossible to represent
            }
            calendar = Calendar.getInstance(TimeZone.getTimeZone("GMT"));
            switch (limit) {
            case ZERO:
                // The epoch.
                calendar.set(Calendar.HOUR_OF_DAY, 0);
                calendar.set(Calendar.MINUTE, 0);
                calendar.set(Calendar.SECOND, 0);
                calendar.set(Calendar.MILLISECOND, 0);
                break;
            case UNDERFLOW:
                return null;
            case OVERFLOW:
                calendar.set(Calendar.HOUR_OF_DAY, 23);
                calendar.set(Calendar.MINUTE, 59);
                calendar.set(Calendar.SECOND, 59);
                int millis =
                    precision >= 3 ? 999 :
                    precision == 2 ? 990 :
                    precision == 1 ? 900 :
                    0;
                calendar.set(Calendar.MILLISECOND, millis);
                break;
            }
            return calendar;

        case Timestamp_ordinal:
            calendar = Calendar.getInstance(TimeZone.getTimeZone("GMT"));
            switch (limit) {
            case ZERO:
                // The epoch.
                calendar.set(Calendar.YEAR, 1970);
                calendar.set(Calendar.MONTH, 0);
                calendar.set(Calendar.DAY_OF_MONTH, 1);
                calendar.set(Calendar.HOUR_OF_DAY, 0);
                calendar.set(Calendar.MINUTE, 0);
                calendar.set(Calendar.SECOND, 0);
                calendar.set(Calendar.MILLISECOND, 0);
                break;
            case UNDERFLOW:
                return null;
            case OVERFLOW:
                if (beyond) {
                    // It is impossible to represent an invalid year as a date
                    // literal. SQL dates are represented as 'yyyy-mm-dd', and
                    // 1 <= yyyy <= 9999 is valid. There is no year 0: the year
                    // before 1AD is 1BC, so SimpleDateFormat renders the day
                    // before 0001-01-01 (AD) as 0001-12-31 (BC), which looks
                    // like a valid date.
                    return null;
                }
                // "SQL:2003 6.1 <data type> Access Rules 6" says that year is
                // between 1 and 9999, and days/months are the valid Gregorian
                // calendar values for these years.
                if (sign) {
                    calendar.set(Calendar.YEAR, 9999);
                    calendar.set(Calendar.MONTH, 11);
                    calendar.set(Calendar.DAY_OF_MONTH, 31);
                    calendar.set(Calendar.HOUR_OF_DAY, 23);
                    calendar.set(Calendar.MINUTE, 59);
                    calendar.set(Calendar.SECOND, 59);
                    int millis =
                        precision >= 3 ? 999 :
                        precision == 2 ? 990 :
                        precision == 1 ? 900 :
                        0;
                    calendar.set(Calendar.MILLISECOND, millis);
                } else {
                    calendar.set(Calendar.YEAR, 1);
                    calendar.set(Calendar.MONTH, 0);
                    calendar.set(Calendar.DAY_OF_MONTH, 1);
                    calendar.set(Calendar.HOUR_OF_DAY, 0);
                    calendar.set(Calendar.MINUTE, 0);
                    calendar.set(Calendar.SECOND, 0);
                    calendar.set(Calendar.MILLISECOND, 0);
                }
                break;
            }
            return calendar;

        default:
            throw unexpected();
        }
    }

    /**
     * Returns the maximum precision (or length) allowed for this type,
     * or -1 if precision/length are not applicable for this type.
     *
     * @return Maximum allowed precision
     */
    public int getMaxPrecision()
    {
        switch (getOrdinal()) {
        case Decimal_ordinal:
            return MAX_NUMERIC_PRECISION;
        case Varchar_ordinal:
        case Char_ordinal:
            return MAX_CHAR_LENGTH;
        case Varbinary_ordinal:
        case Binary_ordinal:
            return MAX_BINARY_LENGTH;
        case Time_ordinal:
        case Timestamp_ordinal:
            return MAX_DATETIME_PRECISION;
        default:
            return -1;
        }
    }

    public enum Limit {
        ZERO,
        UNDERFLOW,
        OVERFLOW
    }

    private BigDecimal getNumericLimit(
        int radix,
        int exponent,
        boolean sign,
        Limit limit,
        boolean beyond)
    {
        switch (limit) {
        case OVERFLOW:
            // 2-based schemes run from -2^(N-1) to 2^(N-1)-1 e.g. -128 to +127
            // 10-based schemas run from -(10^N-1) to 10^N-1 e.g. -99 to +99
            final BigDecimal bigRadix = BigDecimal.valueOf(radix);
            if (radix == 2) {
                --exponent;
            }
            BigDecimal decimal = bigRadix.pow(exponent);
            if (sign || radix != 2) {
                decimal = decimal.subtract(BigDecimal.ONE);
            }
            if (beyond) {
                decimal = decimal.add(BigDecimal.ONE);
            }
            if (!sign) {
                decimal = decimal.negate();
            }
            return decimal;
        case UNDERFLOW:
            return beyond ? null :
                sign ? BigDecimal.ONE :
                    BigDecimal.ONE.negate();
        case ZERO:
            return BigDecimal.ZERO;
        default:
            throw Util.unexpected(limit);
        }
    }

    public SqlLiteral createLiteral(Object o, SqlParserPos pos)
    {
        switch (getOrdinal()) {
        case Boolean_ordinal:
            return SqlLiteral.createBoolean((Boolean) o, pos);
        case Tinyint_ordinal:
        case Smallint_ordinal:
        case Integer_ordinal:
        case Bigint_ordinal:
        case Decimal_ordinal:
            return SqlLiteral.createExactNumeric(o.toString(), pos);
        case Varchar_ordinal:
        case Char_ordinal:
            return SqlLiteral.createCharString((String) o, pos);
        case Varbinary_ordinal:
        case Binary_ordinal:
            return SqlLiteral.createBinaryString((byte[]) o, pos);
        case Date_ordinal:
            return SqlLiteral.createDate((Calendar) o, pos);
        case Time_ordinal:
            return SqlLiteral.createTime((Calendar) o, 0 /* todo */, pos);
        case Timestamp_ordinal:
            return SqlLiteral.createTimestamp((Calendar) o, 0 /* todo */, pos);
        default:
            throw unexpected();
        }
    }
}

// End SqlTypeName.java
