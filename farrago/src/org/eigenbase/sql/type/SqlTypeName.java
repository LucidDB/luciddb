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

import org.eigenbase.util.*;


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
        setNameForJdbcType(Types.DISTINCT, Distinct);
        setNameForJdbcType(Types.STRUCT, Structured);
    }

    //~ Instance fields --------------------------------------------------------

    /**
     * Bitwise-or of flags indicating allowable precision/scale combinations.
     */
    private final int signatures;

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
}

// End SqlTypeName.java
