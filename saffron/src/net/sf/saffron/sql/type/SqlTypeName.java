/*
// $Id$
// Saffron preprocessor and data engine
// (C) Copyright 2003-2003 Disruptive Technologies, Inc.
// (C) Copyright 2003-2004 John V. Sichi
// You must accept the terms in LICENSE.html to use this software.
//
// This program is free software; you can redistribute it and/or
// modify it under the terms of the GNU Lesser General Public License
// as published by the Free Software Foundation; either version 2.1
// of the License, or (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with this program; if not, write to the Free Software
// Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
*/
package net.sf.saffron.sql.type;

import net.sf.saffron.util.EnumeratedValues;

/**
 * Enumeration of the type names which can be used to construct a SQL type.
 *
 * @author jhyde
 * @since Nov 24, 2003
 * @version $Id$
 **/
public class SqlTypeName extends EnumeratedValues.BasicValue {
    /**
     * Bitwise-or of flags indicating allowable precision/scale combinations.
     */
    private final int signatures;

    private SqlTypeName(String name, int ordinal, int signatures) {
        super(name, ordinal, null);
        this.signatures = signatures;
    }

    // Flags indicating precision/scale combinations
    private static final int PrecNoScaleNo = 1;
    private static final int PrecYesScaleNo = 2;
    private static final int PrecYesScaleYes = 4;

    public static final int Boolean_ordinal = 0;
    public static final SqlTypeName Boolean = new SqlTypeName("BOOLEAN", Boolean_ordinal, PrecNoScaleNo);

    public static final int Integer_ordinal = 1;
    public static final SqlTypeName Integer = new SqlTypeName("INTEGER", Integer_ordinal, PrecNoScaleNo);

    public static final int Varchar_ordinal = 2;
    public static final SqlTypeName Varchar = new SqlTypeName("VARCHAR", Varchar_ordinal, PrecYesScaleNo);

    public static final int Date_ordinal = 3;
    public static final SqlTypeName Date = new SqlTypeName("DATE", Date_ordinal, PrecNoScaleNo);

    public static final int Time_ordinal = 4;
    public static final SqlTypeName Time = new SqlTypeName("TIME", Time_ordinal, PrecNoScaleNo|PrecYesScaleNo);

    public static final int Timestamp_ordinal = 5;
    public static final SqlTypeName Timestamp = new SqlTypeName("TIMESTAMP", Timestamp_ordinal, PrecNoScaleNo|PrecYesScaleNo);

    public static final int Null_ordinal = 6;
    public static final SqlTypeName Null = new SqlTypeName("NULL", Null_ordinal, PrecNoScaleNo);

    public static final int Decimal_ordinal = 7;
    public static final SqlTypeName Decimal = new SqlTypeName("DECIMAL", Decimal_ordinal,PrecNoScaleNo|PrecYesScaleYes);

    public static final int Any_ordinal = 8;
    public static final SqlTypeName Any = new SqlTypeName("ANY", Any_ordinal, PrecNoScaleNo);

    public static final int Char_ordinal = 9;
    public static final SqlTypeName Char = new SqlTypeName("CHAR", Char_ordinal, PrecYesScaleNo);

    public static final int Binary_ordinal = 10;
    public static final SqlTypeName Binary = new SqlTypeName("BINARY", Binary_ordinal, PrecYesScaleNo);

    public static final int Varbinary_ordinal = 11;
    public static final SqlTypeName Varbinary = new SqlTypeName("VARBINARY", Varbinary_ordinal, PrecYesScaleNo);

    public static final int Tinyint_ordinal = 12;
    public static final SqlTypeName Tinyint = new SqlTypeName("TINYINT", Tinyint_ordinal, PrecNoScaleNo);

    public static final int Smallint_ordinal = 13;
    public static final SqlTypeName Smallint = new SqlTypeName("SMALLINT", Smallint_ordinal, PrecNoScaleNo);

    public static final int Bigint_ordinal = 14;
    public static final SqlTypeName Bigint = new SqlTypeName("BIGINT", Bigint_ordinal, PrecNoScaleNo);

    public static final int Real_ordinal = 15;
    public static final SqlTypeName Real = new SqlTypeName("REAL", Real_ordinal, PrecNoScaleNo);

    public static final int Double_ordinal = 16;
    public static final SqlTypeName Double = new SqlTypeName("DOUBLE", Double_ordinal, PrecNoScaleNo);

    public static final int Bit_ordinal = 17;
    public static final SqlTypeName Bit = new SqlTypeName("BIT", Bit_ordinal, PrecYesScaleNo);

    public static final int Symbol_ordinal = 18;
    public static final SqlTypeName Symbol = new SqlTypeName("SYMBOL", Symbol_ordinal, PrecNoScaleNo);

    public static final int IntervalYearToMonth_ordinal = 19;
    public static final SqlTypeName IntervalYearToMonth = new SqlTypeName("IntervalYearToMonth", IntervalYearToMonth_ordinal, PrecNoScaleNo);

    public static final int IntervalDayTime_ordinal = 20;
    public static final SqlTypeName IntervalDayTime = new SqlTypeName("IntervalDayTime", IntervalDayTime_ordinal, PrecYesScaleNo);

    /**
     * List of all allowable {@link SqlTypeName} values.
     */
    public static final EnumeratedValues enumeration = new EnumeratedValues(
            new SqlTypeName[] {
                Boolean, Integer, Varchar, Date, Time, Timestamp, Null,
                Decimal, Any, Char, Binary, Varbinary, Tinyint,
                Smallint, Bigint, Real, Double, Bit, Symbol,
                IntervalYearToMonth, IntervalDayTime
            }
    );
    /**
     * Looks up a type name from its ordinal.
     */
    public static SqlTypeName get(int ordinal) {
        return (SqlTypeName) enumeration.getValue(ordinal);
    }
    /**
     * Looks up a type name from its name.
     */
    public static SqlTypeName get(String name) {
        return (SqlTypeName) enumeration.getValue(name);
    }

    public boolean allowsNoPrecNoScale() {
        return (signatures & PrecNoScaleNo) != 0;
    }
    public boolean allowsPrecNoScale() {
        return (signatures & PrecYesScaleNo) != 0;
    }
    /**
     * Returns whether this type can be specified with a given combination
     * of precision and scale.
     *
     * For example,<ul>
     *
     * <li><code>Varchar.allowsPrecScale(true, false)</code>
     *     returns <code>true</code>, because the VARCHAR type allows a
     *     precision parameter, as in <code>VARCHAR(10)</code>.</li>
     *
     * <li><code>Varchar.allowsPrecScale(true, true)</code>
     *     returns <code>true</code>, because the VARCHAR type does not allow a
     *     precision and a scale parameter, as in
     *     <code>VARCHAR(10, 4)</code>.</li>
     *
     * <li><code>allowsPrecScale(false, true)</code> returns <code>false</code>
     *     for every type.</li>
     * </ul>
     *
     * @param precision Whether the precision/length field is part of the type
     *     specification
     * @param scale  Whether the scale field is part of the type specification
     * @return Whether this combination of precision/scale is valid
     */
    public boolean allowsPrecScale(boolean precision, boolean scale) {
        int mask = precision ?
                (scale ? PrecYesScaleYes : PrecYesScaleNo) :
                (scale ? 0 : PrecNoScaleNo);
        return (signatures & mask) != 0;
    }
}

// End SqlTypeName.java
