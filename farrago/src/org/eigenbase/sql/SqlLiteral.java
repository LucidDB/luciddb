/*
// $Id$
// Package org.eigenbase is a class library of database components.
// Copyright (C) 2002-2004 Disruptive Tech
// Copyright (C) 2003-2004 John V. Sichi
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

import java.math.BigDecimal;
import java.util.List;
import java.math.BigDecimal;
import java.util.Calendar;

import org.eigenbase.reltype.RelDataType;
import org.eigenbase.reltype.RelDataTypeFactory;
import org.eigenbase.sql.parser.ParserPosition;
import org.eigenbase.sql.parser.ParserUtil;
import org.eigenbase.sql.type.SqlTypeName;
import org.eigenbase.util.BitString;
import org.eigenbase.util.EnumeratedValues;
import org.eigenbase.util.NlsString;
import org.eigenbase.util.Util;

/**
 * A <code>SqlLiteral</code> is a constant. It is, appropriately, immutable.
 *
 * <p>How is the value stored? In that respect, the class is somewhat of a
 * black box. There is a {@link #getValue} method which returns the value as
 * an object, but the type of that value is implementation detail, and it is
 * best that your code does not depend upon that knowledge. It is better to
 * use task-oriented methods such as {@link #toSqlString(SqlDialect)} and
 * {@link #toValue}.</p>
 *
 * <p>If you really need to access the value directly, you should switch on
 * the value of the {@link #typeName} field, rather than making assumptions
 * about the runtime type of the {@link #value}.</p>
 *
 * <p>The allowable types and combinations are:
 * <table>
 * <tr>
 *   <th>TypeName</th>
 *   <th>Meaing</th>
 *   <th>Value type</th>
 * </tr>
 * <tr>
 *   <td>{@link SqlTypeName#Null}</td>
 *   <td>The null value. It has its own special type.</td>
 *   <td>null</td>
 * </tr>
 * <tr>
 *   <td>{@link SqlTypeName#Boolean}</td>
 *   <td>Boolean, namely <code>TRUE</code>,
 *       <code>FALSE</code> or
 *       <code>UNKNOWN</code>.</td>
 *   <td>{@link Boolean}, or null represents the UNKNOWN value</td>
 * </tr>
 * <tr>
 *   <td>{@link SqlTypeName#Decimal}</td>
 *   <td>Exact number, for example <code>0</code>,
 *       <code>-.5</code>,
 *       <code>12345</code>.</td>
 *   <td>{@link BigDecimal}</td>
 * </tr>
 * <tr>
 *   <td>{@link SqlTypeName#Double}</td>
 *   <td>Approximate number, for example <code>6.023E-23</code>.</td>
 *   <td>{@link BigDecimal}</td>
 * </tr>
 * <tr>
 *   <td>{@link SqlTypeName#Date}</td>
 *   <td>Date, for example <code>DATE '1969-04'29'</code></td>
 *   <td>{@link Calendar}</td>
 * </tr>
 * <tr>
 *   <td>{@link SqlTypeName#Time}</td>
 *   <td>Time, for example <code>TIME '18:37:42.567'</code></td>
 *   <td>{@link Calendar}</td>
 * </tr>
 * <tr>
 *   <td>{@link SqlTypeName#Timestamp}</td>
 *   <td>Timestamp, for example
 *       <code>TIMESTAMP '1969-04-29 18:37:42.567'</code></td>
 *   <td>{@link Calendar}</td>
 * </tr>
 * <tr>
 *   <td>{@link SqlTypeName#Char}</td>
 *   <td>Character constant, for example
 *       <code>'Hello, world!'</code>,
 *       <code>''</code>,
 *       <code>_N'Bonjour'</code>,
 *       <code>_ISO-8859-1'It''s superman!' COLLATE SHIFT_JIS$ja_JP$2</code>.
 *       These are always CHAR, never VARCHAR.</td>
 *   <td>{@link NlsString}</td>
 * </tr>
 * <tr>
 *   <td>{@link SqlTypeName#Bit}</td>
 *   <td>Bit string, for example <code>B'101011'</code>.</td>
 *   <td>{@link BitString}</td>
 * </tr>
 * <tr>
 *   <td>{@link SqlTypeName#Binary}</td>
 *   <td>Binary constant, for example <code>X'ABC'</code>, <code>X'7F'</code>.
 *       Note that strings with an odd number of hexits will later become
 *       values of the BIT datatype, because they have an incomplete number
 *       of bytes. But here, they are all binary constants, because that's
 *       how they were written.
 *       These constants are always BINARY, never VARBINARY.</td>
 *   <td><code>byte[]</code> or {@link BitString}</td>
 * </tr>
 * <tr>
 *   <td>{@link SqlTypeName#Symbol}</td>
 *   <td>A symbol is a special type used to make parsing easier; it is not
 *       part of the SQL standard, and is not exposed to end-users.
 *       It is used to hold a flag, such as the LEADING flag in a call to the
 *       function <code>TRIM([LEADING|TRAILING|BOTH] chars FROM string)</code>.
 *   </td>
 *   <td><{@link SqlSymbol} (which conveniently both extends {@link SqlLiteral}
 *       and also implements {@link EnumeratedValues}), or a class derived from
 *       it.</td>
 * </tr>
 * <tr>
 *   <td>{@link SqlTypeName#IntervalDayTime}</td>
 *   <td>Interval, for example <code>INTERVAL '1:34' HOUR</code>.</td>
 *   <td><{@link SqlIntervalLiteral.IntervalValue}.</td>
 * </tr>
 * </table>
 */
public class SqlLiteral extends SqlNode
{
    //~ Instance fields -------------------------------------------------------

    /**
     * The type with which this literal was declared. This type is very
     * approximate: the literal may have a different type once validated. For
     * example, all numeric literals have a type name of
     * {@link SqlTypeName#Decimal}, but on validation may become
     * {@link SqlTypeName#Integer}.
     */
    public final SqlTypeName typeName;

    /**
     * The value of this literal. The type of the value must be appropriate
     * for the typeName, as defined by the {@link #valueMatchesType} method.
     */
    protected final Object value;

    //~ Constructors ----------------------------------------------------------

    /**
     * Creates a <code>SqlLiteral</code>.
     *
     * @pre typeName != null
     * @pre valueMatchesType(value,typeName)
     */
    protected SqlLiteral(
        Object value,
        SqlTypeName typeName,
        ParserPosition pos)
    {
        super(pos);
        this.value = value;
        this.typeName = typeName;
        Util.pre(typeName != null, "typeName != null");
        Util.pre(
            valueMatchesType(value, typeName),
            "valueMatchesType(value,typeName)");
    }

    //~ Methods ---------------------------------------------------------------

    /**
     * @return value of {@link #typeName}
     */
    public SqlTypeName getTypeName()
    {
        return typeName;
    }

    /**
     * Whether value is appropriate for its type. (We have rules about these
     * things.)
     */
    public static boolean valueMatchesType(
        Object value,
        SqlTypeName typeName)
    {
        switch (typeName.ordinal) {
        case SqlTypeName.Boolean_ordinal:
            return (value == null) || value instanceof Boolean;
        case SqlTypeName.Null_ordinal:
            return value == null;
        case SqlTypeName.Decimal_ordinal:
        case SqlTypeName.Double_ordinal:
            return value instanceof BigDecimal;
        case SqlTypeName.Date_ordinal:
        case SqlTypeName.Time_ordinal:
        case SqlTypeName.Timestamp_ordinal:
            return value instanceof Calendar;
        case SqlTypeName.IntervalDayTime_ordinal:
        case SqlTypeName.IntervalYearMonth_ordinal:
            return value instanceof SqlIntervalLiteral.IntervalValue;
        case SqlTypeName.Binary_ordinal:

            // created from X'ABC' (odd length) or X'AB' (even length)
            return value instanceof BitString; // created from B'0101'
        case SqlTypeName.Bit_ordinal:
            return value instanceof BitString; // created from B'0101'
        case SqlTypeName.Char_ordinal:
            return value instanceof NlsString;

        case SqlTypeName.Symbol_ordinal:
            // We grudgingly allow the value to be a String, because
            // SqlSymbol extends SqlLiteral and implements
            // EnumeratedValues.Value, and it would be silly if it were its
            // own value!
            return value instanceof EnumeratedValues.Value
                || value instanceof String;
        case SqlTypeName.Multiset_ordinal:
            return true;
        case SqlTypeName.Integer_ordinal: // not allowed -- use Decimal
        case SqlTypeName.Varchar_ordinal: // not allowed -- use Char
        case SqlTypeName.Varbinary_ordinal: // not allowed -- use Binary
        default:
            throw typeName.unexpected();
        }
    }

    public SqlKind getKind()
    {
        return SqlKind.Literal;
    }

    /**
     * Returns the value of this literal.
     *
     * <p>Try not to use this method! There are so many different kinds of
     * values, it's better to to let SqlLiteral do whatever it is you want to
     * do.
     */
    public Object getValue()
    {
        return value;
    }

    public static boolean booleanValue(SqlNode node)
    {
        return ((Boolean) ((SqlLiteral) node).value).booleanValue();
    }

    /**
     * For calc program builder - value may be different than {@link #unparse}
     * Typical values:<ul>
     * <li>Hello, world!</li>
     * <li>12.34</li>
     * <li>{null}</li>
     * <li>1969-04-29</li>
     * </ul>
     *
     * @return string representation of the value
     */
    public String toValue()
    {
        if (value == null) {
            return null;
        }
        switch (typeName.ordinal) {
        case SqlTypeName.Char_ordinal:

            // We want 'It''s superman!', not _ISO-8859-1'It''s superman!'
            return ((NlsString) value).getValue();
        default:
            return value.toString();
        }
    }

    /**
     * Creates a NULL literal.
     */
    public static SqlLiteral createNull(ParserPosition pos)
    {
        return new SqlLiteral(null, SqlTypeName.Null, pos);
    }

    /**
     * Creates a boolean literal.
     */
    public static SqlLiteral createBoolean(
        boolean b,
        ParserPosition pos)
    {
        return b
        ? new SqlLiteral(Boolean.TRUE, SqlTypeName.Boolean, pos)
        : new SqlLiteral(Boolean.FALSE, SqlTypeName.Boolean, pos);
    }

    public static SqlLiteral createUnknown(ParserPosition pos)
    {
        return new SqlLiteral(null, SqlTypeName.Boolean, pos);
    }

    public static SqlLiteral create(
        int i,
        ParserPosition pos)
    {
        switch (i) {
        case 0:
            return new SqlLiteral(
                new BigDecimal(0),
                SqlTypeName.Decimal,
                pos);
        case 1:
            return new SqlLiteral(
                new BigDecimal(1),
                SqlTypeName.Decimal,
                pos);
        default:
            return new SqlLiteral(
                new BigDecimal(i),
                SqlTypeName.Decimal,
                pos);
        }
    }

    /**
     * Creates a literal which represents a parser symbol, for example the
     * TRAILING keyword in the call Trim(TRAILING 'x' FROM 'Hello world!').
     *
     * @see SqlSymbol
     */
    public static SqlLiteral createFlag(
        EnumeratedValues.Value o,
        ParserPosition pos)
    {
        return new SqlLiteral(o, SqlTypeName.Symbol, pos);
    }

    public boolean equals(Object obj)
    {
        return (obj instanceof SqlLiteral)
            && equals(((SqlLiteral) obj).value, value);
    }

    public int hashCode()
    {
        return (value == null) ? 0 : value.hashCode();
    }

    public int intValue()
    {
        switch (typeName.ordinal) {
        case SqlTypeName.Decimal_ordinal:
        case SqlTypeName.Double_ordinal:
            BigDecimal bd = (BigDecimal) value;
            return bd.intValue();
        default:
            throw typeName.unexpected();
        }
    }

    public String getStringValue()
    {
        return ((NlsString) value).getValue();
    }

    public Object clone()
    {
        return new SqlLiteral(
            value,
            typeName,
            getParserPosition());
    }

    public void unparse(
        SqlWriter writer,
        int leftPrec,
        int rightPrec)
    {
        switch (typeName.ordinal) {
        case SqlTypeName.Boolean_ordinal:
            writer.print((value == null) ? "UNKNOWN"
                : (((Boolean) value).booleanValue() ? "TRUE" : "FALSE"));
            break;
        case SqlTypeName.Null_ordinal:
            writer.print("NULL");
            break;
        case SqlTypeName.Char_ordinal:
        case SqlTypeName.Decimal_ordinal:
        case SqlTypeName.Double_ordinal:
        case SqlTypeName.Binary_ordinal:
        case SqlTypeName.Bit_ordinal:

            // should be handled in subtype
            throw typeName.unexpected();
        default:
            writer.print(value.toString());
        }
    }

    private static boolean equals(
        Object o1,
        Object o2)
    {
        return (o1 == null) ? (o2 == null) : o1.equals(o2);
    }

    public RelDataType createSqlType(RelDataTypeFactory typeFactory)
    {
        BitString bitString;
        switch (typeName.ordinal) {
        case SqlTypeName.Null_ordinal:
        case SqlTypeName.Boolean_ordinal:
            RelDataType ret = typeFactory.createSqlType(typeName);
            ret = typeFactory.createTypeWithNullability(ret, null == value);
            return ret;
        case SqlTypeName.Binary_ordinal:

            // REVIEW: should this be Binary, not Varbinary?
            bitString = (BitString) value;
            int bitCount = bitString.getBitCount();
            if ((bitCount % 8) == 0) {
                return typeFactory.createSqlType(SqlTypeName.Varbinary,
                    bitCount / 8);
            } else {
                return typeFactory.createSqlType(SqlTypeName.Bit, bitCount);
            }
        case SqlTypeName.Bit_ordinal:
            assert value instanceof BitString;
            bitString = (BitString) value;
            return typeFactory.createSqlType(
                SqlTypeName.Bit,
                bitString.getBitCount());
        case SqlTypeName.Char_ordinal:
            NlsString string = (NlsString) value;
            if (null == string.getCharset()) {
                string.setCharset(Util.getDefaultCharset());
            }
            if (null == string.getCollation()) {
                string.setCollation(
                    new SqlCollation(SqlCollation.Coercibility.Coercible));
            }
            RelDataType type =
                typeFactory.createSqlType(
                    SqlTypeName.Varchar,
                    string.getValue().length());
            type =
                typeFactory.createTypeWithCharsetAndCollation(
                    type,
                    string.getCharset(),
                    string.getCollation());
            return type;
        case SqlTypeName.IntervalYearMonth_ordinal:
        case SqlTypeName.IntervalDayTime_ordinal:
            SqlIntervalLiteral.IntervalValue intervalValue =
                (SqlIntervalLiteral.IntervalValue) value;
            RelDataType t = typeFactory.createIntervalType(
                intervalValue.getIntervalQualifier());
            return typeFactory.createTypeWithNullability(t, false);
        case SqlTypeName.Symbol_ordinal:

            // Existing code expects symbols to have a null type.
            if (true) {
                return null;
            }
            throw Util.newInternal("symbol does not have a SQL type: "
                + value);
        case SqlTypeName.Integer_ordinal: // handled in derived class
        case SqlTypeName.Time_ordinal: // handled in derived class
        case SqlTypeName.Varchar_ordinal: // should never happen
        case SqlTypeName.Varbinary_ordinal: // should never happen

        default:
            throw Util.needToImplement(toString() + ", operand=" + value);
        }
    }

    public static SqlDateLiteral createDate(
        Calendar calendar,
        ParserPosition pos)
    {
        return new SqlDateLiteral(calendar, pos);
    }

    public static SqlTimestampLiteral createTimestamp(
        Calendar calendar,
        int precision,
        ParserPosition pos)
    {
        return new SqlTimestampLiteral(calendar, precision, false, pos);
    }

    public static SqlTimeLiteral createTime(
        Calendar calendar,
        int precision,
        ParserPosition pos)
    {
        return new SqlTimeLiteral(calendar, precision, false, pos);
    }

    /**
     * Creates an interval literal.
     * @param values            Values, e.g. int[]{1, 23, 4} from a
     *                          input string of'1:23:04'
     * @param intervalQualifier describes the interval type and precision
     * @param pos               Parser position
     */
    public static SqlIntervalLiteral createInterval(int[] values,
        SqlIntervalQualifier intervalQualifier, ParserPosition pos)
    {
        SqlTypeName typeName = intervalQualifier.isYearMonth() ?
            SqlTypeName.IntervalYearMonth :
            SqlTypeName.IntervalDayTime;
        return new SqlIntervalLiteral(values, intervalQualifier,
            typeName, pos);
    }

    //~ Inner Classes ---------------------------------------------------------

    // NOTE jvs 26-Jan-2004:  There's no singleton constant for a NULL literal.
    // Instead, nulls must be instantiated via createNull(), because
    // different instances have different context-dependent types.

    public static class Numeric extends SqlLiteral
    {
        private Integer prec;
        private Integer scale;
        private boolean isExact;

        protected Numeric(
            BigDecimal value,
            Integer prec,
            Integer scale,
            boolean isExact,
            ParserPosition pos)
        {
            super(value, isExact ? SqlTypeName.Decimal : SqlTypeName.Double,
                pos);
            this.prec = prec;
            this.scale = scale;
            this.isExact = isExact;
        }

        public Integer getPrec()
        {
            return prec;
        }

        public Integer getScale()
        {
            return scale;
        }

        public static Numeric createExact(
            String s,
            ParserPosition pos)
        {
            BigDecimal value;
            int prec;
            int scale;

            int i = s.indexOf('.');
            if ((i >= 0) && ((s.length() - 1) != i)) {
                value = ParserUtil.parseDecimal(s);
                scale = s.length() - i - 1;
                assert scale == value.scale() : s;
                prec = s.length() - 1;
            } else if ((i >= 0) && ((s.length() - 1) == i)) {
                value = ParserUtil.parseInteger(s.substring(0, i));
                scale = 0;
                prec = s.length() - 1;
            } else {
                value = ParserUtil.parseInteger(s);
                scale = 0;
                prec = s.length();
            }
            return new Numeric(
                value,
                new Integer(prec),
                new Integer(scale),
                true,
                pos);
        }

        public static Numeric createApprox(
            String s,
            ParserPosition pos)
        {
            BigDecimal value = ParserUtil.parseDecimal(s);
            return new Numeric(value, null, null, false, pos);
        }

        public boolean isExact()
        {
            return isExact;
        }

        public void unparse(
            SqlWriter writer,
            int leftPrec,
            int rightPrec)
        {
            writer.print(toValue());
        }

        public String toValue()
        {
            BigDecimal bd = (BigDecimal) value;
            if (isExact) {
                return value.toString();
            }
            return Util.toScientificNotation(bd);
        }

        public RelDataType createSqlType(RelDataTypeFactory typeFactory)
        {
            if (isExact) {
                int scaleValue = scale.intValue();
                if (0 == scaleValue) {
                    BigDecimal bd = (BigDecimal) value;
                    SqlTypeName result;
                    long l = bd.longValue();
                    if ((l >= Integer.MIN_VALUE) && (l <= Integer.MAX_VALUE)) {
                        result = SqlTypeName.Integer;
                    } else {
                        result = SqlTypeName.Bigint;
                    }
                    return typeFactory.createSqlType(result);
                }

                //else we have a decimal
                return typeFactory.createSqlType(
                    SqlTypeName.Decimal,
                    prec.intValue(),
                    scaleValue);
            }

            // else we have a a float, real or double.  make them all double for now.
            return typeFactory.createSqlType(SqlTypeName.Double);
        }
    }

    /** abstract base for char, bit, and binary (hex) strings */
    public static abstract class StringLiteral extends SqlLiteral
    {
        protected StringLiteral(
            Object value,
            SqlTypeName typeName,
            ParserPosition pos)
        {
            super(value, typeName, pos);
        }

        /** Concatenate string literals. A static method, to concatenate all at once,
         * since pairwise concatenation means too much string copying.
         * @param lits a StringLiteral[], not empty, homogeneous to subtype.
         * @return a new StringLiteral, of that same subtype, whose value is the
         * string concatenation of the values of the lits.
         * @throws ClassCastException if the lits are not homogeneous.
         * @throws ArrayIndexOutOfBoundsException if lits is an empty array.
         */
        public static StringLiteral concat(StringLiteral [] lits)
        {
            if (lits.length == 1) {
                return lits[0]; // nothing to do
            }
            return lits[0].concat1(lits);
        }

        /**
         * Helper routine for {@link #concat}.
         * @param lits homogeneous StringLiteral[] args.
         * @return StringLiteral with concatenated value.
         * this == lits[0], used only for method dispatch.
         */
        protected abstract StringLiteral concat1(StringLiteral [] lits);
    }

    /**
     * A character string literal. {@link #value} is an {@link NlsString} and
     * {@link #typeName} is {@link SqlTypeName#Char}.
     */
    public static class CharString extends StringLiteral
    {
        protected CharString(
            NlsString val,
            ParserPosition pos)
        {
            super(val, SqlTypeName.Char, pos);
        }

        /** @return the underlying NlsString */
        public NlsString getNlsString()
        {
            return (NlsString) value;
        }

        /** @return the collation */
        public SqlCollation getCollation()
        {
            return getNlsString().getCollation();
        }

        /** Sets the collation.
         * (Convenient for the sql parser to do this after construction)
         */
        public void setCollation(SqlCollation collation)
        {
            getNlsString().setCollation(collation);
        }

        /**
         * Creates a string literal in the system character set.
         * @param s a string (without the sql single quotes)
         */
        public static CharString create(
            String s,
            ParserPosition pos)
        {
            return create(s, null, pos);
        }

        /**
         * Creates a string literal, with optional character-set.
         * @param s a string (without the sql single quotes)
         * @param charSet character set name, null means take system default
         * @return A string literal
         */
        public static CharString create(
            String s,
            String charSet,
            ParserPosition pos)
        {
            NlsString slit = new NlsString(s, charSet, null);
            return new CharString(slit, pos);
        }

        public void unparse(
            SqlWriter writer,
            int leftPrec,
            int rightPrec)
        {
            if (false) {
                String stringValue = ((NlsString) value).getValue();
                writer.print(writer.dialect.quoteStringLiteral(stringValue));
            }
            assert value instanceof NlsString;
            writer.print(value.toString());
        }

        protected StringLiteral concat1(StringLiteral [] lits)
        {
            NlsString [] args = new NlsString[lits.length];
            for (int i = 0; i < lits.length; i++) {
                args[i] = ((CharString) lits[i]).getNlsString();
            }
            return new CharString(
                NlsString.concat(args),
                lits[0].getParserPosition());
        }
    }

    /**
     * A bit string literal.
     * {@link #value} is a {@link BitString} and {@link #typeName} is
     * {@link SqlTypeName#Bit}.
     */
    public static class BitStringLiteral extends StringLiteral
    {
        protected BitStringLiteral(
            BitString val,
            ParserPosition pos)
        {
            super(val, SqlTypeName.Bit, pos);
        }

        /** @return the underlying BitString */
        public BitString getBitString()
        {
            return (BitString) value;
        }

        /* Creates a literal like B'0101' */
        public static BitStringLiteral create(
            String s,
            ParserPosition pos)
        {
            BitString bits = BitString.createFromBitString(s);
            return new BitStringLiteral(bits, pos);
        }

        public void unparse(
            SqlWriter writer,
            int leftPrec,
            int rightPrec)
        {
            assert value instanceof BitString;
            writer.print("B'");
            writer.print(value.toString());
            writer.print("'");
        }

        protected StringLiteral concat1(StringLiteral [] lits)
        {
            BitString [] args = new BitString[lits.length];
            for (int i = 0; i < lits.length; i++) {
                args[i] = ((BitStringLiteral) lits[i]).getBitString();
            }
            return new BitStringLiteral(
                BitString.concat(args),
                lits[0].getParserPosition());
        }
    }

    /**
     * A binary (or hex) string literal.
     * {@link #value} is again a {@link BitString} and
     * {@link #typeName} is {@link SqlTypeName#Binary}.
     */
    public static class BinaryStringLiteral extends StringLiteral
    {
        protected BinaryStringLiteral(
            BitString val,
            ParserPosition pos)
        {
            super(val, SqlTypeName.Binary, pos);
        }

        /** @return the underlying BitString */
        public BitString getBitString()
        {
            return (BitString) value;
        }

        /* Creates a literal like X'ABAB'. Although it matters when we derive a
         * type for this beastie, we don't care at this point whether the number of
         * hexits is odd or even.
         */
        public static BinaryStringLiteral create(
            String s,
            ParserPosition pos)
        {
            BitString bits = BitString.createFromHexString(s);
            return new BinaryStringLiteral(bits, pos);
        }

        public void unparse(
            SqlWriter writer,
            int leftPrec,
            int rightPrec)
        {
            assert value instanceof BitString;
            writer.print("X'");
            writer.print(((BitString) value).toHexString());
            writer.print("'");
        }

        protected StringLiteral concat1(StringLiteral [] lits)
        {
            BitString [] args = new BitString[lits.length];
            for (int i = 0; i < lits.length; i++) {
                args[i] = ((BinaryStringLiteral) lits[i]).getBitString();
            }
            return new BinaryStringLiteral(
                BitString.concat(args),
                lits[0].getParserPosition());
        }
    }

}


// End SqlLiteral.java

