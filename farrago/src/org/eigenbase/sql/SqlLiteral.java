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

package org.eigenbase.sql;

import org.eigenbase.reltype.RelDataType;
import org.eigenbase.reltype.RelDataTypeFactory;
import org.eigenbase.sql.parser.SqlParserPos;
import org.eigenbase.sql.parser.SqlParserUtil;
import org.eigenbase.sql.type.SqlTypeName;
import org.eigenbase.sql.type.SqlTypeUtil;
import org.eigenbase.sql.util.SqlVisitor;
import org.eigenbase.sql.validate.SqlValidatorScope;
import org.eigenbase.sql.validate.SqlValidator;
import org.eigenbase.sql.fun.SqlLiteralChainOperator;
import org.eigenbase.sql.fun.SqlStdOperatorTable;
import org.eigenbase.util.BitString;
import org.eigenbase.util.EnumeratedValues;
import org.eigenbase.util.NlsString;
import org.eigenbase.util.Util;
import org.eigenbase.resource.EigenbaseResource;

import java.math.BigDecimal;
import java.util.Calendar;
import java.nio.charset.Charset;

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
 *   <td>{@link SqlTypeName#Binary}</td>
 *   <td>Binary constant, for example <code>X'ABC'</code>, <code>X'7F'</code>.
 *       Note that strings with an odd number of hexits will later become
 *       values of the BIT datatype, because they have an incomplete number
 *       of bytes. But here, they are all binary constants, because that's
 *       how they were written.
 *       These constants are always BINARY, never VARBINARY.</td>
 *   <td>{@link BitString}</td>
 * </tr>
 * <tr>
 *   <td>{@link SqlTypeName#Symbol}</td>
 *   <td>A symbol is a special type used to make parsing easier; it is
 *       not part of the SQL standard, and is not exposed to end-users.
 *       It is used to hold a symbol, such as the LEADING flag in a call to the
 *       function <code>TRIM([LEADING|TRAILING|BOTH] chars FROM string)</code>.
 *   </td>
 *   <td>A class which implements the {@link EnumeratedValues.Value}
 *       interface</td>
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
    private final SqlTypeName typeName;

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
        SqlParserPos pos)
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
     * @return whether value is appropriate for its type (we have rules about
     * these things)
     */
    public static boolean valueMatchesType(
        Object value,
        SqlTypeName typeName)
    {
        switch (typeName.getOrdinal()) {
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
            return value instanceof BitString;
        case SqlTypeName.Char_ordinal:
            return value instanceof NlsString;

        case SqlTypeName.Symbol_ordinal:
            // We grudgingly allow the value to be a String, because
            // SqlSymbol extends SqlLiteral and implements
            // EnumeratedValues.Value, and it would be silly if it were its
            // own value!
            return value instanceof EnumeratedValues.Value
                || value instanceof String
                || value instanceof Enum;
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
     *
     * @see #booleanValue(SqlNode)
     * @see #symbolValue(SqlNode)
     */
    public Object getValue()
    {
        return value;
    }

    /**
     * Converts extracts the value from a boolean literal.
     *
     * @throws ClassCastException if the value is not a boolean literal
     */
    public static boolean booleanValue(SqlNode node)
    {
        return ((Boolean) ((SqlLiteral) node).value).booleanValue();
    }

    /**
     * Extracts the enumerated value from a symbol literal.
     *
     * @throws ClassCastException if the value is not a symbol literal
     * @see #createSymbol(EnumeratedValues.Value, SqlParserPos)
     */
    public static EnumeratedValues.Value symbolValue(SqlNode node)
    {
        return (EnumeratedValues.Value) ((SqlLiteral) node).value;
    }

    /**
     * Extracts the Enum from a symbol literal.
     *
     * @throws ClassCastException if the value is not a symbol literal
     * @see #createEnum(Enum, SqlParserPos)
     */
    public static Enum enumValue(SqlNode node)
    {
        return (Enum) ((SqlLiteral) node).value;
    }

    /**
     * Extracts the string value from a string literal, a chain of string
     * literals, or a CAST of a string literal.
     */
    public static String stringValue(SqlNode node)
    {
        if (node instanceof SqlLiteral) {
            SqlLiteral literal = (SqlLiteral) node;
            assert SqlTypeUtil.inCharFamily(literal.getTypeName());
            return literal.toValue();
        } else if (SqlUtil.isLiteralChain(node)) {
            final SqlLiteral literal =
                SqlLiteralChainOperator.concatenateOperands((SqlCall) node);
            assert SqlTypeUtil.inCharFamily(literal.getTypeName());
            return literal.toValue();
        } else if (node instanceof SqlCall &&
            ((SqlCall) node).getOperator() == SqlStdOperatorTable.castFunc) {
            return stringValue(((SqlCall) node).getOperands()[0]);
        } else {
            throw Util.newInternal("invalid string literal: " + node);
        }
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
        switch (typeName.getOrdinal()) {
        case SqlTypeName.Char_ordinal:

            // We want 'It''s superman!', not _ISO-8859-1'It''s superman!'
            return ((NlsString) value).getValue();
        default:
            return value.toString();
        }
    }

    public void validate(SqlValidator validator, SqlValidatorScope scope)
    {
        validator.validateLiteral(this);
    }

    public <R> R accept(SqlVisitor<R> visitor)
    {
        return visitor.visit(this);
    }

    public boolean equalsDeep(SqlNode node, boolean fail)
    {
        if (!(node instanceof SqlLiteral)) {
            assert !fail : this + "!=" + node;
            return false;
        }
        SqlLiteral that = (SqlLiteral) node;
        if (!this.equals(that)) {
            assert !fail : this + "!=" + node;
            return false;
        }
        return true;
    }

    /**
     * Creates a NULL literal.
     *
     * <p>There's no singleton constant for a NULL literal.
     * Instead, nulls must be instantiated via createNull(), because
     * different instances have different context-dependent types.
     */
    public static SqlLiteral createNull(SqlParserPos pos)
    {
        return new SqlLiteral(null, SqlTypeName.Null, pos);
    }

    /**
     * Creates a boolean literal.
     */
    public static SqlLiteral createBoolean(
        boolean b,
        SqlParserPos pos)
    {
        return b
        ? new SqlLiteral(Boolean.TRUE, SqlTypeName.Boolean, pos)
        : new SqlLiteral(Boolean.FALSE, SqlTypeName.Boolean, pos);
    }

    public static SqlLiteral createUnknown(SqlParserPos pos)
    {
        return new SqlLiteral(null, SqlTypeName.Boolean, pos);
    }

    /**
     * Creates a literal which represents a parser symbol, for example the
     * <code>TRAILING</code> keyword in the call
     * <code>Trim(TRAILING 'x' FROM 'Hello world!')</code>.
     *
     * @see #symbolValue(SqlNode)
     */
    public static SqlLiteral createSymbol(
        EnumeratedValues.Value o,
        SqlParserPos pos)
    {
        return new SqlLiteral(o, SqlTypeName.Symbol, pos);
    }

    /**
     * Creates a literal which represents an Enum represented as a symbol.
     *
     * @see #enumValue(SqlNode)
     */
    public static SqlLiteral createEnum(
        Enum o,
        SqlParserPos pos)
    {
        return new SqlLiteral(o, SqlTypeName.Symbol, pos);
    }

    public boolean equals(Object obj)
    {
        if (!(obj instanceof SqlLiteral)) {
            return false;
        }
        SqlLiteral that = (SqlLiteral) obj;
        return Util.equal(value, that.value);
    }

    public int hashCode()
    {
        return (value == null) ? 0 : value.hashCode();
    }

    public int intValue()
    {
        switch (typeName.getOrdinal()) {
        case SqlTypeName.Decimal_ordinal:
        case SqlTypeName.Double_ordinal:
            BigDecimal bd = (BigDecimal) value;
            try {
                return bd.intValueExact();
            } catch (ArithmeticException e) {
                throw SqlUtil.newContextException(
                    getParserPosition(),
                    EigenbaseResource.instance().NumberLiteralOutOfRange.ex(
                        bd.toString()));
            }
        default:
            throw typeName.unexpected();
        }
    }

    public long longValue()
    {
        switch (typeName.getOrdinal()) {
        case SqlTypeName.Decimal_ordinal:
        case SqlTypeName.Double_ordinal:
            BigDecimal bd = (BigDecimal) value;
            try {
                return bd.longValueExact();
            } catch (ArithmeticException e) {
                throw SqlUtil.newContextException(
                    getParserPosition(),
                    EigenbaseResource.instance().NumberLiteralOutOfRange.ex(
                        bd.toString()));
            }
        default:
            throw typeName.unexpected();
        }
    }

    public String getStringValue()
    {
        return ((NlsString) value).getValue();
    }

    public void unparse(
        SqlWriter writer,
        int leftPrec,
        int rightPrec)
    {
        switch (typeName.getOrdinal()) {
        case SqlTypeName.Boolean_ordinal:
            writer.keyword(
                value == null ? "UNKNOWN" :
                ((Boolean) value).booleanValue() ? "TRUE" :
                "FALSE");
            break;
        case SqlTypeName.Null_ordinal:
            writer.keyword("NULL");
            break;
        case SqlTypeName.Char_ordinal:
        case SqlTypeName.Decimal_ordinal:
        case SqlTypeName.Double_ordinal:
        case SqlTypeName.Binary_ordinal:

            // should be handled in subtype
            throw typeName.unexpected();

        case SqlTypeName.Symbol_ordinal:
            if (value instanceof EnumeratedValues.Value) {
                EnumeratedValues.Value enumVal = (EnumeratedValues.Value) value;
                writer.keyword(enumVal.getName().toUpperCase());
            } else {
                Enum enumVal = (Enum) value;
                writer.keyword(enumVal.name());
            }
            break;
        default:
            writer.literal(value.toString());
        }
    }

    public RelDataType createSqlType(RelDataTypeFactory typeFactory)
    {
        BitString bitString;
        switch (typeName.getOrdinal()) {
        case SqlTypeName.Null_ordinal:
        case SqlTypeName.Boolean_ordinal:
            RelDataType ret = typeFactory.createSqlType(typeName);
            ret = typeFactory.createTypeWithNullability(ret, null == value);
            return ret;
        case SqlTypeName.Binary_ordinal:
            bitString = (BitString) value;
            int bitCount = bitString.getBitCount();
            Util.permAssert(bitCount % 8 == 0, "incomplete octet");
            return typeFactory.createSqlType(SqlTypeName.Binary,
                    bitCount / 8);
        case SqlTypeName.Char_ordinal:
            NlsString string = (NlsString) value;
            Charset charset = string.getCharset();
            if (null == charset) {
                charset = Util.getDefaultCharset();
            }
            SqlCollation collation = string.getCollation();
            if (null == collation) {
                collation =
                    new SqlCollation(SqlCollation.Coercibility.Coercible);
            }
            RelDataType type =
                typeFactory.createSqlType(
                    SqlTypeName.Char,
                    string.getValue().length());
            type =
                typeFactory.createTypeWithCharsetAndCollation(
                    type,
                    charset,
                    collation);
            return type;

        case SqlTypeName.IntervalYearMonth_ordinal:
        case SqlTypeName.IntervalDayTime_ordinal:
            SqlIntervalLiteral.IntervalValue intervalValue =
                (SqlIntervalLiteral.IntervalValue) value;
            RelDataType t = typeFactory.createSqlIntervalType(
                intervalValue.getIntervalQualifier());
            return typeFactory.createTypeWithNullability(t, false);

        case SqlTypeName.Symbol_ordinal:
            return typeFactory.createSqlType(SqlTypeName.Symbol);

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
        SqlParserPos pos)
    {
        return new SqlDateLiteral(calendar, pos);
    }

    public static SqlTimestampLiteral createTimestamp(
        Calendar calendar,
        int precision,
        SqlParserPos pos)
    {
        return new SqlTimestampLiteral(calendar, precision, false, pos);
    }

    public static SqlTimeLiteral createTime(
        Calendar calendar,
        int precision,
        SqlParserPos pos)
    {
        return new SqlTimeLiteral(calendar, precision, false, pos);
    }

    /**
     * Creates an interval literal.
     * @param intervalStr       input string of '1:23:04'
     * @param intervalQualifier describes the interval type and precision
     * @param pos               Parser position
     */
    public static SqlIntervalLiteral createInterval(int sign, String intervalStr,
        SqlIntervalQualifier intervalQualifier, SqlParserPos pos)
    {
        SqlTypeName typeName = intervalQualifier.isYearMonth() ?
            SqlTypeName.IntervalYearMonth :
            SqlTypeName.IntervalDayTime;
        return new SqlIntervalLiteral(sign, intervalStr, intervalQualifier,
            typeName, pos);
    }

    public static SqlNumericLiteral createNegative(SqlNumericLiteral num)
    {
        return new SqlNumericLiteral(
            ((BigDecimal) num.getValue()).negate(),
            num.getPrec(),
            num.getScale(),
            num.isExact(),
            num.getParserPosition());
    }

    public static SqlNumericLiteral createExactNumeric(
        String s,
        SqlParserPos pos)
    {
        BigDecimal value;
        int prec;
        int scale;

        int i = s.indexOf('.');
        if ((i >= 0) && ((s.length() - 1) != i)) {
            value = SqlParserUtil.parseDecimal(s);
            scale = s.length() - i - 1;
            assert scale == value.scale() : s;
            prec = s.length() - 1;
        } else if ((i >= 0) && ((s.length() - 1) == i)) {
            value = SqlParserUtil.parseInteger(s.substring(0, i));
            scale = 0;
            prec = s.length() - 1;
        } else {
            value = SqlParserUtil.parseInteger(s);
            scale = 0;
            prec = s.length();
        }
        return new SqlNumericLiteral(
            value,
            new Integer(prec),
            new Integer(scale),
            true,
            pos);
    }

    public static SqlNumericLiteral createApproxNumeric(
        String s,
        SqlParserPos pos)
    {
        BigDecimal value = SqlParserUtil.parseDecimal(s);
        return new SqlNumericLiteral(value, null, null, false, pos);
    }

    /**
     * Creates a literal like X'ABAB'. Although it matters when we derive a
     * type for this beastie, we don't care at this point whether the number of
     * hexits is odd or even.
     */
    public static SqlBinaryStringLiteral createBinaryString(
        String s,
        SqlParserPos pos)
    {
        BitString bits;
        try {
            bits = BitString.createFromHexString(s);
        } catch (NumberFormatException e) {
            throw SqlUtil.newContextException(
                pos,
                EigenbaseResource.instance().BinaryLiteralInvalid.ex());
        }
        return new SqlBinaryStringLiteral(bits, pos);
    }

    /**
     * Creates a string literal in the system character set.
     * @param s a string (without the sql single quotes)
     */
    public static SqlCharStringLiteral createCharString(
        String s,
        SqlParserPos pos)
    {
        return createCharString(s, null, pos);
    }

    /**
     * Creates a string literal, with optional character-set.
     * @param s a string (without the sql single quotes)
     * @param charSet character set name, null means take system default
     * @return A string literal
     */
    public static SqlCharStringLiteral createCharString(
        String s,
        String charSet,
        SqlParserPos pos)
    {
        NlsString slit = new NlsString(s, charSet, null);
        return new SqlCharStringLiteral(slit, pos);
    }
}


// End SqlLiteral.java

