/*
// $Id$
// Saffron preprocessor and data engine
// (C) Copyright 2002-2004 Disruptive Tech
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

package net.sf.saffron.sql;

import net.sf.saffron.core.SaffronType;
import net.sf.saffron.core.SaffronTypeFactory;
import net.sf.saffron.sql.parser.ParserUtil;
import net.sf.saffron.sql.type.SqlTypeName;
import net.sf.saffron.util.*;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.TimeZone;

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
 * the value of the {@link #_typeName} field, rather than making assumptions
 * about the runtime type of the {@link #_value}.</p>
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
 * </table>
 */
public class SqlLiteral extends SqlNode
{
    //~ Static fields/initializers --------------------------------------------

    // NOTE jvs 26-Jan-2004:  There's no singleton constant for a NULL literal.
    // Instead, nulls must be instantiated via createNull(), because
    // different instances have different context-dependent types.

    /** Constant for {@link Boolean#TRUE}. */
    public static final SqlLiteral True =
            new SqlLiteral(Boolean.TRUE, SqlTypeName.Boolean);

    /** Constant for {@link Boolean#FALSE}. */
    public static final SqlLiteral False =
            new SqlLiteral(Boolean.FALSE, SqlTypeName.Boolean);

    /** Constant for the unknown value in 3 valued logic. */
    public static final SqlLiteral Unknown =
            new SqlLiteral(null, SqlTypeName.Boolean);

    /** Constant for the {@link Integer} value 0. */
    public static final SqlLiteral Zero =
            new SqlLiteral(new BigDecimal(0), SqlTypeName.Decimal);

    /** Constant for the {@link Integer} value 1. */
    public static final SqlLiteral One =
            new SqlLiteral(new BigDecimal(1), SqlTypeName.Decimal);

    //~ Inner Classes   -------------------------------------------------------


    /**
     * DateLiteral for wrapping java date/time types.
     * This is sort of an odd thing to do - a static nested class that extends
     * its super class.
     */
    public static class DateLiteral extends SqlLiteral {
        protected boolean _hasTimeZone;
        protected String _formatString = ParserUtil.DateFormatStr;

        /**
         *  Construct a new dateformat object for the given string.
         *  DateFormat objects aren't thread safe
         * @param dfString
         * @return date format object
         */
        public static DateFormat getDateFormat(String dfString) {
            SimpleDateFormat df = new SimpleDateFormat(dfString);
            df.setLenient(false);
            return df;
        }


        public DateLiteral(Calendar d)
        {
            this(d,false, SqlTypeName.Date);
        }

        public DateLiteral(Calendar d, boolean tz) {
            this(d, tz, SqlTypeName.Date);
        }

        protected DateLiteral(Calendar d, boolean tz, SqlTypeName typeName) {
            super(d, typeName);
            _hasTimeZone = tz;
        }

        public String toValue() {
            return Long.toString(getCal().getTimeInMillis());
        }

        public Date getDate() {
            return new java.sql.Date(getCal().getTimeInMillis());
        }

        public Calendar getCal() {
            return (Calendar) _value;
        }
        /**
         * technically, a sql date doesn't come with a tz, but time and ts
         * inherit this, and the calendar object has one, so it seems harmless.
         * @return timezone
         */
        public TimeZone getTimeZone() {
            assert(_hasTimeZone) : "Attempt to get timezone on Literal date: " + getCal() + ", which has no timezone";
            return getCal().getTimeZone();
        }

        public String toString() {
            return "DATE '" +
                    getDateFormat(_formatString).format(getCal().getTime()) +
                    "'";
        }

        public SaffronType createSqlType(SaffronTypeFactory typeFactory) {
            return typeFactory.createSqlType(SqlTypeName.Date);
        }

        public void unparse(SqlWriter writer,int leftPrec,int rightPrec) {
            writer.print(this.toString());
        }

    }

    public static class TimeLiteral extends DateLiteral {
        public final int _precision;

        protected TimeLiteral(Calendar t, int p, boolean hasTZ,
                SqlTypeName typeName) {
            super(t,hasTZ,typeName);
            _precision = p;
            _formatString = ParserUtil.TimeFormatStr;
       }

        /**
         * Constructor is private; use {@link #createTime}.
         */
        private TimeLiteral(Calendar t, int p) {
           this(t,p,false,SqlTypeName.Time);
        }

        public Time getTime() {
            return new java.sql.Time(getCal().getTimeInMillis());
        }

        public int getPrec() {
            return _precision;
        }

        public String toString() {
            return "TIME '" + toFormattedString() + "'";
        }

        public String toFormattedString() {
            String result =
                    new SimpleDateFormat(_formatString).
                                format(getCal().getTime()) ;
            if (_precision > 0) {
                // get the millisecond count.  millisecond => at most 3 digits.
                String digits = Long.toString(getCal().getTimeInMillis());
                result = result + "." +
                        digits.substring(digits.length() - 3, digits.length() - 3 + _precision);
            }
            return result;
        }

        public SaffronType createSqlType(SaffronTypeFactory typeFactory) {
            return typeFactory.createSqlType(_typeName, _precision);
        }
    }


    /**
     * This is largely a place holder, the type allows to determine what this object is,
     *  but most of its functionality comes from above.
     */
    public static class TimestampLiteral extends TimeLiteral {

        public TimestampLiteral(Calendar cal, int p, boolean hasTZ) {
            super(cal, p, hasTZ, SqlTypeName.Timestamp);
            _formatString = ParserUtil.TimestampFormatStr;
        }

        public TimestampLiteral(Calendar cal, int p) {
            this(cal, p, false);
        }

        public Timestamp getTimestamp() {
            return new java.sql.Timestamp(getCal().getTimeInMillis());
        }

        public String toString() {
            return "TIMESTAMP '" + toFormattedString() + "'";
        }
    }

    public static class Numeric extends SqlLiteral {
        //~ Member variables -----------
        private Integer m_prec;
        private Integer m_scale;
        private boolean m_isExact;

        public Integer getPrec() {
            return m_prec;
        }

        public Integer getScale() {
            return m_scale;
        }

        //~ Methods -----------
        public static Numeric createExact(String s){
            BigDecimal value;
            int prec;
            int scale;

            int i=s.indexOf('.');
            if (i>=0 && (s.length()-1!=i)) {
                value = ParserUtil.parseDecimal(s);
                scale = s.length()-i-1;
                assert scale == value.scale() : s;
                prec = s.length()-1;
            }
            else if (i>=0 && (s.length()-1==i)) {
                value = ParserUtil.parseInteger(s.substring(0,i));
                scale = 0;
                prec = s.length()-1;
            }
            else {
                value = ParserUtil.parseInteger(s);
                scale = 0;
                prec = s.length();
            }
            return new Numeric(value,new Integer(prec),new Integer(scale), true);
        }

        public static Numeric createApprox(String s){
            BigDecimal value = ParserUtil.parseDecimal(s);
            return new Numeric(value,null,null, false);
        }

        protected Numeric(BigDecimal value, Integer prec, Integer scale, boolean isExact) {
            super(value, isExact ? SqlTypeName.Decimal : SqlTypeName.Double);
            this.m_prec = prec;
            this.m_scale = scale;
            this.m_isExact = isExact;
        }

        public boolean isExact() {
            return m_isExact;
        }

        public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
            BigDecimal bd = (BigDecimal) _value;
            if (m_isExact && bd.scale() == 0) {
                writer.print(bd.intValue());
            } else {
                writer.print(_value.toString());
            }
        }

        public SaffronType createSqlType(SaffronTypeFactory typeFactory) {
            if (m_isExact){
                int scale = m_scale.intValue();
                if (0 == scale) {
                    return typeFactory.createSqlType(SqlTypeName.Integer);
                }
                //else we have a decimal
                return typeFactory.createSqlType(
                        SqlTypeName.Decimal,
                        m_prec.intValue(),
                        m_scale.intValue());
            }
            // else we have a a float, real or double.  make them all double for now.
            return typeFactory.createSqlType(SqlTypeName.Double);
        }

    }

    //~ Instance fields -------------------------------------------------------

    /**
     * The type with which this literal was declared. This type is very
     * approximate: the literal may have a differnt type once validated. For
     * example, all numeric literals have a type name of
     * {@link SqlTypeName#Decimal}, but on validation may become
     * {@link SqlTypeName#Integer}.
     */
    public final SqlTypeName _typeName;
    /**
     * The value of this literal. The type of the value must be appropriate
     * for the typeName, as defined by the {@link #valueMatchesType} method.
     */
    protected final Object _value;

    //~ Constructors ----------------------------------------------------------

    /**
     * Creates a <code>SqlLiteral</code>.
     *
     * @pre typeName != null
     * @pre valueMatchesType(value,typeName)
     */
    protected SqlLiteral(Object value, SqlTypeName typeName)
    {
        this._value = value;
        this._typeName = typeName;
        Util.pre(typeName != null, "typeName != null");
        Util.pre(valueMatchesType(value, typeName),
                "valueMatchesType(value,typeName)");
    }

    /**
     * Whether value is appropriate for its type. (We have rules about these
     * things.)
     */
    public static boolean valueMatchesType(Object value,
            SqlTypeName typeName) {
        switch (typeName.ordinal_) {
        case SqlTypeName.Boolean_ordinal:
            return value == null || value instanceof Boolean;

        case SqlTypeName.Null_ordinal:
            return value == null;

        case SqlTypeName.Decimal_ordinal:
        case SqlTypeName.Double_ordinal:
            return value instanceof BigDecimal;

        case SqlTypeName.Date_ordinal:
        case SqlTypeName.Time_ordinal:
        case SqlTypeName.Timestamp_ordinal:
            return value instanceof Calendar;

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
            return value instanceof EnumeratedValues.Value ||
                    value instanceof String;

        case SqlTypeName.Integer_ordinal: // not allowed -- use Decimal
        case SqlTypeName.Varchar_ordinal: // not allowed -- use Char
        case SqlTypeName.Varbinary_ordinal: // not allowed -- use Binary
        default:
            throw typeName.unexpected();
        }
    }

    //~ Methods ---------------------------------------------------------------

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
        return _value;
    }

    public static boolean booleanValue(SqlNode node)
    {
        return ((Boolean) ((SqlLiteral) node)._value).booleanValue();
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
    public String toValue() {
        if (_value == null) {
            return null;
        }
        switch (_typeName.ordinal_) {
        case SqlTypeName.Char_ordinal:
            // We want 'It''s superman!', not _ISO-8859-1'It''s superman!'
            return ((NlsString) _value).getValue();
        default:
            return _value.toString();
        }
    }
    /**
     * Creates a NULL literal.
     */
    public static SqlLiteral createNull()
    {
        return new SqlLiteral(null, SqlTypeName.Null);
    }

    /**
     * Returns whether a node represents the NULL value.
     * isNullLiteral({@link #Unknown}) returns false.
     */
    public static boolean isNullLiteral(SqlNode node)
    {
        // We don't regard UNKNOWN -- SqlLiteral(null,Boolean) -- as NULL.
        return (node instanceof SqlLiteral) &&
                ((SqlLiteral) node)._typeName == SqlTypeName.Null;
    }

    /**
     * Creates a boolean literal.
     */
    public static SqlLiteral createBoolean(boolean b)
    {
        return b ? True : False;
    }

    public static SqlLiteral create(int i)
    {
        switch (i) {
        case 0:
            return Zero;
        case 1:
            return One;
        default:
            return new SqlLiteral(new BigDecimal(i), SqlTypeName.Decimal);
        }
    }


    /**
     * Creates a literal which represents a parser symbol, for example the
     * TRAILING keyword in the call Trim(TRAILING 'x' FROM 'Hello world!').
     *
     * @see SqlSymbol
     */
    public static SqlLiteral createFlag(EnumeratedValues.Value o)
    {
        return new SqlLiteral(o, SqlTypeName.Symbol);
    }

    public boolean equals(Object obj)
    {
        return (obj instanceof SqlLiteral)
            && equals(((SqlLiteral) obj)._value,_value);
    }

    public int hashCode()
    {
        return (_value == null) ? 0 : _value.hashCode();
    }

    public int intValue()
    {
        switch (_typeName.ordinal_) {
        case SqlTypeName.Decimal_ordinal:
        case SqlTypeName.Double_ordinal:
            BigDecimal bd = (BigDecimal) _value;
            return bd.intValue();
        default:
            throw _typeName.unexpected();
        }
    }

    public String getStringValue()
    {
        return ((NlsString) _value).getValue();
    }

    public Object clone()
    {
        return new SqlLiteral(_value, _typeName);
    }

    public void unparse(SqlWriter writer,int leftPrec,int rightPrec)
    {
        switch (_typeName.ordinal_) {
        case SqlTypeName.Boolean_ordinal:
            writer.print(_value == null ? "UNKNOWN" :
                    ((Boolean) _value).booleanValue() ? "TRUE" :
                    "FALSE");
            break;
        case SqlTypeName.Binary_ordinal:
            writer.print("X'");
            writer.print(((BitString) _value).toHexString());
            writer.print("'");
            break;
        case SqlTypeName.Bit_ordinal:
            writer.print("B'");
            writer.print(_value.toString());
            writer.print("'");
            break;
        case SqlTypeName.Null_ordinal:
            writer.print("NULL");
            break;
        case SqlTypeName.Char_ordinal:
            if (false) {
                String stringValue = ((NlsString) _value).getValue();
                writer.print(writer.dialect.quoteStringLiteral(stringValue));
            }
            assert _value instanceof NlsString;
            writer.print(_value.toString());
            break;
        case SqlTypeName.Decimal_ordinal:
        case SqlTypeName.Double_ordinal:
            // should be handled in subtype
            throw _typeName.unexpected();
        default:
            writer.print(_value.toString());
        }
    }

    private static boolean equals(Object o1,Object o2)
    {
        return (o1 == null) ? (o2 == null) : o1.equals(o2);
    }

    public SaffronType createSqlType(SaffronTypeFactory typeFactory) {
        BitString bitString;
        switch (_typeName.ordinal_) {
        case SqlTypeName.Null_ordinal:
        case SqlTypeName.Boolean_ordinal:
            return typeFactory.createSqlType(_typeName);
        case SqlTypeName.Binary_ordinal:
            // REVIEW: should this be Binary, not Varbinary?
            bitString = (BitString) _value;
            int bitCount = bitString.getBitCount();
            if (bitCount % 8 == 0) {
                return typeFactory.createSqlType(SqlTypeName.Varbinary,
                        bitCount / 8);
            } else {
                return typeFactory.createSqlType(SqlTypeName.Bit,
                        bitCount);
            }
        case SqlTypeName.Bit_ordinal:
            assert _value instanceof BitString;
            bitString = (BitString) _value;
            return typeFactory.createSqlType(SqlTypeName.Bit,
                    bitString.getBitCount());
        case SqlTypeName.Char_ordinal:
            NlsString string = (NlsString) _value;
            SaffronType type = typeFactory.createSqlType(SqlTypeName.Varchar,
                    string.getValue().length());
            // REVIEW remove dependencies on validator
            SqlValidator.setCharsetIfCharType(type, string.getCharset());
            type.setCollation(string.getCollation());
            SqlValidator.setCollationIfCharType(type, null, SqlCollation.Coercibility.Coercible);
            SqlValidator.checkCharsetAndCollateConsistentIfCharType(type);
            if (null==string.getCharset()) {
                string.setCharset(type.getCharset());
            }
            if (null==string.getCollation()) {
                string.setCollation(type.getCollation());
            }
            return type;

        case SqlTypeName.Symbol_ordinal:
            // Existing code expects symbols to have a null type.
            if (true) {
                return null;
            }
            throw Util.newInternal("symbol does not have a SQL type: " +
                    _value);

        case SqlTypeName.Integer_ordinal: // handled in derived class
        case SqlTypeName.Time_ordinal: // handled in derived class
        case SqlTypeName.Varchar_ordinal: // should never happen
        case SqlTypeName.Varbinary_ordinal: // should never happen

//        } else if (value instanceof String) {
//            return typeFactory.createSqlType(SqlTypeName.Varchar, ((String) value).length());
//        } else if (value instanceof BigInteger) {
            //REVIEW 29-feb-2004 wael: can this else if clause safely be removed?
//            return typeFactory.createSqlType(SqlTypeName.Integer);
//        } else if (value instanceof java.math.BigDecimal) {
            //REVIEW 29-feb-2004 wael: can this else if clause safely be removed?
//            return typeFactory.createSqlType(SqlTypeName.Double);
        default:
            throw Util.needToImplement(toString() + ", operand=" + _value);
        }
    }

    public static DateLiteral createDate(Calendar calendar) {
        return new DateLiteral(calendar);
    }

    public static TimestampLiteral createTimestamp(Calendar calendar,
            int precision) {
        return new TimestampLiteral(calendar,precision);
    }

    public static TimeLiteral createTime(Calendar calendar, int precision) {
        return new TimeLiteral(calendar,precision);
    }

    /**
     * Creates a literal like B'0101'.
     */
    public static SqlLiteral createBitString(String s) {
        BitString bitString = BitString.createFromBitString(s);
        return new SqlLiteral(bitString,SqlTypeName.Bit);
    }

    /**
     * Creates a literal like X'ABAB'. Although it matters when we derive a
     * type for this beastie, we don't care at this point whether the number of
     * hexits is odd or even.
     */
    public static SqlLiteral createBinaryString(BitString b) {
        return new SqlLiteral(b, SqlTypeName.Binary);
    }

    /**
     * Creates a string literal, with optional collation.
     *
     * REVIEW (jhyde, 2004/5/28): There is parsing stuff going on in here --
     *   need to move that stuff into ParserUtil.
     *
     * @param s  String
     * @param collation Collation, may be null
     * @return A string literal
     */
    public static SqlLiteral createString(String s, SqlCollation collation) {

        if (s.charAt(0) == '\'') {
            //we have a "regular" string
            s = ParserUtil.strip(s, "'");
            s = ParserUtil.parseString(s);
            NlsString slit = new NlsString(s, null, collation);
            return new SqlLiteral(slit, SqlTypeName.Char);
        }

        //else we have a National string or a string with a charset
        String charSet;
        if (Character.toUpperCase(s.charAt(0)) == 'N') {
            s = s.substring(1);
            charSet = SaffronProperties.instance().defaultNationalCharset.get();
        } else {
            int i = s.indexOf("'");
            charSet = s.substring(1,i);
            s = s.substring(i);
        }
        s = ParserUtil.strip(s, "'");
        s = ParserUtil.parseString(s);
        NlsString nlsStr = new NlsString(s, charSet, collation);
        return new SqlLiteral(nlsStr, SqlTypeName.Char);
    }
}


// End SqlLiteral.java
