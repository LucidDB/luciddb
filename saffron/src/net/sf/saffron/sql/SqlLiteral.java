/*
// $Id$
// Saffron preprocessor and data engine
// (C) Copyright 2002-2003 Disruptive Technologies, Inc.
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

import net.sf.saffron.sql.parser.ParserUtil;
import net.sf.saffron.util.Util;

import java.math.BigInteger;
import java.nio.charset.Charset;

/**
 * A <code>SqlLiteral</code> is a constant. It is, appropriately, immutable.
 */
public class SqlLiteral extends SqlNode
{
    //~ Static fields/initializers --------------------------------------------

    // NOTE jvs 26-Jan-2004:  There's no singleton constant for a NULL literal.
    // Instead, nulls must be instantiated via createNull(), because
    // different instances have different context-dependent types.

    /** Constant for {@link Boolean#TRUE}. */
    public static final SqlLiteral True = new SqlLiteral(Boolean.TRUE);

    /** Constant for {@link Boolean#FALSE}. */
    public static final SqlLiteral False = new SqlLiteral(Boolean.FALSE);

    /** Constant for the {@link Integer} value 0. */
    public static final SqlLiteral Zero = new SqlLiteral(new Integer(0));

    /** Constant for the {@link Integer} value 1. */
    public static final SqlLiteral One = new SqlLiteral(new Integer(1));

    //~ Inner Classes   -------------------------------------------------------
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
            Object value;
            int prec;
            int scale;

            int i=s.indexOf('.');
            if (i>=0 && (s.length()-1!=i)) {
                value = ParserUtil.parseDecimal(s);
                scale = s.length()-i-1;
                assert(scale == ((java.math.BigDecimal) value).scale());
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
            Object value = ParserUtil.parseDecimal(s);
            return new Numeric(value,null,null, false);
        }

        protected Numeric(Object value, Integer prec, Integer scale, boolean isExact) {
            super(value);
            this.m_prec = prec;
            this.m_scale = scale;
            this.m_isExact = isExact;
        }

        public boolean isExact() {
            return m_isExact;
        }

    }

    public static class BitString {
        //~ Member variables -----------
        private String m_bits;

        //~ Methods -----------
        /**
         * Creates a BitString representation out of a Hex String. Initial zeros will be preserved.
         * Hex String is defined in the sqlstandard to be a string with odd nbr of hex digits.
         * An even nbr of hex digits is in the standard a Binary String.
         */
        public static BitString createFromHexString(String s){
            assert((s.length() & 1) !=0); //must be odd nbr of bits
            int lengthToBe = s.length()*4;

            //trick,add an additional non-zero bits hex digit in case hex string starts with 0,is there a smart(er) way?
            //DONT FORGET TO REMOVE IT AT THE END!!
            s="f"+s;
            String bits = new BigInteger(s,16).toString(2).substring(4,lengthToBe+4);
            assert(bits.length()==lengthToBe);
            return new BitString(bits);
        }

        /**
         * Creates a BitString representation out of a Bit String.  Initial zeros will be preserved.
         */
        public static BitString createFromBitString(String s){
            return new BitString(s);
        }

        protected BitString(String bits) {
            assert(bits.replaceAll("1","").replaceAll("0","").length()==0);//make sure we only have ones and zeros
            m_bits=bits;
        }

        public String toString() {
            return m_bits;
        }

        public int getBitCount() {
            return m_bits.length();
        }

        public byte[] getAsByteArray() {
            return Util.toByteArrayFromBitString(m_bits);
        }


    }

    public static class StringLiteral {
        private String m_charSetName;
        private String m_value;
        private Charset m_charSet;
        //~ Member variables -----------

        //~ Methods -----------
        /**
         * Creates a string in a specfied characher set.
         * @throws java.nio.charset.IllegalCharsetNameException -
         *         If the given charset name is illegal
         * @throws java.nio.charset.UnsupportedCharsetException -
         *         If no support for the named charset is available in this instance of the Java virtual machine
         */
        protected StringLiteral(String charSetName, String s) {
            charSetName = charSetName.toUpperCase();
            m_charSet = Charset.forName(charSetName);
            m_charSetName=charSetName;
            m_value=s;
        }

        public String getCharsetName() {
            return m_charSetName;
        }

        public Charset getCharset() {
            return m_charSet;
        }

        public static SqlLiteral create(String s){

            if (s.charAt(0) == '\'') {
                //we have a "regular" string
                s = ParserUtil.strip(s, "'");
                s = ParserUtil.parseString(s);
                return new SqlLiteral(s);
            }

            //else we have a National string or a string with a char set
            String charSet;
            if (Character.toUpperCase(s.charAt(0)) == 'N') {
                s= s.substring(1);
                charSet = "latin1"; //todo make this value configurable from the outside
            }
            else
            {
                int i = s.indexOf("'");
                charSet = s.substring(1,i);
                s = s.substring(i);
            }
            s = ParserUtil.strip(s, "'");
            s = ParserUtil.parseString(s);
            return new SqlLiteral(new StringLiteral(charSet,s));
        }

        public String toString() {
            return "_"+m_charSetName+"'"+
                    Util.replace(m_value,"'","''")+
                    "'";
        }

        public String getValue(){
            return m_value;
        }
    }

    //~ Instance fields -------------------------------------------------------

    public final Object value;

    //~ Constructors ----------------------------------------------------------

    /**
     * Creates a <code>SqlLiteral</code>.
     */
    public SqlLiteral(Object value)
    {
        this.value = value;
    }

    //~ Methods ---------------------------------------------------------------

    public SqlKind getKind()
    {
        return SqlKind.Literal;
    }

    /**
     * Returns the value of this literal.
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
     * Creates a NULL literal.
     */
    public static SqlLiteral createNull()
    {
        return new SqlLiteral(null);
    }

    /**
     * Creates a boolean literal.
     */
    public static SqlLiteral create(boolean b)
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
            return new SqlLiteral(new Integer(i));
        }
    }

    public static SqlLiteral create(java.sql.Date date)
    {
        return new SqlLiteral(date);
    }

    public static SqlLiteral create(java.sql.Time time)
    {
        return new SqlLiteral(time);
    }

    public static SqlLiteral create(java.sql.Timestamp timestamp)
    {
        return new SqlLiteral(timestamp);
    }

    public static SqlLiteral create(Object o)
    {
        return new SqlLiteral(o);
    }

    public boolean equals(Object obj)
    {
        return (obj instanceof SqlLiteral)
            && equals(((SqlLiteral) obj).value,value);
    }

    public int hashCode()
    {
        return (value == null) ? 0 : value.hashCode();
    }

    public static int intValue(SqlNode node)
    {
        return ((Integer) ((SqlLiteral) node).value).intValue();
    }

    public String getStringValue()
    {
        return (String) value;
    }

    public Object clone()
    {
        return new SqlLiteral(value);
    }

    public void unparse(SqlWriter writer,int leftPrec,int rightPrec)
    {
        if (value instanceof String) {
            writer.print(writer.dialect.quoteStringLiteral(getStringValue()));
        } else if (value instanceof Boolean) {
            writer.print(((Boolean) value).booleanValue() ? "TRUE" : "FALSE");
        } else if (value instanceof byte[]) {
            byte[] byteArray = (byte[]) value;
            writer.print("X'");
            writer.print(Util.toStringFromByteArray((byte[]) value,16));
            writer.print("'");
        } else if (value instanceof BitString) {
            writer.print("B'");
            writer.print(value.toString());
            writer.print("'");
        } else if (value == null) {
            writer.print("NULL");
        } else {
            writer.print(value.toString());
        }
    }

    private static boolean equals(Object o1,Object o2)
    {
        return (o1 == null) ? (o2 == null) : o1.equals(o2);
    }
}


// End SqlLiteral.java
