/*
// $Id$
// Saffron preprocessor and data engine
// (C) Copyright 2003-2003 Disruptive Technologies, Inc.
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
package net.sf.saffron.sql.parser;

import net.sf.saffron.resource.SaffronResource;
import net.sf.saffron.sql.SqlNode;
import net.sf.saffron.util.SaffronProperties;
import net.sf.saffron.util.Util;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.Charset;
import java.util.*;
import java.text.NumberFormat;


/**
 * Utility methods relating to parsing SQL.
 *
 * @author jhyde
 * @since Oct 7, 2003
 * @version $Id$
 **/
public final class ParserUtil {
    public static final String[] emptyStringArray = new String[0];
    public static final SqlNode[] emptySqlNodeArray = new SqlNode[0];
    public static final List emptyList = Collections.EMPTY_LIST;

    public static final String DateFormatStr = "yyyy-MM-dd";
    public static final String TimeFormatStr = "HH:mm:ss";
    public static final String PrecisionTimeFormatStr = TimeFormatStr + ".S";
    public static final String TimestampFormatStr = DateFormatStr + " " + TimeFormatStr;
    public static final String PrecisionTimestampFormatStr = TimestampFormatStr + ".S";

    /**
     * Helper class for {@link ParserUtil#parsePrecisionDateTimeLiteral}
     */
    public static class PrecisionTime {
        public Calendar cal;
        public int precision;
    }

    private ParserUtil() {}

    /** @return the character-set prefix of an sql string literal;
     * returns null if there is none */
    public static String getCharacterSet(String s) {
        if (s.charAt(0) == '\'')
            return null;
        if (Character.toUpperCase(s.charAt(0)) == 'N') 
            return SaffronProperties.instance().defaultNationalCharset.get();
        int i = s.indexOf("'");
        return s.substring(1,i); // skip prefixed '_'
    }
        
    /** converts the contents of an sql quoted string literal into a java string */
    public static String parseString(String s) {
        int i = s.indexOf("'"); // start of body
        if (i > 0) s = s.substring(i);
        return strip(s, "'");
    }

    public static BigDecimal parseDecimal(String s) {
        return new BigDecimal(s);
    }

    public static BigDecimal parseInteger(String s) {
        return new BigDecimal(s);
    }

    public static java.sql.Date parseDate(String s) {
        return java.sql.Date.valueOf(s);
    }

    /**
     * @deprecated Does not parse SQL:99 milliseconds
     */
    public static java.sql.Time parseTime(String s) {
        return java.sql.Time.valueOf(s);
    }

    public static java.sql.Timestamp parseTimestamp(String s) {
        return java.sql.Timestamp.valueOf(s);
    }

    /**
     * Parses a string using {@link java.text.SimpleDateFormat} and a given pattern
     * @param s string to be parsed
     * @param pattern {@link java.text.SimpleDateFormat} pattern
     * @param pp position to start parsing from
     * @return Null if parsing failed.
     * @pre pattern!=null
     */
    private static Calendar parseDateFormat(
            String s, String pattern, java.text.ParsePosition pp) {
        Util.pre(null!=pattern,"null!=pattern");
        java.text.SimpleDateFormat df = new java.text.SimpleDateFormat(pattern);
        java.util.TimeZone tz = new java.util.SimpleTimeZone(0, "GMT+00:00");
        Calendar ret = Calendar.getInstance(tz);
        df.setCalendar(ret);
        df.setLenient(false);

        java.util.Date d = df.parse(s, pp);
        if (null==d) {
            return null;
        }
        ret.setTime(d);
        return ret;
    }

    /**
     * Parses a string using {@link java.text.SimpleDateFormat} and a given pattern
     * @param s string to be parsed
     * @param pattern {@link java.text.SimpleDateFormat} pattern
     * @return Null if parsing failed.
     * @pre pattern!=null
     */
    public static Calendar parseDateFormat(String s, String pattern) {
        java.text.ParsePosition pp = new java.text.ParsePosition(0);
        Calendar ret = parseDateFormat(s, pattern, pp);
        if (pp.getIndex() != s.length()) {
            // Didn't consume entire string - not good
            return null;
        }
        return ret;
    }

    public static PrecisionTime parsePrecisionDateTimeLiteral(String s,
                        String pattern) {
        java.text.ParsePosition pp = new java.text.ParsePosition(0);
        Calendar cal = parseDateFormat(s, pattern, pp);
        if (cal == null) return null; // Invalid date/time format

        int p = 0;
        if (pp.getIndex() < s.length()) {
            // Check to see if rest is decimal portion
            if (s.charAt(pp.getIndex()) != '.') return null;
            // Skip decimal sign
            pp.setIndex(pp.getIndex() + 1);

            // Parse decimal portion
            if (pp.getIndex() < s.length()) {
                String secFraction = s.substring(pp.getIndex());
                if (!secFraction.matches("\\d+")) return null;
                NumberFormat nf = NumberFormat.getIntegerInstance();
                Number num = nf.parse(s, pp);
                if ((num == null) || (pp.getIndex() != s.length())) {
                    // Invalid decimal portion
                    return null;
                }

                // Determine precision - only support prec 3 or lower (milliseconds)
                // Higher precisions are quietly rounded away
                p =  Math.min(3, secFraction.length());

                // Calculate milliseconds
                int ms = (int) Math.round(num.longValue() *
                    Math.pow(10,3 - secFraction.length()));
                cal.add(Calendar.MILLISECOND, ms);
            }
        }

        assert(pp.getIndex() == s.length());
        PrecisionTime ret = new PrecisionTime();
        ret.cal = cal;
        ret.precision = p;
        return ret;
    }

    /**
     * Parses a Binary string. SQL:99 defines a binary string as a hexstring with EVEN nbr of hex digits.
     */
    public static byte[] parseBinaryString(String s) {
        s=s.replaceAll(" ","");
        s=s.replaceAll("\n","");
        s=s.replaceAll("\t","");
        s=s.replaceAll("\r","");
        s=s.replaceAll("\f","");
        s=s.replaceAll("'","");

        if (s.length()==0) {
            return new byte[0];
        }
        assert((s.length()&1)==0); //must be even nbr of hex digits

        //trick, add an additional non-zero bits byte in case hex string starts with 0, is there a smart(er) way?
        //DONT FORGET TO REMOVE IT AT THE END!!
        final int lengthToBe = s.length()/2;
        s="ff"+s;
        BigInteger bigInt = new BigInteger(s,16);
        byte[] ret = new byte[lengthToBe];
        System.arraycopy(bigInt.toByteArray(),2,ret,0,ret.length);
        return ret;
    }

    /**
     * Unquotes a quoted string. For example,
     * <code>strip("'it''s got quotes'")</code> returns
     * <code>"it's got quotes"</code>.
     */
    public static String strip(String s, String quote) {
        assert s.startsWith(quote) && s.endsWith(quote) : s;
        return s.substring(1, s.length() - 1).replaceAll(quote + quote, quote);
    }

    /**
     * Trims a string for given characters from left and right. E.g.
     * <code>trim("aBaac123AabC","abBcC")</code> returns
     * </code>"123A"</code>
     */
    public static String trim(String s, String chars) {
        if (s.length()==0) {
            return "";
        }

        int start;
        for (start = 0; start < s.length(); start++) {
            char c = s.charAt(start);
            if (chars.indexOf(c)<0){
                break;
            }
        }

        int stop;
        for (stop=s.length();stop>start;stop--) {
            char c = s.charAt(stop-1);
            if (chars.indexOf(c)<0){
                break;
            }
        }

        if (start>=stop) {
            return "";
        }

        return s.substring(start,stop);
    }

    /**
     * Extracts the values from a collation name.
     * Collation names are on the form <i>charset$locale$strength</i>
     * @param in The collation name
     * @return An array of length 3. Each element object represents the three
     * parts of the collation name.<br>
     * <i>1st</i> is an object of type <code>{@ java.nio.charset.Charset}</code><br>
     * <i>2nd</i> is an object of type <code>{@ java.util.Locale}</code><br>
     * <i>3rd</i> is an object of type <code>{@ java.lang.String}</code><br>
     */
    public static Object[] parseCollation(String in) {
        Object[] ret = new Object[3];
        StringTokenizer st = new StringTokenizer(in, "$");
        String charsetStr = st.nextToken();
        String localeStr = st.nextToken();
        if (st.countTokens() > 0) {
            ret[2] = st.nextToken();
        } else {
            ret[2] = SaffronProperties.instance().defaultCollationStrength
                    .get();
        }

        ret[0] = Charset.forName(charsetStr);
        String[] localeParts = localeStr.split("_");
        Locale locale;
        if (1==localeParts.length) {
            locale=new Locale(localeParts[0]);
        } else if (2==localeParts.length) {
            locale=new Locale(localeParts[0],localeParts[1]);
        } else if (3==localeParts.length) {
            locale=new Locale(localeParts[0],localeParts[1],localeParts[2]);
        } else {
            throw SaffronResource.instance().
                  newParserError("Locale '"+localeStr+"' in an illegal format");
        }
        ret[1]=locale;
        return ret;
    }

    public static String[] toStringArray(List list) {
        return (String[]) list.toArray(emptyStringArray);
    }

    public static SqlNode[] toNodeArray(List list) {
        return (SqlNode[]) list.toArray(emptySqlNodeArray);
    }

    public static String rightTrim(String s, char c) {
        int stop;
        for (stop=s.length();stop>0;stop--) {
            if (s.charAt(stop-1) != c){
                break;
            }
        }
        if (stop > 0) {
            return s.substring(0,stop);
        }
        return "";
    }
}

// End ParserUtil.java
