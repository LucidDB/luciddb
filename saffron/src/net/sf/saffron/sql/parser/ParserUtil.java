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

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.Charset;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.StringTokenizer;


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
    public static final String PrecisionTimeFormatStr = TimeFormatStr + ".SSSSSS";
    public static final String TimestampFormatStr = DateFormatStr + " " + TimeFormatStr;
    public static final String PrecisionTimestampFormatStr = TimestampFormatStr + ".SSSSSS";


    private ParserUtil() {}

    public static String parseString(String s) {
        return s.replaceAll("''", "'");
    }

    public static Number parseDecimal(String s) {
        return new BigDecimal(s);
    }

    public static Number parseInteger(String s) {
        return new BigInteger(s);
    }

    public static java.sql.Date parseDate(String s) {
        return java.sql.Date.valueOf(s);
    }

    public static java.sql.Time parseTime(String s) {
        return java.sql.Time.valueOf(s);
    }

    public static java.sql.Timestamp parseTimestamp(String s) {
        return java.sql.Timestamp.valueOf(s);
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
}

// End ParserUtil.java
