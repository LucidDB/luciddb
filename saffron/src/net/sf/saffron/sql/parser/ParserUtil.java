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

import net.sf.saffron.sql.SqlNode;

import java.util.List;
import java.util.Collections;
import java.math.BigDecimal;
import java.math.BigInteger;

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
     * <code>trimLeft("aBaac123AabC","abBcC")</code> returns
     * </code>"123AaBc"</code>
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

    public static String[] toStringArray(List list) {
        return (String[]) list.toArray(emptyStringArray);
    }

    public static SqlNode[] toNodeArray(List list) {
        return (SqlNode[]) list.toArray(emptySqlNodeArray);
    }

}

// End ParserUtil.java
