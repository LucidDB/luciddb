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

package org.eigenbase.sql.parser;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.Charset;
import java.text.NumberFormat;
import java.util.*;
import java.util.logging.Logger;

import org.eigenbase.resource.EigenbaseResource;
import org.eigenbase.sql.*;
import org.eigenbase.trace.EigenbaseTrace;
import org.eigenbase.util.SaffronProperties;
import org.eigenbase.util.Util;


/**
 * Utility methods relating to parsing SQL.
 *
 * @author jhyde
 * @since Oct 7, 2003
 * @version $Id$
 **/
public final class ParserUtil
{
    //~ Static fields/initializers --------------------------------------------

    static final Logger tracer = EigenbaseTrace.getParserTracer();
    public static final String [] emptyStringArray = new String[0];
    public static final SqlNode [] emptySqlNodeArray = new SqlNode[0];
    public static final List emptyList = Collections.EMPTY_LIST;
    public static final String DateFormatStr = "yyyy-MM-dd";
    public static final String TimeFormatStr = "HH:mm:ss";
    public static final String PrecisionTimeFormatStr = TimeFormatStr + ".S";
    public static final String TimestampFormatStr =
        DateFormatStr + " " + TimeFormatStr;
    public static final String PrecisionTimestampFormatStr =
        TimestampFormatStr + ".S";

    //~ Constructors ----------------------------------------------------------

    private ParserUtil()
    {
    }

    //~ Methods ---------------------------------------------------------------

    /** @return the character-set prefix of an sql string literal;
     * returns null if there is none */
    public static String getCharacterSet(String s)
    {
        if (s.charAt(0) == '\'') {
            return null;
        }
        if (Character.toUpperCase(s.charAt(0)) == 'N') {
            return SaffronProperties.instance().defaultNationalCharset.get();
        }
        int i = s.indexOf("'");
        return s.substring(1, i); // skip prefixed '_'
    }

    /** converts the contents of an sql quoted string literal into a java string */
    public static String parseString(String s)
    {
        int i = s.indexOf("'"); // start of body
        if (i > 0) {
            s = s.substring(i);
        }
        return strip(s, "'");
    }

    public static BigDecimal parseDecimal(String s)
    {
        return new BigDecimal(s);
    }

    public static BigDecimal parseInteger(String s)
    {
        return new BigDecimal(s);
    }

    public static java.sql.Date parseDate(String s)
    {
        return java.sql.Date.valueOf(s);
    }

    /**
     * @deprecated Does not parse SQL:99 milliseconds
     */
    public static java.sql.Time parseTime(String s)
    {
        return java.sql.Time.valueOf(s);
    }

    public static java.sql.Timestamp parseTimestamp(String s)
    {
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
        String s,
        String pattern,
        java.text.ParsePosition pp)
    {
        Util.pre(null != pattern, "null!=pattern");
        java.text.SimpleDateFormat df =
            new java.text.SimpleDateFormat(pattern);
        java.util.TimeZone tz = new java.util.SimpleTimeZone(0, "GMT+00:00");
        Calendar ret = Calendar.getInstance(tz);
        df.setCalendar(ret);
        df.setLenient(false);

        java.util.Date d = df.parse(s, pp);
        if (null == d) {
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
    public static Calendar parseDateFormat(
        String s,
        String pattern)
    {
        java.text.ParsePosition pp = new java.text.ParsePosition(0);
        Calendar ret = parseDateFormat(s, pattern, pp);
        if (pp.getIndex() != s.length()) {
            // Didn't consume entire string - not good
            return null;
        }
        return ret;
    }

    public static PrecisionTime parsePrecisionDateTimeLiteral(
        String s,
        String pattern)
    {
        java.text.ParsePosition pp = new java.text.ParsePosition(0);
        Calendar cal = parseDateFormat(s, pattern, pp);
        if (cal == null) {
            return null; // Invalid date/time format
        }

        int p = 0;
        if (pp.getIndex() < s.length()) {
            // Check to see if rest is decimal portion
            if (s.charAt(pp.getIndex()) != '.') {
                return null;
            }

            // Skip decimal sign
            pp.setIndex(pp.getIndex() + 1);

            // Parse decimal portion
            if (pp.getIndex() < s.length()) {
                String secFraction = s.substring(pp.getIndex());
                if (!secFraction.matches("\\d+")) {
                    return null;
                }
                NumberFormat nf = NumberFormat.getIntegerInstance();
                Number num = nf.parse(s, pp);
                if ((num == null) || (pp.getIndex() != s.length())) {
                    // Invalid decimal portion
                    return null;
                }

                // Determine precision - only support prec 3 or lower (milliseconds)
                // Higher precisions are quietly rounded away
                p = Math.min(
                        3,
                        secFraction.length());

                // Calculate milliseconds
                int ms =
                    (int) Math.round(num.longValue() * Math.pow(10,
                                3 - secFraction.length()));
                cal.add(Calendar.MILLISECOND, ms);
            }
        }

        assert (pp.getIndex() == s.length());
        PrecisionTime ret = new PrecisionTime();
        ret.cal = cal;
        ret.precision = p;
        return ret;
    }

    /**
     * Parses a Binary string. SQL:99 defines a binary string as a hexstring with EVEN nbr of hex digits.
     */
    public static byte [] parseBinaryString(String s)
    {
        s = s.replaceAll(" ", "");
        s = s.replaceAll("\n", "");
        s = s.replaceAll("\t", "");
        s = s.replaceAll("\r", "");
        s = s.replaceAll("\f", "");
        s = s.replaceAll("'", "");

        if (s.length() == 0) {
            return new byte[0];
        }
        assert ((s.length() & 1) == 0); //must be even nbr of hex digits

        final int lengthToBe = s.length() / 2;
        s = "ff" + s;
        BigInteger bigInt = new BigInteger(s, 16);
        byte [] ret = new byte[lengthToBe];
        System.arraycopy(
            bigInt.toByteArray(),
            2,
            ret,
            0,
            ret.length);
        return ret;
    }

    /**
     * Unquotes a quoted string. For example,
     * <code>strip("'it''s got quotes'")</code> returns
     * <code>"it's got quotes"</code>.
     */
    public static String strip(
        String s,
        String quote)
    {
        assert s.startsWith(quote) && s.endsWith(quote) : s;
        return s.substring(1, s.length() - 1).replaceAll(quote + quote, quote);
    }

    /**
     * Trims a string for given characters from left and right. E.g.
     * <code>trim("aBaac123AabC","abBcC")</code> returns
     * </code>"123A"</code>
     */
    public static String trim(
        String s,
        String chars)
    {
        if (s.length() == 0) {
            return "";
        }

        int start;
        for (start = 0; start < s.length(); start++) {
            char c = s.charAt(start);
            if (chars.indexOf(c) < 0) {
                break;
            }
        }

        int stop;
        for (stop = s.length(); stop > start; stop--) {
            char c = s.charAt(stop - 1);
            if (chars.indexOf(c) < 0) {
                break;
            }
        }

        if (start >= stop) {
            return "";
        }

        return s.substring(start, stop);
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
    public static Object [] parseCollation(String in)
    {
        Object [] ret = new Object[3];
        StringTokenizer st = new StringTokenizer(in, "$");
        String charsetStr = st.nextToken();
        String localeStr = st.nextToken();
        if (st.countTokens() > 0) {
            ret[2] = st.nextToken();
        } else {
            ret[2] =
                SaffronProperties.instance().defaultCollationStrength.get();
        }

        ret[0] = Charset.forName(charsetStr);
        String [] localeParts = localeStr.split("_");
        Locale locale;
        if (1 == localeParts.length) {
            locale = new Locale(localeParts[0]);
        } else if (2 == localeParts.length) {
            locale = new Locale(localeParts[0], localeParts[1]);
        } else if (3 == localeParts.length) {
            locale =
                new Locale(localeParts[0], localeParts[1], localeParts[2]);
        } else {
            // FIXME jvs 28-Aug-2004:  i18n
            throw EigenbaseResource.instance().newParserError("Locale '"
                + localeStr + "' in an illegal format");
        }
        ret[1] = locale;
        return ret;
    }

    public static String [] toStringArray(List list)
    {
        return (String []) list.toArray(emptyStringArray);
    }

    public static SqlNode [] toNodeArray(List list)
    {
        return (SqlNode []) list.toArray(emptySqlNodeArray);
    }

    public static String rightTrim(
        String s,
        char c)
    {
        int stop;
        for (stop = s.length(); stop > 0; stop--) {
            if (s.charAt(stop - 1) != c) {
                break;
            }
        }
        if (stop > 0) {
            return s.substring(0, stop);
        }
        return "";
    }

    /**
     * Replaces a range of elements in a list with a single element.
     * For example, if list contains <code>{A, B, C, D, E}</code> then
     * <code>replaceSublist(list, X, 1, 4)</code> returns
     * <code>{A, X, E}</code>.
     */
    public static void replaceSublist(
        List list,
        int start,
        int end,
        Object o)
    {
        Util.pre(list != null, "list != null");
        Util.pre(start < end, "start < end");
        for (int i = end - 1; i > start; --i) {
            list.remove(i);
        }
        list.set(start, o);
    }

    /**
     * Converts a list of {expression, operator, expression, ...} into a tree,
     * taking operator precedence and associativity into account.
     *
     * @pre list.size() % 2 == 1
     */
    public static SqlNode toTree(List list)
    {
        tracer.finer("Attempting to reduce " + list);
        final SqlNode node = toTreeEx(list, 0, 0, SqlKind.Other);
        tracer.fine("Reduced " + node);
        return node;
    }

    /**
     * Converts a list of {expression, operator, expression, ...} into a tree,
     * taking operator precedence and associativity into account.
     *
     * @param list List of operands and operators. This list is modified as
     *     expressions are reduced.
     * @param start Position of first operand in the list. Anything to the
     *     left of this (besides the immediately preceding operand) is ignored.
     *     Generally use value 1.
     * @param minPrec Minimum precedence to consider. If the method encounters
     *     an operator of lower precedence, it doesn't reduce any further.
     * @param stopperKind If not {@link SqlKind#Other}, stop reading the list
     *     if we encounter a token of this kind.
     * @return
     */
    public static SqlNode toTreeEx(
        List list,
        int start,
        int minPrec,
        SqlKind stopperKind)
    {
// Make several passes over the list, and each pass, coalesce the
// expressions with the highest precedence.
outer: 
        while (true) {
            final int count = list.size();
            if (count <= (start + 1)) {
                break;
            }
            int i = start + 1;
            while (i < count) {
                SqlOperator previous;
                SqlOperator current = ((ToTreeListItem) list.get(i)).op;
                ParserPosition currentPos = ((ToTreeListItem) list.get(i)).pos;
                if ((stopperKind != SqlKind.Other)
                        && (current.kind == stopperKind)) {
                    break outer;
                }
                SqlOperator next;
                int previousRight;
                int left = current.leftPrec;
                int right = current.rightPrec;
                if (left < minPrec) {
                    break outer;
                }
                int nextLeft;
                if (current instanceof SqlBinaryOperator) {
                    if (i == (start + 1)) {
                        previous = null;
                        previousRight = 0;
                    } else {
                        previous = ((ToTreeListItem) list.get(i - 2)).op;
                        previousRight = previous.rightPrec;
                    }
                    if (i == (count - 2)) {
                        next = null;
                        nextLeft = 0;
                    } else {
                        next = ((ToTreeListItem) list.get(i + 2)).op;
                        nextLeft = next.leftPrec;
                        if ((next.kind == stopperKind)
                                && (stopperKind != SqlKind.Other)) {
                            // Suppose we're looking at 'AND' in
                            //    a BETWEEN b OR c AND d
                            //
                            // Because 'AND' is our stopper token, we still
                            // want to reduce 'b OR c', even though 'AND' has
                            // higher precedence than 'OR'.
                            nextLeft = 0;
                        }
                    }
                    if ((previousRight < left) && (right >= nextLeft)) {
                        // For example,
                        //    i:  0 1 2 3 4 5 6 7 8
                        // list:  a + b * c * d + e
                        // prec: 0 1 2 3 4 3 4 1 2 0
                        //
                        // At i == 3, we have the first '*' operator, and its
                        // surrounding precedences obey the relation 2 < 3 and
                        // 4 >= 3, so we can reduce (b * c) to a single node.
                        SqlNode leftExp = (SqlNode) list.get(i - 1);

                        // For example,
                        //    i:  0 1 2 3 4 5 6 7 8
                        // list:  a + b * c * d + e
                        // prec: 0 1 2 3 4 3 4 1 2 0
                        //
                        // At i == 3, we have the first '*' operator, and its
                        // surrounding precedences obey the relation 2 < 3 and
                        // 4 >= 3, so we can reduce (b * c) to a single node.
                        SqlNode rightExp = (SqlNode) list.get(i + 1);
                        final SqlCall newExp =
                            current.createCall(leftExp, rightExp, currentPos);
                        tracer.fine("Reduced infix: " + newExp);

                        // Replace elements {i - 1, i, i + 1} with the new
                        // expression.
                        replaceSublist(list, i - 1, i + 2, newExp);
                        break;
                    }
                    i += 2;
                } else if (current instanceof SqlPostfixOperator) {
                    if (i == (start + 1)) {
                        previous = null;
                        previousRight = 0;
                    } else {
                        previous = ((ToTreeListItem) list.get(i - 2)).op;
                        previousRight = previous.rightPrec;
                    }
                    if (previousRight < left) {
                        // For example,
                        //    i:  0 1 2 3 4 5 6 7 8
                        // list:  a + b * c ! + d
                        // prec: 0 1 2 3 4 3 0 2
                        //
                        // At i == 3, we have the postfix '!' operator. Its
                        // high precedence determines that it binds with 'b *
                        // c'. The precedence of the following '+' operator is
                        // irrelevant.
                        SqlNode leftExp = (SqlNode) list.get(i - 1);

                        final SqlCall newExp =
                            current.createCall(leftExp, currentPos);
                        tracer.fine("Reduced postfix: " + newExp);

                        // Replace elements {i - 1, i} with the new expression.
                        list.remove(i);
                        list.set(i - 1, newExp);
                        break;
                    }
                    ++i;

                    //
                } else if (current instanceof SqlSpecialOperator) {
                    SqlSpecialOperator specOp = (SqlSpecialOperator) current;

                    // We decide to reduce a special operator only on the basis
                    // of what's to the left of it. The operator then decides
                    // how far to the right to chew off.
                    if (i == (start + 1)) {
                        previous = null;
                        previousRight = 0;
                    } else {
                        previous = ((ToTreeListItem) list.get(i - 2)).op;
                        previousRight = previous.rightPrec;
                    }
                    int nextOrdinal = i + 2;
                    if (i == (count - 2)) {
                        next = null;
                        nextLeft = 0;
                    } else {
                        next = ((ToTreeListItem) list.get(nextOrdinal)).op;
                        nextLeft = next.leftPrec;
                        if ((stopperKind != SqlKind.Other)
                                && (next.kind == stopperKind)) {
                            break outer;
                        }
                    }
                    if (nextLeft < minPrec) {
                        break outer;
                    }
                    if ((previousRight < left) && (right >= nextLeft)) {
                        i = specOp.reduceExpr(i, list);
                        tracer.fine("Reduced special op: " + list.get(i));
                        break;
                    }
                    i = nextOrdinal;
                } else {
                    throw Util.newInternal("Unexpected operator type: "
                        + current);
                }
            }

            // Require the list shrinks each time around -- otherwise we will
            // never terminate.
            assert list.size() < count;
        }
        return (SqlNode) list.get(start);
    }

    //~ Inner Classes ---------------------------------------------------------

    /**
     * Helper class for {@link ParserUtil#parsePrecisionDateTimeLiteral}
     */
    public static class PrecisionTime
    {
        public Calendar cal;
        public int precision;
    }

    /**
     * Class that holds a {@link SqlOperator} and a {@link ParserPosition}.
     * Used by {@link #toTree} and the parser to associate a parsed operator
     * with a parser position.
     */
    public static class ToTreeListItem
    {
        public SqlOperator op;
        public ParserPosition pos;

        public ToTreeListItem(
            SqlOperator op,
            ParserPosition pp)
        {
            this.op = op;
            this.pos = pp;
        }
    }
}


// End ParserUtil.java
