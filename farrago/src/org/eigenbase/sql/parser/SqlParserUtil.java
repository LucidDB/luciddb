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

package org.eigenbase.sql.parser;

import org.eigenbase.resource.EigenbaseResource;
import org.eigenbase.sql.*;
import org.eigenbase.trace.EigenbaseTrace;
import org.eigenbase.util.SaffronProperties;
import org.eigenbase.util.Util;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.Charset;
import java.text.NumberFormat;
import java.text.ParsePosition;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.logging.Logger;
import java.util.logging.Level;


/**
 * Utility methods relating to parsing SQL.
 *
 * @author jhyde
 * @since Oct 7, 2003
 * @version $Id$
 **/
public final class SqlParserUtil
{
    //~ Static fields/initializers --------------------------------------------

    static final Logger tracer = EigenbaseTrace.getParserTracer();
    public static final String [] emptyStringArray = new String[0];
    public static final List emptyList = Collections.EMPTY_LIST;
    public static final String DateFormatStr = "yyyy-MM-dd";
    public static final String TimeFormatStr = "HH:mm:ss";
    public static final String PrecisionTimeFormatStr = TimeFormatStr + ".S";
    public static final String TimestampFormatStr =
        DateFormatStr + " " + TimeFormatStr;
    public static final String PrecisionTimestampFormatStr =
        TimestampFormatStr + ".S";

    //~ Constructors ----------------------------------------------------------

    private SqlParserUtil()
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

    /**
     * Converts the contents of an sql quoted string literal into a java
     * string.
     */
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
     * Checks if the date/time format is valid
     * @param pattern {@link SimpleDateFormat} pattern
     */
    public static void checkDateFormat(String pattern)
    {
        SimpleDateFormat df = new SimpleDateFormat(pattern);
    }

    /**
     * Parses a string using {@link SimpleDateFormat} and a given pattern
     *
     * @param s string to be parsed
     * @param pattern {@link SimpleDateFormat} pattern
     * @param pp position to start parsing from
     * @return Null if parsing failed.
     * @pre pattern != null
     */
    private static Calendar parseDateFormat(
        String s,
        String pattern,
        TimeZone tz,
        ParsePosition pp)
    {
        Util.pre(pattern != null, "pattern != null");
        SimpleDateFormat df = new SimpleDateFormat(pattern);
        if (tz == null) {
            tz = new SimpleTimeZone(0, "GMT+00:00");
        }
        Calendar ret = Calendar.getInstance(tz);
        df.setCalendar(ret);
        df.setLenient(false);

        Date d = df.parse(s, pp);
        if (null == d) {
            return null;
        }
        ret.setTime(d);
        return ret;
    }

    /**
     * Parses a string using {@link SimpleDateFormat} and a given pattern.
     *
     * @param s string to be parsed
     * @param pattern {@link SimpleDateFormat} pattern
     * @return Null if parsing failed.
     * @pre pattern != null
     */
    public static Calendar parseDateFormat(
        String s,
        String pattern,
        TimeZone tz)
    {
        Util.pre(pattern != null, "pattern != null");
        ParsePosition pp = new ParsePosition(0);
        Calendar ret = parseDateFormat(s, pattern, tz, pp);
        if (pp.getIndex() != s.length()) {
            // Didn't consume entire string - not good
            return null;
        }
        return ret;
    }

    public static PrecisionTime parsePrecisionDateTimeLiteral(
        String s,
        String pattern,
        TimeZone tz)
    {
        ParsePosition pp = new ParsePosition(0);
        Calendar cal = parseDateFormat(s, pattern, tz, pp);
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
        PrecisionTime ret = new PrecisionTime(cal, p);
        return ret;
    }

    /**
     * Parses a INTERVAL value.
     * @return an int array where each element in the array represents a time
     * unit in the input string.<br>
     * NOTE: that the first element in the array indicates the sign of the value
     * E.g<br>
     * An input string of: <code>'364 23:59:59.9999' INTERVAL DAY TO SECOND'</code><br>
     * would make this method return<br>
     * <code>int[] {1, 364, 23, 59, 59, 9999 }</code><br>
     * An negative interval value: <code>'-364 23:59:59.9999' INTERVAL DAY TO SECOND'</code><br>
     * would make this method return<br>
     * <code>int[] {-1, 364, 23, 59, 59, 9999 }</code><br>
     * @return null if the interval value is illegal.
     * Illegal values are:
     * <ul>
     *  <li>non digit character (except optional minus '-'
     *                          at the first character in the input string.)
     *  </li>
     *  <li>the number of time units described in
     *      intervalQualifer doesn't match the parsed number of time units.
     *  </li>
     * </ul>
     */
    public static int[] parseIntervalValue(SqlIntervalLiteral.IntervalValue interval)
    {
        String value = interval.getIntervalLiteral();
        SqlIntervalQualifier intervalQualifier = interval.getIntervalQualifier();

        value = value.trim();
        if (Util.isNullOrEmpty(value)) {
            return null;
        }

        int sign = 1;
        if ('-' == value.charAt(0)) {
            sign = -1;
            if (value.length()==1) {
                // handles the case when we have a single input value of '-'
                return null;
            }
            value = value.substring(1);
        }

        try {
            if (intervalQualifier.isYearMonth()) {
                //~------ YEAR-MONTH INTERVAL
                int years = 0;
                int months = 0;
                String[] valArray = value.split("-");
                if (2 == valArray.length) {
                    years = parsePositiveInt(valArray[0]);
                    months = parsePositiveInt(valArray[1]);
                    return new int[] { sign, years, months };
                } else if (1 == valArray.length) {
                    return new int[] { sign, parsePositiveInt(valArray[0]) };
                }
                return null;
            } else {
                //~------ DAY-TIME INTERVAL
                String[] withDayPattern = {
                    "(\\d) (\\d+):(\\d+):(\\d+)\\.(\\d+)"  //same trice
                    ,"(\\d) (\\d+):(\\d+):(\\d+)\\.(\\d+)" //same trice
                    ,"(\\d) (\\d+):(\\d+):(\\d+)\\.(\\d+)" //same trice
                    ,"(\\d+)"
                    ,"(\\d+) (\\d+)"
                    ,"(\\d+) (\\d+):(\\d+)"
                    ,"(\\d+) (\\d+):(\\d+):(\\d+)"
                };

                String[] withoutDayPattern = {
                    "(\\d+):(\\d+):(\\d+)\\.(\\d+)"
                    ,"(\\d+):(\\d+)\\.(\\d+)"
                    ,"(\\d+)\\.(\\d+)"
                    ,"(\\d+)"
                    ,"(\\d+):(\\d+)"
                    ,"(\\d+):(\\d+):(\\d+)"
                };

                String[] ps;
                if (SqlIntervalQualifier.TimeUnit.Day.equals(
                    intervalQualifier.getStartUnit())) {
                    ps = withDayPattern;
                } else {
                    ps = withoutDayPattern;
                }

                for (int iPattern = 0; iPattern < ps.length; iPattern++) {
                    String p = ps[iPattern];
                    Matcher m = Pattern.compile(p).matcher(value);
                    if (m.matches()) {
                        int timeUnitsCount = m.groupCount();
                        int[] ret = new int[timeUnitsCount+1];
                        ret[0] = sign;
                        for (int iGroup = 1; iGroup <= m.groupCount(); iGroup++) {
                            ret[iGroup] = Integer.parseInt(m.group(iGroup));
                        }

                        if (iPattern < 3) {
                            timeUnitsCount--;
                        }

                        SqlIntervalQualifier.TimeUnit start =
                            intervalQualifier.getStartUnit();
                        SqlIntervalQualifier.TimeUnit end =
                            intervalQualifier.getEndUnit();
                        if (null==end && timeUnitsCount>1) {
                            return null;
                        } else if ((null!=end) &&
                            ((end.getOrdinal()-start.getOrdinal()+1)!=timeUnitsCount)) {
                            return null;
                        }
                        return ret;
                    }
                }
                return null;
            }
        } catch (NumberFormatException e) {
            return null;
        }
    }

    /**
     * Parses a positive int. All characters have to be digits.
     * @see {@link java.lang.Integer#parseInt(String)}
     */
    public static int parsePositiveInt(String value) throws NumberFormatException
    {
        value = value.trim();
        if (value.charAt(0) == '-') {
            throw new NumberFormatException(value);
        }
        return Integer.parseInt(value);
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
     * Looks for one or two carets in a SQL string, and if present, converts
     * them into a parser position.
     *
     * <p>Examples:<ul>
     * <li>findPos("xxx^yyy") yields {"xxxyyy", position 3, line 1 column 4}
     * <li>findPos("xxxyyy") yields {"xxxyyy", null}
     * <li>findPos("xxx^yy^y") yields {"xxxyyy", position 3, line 4 column 4
     *     through line 1 column 6}
     * </ul>
     */
    public static StringAndPos findPos(String sql)
    {
        int firstCaret = sql.indexOf('^');
        if (firstCaret < 0) {
            return new StringAndPos(sql, -1, null);
        }
        int secondCaret = sql.indexOf('^', firstCaret + 1);
        if (secondCaret < 0) {
            String sqlSansCaret = sql.substring(0, firstCaret) +
                sql.substring(firstCaret + 1);
            int[] start = indexToLineCol(sql, firstCaret);
            SqlParserPos pos = new SqlParserPos(start[0], start[1]);
            return new StringAndPos(sqlSansCaret, firstCaret, pos);
        } else {
            String sqlSansCaret = sql.substring(0, firstCaret) +
                sql.substring(firstCaret + 1, secondCaret) +
                sql.substring(secondCaret + 1);
            int[] start = indexToLineCol(sql, firstCaret);
            // subtract 1 because first caret pushed the string out
            --secondCaret;
            // subtract 1 because the col position needs to be inclusive
            --secondCaret;
            int[] end = indexToLineCol(sql, secondCaret);
            SqlParserPos pos =
                new SqlParserPos(start[0], start[1], end[0], end[1]);
            StringAndPos sap = new StringAndPos(sqlSansCaret, firstCaret, pos);
            return sap;
        }
    }

    /**
     * Returns the (1-based) line and column corresponding to a particular
     * (0-based) offset in a string.
     *
     * <p>Converse of {@link #lineColToIndex(String, int, int)}.
     */
    public static int[] indexToLineCol(String sql, int i)
    {
        int line = 0;
        int j = 0;
        while (true) {
            int prevj = j;
            j = sql.indexOf(Util.lineSeparator, j + 1);
            if (j < 0 || j > i) {
                return new int[] {line + 1, i - prevj + 1};
            }
            j += Util.lineSeparator.length();
            ++line;
        }
    }

    /**
     * Finds the position (0-based) in a string which corresponds to a
     * given line and column (1-based).
     *
     * <p>Converse of {@link #indexToLineCol(String, int)}.
     */
    public static int lineColToIndex(String sql, int line, int column)
    {
        --line;
        --column;
        int i = 0;
        while (line-- > 0) {
            i = sql.indexOf(Util.lineSeparator, i) +
                Util.lineSeparator.length();
        }
        return i + column;
    }

    /**
     * Converts a string to a string with one or two carets in it.
     * For example, <code>addCarets("values (foo)", 1, 9, 1, 12)</code>
     * yields "values (^foo^)".
     */
    public static String addCarets(
        String sql, int line, int col, int endLine, int endCol)
    {
        String sqlWithCarets;
        int cut = lineColToIndex(sql, line, col);
        sqlWithCarets = sql.substring(0, cut) + "^" +
            sql.substring(cut);
        if (col != endCol ||
            line != endLine) {
            cut = lineColToIndex(sqlWithCarets, endLine, endCol);
            ++cut; // for caret
            sqlWithCarets = sqlWithCarets.substring(0, cut) +
                "^" + sqlWithCarets.substring(cut);
        }
        return sqlWithCarets;
    }

    public static class ParsedCollation {
        private final Charset charset;
        private final Locale locale;
        private final String strength;

        public ParsedCollation(Charset charset, Locale locale, String strength)
        {
            this.charset = charset;
            this.locale = locale;
            this.strength = strength;
        }

        public Charset getCharset()
        {
            return charset;
        }

        public Locale getLocale()
        {
            return locale;
        }

        public String getStrength()
        {
            return strength;
        }
    }

    /**
     * Extracts the values from a collation name.
     *
     * <p>Collation names are on the form <i>charset$locale$strength</i>.
     *
     * @param in The collation name
     * @return A link {@link ParsedCollation}
     */
    public static ParsedCollation parseCollation(String in)
    {
        StringTokenizer st = new StringTokenizer(in, "$");
        String charsetStr = st.nextToken();
        String localeStr = st.nextToken();
        String strength;
        if (st.countTokens() > 0) {
            strength = st.nextToken();
        } else {
            strength =
                SaffronProperties.instance().defaultCollationStrength.get();
        }

        Charset charset = Charset.forName(charsetStr);
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
        return new ParsedCollation(charset, locale, strength);
    }

    public static String [] toStringArray(List list)
    {
        return (String []) list.toArray(emptyStringArray);
    }

    public static SqlNode [] toNodeArray(List list)
    {
        return (SqlNode []) list.toArray(SqlNode.emptyArray);
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
        if (tracer.isLoggable(Level.FINER)) {
            tracer.finer("Attempting to reduce " + list);
        }
        final SqlNode node = toTreeEx(list, 0, 0, SqlKind.Other);
        if (tracer.isLoggable(Level.FINE)) {
            tracer.fine("Reduced " + node);
        }
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
                SqlParserPos currentPos = ((ToTreeListItem) list.get(i)).pos;
                if ((stopperKind != SqlKind.Other)
                        && (current.getKind() == stopperKind)) {
                    break outer;
                }
                SqlOperator next;
                int previousRight;
                int left = current.getLeftPrec();
                int right = current.getRightPrec();
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
                        previousRight = previous.getRightPrec();
                    }
                    if (i == (count - 2)) {
                        next = null;
                        nextLeft = 0;
                    } else {
                        next = ((ToTreeListItem) list.get(i + 2)).op;
                        nextLeft = next.getLeftPrec();
                        if ((next.getKind() == stopperKind)
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
                        SqlParserPos callPos = currentPos.plusAll(
                            new SqlNode[] {leftExp, rightExp});
                        final SqlCall newExp =
                            current.createCall(leftExp, rightExp, callPos);
                        if (tracer.isLoggable(Level.FINE)) {
                            tracer.fine("Reduced infix: " + newExp);
                        }
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
                        previousRight = previous.getRightPrec();
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
                        if (tracer.isLoggable(Level.FINE)) {
                            tracer.fine("Reduced postfix: " + newExp);
                        }
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
                        previousRight = previous.getRightPrec();
                    }
                    int nextOrdinal = i + 2;
                    if (i == (count - 2)) {
                        next = null;
                        nextLeft = 0;
                    } else {
                        // find next op
                        next = null;
                        nextLeft = 0;
                        for (;nextOrdinal < count; nextOrdinal++) {
                            Object listItem = list.get(nextOrdinal);
                            if (listItem instanceof ToTreeListItem) {
                                next = ((ToTreeListItem) listItem).op;
                                nextLeft = next.getLeftPrec();
                                if ((stopperKind != SqlKind.Other)
                                        && (next.getKind() == stopperKind)) {
                                    break outer;
                                } else {
                                    break;
                                }
                            }
                        }
                    }
                    if (nextLeft < minPrec) {
                        break outer;
                    }
                    if ((previousRight < left) && (right >= nextLeft)) {
                        i = specOp.reduceExpr(i, list);
                        if (tracer.isLoggable(Level.FINE)) {
                            tracer.fine("Reduced special op: " + list.get(i));
                        }
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
     * Helper class for {@link SqlParserUtil#parsePrecisionDateTimeLiteral}
     */
    public static class PrecisionTime
    {
        private final Calendar cal;
        private final int precision;

        public PrecisionTime(Calendar cal, int precision)
        {
            this.cal = cal;
            this.precision = precision;
        }

        public Calendar getCalendar()
        {
            return cal;
        }

        public int getPrecision()
        {
            return precision;
        }
    }

    /**
     * Class that holds a {@link SqlOperator} and a {@link SqlParserPos}.
     * Used by {@link SqlSpecialOperator#reduceExpr(int, List)} and the parser
     * to associate a parsed operator
     * with a parser position.
     */
    public static class ToTreeListItem
    {
        private final SqlOperator op;
        private final SqlParserPos pos;

        public ToTreeListItem(
            SqlOperator op,
            SqlParserPos pos)
        {
            this.op = op;
            this.pos = pos;
        }

        public SqlOperator getOperator()
        {
            return op;
        }

        public SqlParserPos getPos()
        {
            return pos;
        }
    }

    /**
     * Contains a string, the offset of a token within the string, and a
     * parser position containing the beginning and end line number.
     */
    public static class StringAndPos {
        public final String sql;
        public final int cursor;
        public final SqlParserPos pos;
        StringAndPos(String sql, int cursor, SqlParserPos pos) {
            this.sql = sql;
            this.cursor = cursor;
            this.pos = pos;
        }
    }
}


// End SqlParserUtil.java
