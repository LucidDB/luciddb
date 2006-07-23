/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2005-2005 Disruptive Tech
// Copyright (C) 2005-2005 LucidEra, Inc.
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
package org.eigenbase.util;

import java.util.regex.*;

import junit.framework.*;


/**
 * Static utilities for JUnit tests.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public abstract class TestUtil
{

    //~ Static fields/initializers ---------------------------------------------

    private static final Pattern LineBreakPattern =
        Pattern.compile("\r\n|\r|\n");
    private static final Pattern TabPattern = Pattern.compile("\t");

    /**
     * System-dependent newline character.
     *
     * <p/>Do not use '\n' in strings which are samples for test results. {@link
     * java.io.PrintWriter#println()} produces '\n' on Unix and '\r\n' on
     * Windows, but '\n' is always '\n', so your tests will fail on Windows.
     */
    public static final String NL = Util.lineSeparator;

    private static final String lineBreak = "\" + NL +" + NL + "\"";

    private static final String commaLineBreak = "\"," + NL + "\"";

    //~ Methods ----------------------------------------------------------------

    public static void assertEqualsVerbose(
        String expected,
        String actual)
    {
        if (actual == null) {
            if (expected == null) {
                return;
            } else {
                String message =
                    "Expected:" + NL
                    + expected + NL
                    + "Actual: null";
                throw new ComparisonFailure(message, expected, actual);
            }
        }
        if ((expected != null) && expected.equals(actual)) {
            return;
        }
        String s = quoteForJavaUsingFold(actual);

        String message =
            "Expected:" + NL + expected + NL
            + "Actual: " + NL + actual + NL
            + "Actual java: " + NL + s + NL;
        throw new ComparisonFailure(message, expected, actual);
    }

    /**
     * Converts a string (which may contain quotes and newlines) into a java
     * literal.
     *
     * <p>For example, <code>
     * <pre>string with "quotes" split
     * across lines</pre>
     * </code> becomes <code>
     * <pre>"string with \"quotes\" split" + NL +
     *  "across lines"</pre>
     * </code>
     */
    public static String quoteForJava(String s)
    {
        s = Util.replace(s, "\\", "\\\\");
        s = Util.replace(s, "\"", "\\\"");
        s = LineBreakPattern.matcher(s).replaceAll(lineBreak);
        s = TabPattern.matcher(s).replaceAll("\\\\t");
        s = "\"" + s + "\"";
        final String spurious = " + " + NL + "\"\"";
        if (s.endsWith(spurious)) {
            s = s.substring(0, s.length() - spurious.length());
        }
        return s;
    }

    /**
     * Converts a string (which may contain quotes and newlines) into a java
     * literal.
     *
     * <p>For example, <code>
     * <pre>string with "quotes" split
     * across lines</pre>
     * </code> becomes <code>
     * <pre>fold(new String[] {
     *  "string with \"quotes\" split",
     *  "across lines"})</pre>
     * </code>
     */
    public static String quoteForJavaUsingFold(String s)
    {
        s = Util.replace(s, "\\", "\\\\");
        s = Util.replace(s, "\"", "\\\"");
        final Matcher lineBreakMatcher = LineBreakPattern.matcher(s);
        final boolean lineBreaks = lineBreakMatcher.find();
        s = lineBreakMatcher.replaceAll(commaLineBreak);
        s = TabPattern.matcher(s).replaceAll("\\\\t");
        s = "\"" + s + "\"";
        if (lineBreaks) {
            return "TestUtil.fold(new String[] {" + NL + s + "})";
        } else {
            return s;
        }
    }

    /**
     * Combines an array of strings, each representing a line, into a single
     * string containing line separators.
     */
    public static String fold(String [] strings)
    {
        StringBuffer buf = new StringBuffer();
        for (int i = 0; i < strings.length; i++) {
            if (i > 0) {
                buf.append(NL);
            }
            String string = strings[i];
            buf.append(string);
        }
        return buf.toString();
    }

    /**
     * Converts a string containing newlines (\n) into a string containing
     * os-dependent line endings.
     */

    public static String fold(String string)
    {
        if (!"\n".equals(NL)) {
            string = string.replaceAll("\n", NL);
        }
        return string;
    }

    /**
     * Quotes a pattern.
     */
    public static String quotePattern(String s)
    {
        return
            s.replaceAll("\\\\", "\\\\").replaceAll("\\.", "\\\\.").replaceAll(
                "\\+",
                "\\\\+").replaceAll("\\{", "\\\\{").replaceAll("\\}", "\\\\}")
            .replaceAll("\\|", "\\\\||").replaceAll("[$]", "\\\\\\$")
            .replaceAll("\\?", "\\\\?").replaceAll("\\*", "\\\\*").replaceAll(
                "\\(",
                "\\\\(").replaceAll("\\)", "\\\\)").replaceAll("\\[", "\\\\[")
            .replaceAll("\\]", "\\\\]");
    }
}

// End TestUtil.java
