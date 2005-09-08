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

import junit.framework.ComparisonFailure;

import java.util.regex.Pattern;
import java.util.regex.Matcher;

/**
 * Static utilities for JUnit tests.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public abstract class TestUtil
{
    private static final Pattern LineBreakPattern =
        Pattern.compile("\r\n|\r|\n");
    private static final Pattern TabPattern = Pattern.compile("\t");

    public static void assertEqualsVerbose(
        String expected,
        String actual)
    {
        if (actual == null) {
            if (expected == null) {
                return;
            } else {
                String message =
                    "Expected:" + Util.lineSeparator +
                    expected + Util.lineSeparator +
                    "Actual: null";
                throw new ComparisonFailure(message, expected, actual);
            }
        }
        if (expected != null && expected.equals(actual)) {
            return;
        }
        String s = quoteForJavaUsingFold(actual);

        String message =
            "Expected:" + Util.lineSeparator + expected + Util.lineSeparator
            + "Actual: " + Util.lineSeparator + actual + Util.lineSeparator
            + "Actual java: " + Util.lineSeparator + s + Util.lineSeparator;
        throw new ComparisonFailure(message, expected, actual);
    }

    /**
     * Converts a string (which may contain quotes and newlines) into a
     * java literal.
     *
     * <p>For example,
     *
     * <code><pre>string with "quotes" split
     * across lines</pre></code>
     *
     * becomes
     *
     * <code><pre>"string with \"quotes\" split" + NL +
     *  "across lines"</pre></code>
     *
     */
    public static String quoteForJava(String s)
    {
        s = Util.replace(s, "\\", "\\\\");
        s = Util.replace(s, "\"", "\\\"");
        s = LineBreakPattern.matcher(s).replaceAll(lineBreak);
        s = TabPattern.matcher(s).replaceAll("\\\\t");
        s = "\"" + s + "\"";
        final String spurious = " + " + Util.lineSeparator + "\"\"";
        if (s.endsWith(spurious)) {
            s = s.substring(0, s.length() - spurious.length());
        }
        return s;
    }

    private static final String lineBreak =
        "\" + NL +" + Util.lineSeparator + "\"";


    /**
     * Converts a string (which may contain quotes and newlines) into a
     * java literal.
     *
     * <p>For example,
     *
     * <code><pre>string with "quotes" split
     * across lines</pre></code>
     *
     * becomes
     *
     * <code><pre>fold(new String[] {
     *  "string with \"quotes\" split",
     *  "across lines"})</pre></code>
     *
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
            return "fold(new String[] {" + Util.lineSeparator + s + "})";
        } else {
            return s;
        }
    }

    private static final String commaLineBreak =
        "\"," + Util.lineSeparator + "\"";


    /**
     * Combines an array of strings, each representing a line, into a single
     * string containing line separators.
     */
    public static String fold(String[] strings)
    {
        StringBuffer buf = new StringBuffer();
        for (int i = 0; i < strings.length; i++) {
            if (i > 0) {
                buf.append(Util.lineSeparator);
            }
            String string = strings[i];
            buf.append(string);
        }
        return buf.toString();
    }

    /**
     * Quotes a pattern.
     */
    public static String quotePattern(String s)
    {
        return s
            .replaceAll("\\\\", "\\\\")
            .replaceAll("\\.", "\\\\.")
            .replaceAll("\\{", "\\\\{")
            .replaceAll("\\}", "\\\\}")
            .replaceAll("\\|", "\\\\||")
            .replaceAll("\\?", "\\\\?")
            .replaceAll("\\*", "\\\\*")
            .replaceAll("\\(", "\\\\(")
            .replaceAll("\\)", "\\\\)")
            .replaceAll("\\[", "\\\\[")
            .replaceAll("\\]", "\\\\]");
    }
}

// End TestUtil.java
