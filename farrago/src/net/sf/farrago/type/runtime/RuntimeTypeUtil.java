/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2005-2005 Disruptive Tech
// Copyright (C) 2005-2005 LucidEra, Inc.
// Portions Copyright (C) 2005-2005 Xiaoyang Luo
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
package net.sf.farrago.type.runtime;

import net.sf.farrago.resource.*;


/**
 * Runtime Utility Subroutines.
 *
 * @author Xiaoyang Luo
 * @version $Id$
 */
public class RuntimeTypeUtil
{
    //~ Static fields/initializers ---------------------------------------------

    /**
     * Translate the like pattern to java's regex pattern.
     */

    private static final String javaRegexSpecials = "[]()|^-+*?{}$\\";
    private static final String SqlSimilarSpecials = "[]()|^-+*_%?{}";
    private static final String [] regCharClasses =
    {
        "[:ALPHA:]", "\\p{Alpha}",
        "[:alpha:]", "\\p{Alpha}",
        "[:UPPER:]", "\\p{Upper}",
        "[:upper:]", "\\p{Upper}",
        "[:LOWER:]", "\\p{Lower}",
        "[:lower:]", "\\p{Lower}",
        "[:DIGIT:]", "\\d",
        "[:digit:]", "\\d",
        "[:SPACE:]", " ",
        "[:space:]", " ",
        "[:WHITESPACE:]", "\\s",
        "[:whitespace:]", "\\s",
        "[:ALNUM:]", "\\p{alnum}",
        "[:alnum:]", "\\p{alnum}"
    };

    //~ Methods ----------------------------------------------------------------

    public static String SqlToRegexLike(
        String sqlPattern,
        CharSequence escapeStr)
    {
        int i;
        char escapeChar = (char) 0;
        if (escapeStr != null) {
            if (escapeStr.length() != 1) {
                throw FarragoResource.instance().InvalidEscapeCharacter.ex(
                    escapeStr.toString());
            }
            escapeChar = escapeStr.charAt(0);
        }
        int len = sqlPattern.length();
        StringBuffer javaPattern = new StringBuffer(len + len);
        for (i = 0; i < len; i++) {
            char c = sqlPattern.charAt(i);
            if (javaRegexSpecials.indexOf(c) >= 0) {
                javaPattern.append('\\');
            }
            if (c == escapeChar) {
                if (i == (sqlPattern.length() - 1)) {
                    throw FarragoResource.instance().InvalidEscapeSequence.ex(
                        sqlPattern,
                        new Integer(i));
                }
                char nextChar = sqlPattern.charAt(i + 1);
                if ((nextChar == '_')
                    || (nextChar == '%')
                    || (nextChar == escapeChar))
                {
                    javaPattern.append(nextChar);
                    i++;
                } else {
                    throw FarragoResource.instance().InvalidEscapeSequence.ex(
                        sqlPattern,
                        new Integer(i));
                }
            } else if (c == '_') {
                javaPattern.append('.');
            } else if (c == '%') {
                javaPattern.append(".");
                javaPattern.append('*');
            } else {
                javaPattern.append(c);
            }
        }
        return javaPattern.toString();
    }

    private static void similarEscapeRuleChecking(
        String sqlPattern,
        char escapeChar)
    {
        if (escapeChar == 0) {
            return;
        }
        if (SqlSimilarSpecials.indexOf(escapeChar) >= 0) {
            // The the escape character is a special character
            // SQL 2003 Part 2 Section 8.6 General Rule 3.b
            for (int i = 0; i < sqlPattern.length(); i++) {
                if (sqlPattern.charAt(i) == escapeChar) {
                    if (i == (sqlPattern.length() - 1)) {
                        throw FarragoResource.instance().InvalidEscapeSequence
                        .ex(
                            sqlPattern,
                            new Integer(i));
                    }
                    char c = sqlPattern.charAt(i + 1);
                    if ((SqlSimilarSpecials.indexOf(c) < 0)
                        && (c != escapeChar))
                    {
                        throw FarragoResource.instance().InvalidEscapeSequence
                        .ex(
                            sqlPattern,
                            new Integer(i));
                    }
                }
            }
        }

        // SQL 2003 Part 2 Section 8.6 General Rule 3.c
        if (escapeChar == ':') {
            int position;
            position = sqlPattern.indexOf("[:");
            if (position >= 0) {
                position = sqlPattern.indexOf(":]");
            }
            if (position < 0) {
                throw FarragoResource.instance().InvalidEscapeSequence.ex(
                    sqlPattern,
                    new Integer(position));
            }
        }
    }

    private static String sqlSimilarRewrite(String sqlPattern,
        char escapeChar)
    {
        boolean insideCharacterEnumeration = false;

        StringBuffer javaPattern = new StringBuffer(sqlPattern.length() * 2);
        int len = sqlPattern.length();
        for (int i = 0; i < len; i++) {
            char c = sqlPattern.charAt(i);
            if (c == escapeChar) {
                if (i == (len - 1)) {
                    // It should never reach here after the escape rule
                    // checking.
                    throw FarragoResource.instance().InvalidEscapeSequence.ex(
                        sqlPattern,
                        new Integer(i));
                }
                char nextChar = sqlPattern.charAt(i + 1);
                if (SqlSimilarSpecials.indexOf(nextChar) >= 0) {
                    // special character, use \ to replace the escape char.
                    if (javaRegexSpecials.indexOf(nextChar) >= 0) {
                        javaPattern.append('\\');
                    }
                    javaPattern.append(nextChar);
                } else if (nextChar == escapeChar) {
                    javaPattern.append(nextChar);
                } else {
                    // It should never reach here after the escape rule
                    // checking.
                    throw FarragoResource.instance().InvalidEscapeSequence.ex(
                        sqlPattern,
                        new Integer(i));
                }
                i++; // we already process the next char.
            } else {
                switch (c) {
                case '_':
                    javaPattern.append('.');
                    break;
                case '%':
                    javaPattern.append('.');
                    javaPattern.append('*');
                    break;
                case '[':
                    javaPattern.append('[');
                    insideCharacterEnumeration = true;
                    i = sqlSimilarRewriteCharEnumeration(
                        sqlPattern,
                        javaPattern,
                        i,
                        escapeChar);
                    break;
                case ']':
                    if (!insideCharacterEnumeration) {
                        throw FarragoResource.instance()
                        .InvalidRegularExpression.ex(
                            sqlPattern,
                            new Integer(i));
                    }
                    insideCharacterEnumeration = false;
                    javaPattern.append(']');
                    break;
                case '\\':
                    javaPattern.append("\\\\");
                    break;
                case '$':

                    // $ is special character in java regex, but regular in
                    // SQL regex.
                    javaPattern.append("\\$");
                    break;
                default:
                    javaPattern.append(c);
                }
            }
        }
        if (insideCharacterEnumeration) {
            throw FarragoResource.instance().InvalidRegularExpression.ex(
                sqlPattern,
                new Integer(len));
        }

        return javaPattern.toString();
    }

    public static int sqlSimilarRewriteCharEnumeration(
        String sqlPattern,
        StringBuffer javaPattern,
        int pos,
        char escapeChar)
    {
        int i = pos + 1;
        for (i = pos + 1; i < sqlPattern.length(); i++) {
            char c = sqlPattern.charAt(i);
            if (c == ']') {
                return i - 1;
            } else if (c == escapeChar) {
                i++;
                char nextChar = sqlPattern.charAt(i);
                if (SqlSimilarSpecials.indexOf(nextChar) >= 0) {
                    if (javaRegexSpecials.indexOf(nextChar) >= 0) {
                        javaPattern.append('\\');
                    }
                    javaPattern.append(nextChar);
                } else if (escapeChar == nextChar) {
                    javaPattern.append(nextChar);
                } else {
                    throw FarragoResource.instance().InvalidRegularExpression
                    .ex(
                        sqlPattern,
                        new Integer(i));
                }
            } else if (c == '-') {
                javaPattern.append('-');
            } else if (c == '^') {
                javaPattern.append('^');
            } else if (sqlPattern.startsWith("[:", i)) {
                int numOfRegCharSets = regCharClasses.length / 2;
                boolean found = false;
                for (int j = 0; j < numOfRegCharSets; j++) {
                    if (sqlPattern.startsWith(regCharClasses[j + j], i)) {
                        javaPattern.append(regCharClasses[j + j + 1]);

                        i += regCharClasses[j + j].length() - 1;
                        found = true;
                        break;
                    }
                }
                if (!found) {
                    throw FarragoResource.instance().InvalidRegularExpression
                    .ex(
                        sqlPattern,
                        new Integer(i));
                }
            } else if (SqlSimilarSpecials.indexOf(c) >= 0) {
                throw FarragoResource.instance().InvalidRegularExpression.ex(
                    sqlPattern,
                    new Integer(i));
            } else {
                javaPattern.append(c);
            }
        }
        return i - 1;
    }

    public static String SqlToRegexSimilar(
        String sqlPattern,
        CharSequence escapeStr)
    {
        char escapeChar = (char) 0;
        if (escapeStr != null) {
            if (escapeStr.length() != 1) {
                throw FarragoResource.instance().InvalidEscapeCharacter.ex(
                    escapeStr.toString());
            }
            escapeChar = escapeStr.charAt(0);
        }

        similarEscapeRuleChecking(sqlPattern, escapeChar);

        return sqlSimilarRewrite(sqlPattern, escapeChar);
    }
}

// End RuntimeTypeUtil.java
