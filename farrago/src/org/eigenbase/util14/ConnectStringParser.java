/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2006-2006 The Eigenbase Project
// Copyright (C) 2006-2006 Disruptive Tech
// Copyright (C) 2006-2006 LucidEra, Inc.
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

package org.eigenbase.util14;

import java.sql.*;
import java.util.*;

/**
 * ConnectStringParser is a utility class that parses or creates a JDBC
 * connect string according to the OLE DB connect string syntax described at
 * <a href="http://msdn.microsoft.com/library/default.asp?url=/library/en-us/oledb/htm/oledbconnectionstringsyntax.asp">
 * OLE DB Connection String Syntax</a>.
 * <p>
 * This code adapted from Mondrian code at
 * <a href="http://perforce.eigenbase.org:8080/open/mondrian/src/main/mondrian/olap/Util.java">
 * Util.java</a>.
 * <p>
 * The primary differences between this and its Mondrian progenitor are:
 * <ul>
 * <li>use of regular {@link Properties} for compatibility with the
 * JDBC API (replaces Mondrian's use of its own order-preserving and
 * case-insensitive PropertyList, found in Util.java at link above)</li>
 * <li>ability to pass to {@link #parse} a pre-existing Properties object into
 * which properties are to be parsed, possibly overriding prior values</li>
 * <li>use of {@link SQLException}s rather than
 * unchecked {@link RuntimeException}s</li>
 * <li>static members for parsing and creating connect strings</li>
 * </ul>
 *
 * <p>
 * ConnectStringParser has a private constructor.
 * Callers use the static members:
 * <dl>
 * <dt>{@link #parse(String)}</dt>
 *  <dd>Parses the connect string into a new Properties object.</dd>
 * <dt>{@link #parse(String, Properties)}</dt>
 *  <dd>Parses the connect string into an existing Properties object.</dd>
 * <dt>{@link #getParamString(Properties)}</dt>
 *  <dd>Returns a param string, quoted and escaped as needed,
 *      to represent the supplied name-value pairs.</dd>
 * </dl>
 *
 * @author adapted by Steve Herskovitz from Mondrian
 * @since Apr 03, 2006
 * @version $Id$
 */
public class ConnectStringParser {
    private final String s;
    private final int n;
    private int i;
    private final StringBuffer nameBuf;
    private final StringBuffer valueBuf;

    //~ Methods ---------------------------------------------------------------

    /**
     * Creates a new connect string parser.
     * @param s connect string to parse
     * @see #parse(String)
     * @see #parse(String, Properties)
     */
    private ConnectStringParser(String s) {
        this.s = s;
        this.i = 0;
        this.n = s.length();
        this.nameBuf = new StringBuffer(64);
        this.valueBuf = new StringBuffer(64);
    }

    /**
     * Parses the connect string into a new Properties object.
     * @param s connect string to parse
     * @return properties object with parsed params
     * @throws SQLException error parsing name-value pairs
     */
    public static Properties parse(String s) throws SQLException
    {
        return parse(s, null);
    }

    /**
     * Parses the connect string into an existing Properties object.
     * @param s connect string to parse
     * @param props optional properties object, may be <code>null</code>
     * @return properties object with parsed params; if an input
     *      <code>props</code> was supplied, any duplicate properties
     *      will have been replaced by those from the connect string.
     * @throws SQLException error parsing name-value pairs
     */
    public static Properties parse(String s, Properties props) throws SQLException
    {
        if (props == null) {
            props = new Properties();
        }
        return (new ConnectStringParser(s)).parse(props);
    }

    /**
     * Parses the connect string into a Properties object.
     * Note that the string can only be parsed once.
     * Subsequent calls return empty/unchanged Properties.
     * @param props optional properties object, may be <code>null</code>
     * @return properties object with parsed params; if an input
     *      <code>props</code> was supplied, any duplicate properties
     *      will have been replaced by those from the connect string.
     * @throws SQLException error parsing name-value pairs
     */
    Properties parse(Properties props) throws SQLException
    {
        if (props == null) {
            props = new Properties();
        }
        while (i < n) {
            parsePair(props);
        }
        return props;
    }

    /**
     * Reads "name=value;" or "name=value<EOF>".
     * @throws SQLException error parsing value
     */
    void parsePair(Properties props) throws SQLException
    {
        String name = parseName();
        String value;
        if (i >= n) {
            value = "";
        } else if (s.charAt(i) == ';') {
            i++;
            value = "";
        } else {
            value = parseValue();
        }
        props.put(name, value);
    }

    /**
     * Reads "name=". Name can contain equals sign if equals sign is
     * doubled.
     */
    String parseName()
    {
        nameBuf.setLength(0);
        while (true) {
            char c = s.charAt(i);
            switch (c) {
            case '=':
                i++;
                if (i < n && (c = s.charAt(i)) == '=') {
                    // doubled equals sign; take one of them, and carry on
                    i++;
                    nameBuf.append(c);
                    break;
                }
                String name = nameBuf.toString();
                name = name.trim();
                return name;
            case ' ':
                if (nameBuf.length() == 0) {
                    // ignore preceding spaces
                    i++;
                    break;
                } else {
                    // fall through
                }
            default:
                nameBuf.append(c);
                i++;
                if (i >= n) {
                    return nameBuf.toString().trim();
                }
            }
        }
    }

    /**
     * Reads "value;" or "value<EOF>"
     * @throws SQLException if find an unterminated quoted value
     */
    String parseValue() throws SQLException
    {
        char c;
        // skip over leading white space
        while ((c = s.charAt(i)) == ' ') {
            i++;
            if (i >= n) {
                return "";
            }
        }
        if (c == '"' || c == '\'') {
            String value = parseQuoted(c);
            // skip over trailing white space
            while (i < n && (c = s.charAt(i)) == ' ') {
                i++;
            }
            if (i >= n) {
                return value;
            } else if (s.charAt(i) == ';') {
                i++;
                return value;
            } else {
                throw new SQLException(
                    "quoted value ended too soon, at position " + i +
                    " in '" + s + "'");
            }
        } else {
            String value;
            int semi = s.indexOf(';', i);
            if (semi >= 0) {
                value = s.substring(i, semi);
                i = semi + 1;
            } else {
                value = s.substring(i);
                i = n;
            }
            return value.trim();
        }
    }

    /**
     * Reads a string quoted by a given character. Occurrences of the
     * quoting character must be doubled. For example,
     * <code>parseQuoted('"')</code> reads <code>"a ""new"" string"</code>
     * and returns <code>a "new" string</code>.
     * @throws SQLException if find an unterminated quoted value
     */
    String parseQuoted(char q) throws SQLException
    {
        char c = s.charAt(i++);
        if (c != q) {
            throw new AssertionError("c != q: c=" +c +" q=" +q);
        }
        valueBuf.setLength(0);
        while (i < n) {
            c = s.charAt(i);
            if (c == q) {
                i++;
                if (i < n) {
                    c = s.charAt(i);
                    if (c == q) {
                        valueBuf.append(c);
                        i++;
                        continue;
                    }
                }
                return valueBuf.toString();
            } else {
                valueBuf.append(c);
                i++;
            }
        }
        throw new SQLException(
                "Connect string '" + s +
                "' contains unterminated quoted value '" +
                valueBuf.toString() + "'");
    }

    /**
     * Returns a param string, quoted and escaped as needed, to
     * represent the supplied name-value pairs.
     * @param props name-value pairs
     * @return param string, never <code>null</code>
     */
    public static String getParamString(Properties props)
    {
        if (props == null) {
            return "";
        }

        StringBuffer buf = new StringBuffer();
        Enumeration enumer = props.propertyNames();
        while (enumer.hasMoreElements()) {
            String name = (String)enumer.nextElement();
            String value = props.getProperty(name);
            String quote = "";
            if (buf.length() > 0) {
                buf.append(';');
            }

            // write parameter name
            if (name.startsWith(" ") || name.endsWith(" ")) {
                quote = "'";
                buf.append(quote);
            }
            int len = name.length();
            for (int i=0; i < len; ++i) {
                char c = name.charAt(i);
                if (c == '=') {
                    buf.append('=');
                }
                buf.append(c);
            }

            buf.append(quote);      // might be empty
            quote = "";

            buf.append('=');

            // write parameter value
            len = value.length();
            boolean hasSemi = value.indexOf(';') >= 0;
            boolean hasSQ = value.indexOf("'") >= 0;
            boolean hasDQ = value.indexOf('"') >= 0;
            if (value.startsWith(" ") || value.endsWith(" ")) {
                quote = "'";
            } else if (hasSemi || hasSQ || hasDQ) {
                // try to choose the least painful quote
                if (value.startsWith("\"")) {
                    quote = "'";
                } else if (value.startsWith("'")) {
                    quote = "\"";
                } else {
                   quote = hasSQ? "\"" : "'";
                }
            }
            char q;
            if (quote.length() > 0) {
                buf.append(quote);
                q = quote.charAt(0);
            } else {
                q = '\0';
            }
            for (int i=0; i < len; ++i) {
                char c = value.charAt(i);
                if (c == q) {
                    buf.append(q);
                }
                buf.append(c);
            }
            buf.append(quote);      // might be empty
        }

        return buf.toString();
    }
}

// End ConnectStringParser.java
