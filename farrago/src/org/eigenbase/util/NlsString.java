/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005-2007 The Eigenbase Project
// Copyright (C) 2002-2007 SQLstream, Inc.
// Copyright (C) 2005-2007 LucidEra, Inc.
// Portions Copyright (C) 2003-2007 John V. Sichi
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

import java.nio.*;
import java.nio.charset.*;

import org.eigenbase.resource.*;
import org.eigenbase.sql.*;


/**
 * A string, optionally with {@link Charset character set} and {@link
 * SqlCollation}. It is immutable.
 *
 * @author jhyde
 * @version $Id$
 * @since May 28, 2004
 */
public class NlsString
    implements Comparable<NlsString>
{
    //~ Instance fields --------------------------------------------------------

    private final String charsetName;
    private final String value;
    private final Charset charset;
    private final SqlCollation collation;

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a string in a specfied character set.
     *
     * @param value String constant, must not be null
     * @param charsetName Name of the character set, may be null
     * @param collation Collation, may be null
     *
     * @throws IllegalCharsetNameException If the given charset name is illegal
     * @throws UnsupportedCharsetException If no support for the named charset
     * is available in this instance of the Java virtual machine
     * @throws RuntimeException If the given value cannot be represented in the
     * given charset
     *
     * @pre theString != null
     */
    public NlsString(
        String value,
        String charsetName,
        SqlCollation collation)
        throws IllegalCharsetNameException, UnsupportedCharsetException
    {
        Util.pre(value != null, "theString != null");
        if (null != charsetName) {
            charsetName = charsetName.toUpperCase();
            this.charsetName = charsetName;
            String javaCharsetName =
                SqlUtil.translateCharacterSetName(charsetName);
            if (javaCharsetName == null) {
                throw new UnsupportedCharsetException(charsetName);
            }
            this.charset = Charset.forName(javaCharsetName);
            CharsetEncoder encoder = charset.newEncoder();

            // dry run to see if encoding hits any problems
            try {
                encoder.encode(CharBuffer.wrap(value));
            } catch (CharacterCodingException ex) {
                throw EigenbaseResource.instance().CharsetEncoding.ex(
                    value,
                    javaCharsetName);
            }
        } else {
            this.charsetName = null;
            this.charset = null;
        }
        this.collation = collation;
        this.value = value;
    }

    //~ Methods ----------------------------------------------------------------

    public Object clone()
    {
        return new NlsString(value, charsetName, collation);
    }

    public int hashCode()
    {
        int h = value.hashCode();
        h = Util.hash(h, charsetName);
        h = Util.hash(h, collation);
        return h;
    }

    public boolean equals(Object obj)
    {
        if (!(obj instanceof NlsString)) {
            return false;
        }
        NlsString that = (NlsString) obj;
        return Util.equal(value, that.value)
            && Util.equal(charsetName, that.charsetName)
            && Util.equal(collation, that.collation);
    }

    // implement Comparable
    public int compareTo(NlsString other)
    {
        // TODO jvs 18-Jan-2006:  Actual collation support.  This just uses
        // the default collation.

        return value.compareTo(other.value);
    }

    public String getCharsetName()
    {
        return charsetName;
    }

    public Charset getCharset()
    {
        return charset;
    }

    public SqlCollation getCollation()
    {
        return collation;
    }

    public String getValue()
    {
        return value;
    }

    /**
     * Returns the string quoted for SQL, for example <code>_ISO-8859-1'is it a
     * plane? no it''s superman!'</code>.
     *
     * @param prefix if true, prefix the character set name
     * @param suffix if true, suffix the collation clause
     *
     * @return the quoted string
     */
    public String asSql(
        boolean prefix,
        boolean suffix)
    {
        StringBuilder ret = new StringBuilder();
        if (prefix && (null != charsetName)) {
            ret.append("_");
            ret.append(charsetName);
        }
        ret.append("'");
        ret.append(Util.replace(value, "'", "''"));
        ret.append("'");

        // NOTE jvs 3-Feb-2005:  see FRG-78 for why this should go away
        if (false) {
            if (suffix && (null != collation)) {
                ret.append(" ");
                ret.append(collation.toString());
            }
        }
        return ret.toString();
    }

    /**
     * Returns the string quoted for SQL, for example <code>_ISO-8859-1'is it a
     * plane? no it''s superman!'</code>.
     */
    public String toString()
    {
        return asSql(true, true);
    }

    /**
     * Concatenates some {@link NlsString} objects. The result has the charset
     * and collation of the first element. The other elements must have matching
     * (or null) charset and collation. Concatenates all at once, not pairwise,
     * to avoid string copies.
     *
     * @param args array of {@link NlsString} to be concatenated
     */
    static public NlsString concat(NlsString [] args)
    {
        if (args.length < 2) {
            return args[0];
        }
        String charSetName = args[0].charsetName;
        SqlCollation collation = args[0].collation;
        int length = args[0].value.length();

        // sum string lengths and validate
        for (int i = 1; i < args.length; i++) {
            length += args[i].value.length();
            if (!((args[i].charsetName == null)
                    || args[i].charsetName.equals(charSetName)))
            {
                throw new IllegalArgumentException("mismatched charsets");
            }
            if (!((args[i].collation == null)
                    || args[i].collation.equals(collation)))
            {
                throw new IllegalArgumentException("mismatched collations");
            }
        }

        StringBuilder sb = new StringBuilder(length);
        for (int i = 0; i < args.length; i++) {
            sb.append(args[i].value);
        }
        return new NlsString(
            sb.toString(),
            charSetName,
            collation);
    }
}

// End NlsString.java
