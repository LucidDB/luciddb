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

package org.eigenbase.util;

import java.nio.charset.Charset;
import java.nio.charset.IllegalCharsetNameException;
import java.nio.charset.UnsupportedCharsetException;

import org.eigenbase.sql.SqlCollation;


/**
 * A string, optionally with {@link Charset character set} and
 * {@link SqlCollation}.
 *
 * @author jhyde
 * @since May 28, 2004
 * @version $Id$
 **/
public class NlsString
{
    //~ Instance fields -------------------------------------------------------

    private String charSetName;
    private String value;
    private Charset charset;
    private SqlCollation collation;

    //~ Constructors ----------------------------------------------------------

    /**
     * Creates a string in a specfied character set.
     *
     * @param theString String constant, must not be null
     * @param charSetName Name of the character set, may be null
     * @param collation Collation, may be null
     *
     * @pre theString != null
     *
     * @throws  IllegalCharsetNameException
     *          If the given charset name is illegal
     *
     * @throws  UnsupportedCharsetException
     *          If no support for the named charset is available
     *          in this instance of the Java virtual machine
     */
    public NlsString(
        String theString,
        String charSetName,
        SqlCollation collation)
        throws IllegalCharsetNameException, UnsupportedCharsetException
    {
        Util.pre(theString != null, "theString != null");
        this.charSetName = charSetName;
        if (null != this.charSetName) {
            this.charSetName = charSetName.toUpperCase();
            charset = Charset.forName(this.charSetName);
        } else {
            charset = null;
        }
        this.collation = collation;
        value = theString;
    }

    //~ Methods ---------------------------------------------------------------

    public String getCharsetName()
    {
        return charSetName;
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
     * Returns the string quoted for SQL, for example
     * <code>_ISO-8859-1'is it a plane? no it''s superman!'</code>.
     *  @param prefix if true, prefix the character set name
     *  @param suffix if true, suffix the collation clause
     *  @return the quoted string
     */
    public String asSql(
        boolean prefix,
        boolean suffix)
    {
        StringBuffer ret = new StringBuffer();
        if (prefix && (null != charSetName)) {
            ret.append("_");
            ret.append(charSetName);
        }
        ret.append("'");
        ret.append(Util.replace(value, "'", "''"));
        ret.append("'");
        if (suffix && (null != collation)) {
            ret.append(" ");
            ret.append(collation.toString());
        }
        return ret.toString();
    }

    /**
     * Returns the string quoted for SQL, for example
     * <code>_ISO-8859-1'is it a plane? no it''s superman!'</code>.
     */
    public String toString()
    {
        return asSql(true, true);
    }

    public void setCollation(SqlCollation collation)
    {
        //            assert(null!=collation);
        //            assert(null==this.collation);
        this.collation = collation;
    }

    public void setCharset(Charset charset)
    {
        //            assert(null!=charset);
        //            assert(null==this.charset);
        this.charset = charset;
        charSetName = this.charset.name();
    }

    /** Concatenates some NlsStrings.
     * The result has the charset and collation of the first element.
     * The other elements must have matching (or null) charset and collation.
     * Concatenates all at once, not pairwise, to avoid string copies.
     * @param args an NlString{}
     */
    static public NlsString concat(NlsString [] args)
    {
        if (args.length < 2) {
            return args[0];
        }
        String charSetName = args[0].charSetName;
        SqlCollation collation = args[0].collation;
        int length = args[0].value.length();

        // sum string lengths and validate
        for (int i = 1; i < args.length; i++) {
            length += args[i].value.length();
            if (!((args[i].charSetName == null)
                    || args[i].charSetName.equals(charSetName))) {
                throw new IllegalArgumentException("mismatched charsets");
            }
            if (!((args[i].collation == null)
                    || args[i].collation.equals(collation))) {
                throw new IllegalArgumentException("mismatched collations");
            }
        }

        StringBuffer sb = new StringBuffer(length);
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
