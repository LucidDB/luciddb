/*
// $Id$
// Saffron preprocessor and data engine
// (C) Copyright 2004-2004 Disruptive Technologies, Inc.
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
package net.sf.saffron.util;

import net.sf.saffron.sql.SqlCollation;

import java.nio.charset.Charset;
import java.nio.charset.IllegalCharsetNameException;
import java.nio.charset.UnsupportedCharsetException;

/**
 * A string, optionally with {@link Charset character set} and
 * {@link SqlCollation}.
 *
 * @author jhyde
 * @since May 28, 2004
 * @version $Id$
 **/
public class NlsString {
    private String _charSetName;
    private String _value;
    private Charset _charset;
    private SqlCollation _collation;
    //~ Member variables -----------

    //~ Methods -----------
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
    public NlsString(String theString,
            String charSetName,
            SqlCollation collation)
            throws IllegalCharsetNameException, UnsupportedCharsetException {
        Util.pre(theString != null, "theString != null");
        _charSetName = charSetName;
        if (null != _charSetName){
            _charSetName = charSetName.toUpperCase();
            _charset = Charset.forName(_charSetName);
        } else {
            _charset = null;
        }
        _collation = collation;
        _value = theString;
    }

    public String getCharsetName() {
        return _charSetName;
    }

    public Charset getCharset() {
        return _charset;
    }

    public SqlCollation getCollation() {
        return _collation;
    }

    /**
     * Returns the string quoted for SQL, for example
     * <code>_ISO-8859-1'is it a plane? no it''s superman!'</code>.
     */
    public String toString() {
        StringBuffer ret = new StringBuffer();
        if (null!=_charSetName) {
            ret.append("_");
            ret.append(_charSetName);
        }
        ret.append("'");
        ret.append(Util.replace(_value,"'","''"));
        ret.append("'");
        if (null != _collation) {
            ret.append(" ");
            ret.append(_collation.toString());
        }

        return ret.toString();
    }

    public String getValue() {
        return _value;
    }

    public void setCollation(SqlCollation collation) {
//            assert(null!=collation);
//            assert(null==_collation);
        _collation = collation;
    }

    public void setCharset(Charset charset) {
//            assert(null!=charset);
//            assert(null==_charset);
        _charset = charset;
        _charSetName = _charset.name();
    }
}

// End NlsString.java