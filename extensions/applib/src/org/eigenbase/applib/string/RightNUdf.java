/*
// $Id$
// Applib is a library of SQL-invocable routines for Eigenbase applications.
// Copyright (C) 2006 The Eigenbase Project
// Copyright (C) 2006 SQLstream, Inc.
// Copyright (C) 2006 DynamoBI Corporation
//
// This library is free software; you can redistribute it and/or modify it
// under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation; either version 2.1 of the License, or (at
// your option) any later version.
//
// This library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public
// License along with this library; if not, write to the Free Software
// Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA
*/
package org.eigenbase.applib.string;

import java.nio.*;

import net.sf.farrago.runtime.*;

import org.eigenbase.applib.resource.*;


/**
 * rightN returns the last N characters of the string Ported from
 * //bb/bb713/server/SQL/rightN.java
 */
public class RightNUdf
{
    //~ Methods ----------------------------------------------------------------

    /**
     * Ported from //bb/bb713/server/SQL/BBString.java
     *
     * @param in Input string
     * @param len N number of characters to return
     *
     * @return New String with last N characters of the input string
     *
     * @exception ApplibException
     */
    public static String execute(String in, int len)
        throws ApplibException
    {
        if (len < 0) {
            throw ApplibResource.instance().LenSpecifyNonNegative.ex();
        }

        char [] chars;
        CharBuffer cb = (CharBuffer) FarragoUdrRuntime.getContext();
        if (cb == null) {
            chars = new char[2048];
            cb = CharBuffer.wrap(chars);
            FarragoUdrRuntime.setContext(cb);
        }
        chars = cb.array();

        int inlen = in.length();
        int startPos = Math.max(inlen - len, 0);
        in.getChars(startPos, inlen, chars, 0);

        return new String(chars, 0, inlen - startPos);
    }
}

// End RightNUdf.java
