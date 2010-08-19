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

import org.eigenbase.applib.resource.*;


/**
 * Repeater returns the input string repeated N times Ported from
 * //bb/bb713/server/SQL/repeater.java
 */
public class RepeaterUdf
{
    //~ Methods ----------------------------------------------------------------

    /**
     * Ported from //bb/bb713/server/SQL/BBString.java
     *
     * @param in String to be repeated
     * @param times Number of times to repeat string
     *
     * @return The repeated string
     *
     * @exception ApplibException
     */
    public static String execute(String in, int times)
    {
        if (times < 0) {
            throw ApplibResource.instance().RepSpecifyNonNegative.ex();
        }

        int len = in.length();

        // clip maximum size of output to 64k
        if ((times * len) > (64 * 1024)) {
            times = (64 * 1024) / len;
        }

        char [] outArray = new char[times * len];
        char [] inArray = in.toCharArray();

        for (int i = 0; i < times; i++) {
            System.arraycopy(inArray, 0, outArray, i * len, len);
        }

        return new String(outArray);
    }
}

// End RepeaterUdf.java
