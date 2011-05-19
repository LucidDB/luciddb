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

/**
 * containsNumber returns 1 if a string contains a number Ported from
 * //BB/bb713/server/SQL/containsNumber.java
 */
public class ContainsNumberUdf
{
    //~ Methods ----------------------------------------------------------------

    public static boolean execute(String in)
    {
        int len = in.length();
        for (int i = 0; i < len; i++) {
            if (Character.isDigit(in.charAt(i))) {
                return true;
            }
        }

        return false;
    }
}

// End ContainsNumberUdf.java
