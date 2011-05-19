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
package org.eigenbase.applib.phone;

import net.sf.farrago.runtime.*;

import org.eigenbase.applib.resource.*;


/**
 * Format an input phone number in a specified format. This method has several
 * overloads determining what format to use and what to do if a phone number
 * cannot be formatted into a specific format. Ported from
 * //BB/bb713/server/SQL/CleanPhoneInternational.java
 */
public class CleanPhoneInternationalUdf
{
    //~ Methods ----------------------------------------------------------------

    /**
     * Convert a phone number to the default format, i.e., +1 (999) 999-9999
     *
     * @param in phone number to cleanse
     * @param in phone number to cleanse
     *
     * @return phone number in international +1 (999) 999-9999 format
     */
    public static String execute(String in, boolean reject)
        throws ApplibException
    {
        String ret;

        PhoneNumberContext ctx =
            (PhoneNumberContext) FarragoUdrRuntime.getContext();
        if (ctx == null) {
            ctx = new PhoneNumberContext();
            FarragoUdrRuntime.setContext(ctx);
        }

        try {
            ret = ctx.toCanonicalString(in);
        } catch (ApplibException e) {
            if (reject) {
                throw e;
            } else {
                ret = in;
            }
        }
        return ret;
    }
}

// End CleanPhoneInternationalUdf.java
