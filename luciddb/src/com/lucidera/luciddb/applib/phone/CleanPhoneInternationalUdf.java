/*
// $Id$
// LucidDB is a DBMS optimized for business intelligence.
// Copyright (C) 2006-2006 LucidEra, Inc.
// Copyright (C) 2006-2006 The Eigenbase Project
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
package com.lucidera.luciddb.applib.phone;

import java.sql.Types;
import java.util.Properties;
import java.sql.SQLException;
import com.lucidera.luciddb.applib.resource.*;

/**
 * Format an input phone number in a specified format.
 * This method has several overloads determining what format
 * to use and what to do if a phone number cannot be formatted
 * into a specific format.
 * 
 * Ported from //BB/bb713/server/SQL/CleanPhoneInternational.java
 */
public class CleanPhoneInternationalUdf
{

    /**
     * Convert a phone number to the default
     * format, i.e., +1 (999) 999-9999
     *
     * @param in phone number to cleanse
     * @param in phone number to cleanse
     * @return phone number in international +1 (999) 999-9999 format
     */
    public static String execute( String in, boolean reject ) 
        throws ApplibException
    {
        String ret;
  
        PhoneNumberContext ctx = PhoneNumberContext.get();

        try {
            ret = ctx.toCanonicalString( in );
        } catch (ApplibException e) {
            if( reject ) {
                throw e;
            } else {
                ret = in;
            }
        }
        return ret;
    }
}

// End CleanPhoneInternationalUdf.java
