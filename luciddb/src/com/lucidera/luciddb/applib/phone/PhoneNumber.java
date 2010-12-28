/*
// $Id$
// LucidDB is a DBMS optimized for business intelligence.
// Copyright (C) 2006-2007 LucidEra, Inc.
// Copyright (C) 2006-2007 The Eigenbase Project
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

/**
 * Representation of local, domestic, and international phone numbers.
 * A phone number is represented by the country code, area code, local number,
 * and an optional extension.  For database storage, it can be converted into 
 * the canonical format:
 *      +COUNTRY[ (AREA)] LOCAL[ xEXTENSION]
 * where country, area, and local are strings of digits only, and spacing is 
 * significant.  This format is widely used for representing international 
 * phone numbers.  The local number does not allow separations, exception for 
 * North Americal phone numbers where a single '-' is inserted between the 
 * third and fourth digit.  The rules ensure uniqueness so that phone numbers 
 * can be directly compared, for example in lead de-duplication.
 * Note: this is a copy of the class from rubric.share
 *
 * Ported from //BB/bb713/common/java/Broadbase/util/PhoneNumber.java
 */
public class PhoneNumber
{
    /*
     *  Instance variables
     */
    public String countryCode;
    public String areaCode;
    public String localNumber;
    public String extension;


    /**
     *  Package constructor (used by PhoneNumberContext only)
     */
    PhoneNumber()
    {
        countryCode = areaCode = localNumber = extension = "";
    }


    /**
     *  Convert to canonical phone number format
     */
    public String toString()
    {
        StringBuffer out = new StringBuffer("+");
        out.append(countryCode);

        if (areaCode.length() > 0) {
            out.append(" (").append(areaCode).append(")");
        }

        if (localNumber.length() > 0) {
            if (countryCode.equals("1") && localNumber.length() == 7) {
                out.append(" ").append(localNumber.substring(0,3)).append(
                    "-").append(localNumber.substring(3));
            } else {
                out.append(" ").append(localNumber);
            }
        }

        if (extension.length() > 0) {
            out.append(" x").append(extension);
        }

        return out.toString();
    }
}

// End PhoneNumber.java
