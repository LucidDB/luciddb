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

/**
 * Representation of local, domestic, and international phone numbers. A phone
 * number is represented by the country code, area code, local number, and an
 * optional extension. For database storage, it can be converted into the
 * canonical format: +COUNTRY[ (AREA)] LOCAL[ xEXTENSION] where country, area,
 * and local are strings of digits only, and spacing is significant. This format
 * is widely used for representing international phone numbers. The local number
 * does not allow separations, exception for North Americal phone numbers where
 * a single '-' is inserted between the third and fourth digit. The rules ensure
 * uniqueness so that phone numbers can be directly compared, for example in
 * lead de-duplication. Note: this is a copy of the class from rubric.share
 * Ported from //BB/bb713/common/java/Broadbase/util/PhoneNumber.java
 */
public class PhoneNumber
{
    //~ Instance fields --------------------------------------------------------

    /*
     *  Instance variables
     */
    public String countryCode;
    public String areaCode;
    public String localNumber;
    public String extension;

    //~ Constructors -----------------------------------------------------------

    /**
     * Package constructor (used by PhoneNumberContext only)
     */
    PhoneNumber()
    {
        countryCode = areaCode = localNumber = extension = "";
    }

    //~ Methods ----------------------------------------------------------------

    /**
     * Convert to canonical phone number format
     */
    public String toString()
    {
        StringBuffer out = new StringBuffer("+");
        out.append(countryCode);

        if (areaCode.length() > 0) {
            out.append(" (").append(areaCode).append(")");
        }

        if (localNumber.length() > 0) {
            if (countryCode.equals("1") && (localNumber.length() == 7)) {
                out.append(" ").append(localNumber.substring(0, 3)).append(
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
