/*
// Licensed to DynamoBI Corporation (DynamoBI) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  DynamoBI licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at

//   http://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
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
