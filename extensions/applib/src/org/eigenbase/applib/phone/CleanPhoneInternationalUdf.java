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
