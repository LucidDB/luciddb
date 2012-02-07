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
package org.eigenbase.applib.datetime;

import java.sql.*;

import net.sf.farrago.runtime.*;

import org.eigenbase.applib.resource.*;


/**
 * Convert a string to a date, using a specified date format mask. Ported from
 * //bb/bb713/server/SQL/toDate.java
 */
public class ConvertDateUdf
{
    //~ Methods ----------------------------------------------------------------

    /**
     * @param in convert
     * @param mask mask for the string
     * @param reject the date is invalid, this parameter says whether to an
     * exception or return null.
     *
     * @return datatype
     *
     * @exception ApplibException thrown when the mask is invalid, or the string
     * is invalid for the specified mask AND <code>reject</code> is true.
     */
    public static Date execute(String in, String mask, boolean reject)
        throws ApplibException
    {
        Date ret;
        DateConversionHelper dch =
            (DateConversionHelper) FarragoUdrRuntime.getContext();
        if (dch == null) {
            dch = new DateConversionHelper();
            FarragoUdrRuntime.setContext(dch);
        }

        try {
            ret = dch.toDate(in, mask);
        } catch (ApplibException e) {
            if (reject) {
                throw e;
            } else {
                ret = null;
            }
        }

        return ret;
    }

    /**
     * @param in String to convert
     * @param mask Format mask for the string
     *
     * @return Date datatype
     *
     * @exception ApplibException thrown when the mask is invalid, or the string
     * is invalid for the specified mask.
     */
    public static Date execute(String in, String mask)
        throws ApplibException
    {
        return execute(in, mask, true);
    }
}

// End ConvertDateUdf.java
