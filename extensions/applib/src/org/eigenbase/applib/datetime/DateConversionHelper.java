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

import org.eigenbase.applib.resource.*;


/**
 * Class for date conversions Ported from //bb/bb713/server/SQL/BBDate.java
 */
public class DateConversionHelper
{
    //~ Static fields/initializers ---------------------------------------------

    private static final int MAX_CHAR_LEN = 32;

    //~ Instance fields --------------------------------------------------------

    private char [] caIn = new char[MAX_CHAR_LEN];
    private char [] caMask = new char[MAX_CHAR_LEN];
    private int maskLen = 0;

    private int year;
    private int month;
    private int date;
    private char [] caMonth = new char[3];

    //~ Methods ----------------------------------------------------------------

    /**
     * Convert a string to a date, using a specified date format mask.
     *
     * @param in String to convert
     * @param mask Format mask for the string
     *
     * @return Date datatype
     *
     * @exception ApplibException thrown when the mask is invalid, or the string
     * is invalid for the specified mask.
     */
    public Date toDate(String in, String mask)
        throws ApplibException
    {
        if ((in == null) || (mask == null)) {
            return null;
        } else {
            standardize(in, mask);
            return new Date(this.year, this.month, this.date);
        }
    }

    /**
     * Standardize a date string to yyyy-mm-dd format, given a date mask
     *
     * @param in String to convert
     * @param mask Format mask for the string
     *
     * @exception ApplibException thrown when the mask is invalid, or the string
     * is invalid for the specified mask.
     */
    private void standardize(String in, String mask)
        throws ApplibException
    {
        ApplibResource res = ApplibResource.instance();
        int inLen = in.length();

        if (inLen > MAX_CHAR_LEN) {
            inLen = MAX_CHAR_LEN;
        }

        maskLen = mask.length();
        if (maskLen > MAX_CHAR_LEN) {
            maskLen = MAX_CHAR_LEN;
        }
        mask.toUpperCase().getChars(0, maskLen, caMask, 0);

        // TODO: Improve performance here by eliminating toUpperCase()
        in.toUpperCase().getChars(0, inLen, caIn, 0);

        // Strip trailing spaces (faster than trim())
        while (caIn[inLen - 1] == ' ') {
            inLen--;
        }

        // Maximum number of allowed digits
        int yearDigits = 4;
        int monthDigits = 2;
        int dayDigits = 2;

        // Initialize date values, with year being 1900-based,
        // month being 0-based, and day being 1-based.
        // Note that this affects the arithmetic later.
        this.year = 0; // default year: 1900
        this.month = 0; // default month: January
        this.date = 1; // default date: 1

        for (int i = maskLen - 1; i >= 0; i--) {
            switch (caMask[i]) {
            case 'Y':
                switch (--yearDigits) {
                case 3:
                    year += toDigit(caIn[i]);
                    break;
                case 2:
                    year += toDigit(caIn[i]) * 10;
                    break;
                case 1:
                    year += (toDigit(caIn[i]) - 9) * 100;
                    break;
                case 0:
                    year += (toDigit(caIn[i]) - 1) * 1000;
                    break;
                default:
                    throw res.InvalidYearFmtMask.ex();
                }
                break;
            case 'M':
                switch (--monthDigits) {
                case 1:
                    month += toDigit(caIn[i]) - 1;
                    break;
                case 0:
                    month += toDigit(caIn[i]) * 10;
                    break;
                default:
                    throw res.InvalidMonthFmtMask.ex();
                }
                break;
            case 'D':
                switch (--dayDigits) {
                case 1:
                    date += toDigit(caIn[i]) - 1;
                    break;
                case 0:
                    date += toDigit(caIn[i]) * 10;
                    break;
                default:
                    throw res.InvalidDayFmtMask.ex();
                }
                break;
            case 'N':

                // better have M-O-N
                if (i < 2) {
                    break;
                }
                caMonth[2] = caIn[i];
                if (caMask[i - 1] != 'O') {
                    break;
                }
                caMonth[1] = caIn[--i];
                if (caMask[i - 1] != 'M') {
                    break;
                }
                caMonth[0] = caIn[--i];
                if (--monthDigits < 0) {
                    throw res.InvalidMonthFmtMask.ex();
                }
                monthDigits = 0;

                // Ugly, but faster than string comparisons in Java
                if ((caMonth[0] == 'J')
                    && (caMonth[1] == 'A')
                    && (caMonth[2] == 'N')) // JAN

                {
                    month = 0;
                } else if (
                    (caMonth[0] == 'F')
                    && (caMonth[1] == 'E')
                    && (caMonth[2] == 'B')) // FEB

                {
                    month = 1;
                } else if (
                    (caMonth[0] == 'M')
                    && (caMonth[1] == 'A')
                    && (caMonth[2] == 'R')) // MAR

                {
                    month = 2;
                } else if (
                    (caMonth[0] == 'A')
                    && (caMonth[1] == 'P')
                    && (caMonth[2] == 'R')) // APR

                {
                    month = 3;
                } else if (
                    (caMonth[0] == 'M')
                    && (caMonth[1] == 'A')
                    && (caMonth[2] == 'Y')) // MAY

                {
                    month = 4;
                } else if (
                    (caMonth[0] == 'J')
                    && (caMonth[1] == 'U')
                    && (caMonth[2] == 'N')) // JUN

                {
                    month = 5;
                } else if (
                    (caMonth[0] == 'J')
                    && (caMonth[1] == 'U')
                    && (caMonth[2] == 'L')) // JUL

                {
                    month = 6;
                } else if (
                    (caMonth[0] == 'A')
                    && (caMonth[1] == 'U')
                    && (caMonth[2] == 'G')) // AUG

                {
                    month = 7;
                } else if (
                    (caMonth[0] == 'S')
                    && (caMonth[1] == 'E')
                    && (caMonth[2] == 'P')) // SEP

                {
                    month = 8;
                } else if (
                    (caMonth[0] == 'O')
                    && (caMonth[1] == 'C')
                    && (caMonth[2] == 'T')) // OCT

                {
                    month = 9;
                } else if (
                    (caMonth[0] == 'N')
                    && (caMonth[1] == 'O')
                    && (caMonth[2] == 'V')) // NOV

                {
                    month = 10;
                } else if (
                    (caMonth[0] == 'D')
                    && (caMonth[1] == 'E')
                    && (caMonth[2] == 'C')) // DEC

                {
                    month = 11;
                } else {
                    throw res.InvalidMonthInputString.ex();
                }
                break;
            default:
                break;
            }
        }
    }

    private int toDigit(char in)
        throws ApplibException
    {
        int ret;

        if (in == ' ') {
            ret = 0;
        } else {
            ret = in - '0';
        }

        if ((ret < 0) || (ret > 9)) {
            throw ApplibResource.instance().InvalidDigitInputString.ex(
                String.valueOf(caMask, 0, maskLen),
                String.valueOf(caIn));
        }

        return ret;
    }
}

// End DateConversionHelper.java
