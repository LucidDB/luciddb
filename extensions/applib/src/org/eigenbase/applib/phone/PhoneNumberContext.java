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

import java.io.*;

import java.util.*;

import org.eigenbase.applib.resource.*;


/**
 * This class captures the local information needed to parse or dial a phone
 * number accordingly Ported from
 * //BB/bb713/common/java/Broadbase/util/PhoneNumberContext.java modified to be
 * a singleton class Note: this is a copy of the class from rubric.share
 *
 * @author Elizabeth Lin
 * @version $Id$
 */
public class PhoneNumberContext
{
    //~ Static fields/initializers ---------------------------------------------

    // the following list of country codes where no area codes are needed are
    // looked up from the customer guide section of the mighty Pacific Bell
    // yellow pages
    private static final String [] noAreaCodeCountries =
    {
        "973", "229", "257", "237", "236", "242",
        "506", "45", "503", "679", "596", "689",
        "350", "671", "504", "852", "225", "965",
        "231", "352", "853", "356", "968", "507",
        "221", "65", "597"
    };

    //~ Instance fields --------------------------------------------------------

    /*
     *  Instance variables
     */
    private String localCountryCode;
    private String localAreaCode;
    private int localNumberLength;
    private String outsideLineAccess;
    private String domesticDialCode;
    private String internationalDialCode;
    private ApplibResource res;

    //~ Constructors -----------------------------------------------------------

    public PhoneNumberContext()
    {
        res = ApplibResource.instance();
        localCountryCode = res.PhoneLocalCountryCode.str();
        localAreaCode = res.PhoneLocalAreaCode.str();
        String localNumberLengthStr = res.PhoneLocalNumberLength.str();
        try {
            localNumberLength = Integer.parseInt(localNumberLengthStr);
            if (localNumberLength < 1) {
                localNumberLength = 7;
            }
        } catch (NumberFormatException x) {
            localNumberLength = 7;
        }
        outsideLineAccess = res.PhoneOutsideLineAccess.str();
        domesticDialCode = res.PhoneDomesticDialCode.str();
        internationalDialCode = res.PhoneInternationalDialCode.str();
    }

    //~ Methods ----------------------------------------------------------------

    /**
     * parsePhoneNumber() Parses local representations of local, domestic or
     * international phone number. For example, in San Mateo, California, these
     * numbers are parsed as following: 5133870 (local to San Mateo, California)
     * 513-3870 (local to San Mateo, California) 6505133870 (local to San Mateo,
     * California) 650-513-3870 (local to San Mateo, California) 415-744-9026
     * (domestic to San Francisco, California) 1-650-513-3870 (local to San
     * Mateo, California) 1415-744-9026 (domestic to San Francisco, California)
     * 14157449026 (domestic to San Francisco, California) (+886) 2-9876-5432
     * (international to Taipei, Taiwan) 011-886-2-9876-5432 (international to
     * Taipei, Taiwan) 01185223456789 (international to Hong Kong) In Taipei,
     * Taiwan, these numbers are parsed as following: 2345-6789 (local to
     * Taipei, Taiwan) 0223456789 (local to Taipei, Taiwan) (07) 234-5678
     * (domestic to Kaohsiung, Taiwan) 886-7-234-5678 (domestic to Kaohsiung,
     * Taiwan) +1 650-513-3870 (international to San Mateo, California)
     * +14157449026 (international to San Francisco, California)
     * 002-852-2345-6789 (international to Hong Kong) 00216505133870
     * (international to San Mateo, California) In HongKong where there is no
     * area code, these numbers are parsed as following: 2345-6789 (local to
     * Hong Kong) 852-2345-6789 (local to Hong Kong) 0011-650-513-3870
     * (international to San Mateo, California) In addition, an optional
     * extension may exist for all the above, but is ignored.
     */
    private PhoneNumber parsePhoneNumber(String phoneNumberStr)
        throws ApplibException
    {
        PhoneNumber pn = new PhoneNumber();

        //
        // Break down phone number into sections, ignoring phone extensions
        //
        Vector sections = new Vector();
        boolean hasCountryCode = false;
        int length = phoneNumberStr.length();
        int index = 0;
        while (index < length) {
            char c = phoneNumberStr.charAt(index);
            if (Character.isDigit(c)) {
                // collect consecutive digits into new section
                int begin = index++;
                while (
                    (index < length)
                    && (Character.isDigit(phoneNumberStr.charAt(index))))
                {
                    index++;
                }
                sections.addElement(phoneNumberStr.substring(begin, index));

                // check if '+' immediately preceeds the first section
                if ((sections.size() == 1)
                    && (begin > 0)
                    && (phoneNumberStr.charAt(begin - 1) == '+'))
                {
                    hasCountryCode = true;
                }
            } else if ((c == 'x') || (c == 'X')) {
                // collect all remaining digits as extension
                StringBuffer ext = new StringBuffer();
                while (++index < length) {
                    c = phoneNumberStr.charAt(index);
                    if (Character.isDigit(c)) {
                        ext.append(c);
                    }
                }
                pn.extension = ext.toString();
                break;
            } else {
                // skip non-digit characters
                index++;
            }
        }

        if (sections.size() == 0) {
            throw res.InvalidPhoneNumber.ex();
        }

        //
        // Analysis
        //
        String firstSection = (String) sections.firstElement();

        // handle unsectioned international phone numbers
        if (sections.size() == 1) {
            // international numbers if it begins with the international dial
            // code or '+' was specified
            String entire = null;
            if (hasCountryCode) {
                entire = firstSection;
            } else if (firstSection.startsWith(internationalDialCode)) {
                entire =
                    firstSection.substring(
                        internationalDialCode.length());
            }

            if (entire != null) {
                if (entire.length() == 0) {
                    throw res.IncompletePhoneNumber.ex();
                }

                if (entire.charAt(0) == '1') {
                    // this is a North American phone number.  validateNANP()
                    // will separate out the area code from the local number.
                    pn.countryCode = "1";
                    pn.localNumber = entire.substring(1);
                    validateNANP(pn);
                    return pn;
                }

                // check our static list for countries without area codes
                for (int i = 0; i < noAreaCodeCountries.length; i++) {
                    if (entire.startsWith(noAreaCodeCountries[i])) {
                        // country code found, and the rest is the local number
                        pn.countryCode = noAreaCodeCountries[i];
                        pn.localNumber =
                            entire.substring(
                                pn.countryCode.length());
                        if (pn.localNumber.length() == 0) {
                            throw res.IncompletePhoneNumber.ex();
                        }
                        return pn;
                    }
                }

                // well, we cannot determine what is the country code and what
                // is the area code.  Just set the country code to the entire
                // string so that toDialString() would still work.
                pn.countryCode = entire;
                return pn;
            }
        }

        // check for sectioned international numbers: indicated by '+', or if
        // the first section matches the international dial code or the local
        // country code
        if (hasCountryCode) {
            handleInternational(pn, sections, 0);
            validateNANP(pn);
            return pn;
        }

        if (firstSection.equals(internationalDialCode)) {
            handleInternational(pn, sections, 1);
            validateNANP(pn);
            return pn;
        }

        if (firstSection.startsWith(internationalDialCode)) {
            String remainder =
                firstSection.substring(
                    internationalDialCode.length());
            sections.setElementAt(remainder, 0);
            handleInternational(pn, sections, 0);
            validateNANP(pn);
            return pn;
        }

        // check for sectioned domestic or local phone number with local
        // country code specified
        if (firstSection.equals(localCountryCode)) {
            // notice this handles "1-650-513-3870" correctly in the US too,
            // since the domestic access code (1) is the same as the country
            // code
            handleInternational(pn, sections, 0);
            validateNANP(pn);
            return pn;
        }

        // otherwise, assume domestic or local phone number
        pn.countryCode = localCountryCode;

        // check to see if we are in the local country and there is no area
        // code in the local area
        if (localAreaCode.length() == 0) {
            pn.localNumber = allTheRest(sections, 0);

            // assumes LocalAreaCode is setup correctly, this must not be a
            // NANP number
            return pn;
        }

        if (sections.size() > 1) {
            // check for sectioned domestic numbers
            if (firstSection.equals(domesticDialCode)) {
                if (sections.size() == 2) {
                    throw res.IncompletePhoneNumber.ex();
                }
                pn.areaCode = (String) sections.elementAt(1);
                pn.localNumber = allTheRest(sections, 2);
                validateNANP(pn);
                return pn;
            }

            if (firstSection.startsWith(domesticDialCode)) {
                pn.areaCode =
                    firstSection.substring(
                        domesticDialCode.length());
                pn.localNumber = allTheRest(sections, 1);
                validateNANP(pn);
                return pn;
            }

            // check for sectioned local phone number with local area code
            // specified
            if (firstSection.equals(localAreaCode)) {
                pn.areaCode = localAreaCode;
                pn.localNumber = allTheRest(sections, 1);
                validateNANP(pn);
                return pn;
            }

            // handle sectioned domestic or local phone numbers
            // in order not to mistake the "513" of "513-3870" as an area
            // code, test the total length
            int len = 0;
            int n = sections.size();
            for (int i = 0; i < n; i++) {
                len += ((String) sections.elementAt(i)).length();
            }
            if (len > localNumberLength) {
                // domestic phone number
                pn.areaCode = (String) sections.firstElement();
                pn.localNumber = allTheRest(sections, 1);
            } else {
                // assume local phone number and use local area code
                pn.areaCode = res.PhoneLocalAreaCode.str();
                pn.localNumber = allTheRest(sections, 0);
            }
        } else {
            // handle unsectioned domestic or local phone numbers strip off
            // local country code prefix, which is the same as the domestic
            // access code in the US, or strip off domestic access code
            if (firstSection.startsWith(localCountryCode)) {
                firstSection =
                    firstSection.substring(
                        localCountryCode.length());
            } else if (firstSection.startsWith(domesticDialCode)) {
                firstSection =
                    firstSection.substring(
                        domesticDialCode.length());
            }

            if (firstSection.length() > localNumberLength) {
                // since number is too long to be local, assume separation
                // into area code and local number
                int brk = firstSection.length() - localNumberLength;
                pn.areaCode = firstSection.substring(0, brk);
                pn.localNumber = firstSection.substring(brk);
            } else {
                // assume local phone number and use local area code
                pn.areaCode = localAreaCode;
                pn.localNumber = firstSection;
            }

            if (pn.localNumber.length() == 0) {
                throw res.IncompletePhoneNumber.ex();
            }
        }

        validateNANP(pn);
        return pn;
    }

    private void handleInternational(
        PhoneNumber pn, Vector sections, int begin)
        throws ApplibException
    {
        // check if there are at least two sections available
        if (sections.size() < (begin + 2)) {
            throw res.IncompletePhoneNumber.ex();
        }

        // first section is country code
        pn.countryCode = (String) sections.elementAt(begin);

        // check if we are in the local country and there is no area code
        if ((pn.countryCode.equals(localCountryCode))
            && (localAreaCode.length() == 0))
        {
            pn.localNumber = allTheRest(sections, begin + 1);
            return;
        }

        // assume this country does not have an area code if there is only one
        // remaining section
        if (sections.size() == (begin + 2)) {
            pn.localNumber = (String) sections.elementAt(begin + 1);
            return;
        }

        // check our static list for countries without area codes
        for (int i = 0; i < noAreaCodeCountries.length; i++) {
            if (pn.countryCode.equals(noAreaCodeCountries[i])) {
                pn.localNumber = allTheRest(sections, begin + 1);
                return;
            }
        }

        // assume next section is the area code and remaining sections are
        // local numbers
        pn.areaCode = (String) sections.elementAt(begin + 1);
        pn.localNumber = allTheRest(sections, begin + 2);
        return;
    }

    /**
     * concatenate all sections from the given index
     */
    private String allTheRest(Vector sections, int begin)
    {
        StringBuffer out = new StringBuffer((String) sections.elementAt(begin));
        int n = sections.size();
        for (int i = begin + 1; i < n; i++) {
            out.append((String) sections.elementAt(i));
        }
        return out.toString();
    }

    /**
     * validation for North American Numbering Plan phone numbers
     */
    private void validateNANP(PhoneNumber pn)
        throws ApplibException
    {
        if (pn.countryCode.equals("1")) {
            if ((pn.areaCode.length() == 0)
                && (pn.localNumber.length() == 10))
            {
                // reformat into area code and local number
                pn.areaCode = pn.localNumber.substring(0, 3);
                pn.localNumber = pn.localNumber.substring(3);
            } else if (
                (pn.areaCode.length() != 3)
                || (pn.localNumber.length() != 7))
            {
                throw res.InvalidNaPhoneNumber.ex();
            }
        }
    }

    /**
     * makePhoneNumber() Constructs a phone number from its components.
     */
    public PhoneNumber makePhoneNumber(
        String countryCode,
        String areaCode,
        String localNumber,
        String extension)
        throws ApplibException
    {
        PhoneNumber pn = new PhoneNumber();

        if ((countryCode == null) || (countryCode.length() == 0)) {
            pn.countryCode = localCountryCode;
            pn.areaCode =
                ((areaCode == null) || (areaCode.length() == 0)) ? localAreaCode
                : areaCode;
        } else {
            pn.countryCode = countryCode;
            pn.areaCode =
                ((areaCode == null) || (areaCode.length() == 0)) ? ""
                : areaCode;
        }

        pn.localNumber =
            ((localNumber == null) || (localNumber.length() == 0)) ? ""
            : localNumber;
        pn.extension =
            ((extension == null) || (extension.length() == 0)) ? "" : extension;

        // validations
        int len = pn.countryCode.length();
        for (int i = 0; i < len; i++) {
            if (!Character.isDigit(pn.countryCode.charAt(i))) {
                throw res.PhoneCountryCodeSpecifyDigits.ex();
            }
        }

        len = pn.areaCode.length();
        for (int i = 0; i < len; i++) {
            if (!Character.isDigit(pn.areaCode.charAt(i))) {
                throw res.PhoneAreaCodeSpecifyDigits.ex();
            }
        }

        len = pn.localNumber.length();
        for (int i = 0; i < len; i++) {
            if (!Character.isDigit(pn.localNumber.charAt(i))) {
                throw res.PhoneLocalNumberSpecifyDigits.ex();
            }
        }

        len = pn.extension.length();
        for (int i = 0; i < len; i++) {
            if (!Character.isDigit(pn.extension.charAt(i))) {
                throw res.PhoneExtSpecifyDigits.ex();
            }
        }

        validateNANP(pn);
        return pn;
    }

    /**
     * Convert to dialing number
     */
    public String toDialString(PhoneNumber pn)
    {
        // add the correct prefixes for dialing
        StringBuffer out = new StringBuffer(outsideLineAccess);
        if (!pn.countryCode.equals(localCountryCode)) {
            out.append(internationalDialCode).append(pn.countryCode).append(
                pn.areaCode);
        } else if (!pn.areaCode.equals(localAreaCode)) {
            out.append(domesticDialCode).append(pn.areaCode);
        }

        // add the local numbers
        out.append(pn.localNumber);
        return out.toString();
    }

    /**
     * Shortcuts when given a phone number string
     */
    public String toCanonicalString(String phoneNumberStr)
        throws ApplibException
    {
        return parsePhoneNumber(phoneNumberStr).toString();
    }

    public String toDialString(String phoneNumberStr)
        throws ApplibException
    {
        return toDialString(parsePhoneNumber(phoneNumberStr));
    }

    public Object clone()
        throws CloneNotSupportedException
    {
        throw new CloneNotSupportedException();
    }

    //
    // Testing:
    //
    public static void main(String [] args)
    {
        try {
            System.out.println("San Mateo, California:");
            PhoneNumberContext pnc = new PhoneNumberContext();
            testOne(pnc, "5133870");
            testOne(pnc, "513-3870");
            testOne(pnc, "6505133870");
            testOne(pnc, "650-513-3870");
            testOne(pnc, "415-744-9026 ext123");
            testOne(pnc, "1-650-513-3870");
            testOne(pnc, "1415-744-9026");
            testOne(pnc, "14157449026");
            testOne(pnc, "(+886) 2-9876-5432");
            testOne(pnc, "011-886-2-9876-5432");
            testOne(pnc, "01185223456789");

            System.out.println("Taipei, Taiwan:");
            pnc.localCountryCode = "886";
            pnc.localAreaCode = "2";
            pnc.localNumberLength = 8;
            pnc.domesticDialCode = "0";
            pnc.internationalDialCode = "002";
            testOne(pnc, "2345-6789");
            testOne(pnc, "0223456789");
            testOne(pnc, "(07) 234-5678");
            testOne(pnc, "886-7-234-5678");
            testOne(pnc, "+1 650-513-3870");
            testOne(pnc, "+14157449026x666");
            testOne(pnc, "002-852-2345-6789 x10");
            testOne(pnc, "00216505133870");

            System.out.println("Hong Kong:");
            pnc.localCountryCode = "852";
            pnc.localAreaCode = "";
            pnc.localNumberLength = 8;
            pnc.domesticDialCode = "";
            pnc.internationalDialCode = "001";
            testOne(pnc, "2345-6789");
            testOne(pnc, "852-2345-6789");
            testOne(pnc, "0011-650-513-3870");
        } catch (Exception x) {
            x.printStackTrace();
        }
    }

    private static void testOne(PhoneNumberContext pnc, String phoneNumberStr)
    {
        PhoneNumber pn = pnc.parsePhoneNumber(phoneNumberStr);
        System.out.println(
            "'" + phoneNumberStr + "' -> '" + pn.toString()
            + "' -> '" + pnc.toDialString(pn) + "'");
    }
}

// End PhoneNumberContext.java
