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

#include "fennel/common/CommonPreamble.h"
#include "fennel/calculator/SqlStringAscii.h"
#include "fennel/calculator/SqlState.h"

#include <ctype.h>

FENNEL_BEGIN_NAMESPACE

int
SqlStrCat_Ascii(
    char* dest,
    int destStorageBytes,
    int destLenBytes,
    char const * const str,
    int strLenBytes)
{
    if (destLenBytes + strLenBytes > destStorageBytes) {
        // SQL99 Part 2 Section 22.1 22-001 "String Data Right truncation"
        throw SqlState::instance().code22001();
    }

    memcpy(dest + destLenBytes, str, strLenBytes);
    return destLenBytes + strLenBytes;
}


int
SqlStrCat_Ascii(
    char* dest,
    int destStorageBytes,
    char const * const str1,
    int str1LenBytes,
    char const * const str2,
    int str2LenBytes)
{
    if (str1LenBytes + str2LenBytes > destStorageBytes) {
        // SQL99 Part 2 Section 22.1 22-001 "String Data Right truncation"
        throw SqlState::instance().code22001();
    }

    memcpy(dest, str1, str1LenBytes);
    memcpy(dest + str1LenBytes, str2, str2LenBytes);
    return str1LenBytes + str2LenBytes;
}

int
SqlStrCmp_Ascii_Fix(
    char const * const str1,
    int str1LenBytes,
    char const * const str2,
    int str2LenBytes,
    char trimchar)
{
    char const * start = str1;
    char const * end = str1 + str1LenBytes;

    if (end != start) {
        end--;
        while (end != start && *end == trimchar) {
            end--;
        }
        if (end != start || *end != trimchar) {
            end++;
        }
    }
    int str1TrimLenBytes = end - start;

    start = str2;
    end = str2 + str2LenBytes;

    if (end != start) {
        end--;
        while (end != start && *end == trimchar) {
            end--;
        }
        if (end != start || *end != trimchar) {
            end++;
        }
    }
    int str2TrimLenBytes = end - start;

    if (str1TrimLenBytes > str2TrimLenBytes) {
        return 1;
    } else if (str1TrimLenBytes < str2TrimLenBytes) {
        return -1;
    }

    assert(str1TrimLenBytes == str2TrimLenBytes);

    // comparison must be unsigned to work for > 128
    unsigned char const *s1 = reinterpret_cast<unsigned char const *>(str1);
    unsigned char const *s2 = reinterpret_cast<unsigned char const *>(str2);
    int len = str1TrimLenBytes;

    while (len-- > 0) {
        if (*s1 != *s2) {
            return (*s1 > *s2) ? 1 : -1;
        }
        s1++;
        s2++;
    }
    return 0;
}

int
SqlStrCmp_Ascii_Var(
    char const * const str1,
    int str1LenBytes,
    char const * const str2,
    int str2LenBytes)
{
    // consider strcoll for I18N
    if (str1LenBytes > str2LenBytes) {
        return 1;
    } else if (str1LenBytes < str2LenBytes) {
        return -1;
    }

    assert(str1LenBytes == str2LenBytes);

    // comparison must be unsigned to work for > 128
    unsigned char const *s1 = reinterpret_cast<unsigned char const *>(str1);
    unsigned char const *s2 = reinterpret_cast<unsigned char const *>(str2);
    int len = str1LenBytes;

    while (len-- > 0) {
        if (*s1 != *s2) {
            return (*s1 > *s2) ? 1 : -1;
        }
        s1++;
        s2++;
    }
    return 0;
}


int
SqlStrLenBit_Ascii(
    char const * const str,
    int strLenBytes)
{
    return 8 * strLenBytes;
}

int
SqlStrLenChar_Ascii(
    char const * const str,
    int strLenBytes)
{
    return strLenBytes;
}

int
SqlStrLenOct_Ascii(
    char const * const str,
    int strLenBytes)
{
    return strLenBytes;
}


int
SqlStrOverlay_Ascii(
    char* dest,
    int destStorageBytes,
    char const * const str,
    int strLenBytes,
    char const * const over,
    int overLenBytes,
    int startChar,
    int lengthChar,
    int lenSpecified)
{
    if (!lenSpecified) {
        lengthChar = overLenBytes;
    }
    if (lengthChar < 0 || startChar < 1) {
        // Overlay is defined in terms of substring. These conditions
        // would, I believe, generate a substring error. Also
        // another "reference" sql database gets angry under these
        // conditions. Therefore, per:
        // SQL99 Part 2 Section 6.18 General Rule 3.d generate a
        // SQL99 Part 2 Section 22.1 22-011 "data exception substring error".
        throw SqlState::instance().code22011();
    }

    int leftLenBytes = startChar - 1;         // 1-index to 0-index
    if (leftLenBytes > strLenBytes) {
        leftLenBytes = strLenBytes;
    }
    char const *rightP = str + leftLenBytes + lengthChar;
    int rightLenBytes = strLenBytes - (leftLenBytes + lengthChar);
    if (rightLenBytes < 0) {
        rightLenBytes = 0;
    }
    assert(leftLenBytes >= 0);
    assert(rightLenBytes >= 0);
    assert(rightP >= str);

    if (leftLenBytes + rightLenBytes + overLenBytes > destStorageBytes) {
        // SQL99 Part 2 Section 22.1 22-001 "String Data Right truncation"
        throw SqlState::instance().code22001();
    }

    char *dp = dest;

    memcpy(dp, str, leftLenBytes);
    dp += leftLenBytes;
    memcpy(dp, over, overLenBytes);
    dp += overLenBytes;
    memcpy(dp, rightP, rightLenBytes);
    dp += rightLenBytes;

    return dp - dest;
}

int
SqlStrPos_Ascii(
    char const * const str,
    int strLenBytes,
    char const * const find,
    int findLenBytes)
{
    // SQL99 Part 2 Section 6.17 General Rule 2.a.
    if (!findLenBytes) {
        return 1;
    }
    // SQL99 Part 2 Section 6.17 General Rule 2.c.
    if (findLenBytes > strLenBytes) {
        return 0;
    }
    assert(findLenBytes > 0);
    assert(strLenBytes > 0);
    assert(strLenBytes - findLenBytes >= 0);

    register char const * s = str;
    char const * end = 1 + s + (strLenBytes - findLenBytes);

    while (s < end) {
        // search for first char of find
        s = reinterpret_cast<char const *>(memchr(s, *find, end - s));
        if (!s) {
            return 0;                // Case C.
        }
        if (!memcmp(s, find, findLenBytes)) {
            // add 1 to make result 1-indexed.
            return (s - str) + 1;   // Case B.
        } else {
            s++;
        }
    }
    return 0;                            // Case C.
}


int
SqlStrSubStr_Ascii(
    char const ** dest,
    int destStorageBytes,
    char const * const str,
    int strLenBytes,
    int subStartChar,
    int subLengthChar,
    int subLenSpecified)
{
    int e;
    if (subLenSpecified) {
        e = subStartChar + subLengthChar;
    } else {
        e = strLenBytes + 1;
        if (subStartChar > e) {
            e = subStartChar;
        }
    }

    if (e < subStartChar) {
        // Per SQL99 Part 2 Section 6.18 General Rule 3.d, generate a
        // "data exception substring error". SQL99 Part 2 Section 22.1 22-011
        throw SqlState::instance().code22011();
    }

    if (subStartChar > strLenBytes || e < 1) {
        *dest = str;
        return 0;
    }

    int s1 = 1;
    if (subStartChar > s1) {
        s1 = subStartChar;
    }

    int e1 = strLenBytes + 1;
    if (e < e1) {
        e1 = e;
    }
    int l1 = e1 - s1;


    if (l1 > destStorageBytes) {
        // SQL99 Part 2 Section 22.1 22-001 "String Data Right truncation"
        throw SqlState::instance().code22001();
    }
    if (l1 < 0) {
        // Expected behavior not clear.
        // SQL99 Part 2 Section 22.1 22-011 "data exception substring error".
        throw SqlState::instance().code22011();
    }

    // - 1 converts from 1-indexed to 0-indexed
    *dest = str + s1 - 1;
    return l1;
}

int
SqlStrToLower_Ascii(
    char* dest,
    int destStorageBytes,
    char const * src,
    int srcLenBytes)
{
    register char const * s = src;
    register char* d = dest;
    char* e = dest + srcLenBytes;

    if (srcLenBytes > destStorageBytes) {
        // SQL99 Part 2 Section 22.1 22-001 "String Data Right truncation"
        throw SqlState::instance().code22001();
    }

    while (d < e) {
        *(d++) = tolower(*(s++));
    }
    return srcLenBytes;
}

int
SqlStrToUpper_Ascii(
    char* dest,
    int destStorageBytes,
    char const * src,
    int srcLenBytes)
{
    register char const * s = src;
    register char* d = dest;
    char* e = dest + srcLenBytes;

    if (srcLenBytes > destStorageBytes) {
        // SQL99 Part 2 Section 22.1 22-001 "String Data Right truncation"
        throw SqlState::instance().code22001();
    }

    while (d < e) {
        *(d++) = toupper(*(s++));
    }
    return srcLenBytes;
}

int
SqlStrTrim_Ascii(
    char* dest,
    int destStorageBytes,
    char const * const str,
    int strLenBytes,
    int trimLeft,
    int trimRight,
    char trimchar)
{
    char const * start = str;
    char const * end = str + strLenBytes;
    int newLenBytes;

    // If many pad characters are expected, consider using memrchr()
    if (trimLeft) {
        while (start != end && *start == trimchar) {
            start++;
        }
    }
    if (trimRight && end != start) {
        end--;
        while (end != start && *end == trimchar) {
            end--;
        }
        if (end != start || *end != trimchar) {
            end++;
        }
    }
    newLenBytes = end - start;

    if (newLenBytes > destStorageBytes) {
        // SQL99 Part 2 Section 22.1 22-001 "String Data Right truncation"
        throw SqlState::instance().code22001();
    }
    memcpy(dest, start, newLenBytes);
    return newLenBytes;
}

int
SqlStrTrim_Ascii(
    char const ** result,
    char const * const str,
    int strLenBytes,
    int trimLeft,
    int trimRight,
    char trimchar)
{
    char const * start = str;
    char const * end = str + strLenBytes;

    // If many pad characters are expected, consider using memrchr()
    if (trimLeft) {
        while (start != end && *start == trimchar) {
            start++;
        }
    }
    if (trimRight && end != start) {
        end--;
        while (end != start && *end == trimchar) {
            end--;
        }
        if (end != start || *end != trimchar) {
            end++;
        }
    }

    *result = start;
    return end - start;
}


FENNEL_END_NAMESPACE

// End SqlStringAscii.cpp
