/*
// $Id$
// Fennel is a relational database kernel.
// Copyright (C) 2004-2004 Disruptive Technologies, Inc.
//
// This program is free software; you can redistribute it and/or
// modify it under the terms of the GNU Lesser General Public License
// as published by the Free Software Foundation; either version 2.1
// of the License, or (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with this program; if not, write to the Free Software
// Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
//
// SqlString
//
// An ascii string library that adheres to the SQL99 standard definitions
*/

#include "fennel/calc/SqlString.h"

FENNEL_BEGIN_NAMESPACE

int
SqlStrAsciiCat(char* dest,
                int destWidth,
                int destLen,
                char const * const str,
                int strLen)
{
    if (destLen + strLen > destWidth) {
        // SQL99 22.1 22-001 "String Data Right truncation"
        throw "22001";
    }

    memcpy(dest + destLen, str, strLen);
    return destLen + strLen;
}


int
SqlStrAsciiCat(char* dest,
                int destWidth,
                char const * const str1,
                int str1Len,
                char const * const str2,
                int str2Len)
{
    if (str1Len + str2Len > destWidth) {
        // SQL99 22.1 22-001 "String Data Right truncation"
        throw "22001";
    }

    memcpy(dest, str1, str1Len);
    memcpy(dest+str1Len, str2, str2Len);
    return str1Len + str2Len;
}

int
SqlStrAsciiCmpF(char const * const str1,
                int str1Width,
                char const * const str2,
                int str2Width,
                char trimchar)
{

    char const * start = str1;
    char const * end = str1 + str1Width;
    
    if (end != start) {
        end--;
        while (end != start && *end == trimchar) end--;
        if (end != start || *end != trimchar) end++;
    }
    int str1Len = end - start;
    
    start = str2;
    end = str2 + str2Width;

    if (end != start) {
        end--;
        while (end != start && *end == trimchar) end--;
        if (end != start || *end != trimchar) end++;
    }
    int str2Len = end - start;
    
    if (str1Len > str2Len) {
        return 1;
    } else if (str1Len < str2Len) {
        return -1;
    }

    assert(str1Len == str2Len);
    
    // comparison must be unsigned to work for > 128
    unsigned char const *s1 = reinterpret_cast<unsigned char const *>(str1);
    unsigned char const *s2 = reinterpret_cast<unsigned char const *>(str2);
    int len = str1Len;

    while (len-- > 0) {
        if (*s1 != *s2) {
            return ( (*s1 > *s2) ? 1 : -1 );
        }
        s1++;
        s2++;
    }
    return 0;
}

int
SqlStrAsciiCmpV(char const * const str1,
                int str1Len,
                char const * const str2,
                int str2Len)
{
    // consider strcoll for I18N
    if (str1Len > str2Len) {
        return 1;
    } else if (str1Len < str2Len) {
        return -1;
    }

    assert(str1Len == str2Len);
    
    // comparison must be unsigned to work for > 128
    unsigned char const *s1 = reinterpret_cast<unsigned char const *>(str1);
    unsigned char const *s2 = reinterpret_cast<unsigned char const *>(str2);
    int len = str1Len;

    while (len-- > 0) {
        if (*s1 != *s2) {
            return ( (*s1 > *s2) ? 1 : -1 );
        }
        s1++;
        s2++;
    }
    return 0;
}


int
SqlStrAsciiLenBit(char const * const str,
                  int strLen)
{
    return 8 * strLen;
}

int
SqlStrAsciiLenChar(char const * const str,
                   int strLen)
{
    return strLen;
}

int
SqlStrAsciiLenOct(char const * const str,
                  int strLen)
{
    return strLen;
}


int
SqlStrAsciiOverlay(char* dest,
                   int destWidth,
                   char const * const str,
                   int strLen,
                   char const * const over,
                   int overLen,
                   int start,
                   int len,
                   bool lenSpecified)
{
    if (!lenSpecified) len = overLen;

    if (len < 0 || start < 1) {
        // Overlay is defined in terms of substring. These conditions
        // would, I believe, generate a substring error. Also
        // another "reference" sql database gets angry under these 
        // conditions. Therefore:
        // Per SQL99 6.18, General Rule #3, D, generate a
        // "data exception substring error". SQL99 22.1 22-011
        throw "22011";
    }
    
    int leftLen = start - 1;         // 1-index to 0-index
    if (leftLen > strLen) leftLen = strLen;

    char const *rightP = str + leftLen + len;
    int rightLen = strLen - (leftLen + len);
    if (rightLen < 0) rightLen = 0;

    assert(leftLen >= 0);
    assert(rightLen >= 0);
    assert(rightP >= str);
    
    if (leftLen + rightLen + overLen > destWidth) {
        // SQL99 22.1 22-001 "String Data Right truncation"
        throw "22001";
    }

    char *dp = dest;

    memcpy(dp, str, leftLen);
    dp += leftLen;
    memcpy(dp, over, overLen);
    dp += overLen;
    memcpy(dp, rightP, rightLen);
    dp += rightLen;
    
    return dp - dest;
}

int
SqlStrAsciiPos(char const * const str,
               int strWidth,
               char const * const find,
               int findWidth)
{
    if (!findWidth) return 1;             // SQL99 6.17 General Rule 2 case A.
    if (findWidth > strWidth) return 0;   // Case C.

    assert(findWidth > 0);
    assert(strWidth > 0);
    assert(strWidth - findWidth >= 0);

    register char const * s = str;
    char const * end = 1 + s + (strWidth - findWidth);

    while(s < end) {
        // search for first char of find
        s = reinterpret_cast<char const *>(memchr(s, *find, end - s));
        if (!s) {
            return 0;                // Case C.
        }
        if (!memcmp(s, find, findWidth)) {
            // add 1 to make result 1-indexed.
            return (s - str) + 1;   // Case B.
        } else {
            s++;
        }
    }
    return 0;                            // Case C.
}


int
SqlStrAsciiSubStr(char const ** dest,
                  int destWidth,
                  char const * const str,
                  int strLen,
                  int subStart,
                  int subLen,
                  bool subLenSpecified)
{
    int e;
    if (subLenSpecified) {
        e = subStart + subLen;
    } else {
        e = strLen + 1;
        if (subStart > e) e = subStart;
    }

    if (e < subStart) {
        // Per SQL99 6.18, General Rule #3, D, generate a
        // "data exception substring error". SQL99 22.1 22-011
        throw "22011";
    }

    if (subStart > strLen || e < 1) {
        return 0;
    } 

    int s1 = 1;
    if (subStart > s1) s1 = subStart;
        
    int e1 = strLen + 1;
    if (e < e1) e1 = e;

    int l1 = e1 - s1;


    if (l1 > destWidth) {
        // SQL99 22.1 22-001 "String Data Right truncation"
        throw "22001";
    }
    if (l1 < 0) {
        // Expected behavior not clear. 
        // "data exception substring error". SQL99 22.1 22-011
        throw "22011";
    }
    
    // - 1 converts from 1-indexed to 0-indexed
    *dest = str + s1 - 1;
    return l1;
}

int
SqlStrAsciiToLower(char* dest,
                   int destWidth,
                   char const * src,
                   int srcLen)
{
    register char const * s = src;
    register char* d = dest;
    char* e = dest + srcLen;

    if (srcLen > destWidth) {
        // SQL99 22.1 22-001 "String Data Right truncation"
        throw "22001";
    }

    while (d < e) {
        *(d++) = tolower(*(s++));
    }
    return srcLen;
}

int
SqlStrAsciiToUpper(char* dest,
                   int destWidth,
                   char const * src,
                   int srcLen)
{
    register char const * s = src;
    register char* d = dest;
    char* e = dest + srcLen;

    if (srcLen > destWidth) {
        // SQL99 22.1 22-001 "String Data Right truncation"
        throw "22001";
    }

    while (d < e) {
        *(d++) = toupper(*(s++));
    }
    return srcLen;
}

int 
SqlStrAsciiTrim(char* dest, 
                int destWidth,
                char const * const str,
                int strLen,
                bool trimLeft,
                bool trimRight,
                char trimchar)
{
    char const * start = str;
    char const * end = str + strLen;
    int newLen;
    
    // If many pad characters are expected, consider using memrchr()
    if (trimLeft) {
        while (start != end && *start == trimchar) start++;
    }
    if (trimRight && end != start) {
        end--;
        while (end != start && *end == trimchar) end--;
        if (end != start || *end != trimchar) end++;
    }
    newLen = end - start;

    if (newLen > destWidth) {
        // SQL99 22.1 22-001 "String Data Right truncation"
        throw "22001";
    }
    memcpy(dest, start, newLen);
    return newLen;
}

int 
SqlStrAsciiTrim(char const ** result,
                char const * const str,
                int strLen,
                bool trimLeft,
                bool trimRight,
                char trimchar)
{
    char const * start = str;
    char const * end = str + strLen;
    
    // If many pad characters are expected, consider using memrchr()
    if (trimLeft) {
        while (start != end && *start == trimchar) start++;
    }
    if (trimRight && end != start) {
        end--;
        while (end != start && *end == trimchar) end--;
        if (end != start || *end != trimchar) end++;
    }
    
    *result = start;
    return end - start;
}


FENNEL_END_NAMESPACE

