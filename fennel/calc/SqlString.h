/*
// $Id$
// Fennel is a relational database kernel.
// Copyright (C) 2004-2004 Disruptive Tech
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
// An ASCII & UCS2 string library that adheres to the SQL99 standard definitions
*/
#ifndef Fennel_SqlString_Included
#define Fennel_SqlString_Included

#ifdef HAVE_ICU
#include <unicode/ustring.h>
#endif

FENNEL_BEGIN_NAMESPACE

#if !(defined LITTLEENDIAN || defined BIGENDIAN)
#error "endian not defined"
#endif

/** \file SqlString.h 
 *
 * SqlString is a library of string fuctions that perform according to
 * the SQL99 standard.
 *
 * These functions are called by ExtendedInstructions in ExtString.h
 * 
 * This library supports 8-bit characters, labeled somewhat
 * misleadingly as Ascii, and fixed width 2-byte UCS2 characters. No
 * assumptions are made about alignment of ASCII strings. UCS2 strings
 * are assumed to be aligned, in order to work efficently with the ICU
 * library. Currently there are non-ICU UCS2 routines that make no
 * assumptions about alignment that could probably be made faster.
 *
 * Some functions work for either Ascii or UCS2 encodings. Other
 * functions are templated in order to work for either type. The
 * templating system as defined may be sufficent to also support
 * UTF-8, UTF-16 and UTF-32 encodings.
 *
 * The template is:
 * 
 * <code> template <int CodeUnitBytes, int MaxCodeUnitsPerCodePoint>. </code>
 *
 * A <b>code unit</b> is the atomic unit that represents part or all
 * of a <b>code point</b>.  A code point represents a character. The
 * <b>character encoding form</b> (e.g. ASCII, UCS2, UTF8) determines
 * how each <b>code point</b> is represented by one or more <b>code
 * units</b>.
 *
 * <code>
 * <ul>
 * <li> ASCII: CodeUnitBytes = 1, MaxCodeUnitsPerCodePoint =
 * 1</li><li> UCS2: CodeUnitBytes = 2, MaxCodeUnitsPerCodePoint =
 * 1</li><li> UTF8: CodeUnitBytes = 1, MaxCodeUnitsPerCodePoint =
 * 4</li><li> UTF16: CodeUnitBytes = 2, MaxCodeUnitsPerCodePoint =
 * 2</li><li> UTF32: CodeUnitBytes = 4, MaxCodeUnitsPerCodePoint =
 * 1</li>
 * </ul></code>
 *
 * Currently this templating form does not take endian issues into
 * account, but this information may be encoded in the strings, and
 * may not have to be explicit.
 *
 */


//! Strcat. SQL VARCHAR & CHAR. Ascii & UCS2.  
//! dest = dest || str. Returns new length in bytes.
//!
//! If either string is variable width, the result is variable
//! width: per SQL99 6.27 Syntax Rule 3, Case A, item i.
//! If both strings are fixed width, the result is fixed width, 
//! per item ii.
//!
//! Note that CHAR('1  ') || CHAR('2 ') = CHAR('1  2 ') and 
//! not CHAR('12   ').
//!
//! When called repeatedly to cat multiple strings together (e.g. A || B || C),
//! the final destLength must be exactly equal to the defined resulting width.
//! (e.g. width of A+B+C) for both VARCHAR & CHAR. Take care that these length
//! semantics are adhered to in the final result, even though intermediate
//! results (say A || B) may not have the correct length.
//!
//! When used with CHARs, set strLenBytes to strStorageBytes. On intermediate
//! results set destLenBytes = return value of previous call. Final result
//! should/must have return value == destStorage.
//
//  TODO: Does not implement an implementation defined max length.
int
SqlStrCat(char* dest,
          int destStorageBytes,
          int destLenBytes,
          char const * const str,
          int strLenBytes);

//! StrCat. SQL VARCHAR & CHAR. Ascii & UCS2.
//! dest = str1 || str2. Returns new length in bytes.
//!
//! This is an optimization for creating a concatenated string from
//! two other strings, eliminating a seperate string copy. The
//! assumption is that this is the common case with concatenation. 
//! Subsequent concatenations may occur with other form.
//!
//! If either string is variable width, the result is variable
//! width: per SQL99 6.27 Syntax Rule 3, Case A, item i.
//! If both strings are fixed width, the result is fixed width, 
//! item ii.
//!
//! Note: CHAR('1  ') || CHAR('2 ') is CHAR('1  2 ') and 
//! is not CHAR('12   ').
//!
//! When used with CHARs, ignore the return value, and set
//! destLenBytes = destStorageBytes
//
//  TODO: Does not implement an implementation defined max length.
int
SqlStrCat(char* dest,
          int destStorageBytes,
          char const * const str1,
          int str1LenBytes,
          char const * const str2,
          int str2LenBytes);

//! StrCmp. Fixed Width / SQL CHAR. Ascii, no UCS2 yet.
//!
//! Returns -1, 0, 1.
//!
//! Trim character code points that require more than one code unit are
//! currently unsupported.
template <int CodeUnitBytes, int MaxCodeUnitsPerCodePoint>
int
SqlStrCmp_Fix(char const * const str1,
              int str1LenBytes,
              char const * const str2,
              int str2LenBytes,
              int trimchar = ' ')
{

    char const * start = str1;
    char const * end = str1 + str1LenBytes;
    int str1TrimLenBytes;
    int str2TrimLenBytes;
    
    if (CodeUnitBytes == MaxCodeUnitsPerCodePoint) {
        if (CodeUnitBytes == 1) {
            if (end != start) {
                end--;
                while (end != start && *end == trimchar) end--;
                if (end != start || *end != trimchar) end++;
            }
            str1TrimLenBytes = end - start;

            start = str2;
            end = str2 + str2LenBytes;
            
            if (end != start) {
                end--;
                while (end != start && *end == trimchar) end--;
                if (end != start || *end != trimchar) end++;
            }
            str2TrimLenBytes = end - start;

        } else if (CodeUnitBytes == 2) {
            // TODO: Add UCS2 here
            throw std::logic_error("no UCS2");
        } else {
            throw std::logic_error("no such encoding");
        }
    } else {
        throw std::logic_error("no UTF8/16/32");
    }

    
    if (str1TrimLenBytes > str2TrimLenBytes) {
        return 1;
    } else if (str1TrimLenBytes < str2TrimLenBytes) {
        return -1;
    }

    assert(str1TrimLenBytes == str2TrimLenBytes);

    if (CodeUnitBytes == MaxCodeUnitsPerCodePoint) {
        if (CodeUnitBytes == 1) {
            // comparison must be unsigned to work for > 128
            unsigned char const *s1 = reinterpret_cast<unsigned char const *>(str1);
            unsigned char const *s2 = reinterpret_cast<unsigned char const *>(str2);
            int len = str1TrimLenBytes;
            
            while (len-- > 0) {
                if (*s1 != *s2) {
                    return ( (*s1 > *s2) ? 1 : -1 );
                }
                s1++;
                s2++;
            }
        } else if (CodeUnitBytes == 2) {
            // TODO: Add UCS2 here
            throw std::logic_error("no UCS2");
        } else {
            throw std::logic_error("no such encoding");
        }
    } else {
        throw std::logic_error("no UTF8/16/32");
    }
    return 0;
}


//! StrCmp. Variable Width / VARCHAR. Ascii only. No UCS2 yet.
//!
//! Returns -1, 0, 1
template <int CodeUnitBytes, int MaxCodeUnitsPerCodePoint>
int
SqlStrCmp_Var(char const * const str1,
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

    if (CodeUnitBytes == MaxCodeUnitsPerCodePoint) {
        if (CodeUnitBytes == 1) {
            // comparison must be unsigned to work for > 128
            unsigned char const *s1 = reinterpret_cast<unsigned char const *>(str1);
            unsigned char const *s2 = reinterpret_cast<unsigned char const *>(str2);
            int len = str1LenBytes;

            while (len-- > 0) {
                if (*s1 != *s2) {
                    return ( (*s1 > *s2) ? 1 : -1 );
                }
                s1++;
                s2++;
            }
        } else if (CodeUnitBytes == 2) {
            // TODO: Add UCS2 here
            throw std::logic_error("no UCS2");
        } else {
            throw std::logic_error("no such encoding");
        }
    } else {
        throw std::logic_error("no UTF8/16/32");
    }
    
    return 0;
}


//! StrCpy. String Copy. Fixed Width / SQL CHAR. Ascii and UCS2.
//!
//! This routine may not be compliant with the SQL99 standard.
//!
//! Pad character code points that require more than one code unit are
//! currently unsupported.
template <int CodeUnitBytes, int MaxCodeUnitsPerCodePoint>
int
SqlStrCpy_Fix(char* dest,
              int destStorageBytes,
              char const * const str,
              int strLenBytes,
              int padchar = ' ')
{
    if (strLenBytes > destStorageBytes) {
        // SQL99 22.1 22-001 "String Data Right truncation"
        throw "22001";
    }
    memcpy(dest, str, strLenBytes);

    // pad rest of dest storage
    if (MaxCodeUnitsPerCodePoint == 1) {
        if (CodeUnitBytes == 1) {
            // ASCII
            memset(dest + strLenBytes, padchar, destStorageBytes - strLenBytes);
        } else if (CodeUnitBytes == 2) {
            // UCS2
            assert(!(destStorageBytes & 1));
            assert(!(strLenBytes & 1));
            char *ptr = dest + strLenBytes;
            char *end = dest + destStorageBytes;
            char byte1, byte2;
#ifdef LITTLEENDIAN
            byte2 = (padchar >> 8) & 0xff;
            byte1 = padchar & 0xff;
#else
            byte1 = (padchar >> 8) & 0xff;
            byte2 = padchar & 0xff;
#endif
            assert(!((end - ptr) & 1));
            while (ptr < end) {
                *ptr = byte1;
                *(ptr+1) = byte2;
                ptr+=2;
            }
        } else {
            throw std::logic_error("no UCS-4");
        }
    } else {
        throw std::logic_error("no UTF8/16/32");
    }
    return destStorageBytes;
}


//! StrCpy. String Copy. Variable Width / VARCHAR. Ascii & UCS2.
//! Returns strLenBytes.
//!
//! This routine may not be compliant with the SQL99 standard.
//!
//! May be used for Fixed width strings if strLenBytes == destStorageBytes
//! Otherwise use SqlStrCpy_Fix() to get appropriate padding.
int
SqlStrCpy_Var(char* dest,
              int destStorageBytes,
              char const * const str,
              int strLenBytes);


//! StrLen in bits. CHAR/VARCHAR. Ascii & UCS2.
//!
//! Parameter str is ignored for ascii strings.
int
SqlStrLenBit(int strLenBytes);


//! StrLen in characters. CHAR/VARCHAR. Ascii & UCS2.
//!
//! Parameter str is ignored for ascii strings.
template <int CodeUnitBytes, int MaxCodeUnitsPerCodePoint>
int
SqlStrLenChar(char const * const str,
              int strLenBytes)
{
    if (CodeUnitBytes == 1 && MaxCodeUnitsPerCodePoint == 1) {
        // ASCII
        return strLenBytes;
    } else if (CodeUnitBytes == 2 & MaxCodeUnitsPerCodePoint == 1) {
        // UCS2
        assert(!(strLenBytes & 1));
        return strLenBytes >> 1;
    } else {
        throw std::logic_error("no UTF8/16/32");
    }
}

//! StrLen in octets. CHAR/VARCHAR. Ascii & UCS2.
//!
//! Parameter str is ignored for ascii strings.
int
SqlStrLenOct(int strLenBytes);

//! Overlay. CHAR/VARCHAR. Returns new length in bytes. Ascii. No UCS2 yet.
//!
//! See SQL99 6.18 Syntax Rule 10. Overlay is defined in terms of
//! Substring an concatenation. If start is < 1 or length < 0, a substring error
//! may be thrown.
//! Result is VARCHAR, as the result of substring is always VARCHAR,
//! and concatenation results in VARCHAR if any of its operands are VARCHAR.
//! startChar is 1-indexed, as per SQL standard.
template <int CodeUnitBytes, int MaxCodeUnitsPerCodePoint>
int
SqlStrOverlay(char* dest,
              int destStorageBytes,
              char const * const str,
              int strLenBytes,
              char const * const over,
              int overLenBytes,
              int startChar,
              int lenChar,
              int lenSpecified)
{
    if (CodeUnitBytes == MaxCodeUnitsPerCodePoint) {
        if (CodeUnitBytes == 1) {
            if (!lenSpecified) lenChar = overLenBytes;

            if (lenChar < 0 || startChar < 1) {
                // Overlay is defined in terms of substring. These conditions
                // would, I believe, generate a substring error. Also
                // another "reference" sql database gets angry under these 
                // conditions. Therefore:
                // Per SQL99 6.18, General Rule #3, D, generate a
                // "data exception substring error". SQL99 22.1 22-011
                throw "22011";
            }
    
            int leftLenBytes = startChar - 1;         // 1-index to 0-index
            if (leftLenBytes > strLenBytes) leftLenBytes = strLenBytes;

            char const *rightP = str + leftLenBytes + lenChar;
            int rightLenBytes = strLenBytes - (leftLenBytes + lenChar);
            if (rightLenBytes < 0) rightLenBytes = 0;

            assert(leftLenBytes >= 0);
            assert(rightLenBytes >= 0);
            assert(rightP >= str);
    
            if (leftLenBytes + rightLenBytes + overLenBytes > destStorageBytes) {
                // SQL99 22.1 22-001 "String Data Right truncation"
                throw "22001";
            }

            char *dp = dest;

            memcpy(dp, str, leftLenBytes);
            dp += leftLenBytes;
            memcpy(dp, over, overLenBytes);
            dp += overLenBytes;
            memcpy(dp, rightP, rightLenBytes);
            dp += rightLenBytes;
    
            return dp - dest;
        } else if (CodeUnitBytes == 2) {
            // TODO: Add UCS2 here
            throw std::logic_error("no UCS2");
        } else {
            throw std::logic_error("no such encoding");        }
    } else {
        throw std::logic_error("no UTF8/16/32");
    }
    throw std::logic_error("TODO: Fix this");
    return 0; // TODO: Fix this
}

//! Position. CHAR/VARHCAR. Returns 1-index string position. Ascii. No UCS2.
//!
//! Returns 0 if not found. Returns 1 if find is zero length. See SQL99 6.17
//! General Rule 2.
template <int CodeUnitBytes, int MaxCodeUnitsPerCodePoint>
int
SqlStrPos(char const * const str,
          int strLenBytes,
          char const * const find,
          int findLenBytes)
{
    if (CodeUnitBytes == MaxCodeUnitsPerCodePoint) {
        if (CodeUnitBytes == 1) {
            if (!findLenBytes) return 1;             // SQL99 6.17 General Rule 2 case A.
            if (findLenBytes > strLenBytes) return 0;   // Case C.

            assert(findLenBytes > 0);
            assert(strLenBytes > 0);
            assert(strLenBytes - findLenBytes >= 0);

            register char const * s = str;
            char const * end = 1 + s + (strLenBytes - findLenBytes);

            while(s < end) {
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
        } else if (CodeUnitBytes == 2) {
            // TODO: Add UCS2 here
            throw std::logic_error("no UCS2");
        } else {
            throw std::logic_error("no such encoding");
        }
    } else {
        throw std::logic_error("no UTF8/16/32");
    }
    throw std::logic_error("TODO: Fix this");
    return 0; // TODO: Fix this
}

//! Substring by reference. Returns VARCHAR. Accepts CHAR/VARCHAR. 
//! Ascii, no UCS2. Sets dest to start of of substring.
//! Returns length of substring.
//! 
//! Note that subStart is 1-indexed, as per SQL99 spec.
//! All substring parameters are handled as signed, as spec implies that they 
//! could be negative. Some combinations of subStart and subLenBytes may throw an
//! exception.
//! Results in a VARCHAR.
//! See SQL99 6.18, General Rule 3.
//! subStartChar is 1-indexed.
template <int CodeUnitBytes, int MaxCodeUnitsPerCodePoint>
int
SqlStrSubStr(char const ** dest,
             int destStorageBytes,
             char const * const str,
             int strLenBytes,
             int subStartChar,
             int subLenChar,
             int subLenCharSpecified)
{
    if (CodeUnitBytes == MaxCodeUnitsPerCodePoint) {
        if (CodeUnitBytes == 1) {
            int e;
            if (subLenCharSpecified) {
                e = subStartChar + subLenChar;
            } else {
                e = strLenBytes + 1;
                if (subStartChar > e) e = subStartChar;
            }

            if (e < subStartChar) {
                // Per SQL99 6.18, General Rule #3, D, generate a
                // "data exception substring error". SQL99 22.1 22-011
                throw "22011";
            }

            if (subStartChar > strLenBytes || e < 1) {
                return 0;
            } 

            int s1 = 1;
            if (subStartChar > s1) s1 = subStartChar;
        
            int e1 = strLenBytes + 1;
            if (e < e1) e1 = e;

            int l1 = e1 - s1;


            if (l1 > destStorageBytes) {
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
        } else if (CodeUnitBytes == 2) {
            // TODO: Add UCS2 here
            throw std::logic_error("no UCS2");
        } else {
            throw std::logic_error("no such encoding");
        }
    } else {
        throw std::logic_error("no UTF8/16/32");
    }
    throw std::logic_error("TODO: Fix this");
    return 0; // TODO: Fix this
}

//! Template argument for SqlStrAlterCase
enum SqlStrAlterCaseAction {
    AlterCaseUpper,
    AlterCaseLower
};

//! toLower and toUpper. CHAR/VARCHAR. Returns new length. ASCII & UCS2.
template <int CodeUnitBytes,
          int MaxCodeUnitsPerCodePoint,
          SqlStrAlterCaseAction Action>
int
SqlStrAlterCase(char* dest,
                int destStorageBytes,
                char const * const src,
                int srcLenBytes,
                char const * const locale = 0)
{
    int retVal;

    if (srcLenBytes > destStorageBytes) {
        // SQL99 22.1 22-001 "String Data Right truncation"
        throw "22001";
    }

    if (MaxCodeUnitsPerCodePoint == 1) {
        if (CodeUnitBytes == 1) {
            register char const * s = src;
            register char* d = dest;
            char* e = dest + srcLenBytes;
            while (d < e) {
                switch(Action) {
                case AlterCaseUpper:
                    *(d++) = toupper(*(s++));
                    break;
                case AlterCaseLower:
                    *(d++) = tolower(*(s++));
                    break;
                default:
                    throw std::logic_error("AlterCase Action");
                    break;
                }
            }
            retVal = srcLenBytes;
        }
        else if (CodeUnitBytes == 2) {
            // UCS2
#ifdef HAVE_ICU
            assert(!(destStorageBytes & srcLenBytes & 1));
            // strings must be short aligned
            // TODO: Change tuples to force strings to be short aligned.
            assert(!(reinterpret_cast<int>(src) & 1));
            assert(!(reinterpret_cast<int>(dest) & 1));
            assert(sizeof(UChar) == 2);
            assert(locale);   // Don't allow locale defaulting

            int32_t destStorageUChar = destStorageBytes >> 1;
            int32_t srcLenUChar = srcLenBytes >> 1;
            int32_t newLenUChar;
            UErrorCode errorCode = U_ZERO_ERROR;

            switch(Action) {
            case AlterCaseUpper:
                newLenUChar = u_strToUpper(reinterpret_cast<UChar*>(dest), 
                                           destStorageUChar,
                                           reinterpret_cast<UChar const *>(src),
                                           srcLenUChar,
                                           locale,
                                           &errorCode);

                break;
            case AlterCaseLower:
                newLenUChar = u_strToLower(reinterpret_cast<UChar*>(dest), 
                                           destStorageUChar,
                                           reinterpret_cast<UChar const *>(src),
                                           srcLenUChar,
                                           locale,
                                           &errorCode);
                break;
            default:
                throw std::logic_error("AlterCase Action");
                break;
            }

            if (newLenUChar > destStorageUChar) {
                // SQL99 22.1 22-001 "String Data Right truncation"
                throw "22001";
            }
            if (U_FAILURE(errorCode)) {
                // TODO: Clean up ICU error handling.
                // Other ICU error. Unlikely to occur?
                throw u_errorName(errorCode);
            }
            retVal = newLenUChar << 1;
#else
            throw std::logic_error("no UCS2");
#endif
        } else {
            throw std::logic_error("no such encoding");
        }
    } else {
        // Note: Potentially UTF16 can be handled by UCS2 code
        throw std::logic_error("no UTF8/16/32");
    }
    return retVal;
}

//! Trim padding. CHAR/VARCHAR. Returns new length.
//!
//! See SQL99 6.18 General Rule 8.
//! Results in a VARCHAR.
//!
//! Trim character code points that require more than one code unit are
//! currently unsupported.
template <int CodeUnitBytes, int MaxCodeUnitsPerCodePoint>
int 
SqlStrTrim(char* dest,
           int destStorageBytes,
           char const * const str,
           int strLenBytes,
           int trimLeft,
           int trimRight,
           int trimchar = ' ')
{
    char const * start = str;
    char const * end = str + strLenBytes;
    assert(strLenBytes >= 0);
    int newLenBytes;
    
    if (MaxCodeUnitsPerCodePoint == 1) {
        if (CodeUnitBytes == 1) {
            // ASCII
            // If many pad characters are expected, consider using memrchr()
            if (trimLeft) {
                while (start != end && *start == trimchar) start++;
            }
            if (trimRight && end != start) {
                end--;
                while (end != start && *end == trimchar) end--;
                if (end != start || *end != trimchar) end++;
            }
            newLenBytes = end - start;
        } else if (CodeUnitBytes == 2) {
            // UCS2
            assert(!(strLenBytes & 1));
            char byte1, byte2;
#ifdef LITTLEENDIAN
            byte2 = (trimchar >> 8) & 0xff;
            byte1 = trimchar & 0xff;
#else
            byte1 = (trimchar >> 8) & 0xff;
            byte2 = trimchar & 0xff;
#endif
            if (trimLeft) {
                while (start < end && *start == byte1 && *(start+1) == byte2) {
                    start += 2;
                }
            }
            if (trimRight && end != start) {
                end -= 2;
                while (end > start && *end == byte1 && *(end+1) == byte2) {
                    end -=2;
                }
                if (end != start || *end != trimchar) end += 2;
            }
            newLenBytes = end - start;
            assert(!(newLenBytes & 1));
        } else {
            throw std::logic_error("no such encoding");
        }
    } else {
        // Note: Potentially UTF16 can be handled by UCS2 code
        throw std::logic_error("no UTF8/16/32");
    }

    if (newLenBytes > destStorageBytes) {
        // SQL99 22.1 22-001 "String Data Right truncation"
        throw "22001";
    }
    memcpy(dest, start, newLenBytes);
    return newLenBytes;
}

//! Trim padding by reference. CHAR/VARCHAR. Returns new length.
//!
//! See SQL99 6.18 General Rule 8.
//! Results in a VARCHAR.
//! Note: Does not check that result has enough capacity to contain
//! substring as this is irrelevant. If a program depends on the size
//! of result not changing, and this instruction inforcing that
//! invariant -- probably a bad practice anyway -- trouble could result.
//!
//! Trim character code points that require more than one code unit are
//! currently unsupported.
template <int CodeUnitBytes, int MaxCodeUnitsPerCodePoint>
int 
SqlStrTrim(char const ** result,
           char const * const str,
           int strLenBytes,
           int trimLeft,
           int trimRight,
           int trimchar = ' ')
{
    char const * start = str;
    char const * end = str + strLenBytes;
    assert(strLenBytes >= 0);

    if (MaxCodeUnitsPerCodePoint == 1) {
        if (CodeUnitBytes == 1) {
            // ASCII
            // If many pad characters are expected, consider using memrchr()
            if (trimLeft) {
                while (start != end && *start == trimchar) start++;
            }
            if (trimRight && end != start) {
                end--;
                while (end != start && *end == trimchar) end--;
                if (end != start || *end != trimchar) end++;
            }
        } else if (CodeUnitBytes == 2) {
            // UCS2
            assert(!(strLenBytes & 1));
            char byte1, byte2;
#ifdef LITTLEENDIAN
            byte2 = (trimchar >> 8) & 0xff;
            byte1 = trimchar & 0xff;
#else
            byte1 = (trimchar >> 8) & 0xff;
            byte2 = trimchar & 0xff;
#endif
            if (trimLeft) {
                while (start < end && *start == byte1 && *(start+1) == byte2) {
                    start += 2;
                }
            }
            if (trimRight && end != start) {
                end -= 2;
                while (end > start && *end == byte1 && *(end+1) == byte2) {
                    end -=2;
                }
                if (end != start || *end != trimchar) end += 2;
            }
            assert(!((end - start) & 1));
        } else {
            throw std::logic_error("no such encoding");
        }
    } else {
        // Note: Potentially UTF16 can be handled by UCS2 code
        throw std::logic_error("no UTF8/16/32");
    }
    
    *result = start;
    return end - start;
}


FENNEL_END_NAMESPACE

#endif

// End SqlString.h

