/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2004-2005 Disruptive Tech
// Copyright (C) 2005-2005 The Eigenbase Project
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

#include <limits>

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
//! width: per SQL99 Part 2 Section 6.27 Syntax Rule 3.a.i.
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
//! two other strings, eliminating a separate string copy. The
//! assumption is that this is the common case with concatenation. 
//! Subsequent concatenations may occur with other form.
//!
//! If either string is variable width, the result is variable
//! width: per SQL99 Part 2 Section 6.27 Syntax Rule 3.a.i.
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

//! StrCmp. Binary.
//! See SQL2003 Part 2 Section 4.3.2.
//! As an extension to SQL2003, allow inequalities (>,>=, etc.)
//! Follows byte-wise comparison semantics of memcmp().
//!
//! Returns -1, 0, 1.
int
SqlStrCmp_Bin(char const * const str1,
              int str1LenBytes,
              char const * const str2,
              int str2LenBytes);

//! StrCmp. SQL VARCHAR & CHAR. Ascii, no UCS2 yet.
//!
//! str1 and str2 can be any combination of VARCHAR and/or CHAR.
//! Supports only PAD SPACE mode. TODO: Support NO PAD mode.
//!
//! References: 
//! SQL2003 Part 2 Section 4.2.2.
//! SQL99 Part 2 Section 4.2.1.
//! SQL2003 Part 2 Section 8.2 General Rule 3.
//! SQL99 Part 2 Section 8.2, General Rule 3.
//! SQL2003 Part 2 Section 10.5 General Rule 2 (default to PAD SPACE).
//! SQL99 Part 2 Section 10.6 General Rule 2: (default to PAD SPACE).
//!
//! Note: Extending a shorter string with spaces is equivalent to 
//! ignoring trailing spaces on both strings, and performing a more
//! conventional strcmp-type operation. For example 'A ' == 'A'
//! becomes 'A ' == 'A ' or equivalently, 'A', 'A'. 'AB ' == 'A'
//! becomes 'AB ' == 'A  ' or equivalently, 'AB', 'A'. Extending with
//! spaces is the same as comparison of the longer string with nothing
//! as any non-space character is assumed to be greater than space.
//! Note that this means that passing strings with values less than
//! space will result in some confusion.
//!
//! Returns -1, 0, 1.
//!
//! Trim character code points that require more than one code unit are
//! currently unsupported.
template <int CodeUnitBytes, int MaxCodeUnitsPerCodePoint>
int
SqlStrCmp(char const * const str1,
              int str1LenBytes,
              char const * const str2,
              int str2LenBytes,
              int trimchar = ' ')
{
    assert(str1LenBytes >= 0);
    assert(str2LenBytes >= 0);
    
    if (CodeUnitBytes == MaxCodeUnitsPerCodePoint) {
        if (CodeUnitBytes == 1) {
            char const * start = str1;
            char const * end = str1 + str1LenBytes;
            int str1TrimLenBytes;
            int str2TrimLenBytes;
    
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
            return SqlStrCmp_Bin(str1, str1TrimLenBytes, 
                                 str2, str2TrimLenBytes);
#if 0
            int minLenBytes = str1TrimLenBytes > str2TrimLenBytes ?
                str2TrimLenBytes : str1TrimLenBytes;


            // To allow 0, "Null", values in string, uses memcmp over
            // strcmp. Not strictly needed, may have some future value.
            // First, check for differences in "common" length. If same
            // values, declare the longer string be declared "larger".
            int memc = memcmp(str1, str2, minLenBytes);
            if (memc > 0) {
                // Normalize to -1, 0, 1
                return 1;
            } else if (memc < 0) {
                // Normalize to -1, 0, 1
                return -1;
            } else if (str1TrimLenBytes == str2TrimLenBytes) {
                // memc == 0
                // Equal length & contain same data -> equal
                return 0;
            } else if (str1TrimLenBytes > str2TrimLenBytes) {
                return 1;
            } else {
                return -1;
            }
#endif
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
//! See SQL99 Part 2 Section 6.18 Syntax Rule 10. Overlay is defined in terms of
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
                // Per SQL99 Part 2 Section 6.18 General Rule 3.d, generate a
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
//! Returns 0 if not found. Returns 1 if find is zero length.
//! See SQL99 Part 2 Section 6.17 General Rule 2.
template <int CodeUnitBytes, int MaxCodeUnitsPerCodePoint>
int
SqlStrPos(char const * const str,
          int strLenBytes,
          char const * const find,
          int findLenBytes)
{
    if (CodeUnitBytes == MaxCodeUnitsPerCodePoint) {
        if (CodeUnitBytes == 1) {
            // SQL99 Part 2 Section 6.17 General Rule 2.a.
            if (!findLenBytes) return 1;             
            // SQL99 Part 2 Section 6.17 General Rule 2.c.
            if (findLenBytes > strLenBytes) return 0;

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
//! See SQL99 Part 2 Section 6.18 General Rule 3.
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
                // Per SQL99 Part 2 Section 6.18 General Rule 3.d, generate a
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
//! See SQL99 Part 2 Section 6.18 General Rule 8.
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
//! See SQL99 Part 2 Section 6.18 General Rule 8.
//! Results in a VARCHAR.
//! Note: Does not check that result has enough capacity to contain
//! substring as this is irrelevant. If a program depends on the size
//! of result not changing, and this instruction enforcing that
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

//! SqlStrCastToExact. Char & VarChar. Ascii only.
//!
//! Cast a string to an exact numeric.
template <int CodeUnitBytes, int MaxCodeUnitsPerCodePoint>
int64_t
SqlStrCastToExact(char const * const str,
                  int strLenBytes,
                  int padChar = ' ')
{
    int64_t rv = 0;
    bool negative = false;

    if (MaxCodeUnitsPerCodePoint == 1) {
        if (CodeUnitBytes == 1) {
            // ASCII
            // comparison must be unsigned to work for > 128
            unsigned char const *ptr =
                reinterpret_cast<unsigned char const *>(str);
            unsigned char const *end =
                reinterpret_cast<unsigned char const *>(str + strLenBytes);

            // STATE: parse optional sign, consume leading white space
            while (ptr < end) {
                if (*ptr == '-') {
                    // move onto next state, do not allow whitespace
                    // after -, for example '- 4' is not allowed
                    negative = true;
                    ptr++;
                    break;
                } else if (*ptr == '+') {
                    // move onto next state, do not allow whitespace
                    // after +, for example '+ 4' is not allowed
                    ptr++;
                    break;
                } else if (*ptr == padChar) {
                    // consume leading whitespace
                    ptr++;
                } else if (*ptr >= '0' &&  *ptr <= '9') {
                    // found a number. don't advance, move onto next state
                    break;
                } else {
                    // unexpected character found
                    // SQL99 Part 2 Section 6.22 General Rule 6.b.i data
                    // exception -- invalid character value for cast
                    throw "22018";
                }
            }

            if (ptr >= end) {
                // no number found
                // SQL99 Part 2 Section 6.22 General Rule 6.b.i data
                // exception -- invalid character value for cast
                throw "22018";
            }

            // STATE: Parse numbers until padChar, end, or illegal char
            bool parsed = false;
            bool got_nonzero = false;
            bool overflow = false;
            int ndigits = 0;
            while (ptr < end) {
                if (*ptr >= '0' && *ptr <= '9') {
                    // number
                    if (*ptr != '0') {
                        got_nonzero = true;
                    }
                    // Only start counting digits after 1st nonzero digit
                    if (got_nonzero) {
                        ndigits++;
                    }
                    if (ndigits <= 18) {
                        rv = (rv * 10) + (*(ptr++) - '0');
                    } else if (ndigits == 19) {
                        // Handle 19th digit overflow
                        int64_t tmp;
                        tmp = rv * 10 + (*(ptr++) - '0');
                        if (tmp < rv) {
                            if (negative) { 
                                if (-tmp == std::numeric_limits<int64_t>::min()) {
                                    // okay
                                } else {
                                    overflow = true;
                                }
                            } else {
                                overflow = true;
                            }                                
                        }
                        rv = tmp;
                    } else {
                        rv = (rv * 10) + (*(ptr++) - '0');
                        overflow = true;
                    }
                    parsed = true;
                } else if (*ptr == padChar) {
                    // move onto next state, end of number
                    ptr++;
                    break;
                } else {
                    // illegal character
                    parsed = false;
                    break;
                }
            }

            // STATE: Parse padChar until end or illegal char
            while (ptr < end) {
                if (*(ptr++) != padChar) {
                    // unexpected character after end of number
                    parsed = false;
                    break;
                }
            }
            if (!parsed) {
                // SQL99 Part 2 Section 6.22 General Rule 6.b.i data
                // exception -- invalid character value for cast
                throw "22018";
            }

            // Throw overflow exception only if parse okay
            if (overflow) {
                // data exception -- numeric value out of range
                throw "22003";
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

    if (negative) {
        return rv * -1;
    } else {
        return rv;
    }
}

//! SqlExactMax
//!
//! Returns the maximum integer with the given precision
inline int64_t SqlExactMax(int precision, bool negative)
{
    int64_t rv;
    if (precision < 19) {
        rv = 1;
        for (int i = 0; i < precision; i++) {
            rv *= 10;
        }
        rv--;
    } else {
        if (negative) {
            rv = -std::numeric_limits<int64_t>::min();
        } else {
            rv = std::numeric_limits<int64_t>::max();
        }
    }
    return rv;
}

//! SqlStrCastToExact. Char & VarChar. Ascii only.
//!
//! Cast a string to an exact numeric with precision and scale.
template <int CodeUnitBytes, int MaxCodeUnitsPerCodePoint>
int64_t
SqlStrCastToExact(char const * const str,
                  int strLenBytes,
                  int precision,
                  int scale,
                  int padChar = ' ')
{
    int64_t rv = 0;
    bool negative = false;

    assert(precision > 0);
    if (MaxCodeUnitsPerCodePoint == 1) {
        if (CodeUnitBytes == 1) {
            // ASCII
            // comparison must be unsigned to work for > 128
            unsigned char const *ptr =
                reinterpret_cast<unsigned char const *>(str);
            unsigned char const *end =
                reinterpret_cast<unsigned char const *>(str + strLenBytes);

            // STATE: parse optional sign, consume leading white space
            while (ptr < end) {
                if (*ptr == '-') {
                    // move onto next state, do not allow whitespace
                    // after -, for example '- 4' is not allowed
                    negative = true;
                    ptr++;
                    break;
                } else if (*ptr == '+') {
                    // move onto next state, do not allow whitespace
                    // after +, for example '+ 4' is not allowed
                    ptr++;
                    break;
                } else if (*ptr == padChar) {
                    // consume leading whitespace
                    ptr++;
                } else if (*ptr >= '0' &&  *ptr <= '9') {
                    // found a number. don't advance, move onto next state
                    break;
                } else if (*ptr == '.') {
                    // found decimal point. don't advance, move onto next state
                    break;
                } else {
                    // unexpected character found
                    // SQL99 Part 2 Section 6.22 General Rule 6.b.i data
                    // exception -- invalid character value for cast
                    throw "22018";
                }
            }

            if (ptr >= end) {
                // no number found
                // SQL99 Part 2 Section 6.22 General Rule 6.b.i data
                // exception -- invalid character value for cast
                throw "22018";
            }

            // STATE: Parse numbers until padChar, end, or illegal char
            bool parsed = false;
            bool decimal_parsed = false;
            bool roundup = false;
            bool overflow = false;
            int ignored = 0; 
            int decimal_position = 0;
            int mantissa_digits = 0, parsed_digits = 0;
            int digit;         
            int64_t mantissa = 0;
            int64_t exponent = 0;
            while (ptr < end) {
                if (*ptr >= '0' && *ptr <= '9') {
                    // number
                    digit = (*(ptr++) - '0');

                    // Only add to mantissa if precision not reached
                    if (mantissa_digits < precision) {
                        if (mantissa_digits < 18) {
                            mantissa = mantissa * 10 + digit;
                        } else if (mantissa_digits == 18) {
                            // Handle 19th digit overflow
                            int64_t tmp;
                            tmp = mantissa * 10 + digit;
                            if (tmp < mantissa) {
                                if (negative) { 
                                    if (-tmp == std::numeric_limits<int64_t>::min()) {
                                        // okay
                                    } else {
                                        // data exception -- numeric value out of range
                                        overflow = true;
                                    }
                                } else {
                                    // data exception -- numeric value out of range
                                    overflow = true;
                                }                                
                            }
                            mantissa = tmp;
                        } else {
                            overflow = true;
                        }

                        if (mantissa != 0) {
                            mantissa_digits++;
                        }
                    } else {
                        // Decide if ignored digits (after precision is lost)
                        // causes the final result to be rounded up or not
                        ignored++;
                        if (ignored == 1) {
                            roundup = (digit >= 5);
                        }
                    }
                    parsed = true;
                    if (decimal_parsed || mantissa != 0) {
                        parsed_digits++;
                    }
                } else if (!decimal_parsed && (*ptr == '.')) {
                    // decimal point
                    ptr++;
                    decimal_parsed = true;
                    decimal_position = parsed_digits;
                } else if ((*ptr == 'E') || (*ptr == 'e')) {
                    // parse exponent, move into next state
                    ptr++;
                    if (ptr < end) {
                        if (*ptr == '+' || *ptr == '-' || 
                            (*ptr >= '0' && *ptr <= '9')) {
                            exponent = SqlStrCastToExact
                                <CodeUnitBytes, MaxCodeUnitsPerCodePoint>
                                ((char const * const) ptr, end - ptr, padChar);
                        } else {
                            parsed = false;
                        }
                    } else {
                        parsed = false;
                    } 
                    ptr = end;
                    break;
                } else if (*ptr == padChar) {
                    // move onto next state, end of number
                    ptr++;
                    break;
                } else {
                    // illegal character
                    parsed = false;
                    break;
                }
            }

            // STATE: Parse padChar until end or illegal char
            while (ptr < end) {
                if (*(ptr++) != padChar) {
                    // unexpected character after end of number
                    parsed = false;
                    break;
                }
            }
            if (!parsed) {
                // SQL99 Part 2 Section 6.22 General Rule 6.b.i data
                // exception -- invalid character value for cast
                throw "22018";
            }

            // Throw overflow exception only if parse okay
            if (overflow) {
                // data exception -- numeric value out of range
                throw "22003";
            }

            if (!decimal_parsed) {
                decimal_position = parsed_digits;
            }

            if (roundup) {
                // Check if digits will increase/overflow
                if (mantissa == SqlExactMax(mantissa_digits, negative)) {
                    mantissa_digits++;
                }
                mantissa++;
            }

            int parsed_scale = 
                parsed_digits - ignored - decimal_position - exponent;

            if (mantissa_digits - parsed_scale > precision - scale) {
                // SQL2003 Part 2 Section 6.12 General Rule 8.a.ii
                // data exception -- numeric value out of range
                // (if leading significant digits are lost)
                throw "22003";
            }                          

            rv = mantissa;           

            if (scale > parsed_scale) {
                int64_t tmp;
                for (int i = 0; i < scale - parsed_scale; i++) {
                    tmp = rv*10;
                    // Check for overflow
                    if (tmp < rv) {
                        // data exception -- numeric value out of range
                        throw "22003";
                    }
                    rv = tmp;
                }
            } else if (scale < parsed_scale) {
                int adjust_scale = parsed_scale - scale;
                for (int i = 0; i < adjust_scale; i++) {
                    rv = rv/10;
                }

                // Do Rounding
                int64_t factor = 1;
                for (int i = 0; i < adjust_scale; i++) {
                    factor *= 10;
                }
                if (mantissa % factor >= factor/2) {
                    // Check if digit will increase/overflow
                    if (rv == SqlExactMax(mantissa_digits - adjust_scale, negative)) {
                        mantissa_digits++;
                        if (mantissa_digits - parsed_scale 
                            > precision - scale) {
                            // SQL2003 Part 2 Section 6.12 General Rule 8.a.ii
                            // data exception -- numeric value out of range
                            // (if leading significant digits are lost)
                            throw "22003";
                        }
                    }
                    rv++;
                }
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

    if (negative) {
        return rv * -1;
    } else {
        return rv;
    }
}

//! SqlStrCastToApprox. Char & VarChar. Ascii only.
//!
//! Cast a string to an approximate (e.g. floating point) numeric.
//!
//! See SQL99 Part 2 Section 5.3 Format &lt;approximate numeric literal&gt; for
//! details on the format of an approximate numeric.
//! Basically nnn.mmm[Exxx].
template <int CodeUnitBytes, int MaxCodeUnitsPerCodePoint>
double
SqlStrCastToApprox(char const * const str,
                   int strLenBytes,
                   int padChar = ' ')
{
    double rv;
    if (MaxCodeUnitsPerCodePoint == 1) {
        if (CodeUnitBytes == 1) {
            // ASCII

            char const *ptr = str;
            char const *end = str + strLenBytes;
            char *endptr;

            // Skip past any leading whitespace. Allows string with
            // arbitrary amounts of leading whitespace.
            while (ptr < end && *ptr == padChar) ptr++;

            int max = end - ptr;
            char tmp[max+1];
            memcpy(tmp, ptr, max);
            tmp[max] = 0;
            rv = strtod(tmp, &endptr);

            if (endptr == tmp) {
                // SQL99 Part 2 Section 6.22 General Rule 7.b.i "22018"
                // data exception -- invalid character value for cast
                throw "22018";
            }

            // verify that trailing characters are all padChar
            ptr += endptr - tmp; // advance past parsed digits
            while (ptr < end) {
                if (*ptr != padChar) {
                    // SQL99 Part 2 Section 6.22 General Rule 7.b.i "22018"
                    // data exception -- invalid character value for cast
                    throw "22018";
                }
                ptr++;
            }

            // Throw exception if overflow
            double dmax = std::numeric_limits<double>::max();
            if (rv > dmax || rv < -dmax) {
                // Overflow
                throw "22003";
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

    return rv;
}

//! SqlStrCastFromExact. Char & VarChar. Ascii only.
//!
//! Cast an exact numeric to a string.
//!
//! This routine may not be compliant with the SQL99 standard.
//!
//! Pad character code points that require more than one code unit are
//! currently unsupported.
template <int CodeUnitBytes, int MaxCodeUnitsPerCodePoint>
int
SqlStrCastFromExact(char* dest,
                    int destStorageBytes,
                    int64_t src,
                    bool fixed,  // e.g. char, else variable (varchar)
                    int padchar = ' ')
{
    int rv;

    if (MaxCodeUnitsPerCodePoint == 1) {
        if (CodeUnitBytes == 1) {
            // ASCII

            // TODO: Check performance of doing snprintf and a memcpy vs
            // TODO: a home-rolled version with % and /10, etc. Both
            // TOOD: require a copy, or some precomputation of string length.
            // TODO: Hard to say which would be faster w/o implementing both.
            // TODO: Note: can't always snprintf directly into dest, due to
            // TODO: null termination wasting a byte.

            // A previous implementation (retained below)
            // optimistically tried to snprintf into the output
            // buffer, and retried if it would have fit, save for the
            // null termination. The logic gets complicated in the
            // face of snprintf implementatins that return -1, where
            // such information is not possible.  Until an
            // optimization pass can be made, always snprintf into a
            // temporary buffer, and memcpy the result back if it
            // would fit.

            char buf[36];      // #%lld should always fit in 21 bytes.

            // TODO: Bug on mingw where int64_t is not handled correctly
            //       by snprintf %lld
            // Windows (MS dll) uses %I64d but that causes warning with gcc -Wall
            rv = snprintf(buf, 35, "%" FMT_INT64, src);

            // snprintf does not return null termination in length
            assert(rv >= 0 && rv <= 35);
            if (rv <= destStorageBytes) {
                memcpy(dest, buf, rv);
            } else {
                // SQL99 Part 2 Section 6.22 General Rule 8.a.iv (fixed
                // length), "22001" data exception -- string data, right
                // truncation

                // SQL99 Part 2 Section 6.22 General Rule 9.a.iii (variable
                // length) "22001" data exception -- string data, right
                // truncation
                throw "22001";
            }
            
            
#ifdef SECOND_ALTERNATIVE_IMPLEMENTATION_DOES_NOT_WORK_ON_CYGWIN
            
            // Older glibc returns -1 from snprintf. logic gets 
            // complicated
            
            rv = snprintf(dest, destStorageBytes, "%" FMT_INT64, src);
            if (rv == destStorageBytes) {
                // Would have fit, except for the null termination. Do
                // over into a temporary buf, copy results back.
                // Dreary performance in this case, which may be more common
                // than random chance would predict. If this is
                // not acceptable, see ALTERNATIVE_IMPLEMENTATION below

                char buf[36];      // should always fit in 21 bytes.
                rv = snprintf(buf, 35, "%" FMT_INT64, src);
                assert(rv == destStorageBytes);
                memcpy(dest, buf, destStorageBytes);
            } else if (rv > destStorageBytes) {
                // SQL99 Part 2 Section 6.22 General Rule 8.a.iv (fixed
                // length), "22001" data exception -- string data, right
                // truncation

                // SQL99 Part 2 Section 6.22 General Rule 9.a.iii (variable
                // length) "22001" data exception -- string data, right
                // truncation
                throw "22001";
            }
#endif

#ifdef ALTERNATIVE_IMPLEMENTATION_UNTESTED_UNPROFILED_AND_PERHAPS_UNHOLY
            // assume any int64_t will fit in 22 bytes:
            // int64_t has 19 digits, plus space for a - sign and null
            // termination is 21 bytes. Add one to round up to 22 bytes.

            // if storage >= 22 bytes, snprintf directly into dest
            // as a first-order optimization. 
            if (destStorageBytes >= 22) {
                rv = snprintf(dest, destStorageBytes, "%" FMT_INT64, src);
                assert(rv <= destStorageBytes); // impossible?
                if (rv > destStorageBytes) {
                    // Just in case 22 byte assumption isn't valid

                    // SQL99 Part 2 Section 6.22 General Rule 8.a.iv (fixed
                    // length) "22001" data exception -- string data, right
                    // truncation

                    // SQL99 Part 2 Section 6.22 General Rule 9.a.iii (variable
                    // length), "22001" data exception -- string data, right
                    // truncation
                    throw "22001";
                }
            } else {
                // If src is somewhat less than max or min, it might
                // be short enough to fit into destStorageBytes anyway.
                // Write to buf to get around annoying null termination
                // issue wasting one byte.

                char buf[24];
                rv = snprintf(buf, destStorageBytes, "%" FMT_INT64, src);
                if (rv > destStorageBytes) {
                    // SQL99 Part 2 Section 6.22 General Rule 8.a.iv (fixed
                    // length) "22001" data exception -- string data, right
                    // truncation

                    // SQL99 Part 2 Section 6.22 General Rule 9.a.iii (variable
                    // length) "22001" data exception -- string data, right
                    // truncation
                    throw "22001";
                }
                memcpy(dest, buf, rv);
            }
#endif
            if (fixed) {
                memset(dest + rv, padchar, destStorageBytes - rv);
                rv = destStorageBytes;
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

    return rv;
}

//! SqlStrCastFromExact. Char & VarChar. Ascii only.
//!
//! Cast an exact numeric with precision and scale to a string.
//!
//! This routine may not be compliant with the SQL99 standard.
//!
//! Pad character code points that require more than one code unit are
//! currently unsupported.
template <int CodeUnitBytes, int MaxCodeUnitsPerCodePoint>
int
SqlStrCastFromExact(char* dest,
                    int destStorageBytes,
                    int64_t src,
                    int precision,
                    int scale,
                    bool fixed,  // e.g. char, else variable (varchar)
                    int padchar = ' ')
{
    int rv;

    if (MaxCodeUnitsPerCodePoint == 1) {
        if (CodeUnitBytes == 1) {
            // ASCII

            // TODO: Check performance of current implementation against 
            // TODO: a version with % and /10, etc.

            if (scale == 0) {
                // Scale is 0, same as normal cast
                rv = SqlStrCastFromExact
                    <CodeUnitBytes, MaxCodeUnitsPerCodePoint>
                    (dest, destStorageBytes, src, fixed, padchar);        
            } else if (scale > 0) {
                // Positive Scale
                int ndigits, decimal, sign = 0;
                char buf[36];      // #%lld should always fit in 21 bytes.
                rv = snprintf(buf, 35, "%" FMT_INT64, abs(src));
                // snprintf does not return null termination in length
                assert(rv >= 0 && rv <= 35);
                
                ndigits = rv;
                if (src < 0) {
                    sign = 1;
                    rv++;
                }
                
                // Figure out where to add decimal point
                decimal = ndigits - scale;
                if (decimal < 0) {
                    // Need to pad with 0s
                    rv += (-decimal) + 1;
                } else {
                    rv += 1;
                }
                
                // Check if there is enough space
                if (rv > destStorageBytes) {
                    // SQL99 Part 2 Section 6.22 General Rule 8.a.iv (fixed
                    // length) "22001" data exception -- string data, right
                    // truncation
                    
                    // SQL99 Part 2 Section 6.22 General Rule 9.a.iii (variable
                    // length) "22001" data exception -- string data, right
                    // truncation
                    throw "22001";
                }
                
                // Copy into destination buffer, placing the '.' appropriately
                if (sign) {
                    dest[0] = '-';
                }
                
                if (decimal < 0) {
                    int pad = -decimal;
                    dest[sign] = '.';
                    memset(dest + sign + 1, '0', pad);
                    memcpy(dest + sign + 1 + pad, buf, ndigits);
                } else {
                    memcpy(dest + sign, buf, decimal);
                    dest[decimal + sign] = '.';
                    memcpy(dest + sign + 1 + decimal, buf + decimal, scale);
                }
                
                if (fixed) {
                    memset(dest + rv, padchar, destStorageBytes - rv);
                    rv = destStorageBytes;
                }        
                
            } else {
                // Negative Scale
                int nzeros = (src != 0)? -scale: 0;
                int len;
                char buf[36];      // #%lld should always fit in 21 bytes.
                rv = snprintf(buf, 35, "%" FMT_INT64, src);
                // snprintf does not return null termination in length
                assert(rv >= 0 && rv <= 35);

                len = rv;

                // Check if there is enough space
                rv += nzeros;
                if (rv > destStorageBytes) {
                    // SQL99 Part 2 Section 6.22 General Rule 8.a.iv (fixed
                    // length) "22001" data exception -- string data, right
                    // truncation
            
                    // SQL99 Part 2 Section 6.22 General Rule 9.a.iii (variable
                    // length) "22001" data exception -- string data, right
                    // truncation
                    throw "22001";
                }

                // Add zeros
                memcpy(dest, buf, len);
                memset(dest + len, '0', nzeros);
                
                if (fixed) {
                    memset(dest + rv, padchar, destStorageBytes - rv);
                    rv = destStorageBytes;
                }        
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

    return rv;
}

//! SqlStrCastFromApprox. Char & VarChar. Ascii only.
//!
//! Cast an approximate (e.g. floating point) numeric to a string.
//!
//! This routine is not fully SQL99 compliant. Deltas are
//! in 6.22 General Rule 8 b i 2 and 9 b i 2. Currently the
//! minimal string is not produced. Instead, the maximum precision
//! (roughly 16 digits) is always produced. Trade brevity for
//! precision for now as compliance is not trivial with printf.
//!
//! Pad character code points that require more than one code unit are
//! currently unsupported.
//!
//! See SQL99 Part 2 Section 5.3 Format &lt;approximate numeric literal&gt; for
//! details on the format of an approximate numeric.
//! Basically nnn.mmmExxx.
//!
//!
//! See SqlStringCastFromExact for an alternative implementation that
//! could be better/cheaper/faster.
template <int CodeUnitBytes, int MaxCodeUnitsPerCodePoint>
int
SqlStrCastFromApprox(char* dest,
                     int destStorageBytes,
                     double src,
                     bool isFloat,
                     bool fixed,  // e.g. char, else variable (varchar)
                     int padchar = ' ')
{
    int rv;

    if (MaxCodeUnitsPerCodePoint == 1) {
        if (CodeUnitBytes == 1) {
            // ASCII
            if (src == 0.0) {
                // 6.22 General Rule 8, case b i 2 and
                // 6.22 General Rule 9, case b i 2
                if (destStorageBytes >= 3) {
                    memcpy(dest, "0E0", 3);
                    rv = 3;
                } else {
                    // SQL99 Part 2 Section 6.22 General Rule 8.b.iii.4 (fixed
                    // length) "22001" data exception - string
                    
                    // SQL99 Part 2 Section 6.22 General Rule 9.b.iii.3
                    // (variable length) "22001" data exception -- string data,
                    // right truncation
                    throw "22001";
                }
            } else {

                // Note: can't always snprintf directly into dest, due to
                // null termination wasting a byte.
                
                //! %E gives [-]d.dddE[+,-]dd format
                //! %.16 gives, roughly, maximum precision for a double on
                //! a x86. This should be parameterized.
                //! TODO: Parameterize precision and format of conversion.

                int max_precision = (isFloat)? 7: 16;
                char buf[36];      // #.16E should always fit in 22 bytes.
                rv = snprintf(buf, 35, "%.*E", max_precision, src);

                // snprintf does not include null termination in length
                assert(rv >= 0 && rv <= 35);

                if (src > std::numeric_limits<double>::max()) {
                    strcpy(buf, "INF");
                    rv = 3;
                } else if (src < -std::numeric_limits<double>::max()) {
                    strcpy(buf, "-INF");
                    rv = 4;
                }
                
                // Trim trailing zeros from mantissa, and initial zeros
                // from exponent
                int buflen = rv;
                int last_nonzero = (src < 0)? 1:0;
                int eindex = last_nonzero + max_precision + 2;
                int eneg = 0;
                int explen = 1;

                if ((buflen > eindex) && buf[eindex] == 'E') {
                    // Normal number with exponent

                    // Round up if needed                    
                    if ((buf[eindex-1] >= '5') && (buf[eindex-1] <= '9')) {
                        buf[eindex-1] = '0';
                        for (int i=eindex-2; i>=last_nonzero; i--) {
                            if (buf[i] == '9') {
                                buf[i] = '0';
                            } else if (buf[i] != '.') {
                                buf[i]++;
                                break;
                            }
                        }

                        // See if initial digit overflowed (very unlikely)
                        if (buf[last_nonzero] == '0') {
                            buf[last_nonzero] = '1';
                            for (int i=eindex-1; i>last_nonzero+2; i--) {
                                buf[i] = buf[i-1];
                            }
                            buf[last_nonzero+2] = '0';

                            // increment exponent
                            int exp;
                            sscanf(buf + eindex + 1, "%d", &exp);
                            sprintf(buf + eindex + 1, "%d", exp + 1);
                            buflen = strlen(buf);
                        }
                    }

                    // Ignore last digit 
                    // only need 16 digits in total, 15 digits after '.'
                    for (int i=eindex-2; i>=0; i--) {
                        if ((buf[i] >= '1') && (buf[i] <= '9')) {
                            last_nonzero = i;
                            break;
                        }                   
                    }
                    eneg = (buf[eindex+1] == '-')? 1:0;
                    for (int i = eindex + 1; i < buflen; i++) {
                        if ((buf[i] >= '1') && (buf[i] <= '9')) {
                            explen = buflen - i;
                            break;
                        }
                    }

                    // final length = mantissa + 'E' + optional '-' + explen
                    rv = last_nonzero+1 + 1 + eneg + explen;
                } else {
                    // Special number (INF, -INF, NaN)
                    rv = buflen;
                }

                if (rv <= destStorageBytes) {
                    if (rv == buflen) {
                        // Copy all
                        memcpy(dest, buf, rv);
                    } else {
                        // Don't copy trailing zeros of mantissa
                        memcpy(dest, buf, last_nonzero+1);
                        rv = last_nonzero+1;
                        dest[rv++] = 'E';
                        if (eneg) {
                            dest[rv++] = '-';
                        } 
                        // Copy exponent
                        memcpy(dest + rv, buf + (buflen - explen), explen);
                        rv += explen;
                    }
                } else {
                    // SQL99 Part 2 Section 6.22 General Rule 8.b.iii.4 (fixed
                    // length) "22001" data exception - string
                    
                    // SQL99 Part 2 Section 6.22 General Rule 9.b.iii.3
                    // (variable length) "22001" data exception -- string data,
                    // right truncation
                    throw "22001";
                }
            }

            if (fixed) {
                memset(dest + rv, padchar, destStorageBytes - rv);
                rv = destStorageBytes;
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

    return rv;
}


//! SqlStrCastToVarChar.  Ascii only.
//!
//! Pad character code points that require more than one code unit are
//! currently unsupported.  Assumes that non-NULL rightTruncWarning
//! points to an int initialized to zero (0).
//!
//! See SQL99 Part 2 Section 6.22 General Rule 8.c for rules on this cast.
template <int CodeUnitBytes, int MaxCodeUnitsPerCodePoint>
int
SqlStrCastToVarChar(char *dest,
                    int destStorageBytes,
                    char *src,
                    int srcLenBytes,
                    int *rightTruncWarning = NULL,
                    int padchar = ' ')
{
    int rv;

    if (MaxCodeUnitsPerCodePoint == 1) {
        if (CodeUnitBytes == 1) {
            // ASCII
            if (srcLenBytes <= destStorageBytes) {
                memcpy(dest, src, srcLenBytes);
                rv = srcLenBytes;

                if (srcLenBytes < destStorageBytes) {
                    memset(dest + srcLenBytes,
                           padchar,
                           destStorageBytes - srcLenBytes);

                    // Do not alter rv.
                }
            } else {
                memcpy(dest, src, destStorageBytes);
                rv = destStorageBytes;

                for(char *trunc = src + destStorageBytes,
                        *end = src + srcLenBytes;
                    trunc != end;
                    trunc++) {
                    if (*trunc != padchar) {
                        // Spec says this is just a warning (see SQL99 Part 2
                        // Section 6.22 General Rule 8.c.ii).  Let the caller
                        // handle it.
                        if (rightTruncWarning != NULL) {
                            *rightTruncWarning = 1;
                        }
                    }
                }
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

    return rv;
}


//! SqlStrCastToChar.  Ascii only.
//!
//! Pad character code points that require more than one code unit are
//! currently unsupported.  Assumes that non-NULL rightTruncWarning
//! points to an int initialized to zero (0).
//!
//! See SQL99 Part 2 Section 6.22 General Rule 9.c for rules on this cast.
template <int CodeUnitBytes, int MaxCodeUnitsPerCodePoint>
int
SqlStrCastToChar(char *dest,
                 int destStorageBytes,
                 char *src,
                 int srcLenBytes,
                 int *rightTruncWarning = NULL,
                 int padchar = ' ')
{
    int rv;

    if (MaxCodeUnitsPerCodePoint == 1) {
        if (CodeUnitBytes == 1) {
            // ASCII
            if (srcLenBytes <= destStorageBytes) {
                memcpy(dest, src, srcLenBytes);
                rv = srcLenBytes;

                if (srcLenBytes < destStorageBytes) {
                    memset(dest + srcLenBytes,
                           padchar,
                           destStorageBytes - srcLenBytes);
                    rv = destStorageBytes;
                }
            } else {
                memcpy(dest, src, destStorageBytes);
                rv = destStorageBytes;

                for(char *trunc = src + destStorageBytes,
                        *end = src + srcLenBytes;
                    trunc != end;
                    trunc++) {
                    if (*trunc != padchar) {
                        // Spec says this is just a warning (see SQL99 Part 2
                        // Section 6.22 General Rule 9.c.ii).  Let the caller
                        // handle it.
                        if (rightTruncWarning != NULL) {
                            *rightTruncWarning = 1;
                        }
                    }
                }
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

    return rv;
}


//! SqlStrCastToBoolean. Char & VarChar. Ascii only.
//!
//! Cast a string to an boolean
//!
//! See SQL2003 Part 2 Section 5.3 Format &lt;boolean literal&gt; and
//! General Rules 10 for details on the format of a boolean.
//! Boolean can be true, false, or unknown (null). 
//! This method only casts from 'true' and 'false'.
template <int CodeUnitBytes, int MaxCodeUnitsPerCodePoint>
bool
SqlStrCastToBoolean(char const * const str,
                    int strLenBytes,
                    int padChar = ' ')
{
    bool rv;
    if (MaxCodeUnitsPerCodePoint == 1) {
        if (CodeUnitBytes == 1) {
            // ASCII
            char const *ptr = str;
            char const *end = str + strLenBytes;

            // Skip past any leading whitespace.
            while (ptr < end && *ptr == padChar) ptr++;

            // Check if true, false, or unknown
            if ((end - ptr) >= 4 && strncasecmp(ptr, "TRUE", 4) == 0) {
                rv = true;
                ptr += 4; // advance past true
            } else if ((end - ptr) >= 5 && strncasecmp(ptr, "FALSE", 5) == 0) {
                rv = false;
                ptr += 5; // advance past false;
            } else {
                // SQL2003 Part 2 Section 6.12 General Rule 20.a.ii "22018"
                // data exception -- invalid character value for cast
                throw "22018";
            }

            // verify that trailing characters are all padChar
            while (ptr < end) {
                if (*ptr != padChar) {
                    // SQL2003 Part 2 Section 6.12 General Rule 20.a.ii "22018"
                    // data exception -- invalid character value for cast
                    throw "22018";
                }
                ptr++;
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

    return rv;
}

//! SqlStrCastFromBoolean Char & VarChar. Ascii only.
//!
//! Cast a boolea to a string.
//!
//! SQL2003 Part 2 Section 6.12 General Rules 10.e and 11.e specifies
//! that false should be cast to 'false' and true to 'true'.
template <int CodeUnitBytes, int MaxCodeUnitsPerCodePoint>
int
SqlStrCastFromBoolean(char* dest,
                      int destStorageBytes,
                      bool src,
                      bool fixed,  // e.g. char, else variable (varchar)
                      int padchar = ' ')
{
    int rv;

    if (MaxCodeUnitsPerCodePoint == 1) {
        if (CodeUnitBytes == 1) {
            // ASCII

            // SQL2003 6.12 General Rule 10, case e i,ii  and
            // SQL2003 6.12 General Rule 11, case e i,ii
            if (src && destStorageBytes >= 4) {
                memcpy(dest, "TRUE", 4);
                rv = 4;
            } else if (!src && destStorageBytes >= 5) {
                memcpy(dest, "FALSE", 5);
                rv = 5;
            } else {
                // SQL2003 Part 2 Section 6.12 General Rule 
                // 10.e.iii (fixed length) and 11.e.iii (variable length)
                // "22018" data exception -- invalid character value for cast
                throw "22018";
            }

            if (fixed) {
                memset(dest + rv, padchar, destStorageBytes - rv);
                rv = destStorageBytes;
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

    return rv;
}


FENNEL_END_NAMESPACE

#endif

// End SqlString.h

