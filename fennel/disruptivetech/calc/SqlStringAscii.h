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
// An ascii string library that adheres to the SQL99 standard definitions
*/
#ifndef Fennel_SqlStringAscii_Included
#define Fennel_SqlStringAscii_Included

FENNEL_BEGIN_NAMESPACE

//! Strcat. Ascii. SQL VARCHAR & CHAR. dest = dest || str. Returns new length in bytes.
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
SqlStrCat_Ascii(char* dest,
                int destStorageBytes,
                int destLenBytes,
                char const * const str,
                int strLenBytes);

//! StrCat. Ascii. SQL VARCHAR & CHAR. dest = str1 || str2.
//! dest = str1 || str2
//!
//! Returns new length in bytes.
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
SqlStrCat_Ascii(char* dest,
                int destStorageBytes,
                char const * const str1,
                int str1LenBytes,
                char const * const str2,
                int str2LenBytes);

//! StrCmp. Ascii. Fixed Width / SQL CHAR.
//!
//! Returns -1, 0, 1.
int
SqlStrCmp_Ascii_Fix(char const * const str1,
                    int str1LenBytes,
                    char const * const str2,
                    int str2LenBytes,
                    char trimchar = ' ');

//! StrCmp. Ascii. Variable Width / VARCHAR.
//!
//! Returns -1, 0, 1
int
SqlStrCmp_Ascii_Var(char const * const str1,
                    int str1LenBytes,
                    char const * const str2,
                    int str2LenBytes);

//! StrLen in bits. Ascii. CHAR/VARCHAR.
//!
//! Parameter str is ignored for ascii strings.
int
SqlStrLenBit_Ascii(char const * const str,
                   int strLenBytes);

//! StrLen in characters. Ascii. CHAR/VARCHAR.
//!
//! Parameter str is ignored for ascii strings.
int
SqlStrLenChar_Ascii(char const * const str,
                    int strLenBytes);

//! StrLen in octets. Ascii. CHAR/VARCHAR.
//!
//! Parameter str is ignored for ascii strings.
int
SqlStrLenOct_Ascii(char const * const str,
                   int strLenBytes);

//! Overlay. Ascii. CHAR/VARCHAR. Returns new length in bytes
//!
//! See SQL99 Part 2 Section 6.18 Syntax Rule 10. Overlay is defined in terms of
//! Substring an concatenation. If start is < 1 or length < 0, a substring error
//! may be thrown.
//! Result is VARCHAR, as the result of substring is always VARCHAR,
//! and concatenation results in VARCHAR if any of its operands are VARCHAR.
//! startChar is 1-indexed, as per SQL standard.
int
SqlStrOverlay_Ascii(char* dest,
                    int destStorageBytes,
                    char const * const str,
                    int strLenBytes,
                    char const * const over,
                    int overLenBytes,
                    int startChar,
                    int lengthChar,
                    int lenSpecified);

//! Position. Ascii. CHAR/VARHCAR. Returns 1-index string position.
//!
//! Returns 0 if not found. Returns 1 if find is zero length. See
//! SQL99 Part 2 Section 6.17 General Rule 2.
int
SqlStrPos_Ascii(char const * const str,
                int strLenBytes,
                char const * const find,
                int findLenBytes);

//! Substring by reference. Ascii. Returns VARCHAR. Accepts CHAR/VARCHAR. 
//! Sets dest to start of of substring. Returns length of substring.
//! 
//! Note that subStart is 1-indexed, as per SQL99 spec.
//! All substring parameters are handled as signed, as spec implies that they 
//! could be negative. Some combinations of subStart and subLenBytes may throw an
//! exception.
//! Results in a VARCHAR.
//! See SQL99 Part 2 Section 6.18 General Rule 3.
//! subStartChar is 1-indexed.
int
SqlStrSubStr_Ascii(char const ** dest,
                   int destStorageBytes,
                   char const * const str,
                   int strLenBytes,
                   int subStartChar,
                   int subLenChar,
                   int subLenBytesSpecified);

//! toLower. Ascii. CHAR/VARCHAR. Returns length.
int
SqlStrToLower_Ascii(char* dest,
                    int destStorageBytes,
                    char const * const src,
                    int srcLenBytes);

//! toUpper. Ascii. CHAR/VARCHAR. Returns length.
int
SqlStrToUpper_Ascii(char* dest,
                    int destStorageBytes,
                    char const * const src,
                    int srcLenBytes);

//! Trim padding. Ascii. CHAR/VARCHAR. Returns new length.
//!
//! See SQL99 Part 2 Section 6.18 General Rule 8.
//! Results in a VARCHAR.
int 
SqlStrTrim_Ascii(char* dest,
                 int destStorageBytes,
                 char const * const str,
                 int strLenBytes,
                 int trimLeft,
                 int trimRight,
                 char trimchar = ' ');

//! Trim padding by reference. Ascii. CHAR/VARCHAR. Returns new length.
//!
//! See SQL99 Part 2 Section 6.18 General Rule 8.
//! Results in a VARCHAR.
//! Note: Does not check that result has enough capacity to contain
//! substring as this is irrelevant. If a program depends on the size
//! of result not changing, and this instruction enforcing that
//! invariant -- probably a bad practice anyway -- trouble could result.
int 
SqlStrTrim_Ascii(char const ** result,
                 char const * const str,
                 int strLenBytes,
                 int trimLeft,
                 int trimRight,
                 char trimchar = ' ');


FENNEL_END_NAMESPACE

#endif

// End SqlStringAscii.h

