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
#ifndef Fennel_SqlString_Included
#define Fennel_SqlString_Included

#include "fennel/common/CommonPreamble.h"

FENNEL_BEGIN_NAMESPACE

//! Strcat. Ascii. SQL VARCHAR & CHAR. dest = dest || str. Returns new length.
//!
//! If either string is variable width, the result is variable
//! width: per SQL99 6.27 Syntax Rule 3, Case A, item i.
//! If both strings are fixed witdth, the reuslt is fixed width, 
//! item ii.
//!
//! Note that CHAR('1  ') || CHAR('2 ') = CHAR('1  2 ') and 
//! not CHAR('12   ').
//!
//! When called repeatedly to cat multiple strings together (e.g. A || B || C),
//! the final destWidth must be exactly equal to the resulting width.
//! (e.g. of A+B+C) for both VARCHAR & CHAR. Take care that these length
//! semantics are adhered to in the final result, even though intermediate
//! results (say A || B) may not have the correct length.
//!
//! When used with CHARs, set strLen to strWidth. On intermediate results
//! set destLen = return value of previous call. Final result
//! should/must have return value == destWidth.
//
//  TODO: Does not implement an implementation defined max length.
int
SqlStrAsciiCat(char* dest,
                int destWidth,
                int destLen,
                char const * const str,
                int strLen);

//! StrCat. Ascii. SQL VARCHAR & CHAR. dest = str1 || str2.
//! dest = str1 || str2
//!
//! Returns new destLen.
//!
//! This is an optimization for creating a concatenated string from
//! two other strings, eliminating a seperate string copy. The
//! assumption is that this is the common case with concatenation. 
//! Subsequent concatenations may occur with other form.
//!
//! If either string is variable width, the result is variable
//! width: per SQL99 6.27 Syntax Rule 3, Case A, item i.
//! If both strings are fixed witdth, the reuslt is fixed width, 
//! item ii.
//!
//! Note: CHAR('1  ') || CHAR('2 ') is CHAR('1  2 ') and 
//! is not CHAR('12   ').
//!
//! When used with CHARs, ignore return value, and set destLen = destWidth
//
//  TODO: Does not implement an implementation defined max length.
int
SqlStrAsciiCat(char* dest,
                int destWidth,
                char const * const str1,
                int str1Len,
                char const * const str2,
                int str2Len);

//! StrCmp. Ascii. Fixed Width / SQL CHAR.
//!
//! Returns -1, 0, 1
int
SqlStrAsciiCmpF(char const * const str1,
                int str1Width,
                char const * const str2,
                int str2Width,
                char trimchar = ' ');

//! StrCmp. Ascii. VARCHAR.
//!
//! Returns -1, 0, 1
int
SqlStrAsciiCmpV(char const * const str1,
                int str1Len,
                char const * const str2,
                int str2Len);

//! StrLen in bits. Ascii. CHAR/VARCHAR.
//!
//! Parameter str is ignored for ascii strings.
int
SqlStrAsciiLenBit(char const * const str,
                  int strLen);

//! StrLen in characters. Ascii. CHAR/VARCHAR.
//!
//! Parameter str is ignored for ascii strings.
int
SqlStrAsciiLenChar(char const * const str,
                   int strLen);

//! StrLen in octets. Ascii. CHAR/VARCHAR.
//!
//! Parameter str is ignored for ascii strings.
int
SqlStrAsciiLenOct(char const * const str,
                  int strLen);

//! Overlay. Ascii. CHAR/VARCHAR. Returns new length.
//!
//! See SQL99 6.18 Syntax Rule 10. Overlay is defined in terms of
//! Substring an concatenation. If start is < 1 or length < 0, a substring error
//! may be thrown.
//! Result is VARCHAR, as the result of substring is always VARCHAR,
//! and concatenation results in VARCHAR if any of its operands are VARCHAR.
int
SqlStrAsciiOverlay(char* dest,
                   int destWidth,
                   char const * const str,
                   int strLen,
                   char const * const over,
                   int overLen,
                   int start,
                   int length,
                   bool lenSpecified);

//! Position. Ascii. CHAR/VARHCAR. Returns 1-index string position.
//!
//! Returns 0 if not found. Returns 1 if find is zero length. See SQL99 6.17
//! General Rule 2.
int
SqlStrAsciiPos(char const * const str,
               int strWidth,
               char const * const find,
               int findWidth);

//! Substring. Ascii. CHAR/VARCHAR. 
//! Sets dest to start of of substring. Returns length of substring.
//! 
//! Note that subStart is 1-indexed, as per SQL99 spec.
//! All substring parameters are handled as signed, as spec implies that they 
//! could be negative. Some combinations of subStart and subLen may throw an
//! exception.
//! Results in a VARCHAR.
//! See SQL99 6.18, General Rule 3.
int
SqlStrAsciiSubStr(char const ** dest,
                  int destWidth,
                  char const * const str,
                  int strLen,
                  int subStart,
                  int subLen,
                  bool subLenSpecified);

//! toLower. Ascii. CHAR/VARCHAR. Returns length.
int
SqlStrAsciiToLower(char* dest,
                   int destWidth,
                   char const * const src,
                   int srcLen);

//! toUpper. Ascii. CHAR/VARCHAR. Returns length.
int
SqlStrAsciiToUpper(char* dest,
                   int destWidth,
                   char const * const src,
                   int srcLen);

//! Trim padding. Ascii. CHAR/VARCHAR. Returns new length.
//!
//! See SQL99 6.18 General Rule 8.
//! Results in a VARCHAR.
int 
SqlStrAsciiTrim(char* dest,
                int destWidth,
                char const * const str,
                int strLen,
                bool trimLeft,
                bool trimRight,
                char trimchar = ' ');

//! Trim padding by reference. Ascii. CHAR/VARCHAR. Returns new length.
//!
//! See SQL99 6.18 General Rule 8.
//! Results in a VARCHAR.
int 
SqlStrAsciiTrim(char const ** result,
                char const * const str,
                int strLen,
                bool trimLeft,
                bool trimRight,
                char trimchar = ' ');


FENNEL_END_NAMESPACE

#endif

// End SqlString.h

