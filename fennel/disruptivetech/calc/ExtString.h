/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2004-2007 SQLstream, Inc.
// Copyright (C) 2005-2007 The Eigenbase Project
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
*/

#ifndef Fennel_ExtString_Included
#define Fennel_ExtString_Included

#include "fennel/disruptivetech/calc/RegisterReference.h"
#include "fennel/disruptivetech/calc/ExtendedInstruction.h"

FENNEL_BEGIN_NAMESPACE

//! Strcat. Ascii. dest = dest || str.
//!
//! Sets cbData to length for char as well as varchar.
//!
//! May throw "22001"
//!
//! If calling with fixed: Must call StrCatA3() first to have
//! length set correctly. Only then subsequent calls to strCatA2() are
//! possible.  If concatenating multiple strings, strCatA2 will honor
//! the intermediate length.
//! After final call to strCatA2(), length should equal
//! width, to maintain fixed width string length == width.
//! Behavior may be undefined if, after Calculator exits, length != width.
void
strCatA2(RegisterRef<char*>* result,
         RegisterRef<char*>* str1);

//! Strcat. Ascii. dest = str1 || str2.
//!
//! May throw "22001"
//
//! Sets cbData to length for char as well as varchar.
//! After final call to strCatA3(), length should equal
//! width, to maintain fixed width string length == width.
//! Behavior may be undefined if, after Calculator exits, length != width.
//! If concatenating multiple strings, strCatA2 will honor
//! the intermediate length set by strCatAF3()
void
strCatAF3(RegisterRef<char*>* result,
          RegisterRef<char*>* str1,
          RegisterRef<char*>* str2);

//! StrCmp. Ascii.
//! Str1 and str2 may be any combination of VARCHAR and/or CHAR.
//! Returns -1, 0, 1.
void
strCmpA(RegisterRef<int32_t>* result,
        RegisterRef<char*>* str1,
        RegisterRef<char*>* str2);

//! StrCmp. Binary (Octal-- comparison is byte-wise)
//! See SQL2003 Part 2 Section 4.3.2. All binary strings can be compared.
//! As an extension to SQL2003, allow inequalities (>,>=, etc.)
//! Follows byte-wise comparison semantics of memcmp().
//! Returns -1, 0, 1.
void
strCmpOct(RegisterRef<int32_t>* result,
          RegisterRef<char*>* str1,
          RegisterRef<char*>* str2);

//! StrCpy. Ascii.
//!
//! May throw "22001"
//!
void
strCpyA(RegisterRef<char*>* result,
        RegisterRef<char*>* str);

//! StrLen in Bits. Ascii.
void
strLenBitA(RegisterRef<int32_t>* result,
           RegisterRef<char*>* str);

//! StrLen in Characters. Ascii.
void
strLenCharA(RegisterRef<int32_t>* result,
            RegisterRef<char*>* str);

//! StrLen in Octets. Ascii.
void
strLenOctA(RegisterRef<int32_t>* result,
           RegisterRef<char*>* str);

//! Overlay. Length unspecified -- to end. Ascii.
//!
//! May throw "22001" or "22011"
//!
void
strOverlayA4(RegisterRef<char*>* result,
             RegisterRef<char*>* str,
             RegisterRef<char*>* overlay,
             RegisterRef<int32_t>* start);

//! Overlay. Length specified. Ascii.
void
strOverlayA5(RegisterRef<char*>* result,
             RegisterRef<char*>* str,
             RegisterRef<char*>* overlay,
             RegisterRef<int32_t>* start,
             RegisterRef<int32_t>* len);


//! Position of find string in str string. Ascii.
void
strPosA(RegisterRef<int32_t>* result,
        RegisterRef<char*>* str,
        RegisterRef<char*>* find);

//! SubString. By reference. Length not specified -- to end. Ascii.
//!
//! May throw "22001" or "22011"
//!
void
strSubStringA3(RegisterRef<char*>* result,
               RegisterRef<char*>* str,
               RegisterRef<int32_t>* start);

//! SubString. By Reference. Length specified. Ascii.
void
strSubStringA4(RegisterRef<char*>* result,
               RegisterRef<char*>* str,
               RegisterRef<int32_t>* start,
               RegisterRef<int32_t>* len);

//! ToLower. Ascii.
//!
//! May throw "22001".
//!
void
strToLowerA(RegisterRef<char*>* result,
            RegisterRef<char*>* str);


//! ToUpper. Ascii.
//!
//! May throw "22001".
//!
void
strToUpperA(RegisterRef<char*>* result,
            RegisterRef<char*>* str);


//! Trim. By Reference. Ascii.
//!
//! May throw "22001".
//!
void
strTrimA(RegisterRef<char*>* result,
         RegisterRef<char*>* str,
         RegisterRef<char*>* trimchar,
         RegisterRef<int32_t>* trimLeft,
         RegisterRef<int32_t>* trimRight);

class ExtendedInstructionTable;

void
ExtStringRegister(ExtendedInstructionTable* eit);


FENNEL_END_NAMESPACE

#endif

// End ExtString.h
