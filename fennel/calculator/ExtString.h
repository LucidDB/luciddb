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

#ifndef Fennel_ExtString_Included
#define Fennel_ExtString_Included

#include "fennel/calculator/RegisterReference.h"
#include "fennel/calculator/ExtendedInstruction.h"

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
strCatA2(
    RegisterRef<char*>* result,
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
strCatAF3(
    RegisterRef<char*>* result,
    RegisterRef<char*>* str1,
    RegisterRef<char*>* str2);

//! StrCmp. Ascii.
//! Str1 and str2 may be any combination of VARCHAR and/or CHAR.
//! Returns -1, 0, 1.
void
strCmpA(
    RegisterRef<int32_t>* result,
    RegisterRef<char*>* str1,
    RegisterRef<char*>* str2);

//! StrCmp. Binary (Octal-- comparison is byte-wise)
//! See SQL2003 Part 2 Section 4.3.2. All binary strings can be compared.
//! As an extension to SQL2003, allow inequalities (>,>=, etc.)
//! Follows byte-wise comparison semantics of memcmp().
//! Returns -1, 0, 1.
void
strCmpOct(
    RegisterRef<int32_t>* result,
    RegisterRef<char*>* str1,
    RegisterRef<char*>* str2);

//! StrCpy. Ascii.
//!
//! May throw "22001"
//!
void
strCpyA(
    RegisterRef<char*>* result,
    RegisterRef<char*>* str);

//! StrLen in Bits. Ascii.
void
strLenBitA(
    RegisterRef<int32_t>* result,
    RegisterRef<char*>* str);

//! StrLen in Characters. Ascii.
void
strLenCharA(
    RegisterRef<int32_t>* result,
    RegisterRef<char*>* str);

//! StrLen in Octets. Ascii.
void
strLenOctA(
    RegisterRef<int32_t>* result,
    RegisterRef<char*>* str);

//! Overlay. Length unspecified -- to end. Ascii.
//!
//! May throw "22001" or "22011"
//!
void
strOverlayA4(
    RegisterRef<char*>* result,
    RegisterRef<char*>* str,
    RegisterRef<char*>* overlay,
    RegisterRef<int32_t>* start);

//! Overlay. Length specified. Ascii.
void
strOverlayA5(
    RegisterRef<char*>* result,
    RegisterRef<char*>* str,
    RegisterRef<char*>* overlay,
    RegisterRef<int32_t>* start,
    RegisterRef<int32_t>* len);


//! Position of find string in str string. Ascii.
void
strPosA(
    RegisterRef<int32_t>* result,
    RegisterRef<char*>* str,
    RegisterRef<char*>* find);

//! SubString. By reference. Length not specified -- to end. Ascii.
//!
//! May throw "22001" or "22011"
//!
void
strSubStringA3(
    RegisterRef<char*>* result,
    RegisterRef<char*>* str,
    RegisterRef<int32_t>* start);

//! SubString. By Reference. Length specified. Ascii.
void
strSubStringA4(
    RegisterRef<char*>* result,
    RegisterRef<char*>* str,
    RegisterRef<int32_t>* start,
    RegisterRef<int32_t>* len);

//! ToLower. Ascii.
//!
//! May throw "22001".
//!
void
strToLowerA(
    RegisterRef<char*>* result,
    RegisterRef<char*>* str);


//! ToUpper. Ascii.
//!
//! May throw "22001".
//!
void
strToUpperA(
    RegisterRef<char*>* result,
    RegisterRef<char*>* str);


//! Trim. By Reference. Ascii.
//!
//! May throw "22001".
//!
void
strTrimA(
    RegisterRef<char*>* result,
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
