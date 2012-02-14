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

#ifndef Fennel_ExtCast_Included
#define Fennel_ExtCast_Included

#include "fennel/calculator/RegisterReference.h"
#include "fennel/calculator/ExtendedInstruction.h"

FENNEL_BEGIN_NAMESPACE

//! castA. Ascii. Char & Varchar
//!
//! Casts an exact numeric to an Ascii string.
//!
//! May throw "22001" data exception - string data, right truncation
void
castExactToStrA(
    RegisterRef<char*>* result,
    RegisterRef<int64_t>* src);

//! castA. Ascii. Char & Varchar
//!
//! Casts an exact numeric with precision and scale to an Ascii string.
//!
//! May throw "22001" data exception - string data, right truncation
void
castExactToStrA(
    RegisterRef<char*>* result,
    RegisterRef<int64_t>* src,
    RegisterRef<int32_t>* precision,
    RegisterRef<int32_t>* scale);

//! castA. Ascii. Char & Varchar
//!
//! Casts an approximate numeric to an Ascii string.
//!
//! May throw "22001" data exception - string data, right truncation
void
castApproxToStrA(
    RegisterRef<char*>* result,
    RegisterRef<double>* src);

//! castA. Ascii. Char & Varchar
//!
//! Casts a string to an exact numeric.
//!
//! May throw "22018" data exception - invalid character value for cast
void
castStrtoExactA(
    RegisterRef<int64_t>* result,
    RegisterRef<char*>* src);

//! castA. Ascii. Char & Varchar
//!
//! Casts a string to an exact numeric with precision and scale.
//!
//! May throw "22018" data exception - invalid character value for cast
//! May throw "22003" data exception - numeric value out of range
void
castStrToExactA(
    RegisterRef<int64_t>* result,
    RegisterRef<char*>* src,
    RegisterRef<int32_t>* precision,
    RegisterRef<int32_t>* scale);

//! castA. Ascii. Char & Varchar
//!
//! Casts a string to an approximate numeric.
//!
//! May throw "22018" data exception - invalid character value for cast
void
castStrToApproxA(
    RegisterRef<double>* result,
    RegisterRef<char*>* src);

//! castA. Ascii. Char & Varchar
//!
//! Casts a boolean to an Ascii string.
//!
//! May throw "22018" data exception - invalid character value for cast
void
castBooleanToStrA(
    RegisterRef<char*>* result,
    RegisterRef<int64_t>* src);


//! castA. Ascii. Char & Varchar
//!
//! Casts a string to an boolean.
//!
//! May throw "22018" data exception - invalid character value for cast
void
castStrtoBooleanA(
    RegisterRef<bool>* result,
    RegisterRef<char*>* src);

//! castA. Ascii. String to Varchar
//!
//! Casts a string to a variable-length string.
//!
//! May throw "22001" string data, right truncation
void
castStrToVarCharA(
    RegisterRef<char*>* result,
    RegisterRef<char*>* src);

//! castA. Ascii. String to Char
//!
//! Casts a string to a fixed-length string.
//!
//! May throw "22001" string data, right truncation
void
castStrToCharA(
    RegisterRef<char*>* result,
    RegisterRef<char*>* src);


class ExtendedInstructionTable;

void
ExtCastRegister(ExtendedInstructionTable* eit);


FENNEL_END_NAMESPACE

#endif

// End ExtCast.h
