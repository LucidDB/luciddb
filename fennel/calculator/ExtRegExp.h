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

#ifndef Fennel_ExtRegExp_Included
#define Fennel_ExtRegExp_Included

#include "fennel/calculator/RegisterReference.h"
#include "fennel/calculator/ExtendedInstruction.h"

FENNEL_BEGIN_NAMESPACE

//! StrLike. Ascii. Reuses pattern.
//! Pass a zero length string into escape if not defined
//! Passing a null into escape will result in null, per SQL99.
void
strLikeEscapeA(
    RegisterRef<bool>* result,
    RegisterRef<char*>* matchValue,
    RegisterRef<char*>* pattern,
    RegisterRef<char*>* escape);

//! StrLike. Ascii. Reuses pattern.
//! ESCAPE clause not defined.
void
strLikeA(
    RegisterRef<bool>* result,
    RegisterRef<char*>* matchValue,
    RegisterRef<char*>* pattern);

//! StrSimilar. Ascii. Reuses pattern.
//! Pass a zero length string into escape if not defined
//! Passing a null into escape will result in null, per SQL99 & SQL2003
void
strSimilarEscapeA(
    RegisterRef<bool>* result,
    RegisterRef<char*>* matchValue,
    RegisterRef<char*>* pattern,
    RegisterRef<char*>* escape);

//! StrSimilar. Ascii. Reuses pattern.
//! ESCAPE clause not defined.
void
strSimilarA(
    RegisterRef<bool>* result,
    RegisterRef<char*>* matchValue,
    RegisterRef<char*>* pattern);

class ExtendedInstructionTable;

void
ExtRegExpRegister(ExtendedInstructionTable* eit);


FENNEL_END_NAMESPACE

#endif

// End ExtRegExp.h
