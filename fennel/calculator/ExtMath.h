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

#ifndef Fennel_ExtMath_Included
#define Fennel_ExtMath_Included

#include "fennel/calculator/RegisterReference.h"
#include "fennel/calculator/ExtendedInstruction.h"

FENNEL_BEGIN_NAMESPACE

//! mathLn. Calculates the natural logarithm
void
mathLn(
    RegisterRef<double> *result,
    RegisterRef<double> *x);

void
mathLn(
    RegisterRef<double> *result,
    RegisterRef<long long> *x);

//! mathLog10. Calculates the base-ten logarithm
void
mathLog10(
    RegisterRef<double> *result,
    RegisterRef<double> *x);

//! mathAbs. Returns the absolute value.
void
mathAbs(
    RegisterRef<double>* result,
    RegisterRef<double>* x);

//! mathAbs. Returns the absolute value.
void
mathAbs(
    RegisterRef<long long>* result,
    RegisterRef<long long>* x);

//! mathPow. Calculates x^y.
//!
//! Throws an error and sets the result to null if x<0 and y is not an integer
//! value
void
mathPow(
    RegisterRef<double>* result,
    RegisterRef<double>* x,
    RegisterRef<double>* y);


class ExtendedInstructionTable;

void
ExtMathRegister(ExtendedInstructionTable* eit);


FENNEL_END_NAMESPACE

#endif

// End ExtMath.h
