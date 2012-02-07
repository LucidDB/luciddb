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

#ifndef Fennel_ExtDynamicVariable_Included
#define Fennel_ExtDynamicVariable_Included

#include "fennel/calculator/RegisterReference.h"
#include "fennel/calculator/ExtendedInstruction.h"

FENNEL_BEGIN_NAMESPACE

//! dynamicVaraiable. Gets the dynamic variable corresponding to id and casts
//! into a 4 byte integer
void
dynamicVariable(
    RegisterRef<int> *result,
    RegisterRef<int> *id);

class ExtendedInstructionTable;

void
ExtDynamicVariableRegister(ExtendedInstructionTable* eit);


FENNEL_END_NAMESPACE

#endif

// End ExtDynamicVariable.h
