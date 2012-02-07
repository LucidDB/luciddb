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

#include "fennel/common/CommonPreamble.h"
#include "fennel/calculator/CalcInit.h"

// to allow calls to InstructionFactory::registerInstructions()
#include "fennel/calculator/CalcAssembler.h"
#include "fennel/calculator/InstructionFactory.h"

FENNEL_BEGIN_NAMESPACE

CalcInit* CalcInit::_instance = NULL;

CalcInit*
CalcInit::instance()
{
    // Warning: Not thread safe
    if (_instance) {
        return _instance;
    }

    _instance = new CalcInit;

    InstructionFactory::registerInstructions();

    ExtStringRegister(InstructionFactory::getExtendedInstructionTable());
    ExtMathRegister(InstructionFactory::getExtendedInstructionTable());
    ExtDateTimeRegister(InstructionFactory::getExtendedInstructionTable());
    ExtRegExpRegister(InstructionFactory::getExtendedInstructionTable());
    ExtCastRegister(InstructionFactory::getExtendedInstructionTable());
    ExtDynamicVariableRegister(
        InstructionFactory::getExtendedInstructionTable());
    ExtWinAggFuncRegister(InstructionFactory::getExtendedInstructionTable());

    // Add new init calls here

    return _instance;
}


FENNEL_END_NAMESPACE

// End CalcInit.cpp
