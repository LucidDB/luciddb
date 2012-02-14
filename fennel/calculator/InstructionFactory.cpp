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
#include "fennel/calculator/InstructionFactory.h"
#include "fennel/calculator/InstructionCommon.h"

FENNEL_BEGIN_CPPFILE("$Id$");

StringToCreateFn InstructionFactory::instructionTable;
ExtendedInstructionTable InstructionFactory::extendedTable;

// Create a typical instruction
Instruction*
InstructionFactory::createInstruction(
    string const & name,
    vector<RegisterReference*> const &operands)
{
    InstructionSignature signature(name, operands);
    return createInstructionHelper(signature);
}

// Create a Jump instruction
Instruction*
InstructionFactory::createInstruction(
    string const & name,
    TProgramCounter pc,
    RegisterReference* operand)
{
    vector<RegisterReference*> v;
    if (operand) {
        v.push_back(operand);
    }

    InstructionSignature signature(name, pc, v);
    return createInstructionHelper(signature);
}

Instruction*
InstructionFactory::createInstructionHelper(InstructionSignature const &sig)
{
    InstructionCreateFunction createFn = instructionTable[sig.compute()];

    if (createFn) {
        return createFn(sig);
    } else {
        throw FennelExcn(sig.compute() + " is not a registered instruction");
    }
}

// Create an Extended instruction
Instruction*
InstructionFactory::createInstruction(
    string const & name,
    string const & function,
    vector<RegisterReference*> const &operands)
{
    InstructionSignature signature(function, operands);
    ExtendedInstructionDef* instDef = extendedTable[signature.compute()];
    if (instDef == NULL) {
        return NULL;
    }
    return instDef->createInstruction(operands);
}

string
InstructionFactory::signatures()
{
    ostringstream s("");

    StringToCreateFnIter i = instructionTable.begin();
    StringToCreateFnIter end = instructionTable.end();

    while (i != end) {
        s << (*i).first << endl;
        i++;
    }
    return s.str();
}

string
InstructionFactory::extendedSignatures()
{
    return extendedTable.signatures();
}

void
InstructionFactory::registerInstructions()
{
    BoolInstructionRegister::registerInstructions();
    BoolNativeInstructionRegister::registerInstructions();
    BoolPointerInstructionRegister::registerInstructions();
    CastInstructionRegister::registerInstructions();
    IntegralNativeInstructionRegister::registerInstructions();
    IntegralPointerInstructionRegister::registerInstructions();
    JumpInstructionRegister::registerInstructions();
    NativeNativeInstructionRegister::registerInstructions();
    PointerIntegralInstructionRegister::registerInstructions();
    PointerPointerInstructionRegister::registerInstructions();
    ReturnInstructionRegister::registerInstructions();
}

FENNEL_END_CPPFILE("$Id$");

// End InstructionFactory.cpp
