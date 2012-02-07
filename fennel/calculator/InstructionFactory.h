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

#ifndef Fennel_InstructionFactory_Included
#define Fennel_InstructionFactory_Included

#include "fennel/calculator/InstructionSignature.h"
#include "fennel/calculator/CalcAssemblerException.h"
#include "fennel/calculator/RegisterReference.h"
#include "fennel/tuple/StandardTypeDescriptor.h"

FENNEL_BEGIN_NAMESPACE

class ExtendedInstructionTable;

//! Dynamically create Instruction objects given an InstructionDescription
//! description of the desired Instruction.
class FENNEL_CALCULATOR_EXPORT InstructionFactory
{
public:
    explicit
    InstructionFactory() {}

    // Creates a typical instruction
    static Instruction*
    createInstruction(
        string const &name,
        vector<RegisterReference*> const &operands);

    // Creates a Jump instruction
    static Instruction*
    createInstruction(
        string const &name,
        TProgramCounter pc,
        RegisterReference* operand); // add const?

    // Creates an Extended instruction
    static Instruction*
    createInstruction(
        string const &name,
        string const &function,
        vector<RegisterReference*> const &operands);

    // debugging & tracing
    static string
    signatures();

    static string
    extendedSignatures();

    // Start-of-world setup
    static void
    registerInstructions();

    static StringToCreateFn*
    getInstructionTable()
    {
        return &instructionTable;
    }

    // Extended Instruction hooks
    static ExtendedInstructionTable*
    getExtendedInstructionTable()
    {
        return &extendedTable;
    }

private:
    static Instruction*
    createInstructionHelper(InstructionSignature const & sig);

    //! A map to hold regular instructions
    static StringToCreateFn instructionTable;
    //! A map to hold extended instructions
    static ExtendedInstructionTable extendedTable;
};

FENNEL_END_NAMESPACE

#endif

// End InstructionFactory.h
