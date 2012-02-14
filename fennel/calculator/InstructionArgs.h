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
#ifndef Fennel_InstructionArgs_Included
#define Fennel_InstructionArgs_Included

#include <string>
#include "fennel/calculator/Calculator.h"
#include "fennel/calculator/RegisterReference.h"
#include "fennel/tuple/StandardTypeDescriptor.h"

FENNEL_BEGIN_NAMESPACE

// InstructionArgs
// A class that can represent all possible arguments to
// an Instruction constructor.
class FENNEL_CALCULATOR_EXPORT InstructionArgs
{
public:
    explicit
    InstructionArgs(const vector<RegisterReference*> o)
        : operands(o),
          pcSet(false)
    {
    }

    explicit
    InstructionArgs(
        const vector<RegisterReference*> o,
        TProgramCounter p)
        : operands(o),
          pc(p),
          pcSet(true)
    {
    }

    const TProgramCounter
    getPC()
    {
        assert(pcSet);
        return pc;
    }

    const vector<RegisterReference*>&
    getOperands()
    {
        return operands;
    }

    const RegisterReference*
    operator[] (int i)
    {
        return operands[i];
    }

private:
    vector<RegisterReference*> operands;
    TProgramCounter pc;
    bool pcSet;
};




FENNEL_END_NAMESPACE

#endif

// End InstructionArgs.h

