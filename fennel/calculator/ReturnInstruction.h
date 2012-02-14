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
#ifndef Fennel_ReturnInstruction_Included
#define Fennel_ReturnInstruction_Included

#include "fennel/calculator/Instruction.h"

FENNEL_BEGIN_NAMESPACE

class FENNEL_CALCULATOR_EXPORT ReturnInstruction
    : public Instruction
{
public:
    explicit
    ReturnInstruction() {}

    virtual
    ~ReturnInstruction() {}

    virtual void exec(TProgramCounter& pc) const {
        // Force pc past end of program
        pc = TPROGRAMCOUNTERMAX;
    }

    static const char * longName()
    {
        return "Return";
    }

    static const char * shortName()
    {
        return "RETURN";
    }

    static int numArgs()
    {
        return 0;
    }

    void describe(string& out, bool values) const {
        out = longName();
    }

    static InstructionSignature
    signature() {
        return InstructionSignature(shortName());
    }

    static Instruction*
    create(InstructionSignature const & sig)
    {
        assert(sig.size() == numArgs());
        return new ReturnInstruction();
    }
};

//! Add a warning/exception to the message queue.
//!
//! Code is expected to be a valid SQL99 error code, eg, 22011, or a
//! valid extension thereof. When code is NULL, an exception is not
//! raised- becomes a no-op. Note: instruction does not
//! terminate execution.
class FENNEL_CALCULATOR_EXPORT RaiseInstruction
    : public Instruction
{
public:
    explicit
    RaiseInstruction(RegisterRef<char*>* code)
        : mCode(code)
    {}

    virtual
    ~RaiseInstruction() {}

    virtual void exec(TProgramCounter& pc) const {
        pc++;
        if (!mCode->isNull()) {
            SqlStateInfo const *stateInfo =
                SqlState::instance().lookup(mCode->pointer());
            throw CalcMessage(*stateInfo, pc - 1);
        }
    }

    static const char * longName()
    {
        return "Raise";
    }

    static const char * shortName()
    {
        return "RAISE";
    }

    static int numArgs()
    {
        return 1;
    }

    void describe(string& out, bool values) const {
        out = longName();
    }

    static InstructionSignature
    signature(StandardTypeDescriptorOrdinal type) {
        vector<StandardTypeDescriptorOrdinal> v(numArgs(), type);
        return InstructionSignature(shortName(), v);
    }

    static Instruction*
    create(InstructionSignature const & sig)
    {
        assert(sig.size() == numArgs());
        return new
            RaiseInstruction(static_cast<RegisterRef<char*>*> (sig[0]));
    }
private:
    RegisterRef<char*>* mCode;
};

class FENNEL_CALCULATOR_EXPORT ReturnInstructionRegister
    : InstructionRegister
{
public:
    static void
    registerInstructions()
    {
        // Shortcut registration system, as there are neither args nor types.
        StringToCreateFn* instMap = InstructionFactory::getInstructionTable();
        (*instMap)[ReturnInstruction::signature().compute()] =
            &ReturnInstruction::create;

        // Again, shortcut for RAISE, which has simple needs
        InstructionRegister::registerInstance
            < char*, fennel::RaiseInstruction >
            (STANDARD_TYPE_VARCHAR);
    }
};



FENNEL_END_NAMESPACE

#endif

// End ReturnInstruction.h

