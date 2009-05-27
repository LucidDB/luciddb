/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005-2009 The Eigenbase Project
// Copyright (C) 2004-2009 SQLstream, Inc.
// Copyright (C) 2009-2009 LucidEra, Inc.
//
// This program is free software; you can redistribute it and/or modify it
// under the terms of the GNU General Public License as published by the Free
// Software Foundation; either version 2 of the License, or (at your option)
// any later version approved by The Eigenbase Project.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program; if not, write to the Free Software
// Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
//
// ReturnInstruction
//
// Instruction->ReturnInstruction
//
// Template for all native types
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
    RaiseInstruction(RegisterRef<char*>* code) :
        mCode(code)
    {}

    virtual
    ~RaiseInstruction() {}

    virtual void exec(TProgramCounter& pc) const {
        pc++;
        if (!mCode->isNull()) {
            throw CalcMessage(mCode->pointer(), pc - 1);
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
    : InstructionRegister {
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

