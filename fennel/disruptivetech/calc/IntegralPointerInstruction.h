/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2004-2005 Disruptive Tech
// Copyright (C) 2005-2005 The Eigenbase Project
//
// This program is free software; you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation; either version 2 of the License, or
// (at your option) any later Eigenbase-approved version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program; if not, write to the Free Software
// Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
*/
#ifndef Fennel_IntegralPointerInstruction_Included
#define Fennel_IntegralPointerInstruction_Included

#include "fennel/disruptivetech/calc/PointerInstruction.h"

FENNEL_BEGIN_NAMESPACE

//! PointerSizeT is the only valid result type defined for IntegralPointerInstruction.


template<typename PTR_TYPE>
class IntegralPointerInstruction : public PointerInstruction
{
public:
    explicit
    IntegralPointerInstruction(RegisterRef<PointerSizeT>* result,
                               RegisterRef<PTR_TYPE>* op1,
                               StandardTypeDescriptorOrdinal pointerType)
        : mResult(result),
          mOp1(op1),
          mPointerType(pointerType)
    { }
    ~IntegralPointerInstruction() { 
        // If (0) to reduce performance impact of template type checking
        if (0) PointerInstruction_NotAPointerType<PTR_TYPE>();
    }

protected:
    RegisterRef<PointerSizeT>* mResult;
    RegisterRef<PTR_TYPE>* mOp1;
    StandardTypeDescriptorOrdinal mPointerType;
};

template <typename PTR_TYPE>
class PointerGetSize : public IntegralPointerInstruction<PTR_TYPE>
{
public: 
    explicit
    PointerGetSize(RegisterRef<PointerSizeT>* result,
                   RegisterRef<PTR_TYPE>* op1, 
                   StandardTypeDescriptorOrdinal pointerType)
        : IntegralPointerInstruction<PTR_TYPE>(result, op1, pointerType)
    { }
    virtual
    ~PointerGetSize() { }

    virtual void exec(TProgramCounter& pc) const { 
        pc++;

        if (mOp1->isNull()) {
            mResult->toNull();
        } else {
            mResult->value(mOp1->length());  // get size, put value
        }
    }

    static const char * longName() { return "PointerGetSize"; }
    static const char * shortName() { return "GETS"; }
    static int numArgs() { return 2; }
    void describe(string& out, bool values) const {
        RegisterRef<PTR_TYPE> mOp2; // create invalid regref
        describeHelper(out, values, longName(), shortName(),
                       mResult, mOp1, &mOp2);
    }

    static InstructionSignature
    signature(StandardTypeDescriptorOrdinal type) {
        return InstructionSignature(shortName(),
                                    regDesc(1, numArgs() - 1, type, 0));
    }

    static Instruction*
    create(InstructionSignature const & sig)
    {
        assert(sig.size() == numArgs());
        assert((sig[0])->type() == POINTERSIZET_STANDARD_TYPE);
        return new
            PointerGetSize(static_cast<RegisterRef<PointerSizeT>*> (sig[0]),
                           static_cast<RegisterRef<PTR_TYPE>*> (sig[1]),
                           (sig[1])->type());
    }
};

template <typename PTR_TYPE>
class PointerGetMaxSize : public IntegralPointerInstruction<PTR_TYPE>
{
public: 
    explicit
    PointerGetMaxSize(RegisterRef<PointerSizeT>* result,
                      RegisterRef<PTR_TYPE>* op1, 
                      StandardTypeDescriptorOrdinal pointerType)
        : IntegralPointerInstruction<PTR_TYPE>(result, op1, pointerType)
    { }
    virtual
    ~PointerGetMaxSize() { }

    virtual void exec(TProgramCounter& pc) const { 
        pc++;

        if (mOp1->isNull()) {
            mResult->toNull();
        } else {
            mResult->value(mOp1->storage());  // get size, put value
        }
    }

    static const char * longName() { return "PointerGetMaxSize"; }
    static const char * shortName() { return "GETMS"; }
    static int numArgs() { return 2; }
    void describe(string& out, bool values) const {
        RegisterRef<PTR_TYPE> mOp2; // create invalid regref
        describeHelper(out, values, longName(), shortName(),
                       mResult, mOp1, &mOp2);
    }

    static InstructionSignature
    signature(StandardTypeDescriptorOrdinal type) {
        return InstructionSignature(shortName(),
                                    regDesc(1, numArgs() - 1, type, 0));
    }

    static Instruction*
    create(InstructionSignature const & sig)
    {
        assert(sig.size() == numArgs());
        assert((sig[0])->type() == POINTERSIZET_STANDARD_TYPE);
        return new
            PointerGetMaxSize(static_cast<RegisterRef<PointerSizeT>*> (sig[0]),
                              static_cast<RegisterRef<PTR_TYPE>*> (sig[1]),
                              (sig[1])->type());
    }
};


class IntegralPointerInstructionRegister : InstructionRegister {

    // TODO: Refactor registerTypes to class InstructionRegister
    template < template <typename> class INSTCLASS2 >
    static void
    registerTypes(vector<StandardTypeDescriptorOrdinal> const &t) {

        for (uint i = 0; i < t.size(); i++) {
            StandardTypeDescriptorOrdinal type = t[i];
            // Type <char> below is a placeholder and is ignored.
            InstructionSignature sig = INSTCLASS2<char>::signature(type);
            switch(type) {
                // Array_Text, below, does not allow assembly programs
                // of to have say, pointer to int16s, but the language
                // does not have pointers defined other than
                // c,vc,b,vb, so this is OK for now.
#define Fennel_InstructionRegisterSwitch_Array 1
#include "fennel/disruptivetech/calc/InstructionRegisterSwitch.h"
            default:
                throw std::logic_error("Default InstructionRegister");
            }
        }
    }

public:
    static void
    registerInstructions() {

        vector<StandardTypeDescriptorOrdinal> t;
        // isArray, below, does not allow assembly programs of to
        // have say, pointer to int16s, but the language does not have
        // pointers defined other than c,vc,b,vb, so this is OK for now.
        t = InstructionSignature::typeVector(StandardTypeDescriptor::isArray);

        // Have to do full fennel:: qualification of template
        // arguments below to prevent template argument 'TMPLT', of
        // this encapsulating class, from perverting NativeAdd into
        // NativeAdd<TMPLT> or something like
        // that. Anyway. Fennel::NativeAdd works just fine.
        registerTypes<fennel::PointerGetSize>(t);
        registerTypes<fennel::PointerGetMaxSize>(t);
    }
};

FENNEL_END_NAMESPACE

#endif

// End IntegralPointerInstruction.h

