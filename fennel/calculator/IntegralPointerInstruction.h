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
#ifndef Fennel_IntegralPointerInstruction_Included
#define Fennel_IntegralPointerInstruction_Included

#include "fennel/calculator/PointerInstruction.h"

FENNEL_BEGIN_NAMESPACE

//! PointerSizeT is the only valid result type defined for
//! IntegralPointerInstruction.


template<typename PTR_TYPE>
class IntegralPointerInstruction
    : public PointerInstruction
{
public:
    explicit
    IntegralPointerInstruction(
        RegisterRef<PointerSizeT>* result,
        RegisterRef<PTR_TYPE>* op1,
        StandardTypeDescriptorOrdinal pointerType)
        : mResult(result),
          mOp1(op1),
          mPointerType(pointerType)
    {}

    ~IntegralPointerInstruction() {
#ifndef __MSVC__
        // If (0) to reduce performance impact of template type checking
        if (0) {
            PointerInstruction_NotAPointerType<PTR_TYPE>();
        }
#endif
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
    PointerGetSize(
        RegisterRef<PointerSizeT>* result,
        RegisterRef<PTR_TYPE>* op1,
        StandardTypeDescriptorOrdinal pointerType)
        : IntegralPointerInstruction<PTR_TYPE>(result, op1, pointerType)
    {}

    virtual
    ~PointerGetSize() {}

    virtual void exec(TProgramCounter& pc) const {
        pc++;

        if (IntegralPointerInstruction<PTR_TYPE>::mOp1->isNull()) {
            IntegralPointerInstruction<PTR_TYPE>::mResult->toNull();
        } else {
            // get size, put value
            IntegralPointerInstruction<PTR_TYPE>::mResult->value(
                IntegralPointerInstruction<PTR_TYPE>::mOp1->length());
        }
    }

    static const char * longName()
    {
        return "PointerGetSize";
    }

    static const char * shortName()
    {
        return "GETS";
    }

    static int numArgs()
    {
        return 2;
    }

    void describe(string& out, bool values) const {
        RegisterRef<PTR_TYPE> mOp2; // create invalid regref
        describeHelper(
            out, values, longName(), shortName(),
            IntegralPointerInstruction<PTR_TYPE>::mResult,
            IntegralPointerInstruction<PTR_TYPE>::mOp1, &mOp2);
    }

    static InstructionSignature
    signature(StandardTypeDescriptorOrdinal type) {
        return InstructionSignature(
            shortName(),
            regDesc(1, numArgs() - 1, type, 0));
    }

    static Instruction*
    create(InstructionSignature const & sig)
    {
        assert(sig.size() == numArgs());
        assert((sig[0])->type() == POINTERSIZET_STANDARD_TYPE);
        return new
            PointerGetSize(
                static_cast<RegisterRef<PointerSizeT>*> (sig[0]),
                static_cast<RegisterRef<PTR_TYPE>*> (sig[1]),
                (sig[1])->type());
    }
};

template <typename PTR_TYPE>
class PointerGetMaxSize : public IntegralPointerInstruction<PTR_TYPE>
{
public:
    explicit
    PointerGetMaxSize(
        RegisterRef<PointerSizeT>* result,
        RegisterRef<PTR_TYPE>* op1,
        StandardTypeDescriptorOrdinal pointerType)
        : IntegralPointerInstruction<PTR_TYPE>(result, op1, pointerType)
    {}

    virtual
    ~PointerGetMaxSize() {}

    virtual void exec(TProgramCounter& pc) const {
        pc++;

        if (IntegralPointerInstruction<PTR_TYPE>::mOp1->isNull()) {
            IntegralPointerInstruction<PTR_TYPE>::mResult->toNull();
        } else {
            // get size, put value
            IntegralPointerInstruction<PTR_TYPE>::mResult->value(
                IntegralPointerInstruction<PTR_TYPE>::mOp1->storage());
        }
    }

    static const char * longName()
    {
        return "PointerGetMaxSize";
    }

    static const char * shortName()
    {
        return "GETMS";
    }

    static int numArgs()
    {
        return 2;
    }

    void describe(string& out, bool values) const {
        RegisterRef<PTR_TYPE> mOp2; // create invalid regref
        describeHelper(
            out, values, longName(), shortName(),
            IntegralPointerInstruction<PTR_TYPE>::mResult,
            IntegralPointerInstruction<PTR_TYPE>::mOp1, &mOp2);
    }

    static InstructionSignature
    signature(StandardTypeDescriptorOrdinal type) {
        return InstructionSignature(
            shortName(),
            regDesc(1, numArgs() - 1, type, 0));
    }

    static Instruction*
    create(InstructionSignature const & sig)
    {
        assert(sig.size() == numArgs());
        assert((sig[0])->type() == POINTERSIZET_STANDARD_TYPE);
        return new
            PointerGetMaxSize(
                static_cast<RegisterRef<PointerSizeT>*> (sig[0]),
                static_cast<RegisterRef<PTR_TYPE>*> (sig[1]),
                (sig[1])->type());
    }
};


class FENNEL_CALCULATOR_EXPORT IntegralPointerInstructionRegister
    : InstructionRegister
{
    // TODO: Refactor registerTypes to class InstructionRegister
    template < template <typename> class INSTCLASS2 >
    static void
    registerTypes(vector<StandardTypeDescriptorOrdinal> const &t) {
        for (uint i = 0; i < t.size(); i++) {
            StandardTypeDescriptorOrdinal type = t[i];
            // Type <char> below is a placeholder and is ignored.
            InstructionSignature sig = INSTCLASS2<char>::signature(type);
            switch (type) {
                // Array_Text, below, does not allow assembly programs
                // of to have say, pointer to int16s, but the language
                // does not have pointers defined other than
                // c,vc,b,vb, so this is OK for now.
#define Fennel_InstructionRegisterSwitch_Array 1
#include "fennel/calculator/InstructionRegisterSwitch.h"
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

