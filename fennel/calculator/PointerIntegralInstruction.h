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
#ifndef Fennel_PointerIntegralInstruction_Included
#define Fennel_PointerIntegralInstruction_Included

#include "fennel/calculator/PointerInstruction.h"

FENNEL_BEGIN_NAMESPACE


template<typename PTR_TYPE>
class PointerIntegralInstruction : public PointerInstruction
{
public:
    explicit
    PointerIntegralInstruction(
        RegisterRef<PTR_TYPE>* result,
        RegisterRef<PointerSizeT>* op1,
        StandardTypeDescriptorOrdinal pointerType)
        : mResult(result),
          mOp1(op1),
          mPointerType(pointerType)
    {}

    ~PointerIntegralInstruction() {
#ifndef __MSVC__
        // If (0) to reduce performance impact of template type checking
        if (0) {
            PointerInstruction_NotAPointerType<PTR_TYPE>();
        }
#endif
    }

protected:
    RegisterRef<PTR_TYPE>* mResult;
    RegisterRef<PointerSizeT>* mOp1;
    StandardTypeDescriptorOrdinal mPointerType;
};

// TODO: Rename to PointerPutLength to be consistent with RegisterReference
// TODO: accessors.
template <typename PTR_TYPE>
class PointerPutSize : public PointerIntegralInstruction<PTR_TYPE>
{
public:
    explicit
    PointerPutSize(
        RegisterRef<PTR_TYPE>* result,
        RegisterRef<PointerSizeT>* op1,
        StandardTypeDescriptorOrdinal pointerType)
        : PointerIntegralInstruction<PTR_TYPE>(result, op1, pointerType)
    {}

    virtual
    ~PointerPutSize() {}

    virtual void exec(TProgramCounter& pc) const {
        pc++;

        if (PointerIntegralInstruction<PTR_TYPE>::mOp1->isNull()) {
            PointerIntegralInstruction<PTR_TYPE>::mResult->toNull();
            PointerIntegralInstruction<PTR_TYPE>::mResult->length(0);
        } else {
            // get value, put size
            PointerIntegralInstruction<PTR_TYPE>::mResult->length
               (PointerIntegralInstruction<PTR_TYPE>::mOp1->value());
        }
    }

    static const char * longName()
    {
        return "PointerPutSize";
    }

    static const char * shortName()
    {
        return "PUTS";
    }

    static int numArgs()
    {
        return 2;
    }

    void describe(string& out, bool values) const {
        RegisterRef<PTR_TYPE> mOp2; // create invalid regref
        describeHelper(
            out, values, longName(), shortName(),
            PointerIntegralInstruction<PTR_TYPE>::mResult,
            PointerIntegralInstruction<PTR_TYPE>::mOp1, &mOp2);
    }

    static InstructionSignature
    signature(StandardTypeDescriptorOrdinal type) {
        vector<StandardTypeDescriptorOrdinal> v;
        v.push_back(type);
        v.push_back(POINTERSIZET_STANDARD_TYPE);
        return InstructionSignature(shortName(), v);
    }

    static Instruction*
    create(InstructionSignature const & sig)
    {
        assert(sig.size() == numArgs());
        assert((sig[1])->type() == POINTERSIZET_STANDARD_TYPE);
        return new
            PointerPutSize(
                static_cast<RegisterRef<PTR_TYPE>*> (sig[0]),
                static_cast<RegisterRef<PointerSizeT>*> (sig[1]),
                (sig[0])->type());
    }
};

//! Note: There cannot be a PointerIntegralPutStorage() as cbStorage,
//! the maximum size, is always read-only.

class FENNEL_CALCULATOR_EXPORT PointerIntegralInstructionRegister
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
        registerTypes<fennel::PointerPutSize>(t);
        // Note: Cannot have PointerPutSizeMax. See comment above
    }
};


FENNEL_END_NAMESPACE

#endif

// End PointerIntegralInstruction.h

