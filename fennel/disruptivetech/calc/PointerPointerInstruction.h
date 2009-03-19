/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2004-2007 SQLstream, Inc.
// Copyright (C) 2005-2007 The Eigenbase Project
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
// PointerPointerInstruction
//
// Instruction->PointerInstruction->PointerPointerInstruction
//
// PointerInstructions that return a Pointer
*/
#ifndef Fennel_PointerPointerInstruction_Included
#define Fennel_PointerPointerInstruction_Included

#include "fennel/disruptivetech/calc/PointerInstruction.h"

FENNEL_BEGIN_NAMESPACE

template<typename PTR_TYPE, typename OP2T>
class PointerPointerInstruction : public PointerInstruction
{
public:
    explicit
    PointerPointerInstruction(RegisterRef<PTR_TYPE>* result,
                              StandardTypeDescriptorOrdinal pointerType)
        : mResult(result),
          mOp1(),            // unused
          mOp2(),            // unused
          mPointerType(pointerType)
    {
        assert(StandardTypeDescriptor::isArray(pointerType));
    }
    explicit
    PointerPointerInstruction(RegisterRef<PTR_TYPE>* result,
                              RegisterRef<PTR_TYPE>* op1,
                              StandardTypeDescriptorOrdinal pointerType)
        : mResult(result),
          mOp1(op1),
          mOp2(),            // unused
          mPointerType(pointerType)
    {
        assert(StandardTypeDescriptor::isArray(pointerType));
    }
    explicit
    PointerPointerInstruction(RegisterRef<PTR_TYPE>* result,
                              RegisterRef<PTR_TYPE>* op1,
                              RegisterRef<OP2T>* op2,
                              StandardTypeDescriptorOrdinal pointerType)
        : mResult(result),
          mOp1(op1),
          mOp2(op2),
          mPointerType(pointerType)
    {
        assert(StandardTypeDescriptor::isArray(pointerType));
    }
    ~PointerPointerInstruction() {
        // If (0) to reduce performance impact of template type checking
        if (0) PointerInstruction_NotAPointerType<PTR_TYPE>();
    }

protected:
    RegisterRef<PTR_TYPE>* mResult;
    RegisterRef<PTR_TYPE>* mOp1;
    RegisterRef<OP2T>* mOp2;
    StandardTypeDescriptorOrdinal mPointerType;
};

//! Decreases length by op2, which may be completely invalid.
//! Reset with PointerPutSize.
template <typename PTR_TYPE>
class PointerAdd : public PointerPointerInstruction<PTR_TYPE, PointerOperandT>
{
public:
    explicit
    PointerAdd(RegisterRef<PTR_TYPE>* result,
               RegisterRef<PTR_TYPE>* op1,
               RegisterRef<PointerOperandT>* op2,
               StandardTypeDescriptorOrdinal pointerType)
        : PointerPointerInstruction<PTR_TYPE, PointerOperandT>(result,
                                                               op1,
                                                               op2,
                                                               pointerType)
    { }
    virtual
    ~PointerAdd() { }

    virtual void exec(TProgramCounter& pc) const {
        pc++;
        if (PointerPointerInstruction<PTR_TYPE, PointerOperandT>::mOp1->isNull() ||
            PointerPointerInstruction<PTR_TYPE, PointerOperandT>::mOp2->isNull()) {
            PointerPointerInstruction<PTR_TYPE, PointerOperandT>::mResult->toNull();
            PointerPointerInstruction<PTR_TYPE, PointerOperandT>::mResult->length(0);
        } else {
            // Educated guess: Length decreases. If incorrect, compiler is
            // responsible for resetting the length correctly with
            // Instruction PointerPutSize
            uint oldLength = PointerPointerInstruction<PTR_TYPE, PointerOperandT>::mOp1->length();
            uint delta = PointerPointerInstruction<PTR_TYPE, PointerOperandT>::mOp2->value();
            uint newLength;
            if (oldLength > delta) {
                newLength = oldLength - delta;
            } else {
                newLength = 0;
            }
            PointerPointerInstruction<PTR_TYPE, PointerOperandT>::mResult->
                pointer(reinterpret_cast<PTR_TYPE>
                    (PointerPointerInstruction<PTR_TYPE, PointerOperandT>::mOp1->pointer()) +
                     PointerPointerInstruction<PTR_TYPE, PointerOperandT>::mOp2->value(),
                     newLength);
        }
    }

    static const char* longName() { return "PointerAdd"; }
    static const char* shortName() { return "ADD"; }
    static int numArgs() { return 3; }
    void describe(string& out, bool values) const {
        describeHelper(out, values, longName(), shortName(),
                       PointerPointerInstruction<PTR_TYPE, PointerOperandT>::mResult,
                       PointerPointerInstruction<PTR_TYPE, PointerOperandT>::mOp1,
                       PointerPointerInstruction<PTR_TYPE, PointerOperandT>::mOp2);
    }

    static InstructionSignature
    signature(StandardTypeDescriptorOrdinal type) {
        return InstructionSignature(shortName(),
                                    regDesc(0, numArgs()-1, type, 1));
    }

    static Instruction*
    create(InstructionSignature const & sig)
    {
        assert(sig.size() == numArgs());
        return new PointerAdd(static_cast<RegisterRef<PTR_TYPE>*> (sig[0]),
                              static_cast<RegisterRef<PTR_TYPE>*> (sig[1]),
                              static_cast<RegisterRef<PointerOperandT>*> (sig[2]),
                              (sig[0])->type());
    }
};

//! Increases length by op2, which may be completely invalid.
//! Reset with PointerPutSize.
//!
//! Will only increase length to at most op1->cbStorage length to avoid
//! needless invariant breakage.
template <typename PTR_TYPE>
class PointerSub : public PointerPointerInstruction<PTR_TYPE, PointerOperandT>
{
public:
    explicit
    PointerSub(RegisterRef<PTR_TYPE>* result,
               RegisterRef<PTR_TYPE>* op1,
               RegisterRef<PointerOperandT>* op2,
               StandardTypeDescriptorOrdinal pointerType)
        : PointerPointerInstruction<PTR_TYPE, PointerOperandT>(result,
                                                               op1,
                                                               op2,
                                                               pointerType)
    { }
    virtual
    ~PointerSub() { }

    virtual void exec(TProgramCounter& pc) const {
        pc++;
        if (PointerPointerInstruction<PTR_TYPE, PointerOperandT>::mOp1->isNull() ||
            PointerPointerInstruction<PTR_TYPE, PointerOperandT>::mOp2->isNull()) {
            PointerPointerInstruction<PTR_TYPE, PointerOperandT>::mResult->toNull();
            PointerPointerInstruction<PTR_TYPE, PointerOperandT>::mResult->length(0);
        } else {
            // Educated guess: Length increases. If incorrect, compiler is
            // responsible for resetting the length correctly with
            // Instruction PointerPutLength
            uint newLength =
               PointerPointerInstruction<PTR_TYPE, PointerOperandT>::mOp1->length() +
               PointerPointerInstruction<PTR_TYPE, PointerOperandT>::mOp2->value();
            uint maxLength = PointerPointerInstruction<PTR_TYPE, PointerOperandT>::mOp1->storage();
            if (newLength > maxLength) newLength = maxLength;
            PointerPointerInstruction<PTR_TYPE, PointerOperandT>::mResult->
                pointer(reinterpret_cast<PTR_TYPE>
                   (PointerPointerInstruction<PTR_TYPE, PointerOperandT>::mOp1->pointer()) -
                    PointerPointerInstruction<PTR_TYPE, PointerOperandT>::mOp2->value(),
                    newLength);
        }
    }

    static const char* longName() { return "PointerSub"; }
    static const char* shortName() { return "SUB"; }
    static int numArgs() { return 3; }
    void describe(string& out, bool values) const {
        describeHelper(out, values, longName(), shortName(),
                       PointerPointerInstruction<PTR_TYPE, PointerOperandT>::mResult,
                       PointerPointerInstruction<PTR_TYPE, PointerOperandT>::mOp1,
                       PointerPointerInstruction<PTR_TYPE, PointerOperandT>::mOp2);
    }

    static InstructionSignature
    signature(StandardTypeDescriptorOrdinal type) {
        return InstructionSignature(shortName(),
                                    regDesc(0, numArgs()-1, type, 1));
    }

    static Instruction*
    create(InstructionSignature const & sig)
    {
        assert(sig.size() == numArgs());
        return new PointerSub(static_cast<RegisterRef<PTR_TYPE>*> (sig[0]),
                              static_cast<RegisterRef<PTR_TYPE>*> (sig[1]),
                              static_cast<RegisterRef<PointerOperandT>*> (sig[2]),
                              (sig[0])->type());
    }
};


template <typename PTR_TYPE>
class PointerMove : public PointerPointerInstruction<PTR_TYPE, PTR_TYPE>
{
public:
    explicit
    PointerMove(RegisterRef<PTR_TYPE>* result,
                RegisterRef<PTR_TYPE>* op1,
                StandardTypeDescriptorOrdinal pointerType)
        : PointerPointerInstruction<PTR_TYPE, PTR_TYPE>(result,
                                                        op1,
                                                        pointerType)
    { }
    virtual
    ~PointerMove() { }

    virtual void exec(TProgramCounter& pc) const {
        pc++;
        if (PointerPointerInstruction<PTR_TYPE, PTR_TYPE>::mOp1->isNull()) {
            PointerPointerInstruction<PTR_TYPE, PTR_TYPE>::mResult->toNull();
            PointerPointerInstruction<PTR_TYPE, PTR_TYPE>::mResult->length(0);
        } else {
            PointerPointerInstruction<PTR_TYPE, PTR_TYPE>::mResult->
               pointer(PointerPointerInstruction<PTR_TYPE, PTR_TYPE>::mOp1->pointer(),
                       PointerPointerInstruction<PTR_TYPE, PTR_TYPE>::mOp1->length());
        }
    }
    static const char* longName() { return "PointerMove"; }
    static const char* shortName() { return "MOVE"; }
    static int numArgs() { return 2; }
    void describe(string& out, bool values) const {
        describeHelper(out, values, longName(), shortName(),
                       PointerPointerInstruction<PTR_TYPE, PTR_TYPE>::mResult,
                       PointerPointerInstruction<PTR_TYPE, PTR_TYPE>::mOp1,
                       PointerPointerInstruction<PTR_TYPE, PTR_TYPE>::mOp2);
    }

    static InstructionSignature
    signature(StandardTypeDescriptorOrdinal type) {
        return InstructionSignature(shortName(),
                                    regDesc(0, numArgs(), type, 0));
    }

    static Instruction*
    create(InstructionSignature const & sig)
    {
        assert(sig.size() == numArgs());
        return new PointerMove(static_cast<RegisterRef<PTR_TYPE>*> (sig[0]),
                               static_cast<RegisterRef<PTR_TYPE>*> (sig[1]),
                               (sig[0])->type());
    }
};

template <typename PTR_TYPE>
class PointerRef : public PointerPointerInstruction<PTR_TYPE, PTR_TYPE>
{
public:
    explicit
    PointerRef(RegisterRef<PTR_TYPE>* result,
               RegisterRef<PTR_TYPE>* op1,
               StandardTypeDescriptorOrdinal pointerType)
        : PointerPointerInstruction<PTR_TYPE, PTR_TYPE>(result,
                                                        op1,
                                                        pointerType)
    { }
    virtual
    ~PointerRef() { }

    virtual void exec(TProgramCounter& pc) const {
        pc++;
        PointerPointerInstruction<PTR_TYPE, PTR_TYPE>::mResult->
            refer(PointerPointerInstruction<PTR_TYPE, PTR_TYPE>::mOp1);
    }
    static const char* longName() { return "PointerRef"; }
    static const char* shortName() { return "REF"; }
    static int numArgs() { return 2; }
    void describe(string& out, bool values) const {
        describeHelper(out, values, longName(), shortName(),
                       PointerPointerInstruction<PTR_TYPE, PTR_TYPE>::mResult,
                       PointerPointerInstruction<PTR_TYPE, PTR_TYPE>::mOp1,
                       PointerPointerInstruction<PTR_TYPE, PTR_TYPE>::mOp2);
    }

    static InstructionSignature
    signature(StandardTypeDescriptorOrdinal type) {
        return InstructionSignature(shortName(),
                                    regDesc(0, numArgs(), type, 0));
    }

    static Instruction*
    create(InstructionSignature const & sig)
    {
        assert(sig.size() == numArgs());
        return new PointerRef(static_cast<RegisterRef<PTR_TYPE>*> (sig[0]),
                              static_cast<RegisterRef<PTR_TYPE>*> (sig[1]),
                              (sig[0])->type());
    }
};


template <typename PTR_TYPE>
class PointerToNull : public PointerPointerInstruction<PTR_TYPE, PTR_TYPE>
{
public:
    explicit
    PointerToNull(RegisterRef<PTR_TYPE>* result,
                  StandardTypeDescriptorOrdinal pointerType)
        : PointerPointerInstruction<PTR_TYPE, PTR_TYPE>(result, pointerType)
    { }
    virtual
    ~PointerToNull() { }

    virtual void exec(TProgramCounter& pc) const {
        pc++;
        PointerPointerInstruction<PTR_TYPE, PTR_TYPE>::mResult->toNull();
        PointerPointerInstruction<PTR_TYPE, PTR_TYPE>::mResult->length(0);
    }
    static const char* longName() { return "PointerToNull"; }
    static const char* shortName() { return "TONULL"; }
    static int numArgs() { return 1; }
    void describe(string& out, bool values) const {
        describeHelper(out, values, longName(), shortName(),
                       PointerPointerInstruction<PTR_TYPE, PTR_TYPE>::mResult,
                       PointerPointerInstruction<PTR_TYPE, PTR_TYPE>::mOp1,
                       PointerPointerInstruction<PTR_TYPE, PTR_TYPE>::mOp2);
    }

    static InstructionSignature
    signature(StandardTypeDescriptorOrdinal type) {
        return InstructionSignature(shortName(),
                                    regDesc(0, numArgs(), type, 0));
    }

    static Instruction*
    create(InstructionSignature const & sig)
    {
        assert(sig.size() == numArgs());
        return new PointerToNull(static_cast<RegisterRef<PTR_TYPE>*> (sig[0]),
                                 (sig[0])->type());
    }
};


class PointerPointerInstructionRegister : InstructionRegister {

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
        registerTypes<fennel::PointerAdd>(t);
        registerTypes<fennel::PointerSub>(t);
        registerTypes<fennel::PointerMove>(t);
        registerTypes<fennel::PointerRef>(t);
        registerTypes<fennel::PointerToNull>(t);
    }
};

FENNEL_END_NAMESPACE

#endif

// End PointerPointerInstruction.h

