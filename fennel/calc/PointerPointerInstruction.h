/*
// $Id$
// Fennel is a relational database kernel.
// Copyright (C) 2004-2004 Disruptive Technologies, Inc.
//
// This program is free software; you can redistribute it and/or
// modify it under the terms of the GNU Lesser General Public License
// as published by the Free Software Foundation; either version 2.1
// of the License, or (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with this program; if not, write to the Free Software
// Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
//
// PointerPointerInstruction
//
// Instruction->PointerInstruction->PointerPointerInstruction
//
// PointerInstructions that return a Pointer
*/
#ifndef Fennel_PointerPointerInstruction_Included
#define Fennel_PointerPointerInstruction_Included

FENNEL_BEGIN_NAMESPACE

#include "fennel/calc/PointerInstruction.h"


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

    virtual void setCalc(Calculator* calcP) {
        mResult->setCalc(calcP);
        mOp1->setCalc(calcP);
        mOp2->setCalc(calcP);  // note: may be unused if instruction has 1 operand
    }
    
protected:
    RegisterRef<PTR_TYPE>* mResult;
    RegisterRef<PTR_TYPE>* mOp1;
    RegisterRef<OP2T>* mOp2;
    StandardTypeDescriptorOrdinal mPointerType;

};

//! Decreases length by op2, which may be completely invalid. Reset with PointerPutSize.
template <typename PTR_TYPE>
class PointerAdd : public PointerPointerInstruction<PTR_TYPE, PointerOperandT>
{
public: 
    explicit
    PointerAdd(RegisterRef<PTR_TYPE>* result,
               RegisterRef<PTR_TYPE>* op1, 
               RegisterRef<PointerOperandT>* op2,
               StandardTypeDescriptorOrdinal pointerType)
        : PointerPointerInstruction<PTR_TYPE, PointerOperandT>(result, op1, op2, pointerType)
    { }
    virtual
    ~PointerAdd() { }

    virtual void exec(TProgramCounter& pc) const { 
        pc++;
        if (mOp1->isNull() || mOp2->isNull()) {
            mResult->toNull();
            mResult->length(0);
        } else {
            // Educated guess: Length decreases. If incorrect, compiler is
            // responsible for resetting the length correctly with
            // Instruction PointerPutSize
            uint oldLength = mOp1->length();
            uint delta = mOp2->value();
            uint newLength;
            if (oldLength > delta) {
                newLength = oldLength - delta;
            } else {
                newLength = 0;
            }
            mResult->pointer(reinterpret_cast<PTR_TYPE>(mOp1->pointer()) + mOp2->value(),
                             newLength);
        }
    }

    const char* longName() const { return "PointerAdd"; }
    const char* shortName() const { return "ADD"; } 
    void describe(string& out, bool values) const {
        describeHelper(out, values, longName(), shortName(), mResult, mOp1, mOp2);
    }
};

//! Increases length by op2, which may be completely invalid. Reset with PointerPutSize.
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
        : PointerPointerInstruction<PTR_TYPE, PointerOperandT>(result, op1, op2, pointerType)
    { }
    virtual 
    ~PointerSub() { }

    virtual void exec(TProgramCounter& pc) const { 
        pc++;
        if (mOp1->isNull() || mOp2->isNull()) {
            mResult->toNull();
            mResult->length(0);
        } else {
            // Educated guess: Length increases. If incorrect, compiler is 
            // responsible for resetting the length correctly with
            // Instruction PointerPutLength
            uint newLength = mOp1->length() + mOp2->value();
            uint maxLength = mOp1->storage();
            if (newLength > maxLength) newLength = maxLength;
            mResult->pointer(reinterpret_cast<PTR_TYPE>(mOp1->pointer()) - mOp2->value(),
                             newLength);
        }
    }

    const char* longName() const { return "PointerSub"; }
    const char* shortName() const { return "SUB"; }
    void describe(string& out, bool values) const {
        describeHelper(out, values, longName(), shortName(), mResult, mOp1, mOp2);
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
        : PointerPointerInstruction<PTR_TYPE, PTR_TYPE>(result, op1, pointerType)
    { }
    virtual 
    ~PointerMove() { }

    virtual void exec(TProgramCounter& pc) const { 
        pc++;
        if (mOp1->isNull()) {
            mResult->toNull();
            mResult->length(0);
        } else {
            mResult->pointer(mOp1->pointer(), mOp1->length());
        }
    }
    const char* longName() const { return "PointerMove"; }
    const char* shortName() const { return "MOVE"; }
    void describe(string& out, bool values) const {
        describeHelper(out, values, longName(), shortName(), mResult, mOp1, mOp2);
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
        mResult->toNull();
        mResult->length(0);
    }
    const char* longName() const { return "PointerToNull"; }
    const char* shortName() const { return "TONULL"; }
    void describe(string& out, bool values) const {
        describeHelper(out, values, longName(), shortName(), mResult, mOp1, mOp2);
    }
};

FENNEL_END_NAMESPACE

#endif

// End PointerPointerInstruction.h

