/*
// $Id$
// Fennel is a relational database kernel.
// Copyright (C) 2004-2004 Disruptive Tech
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
// NativeNativeInstruction
//
// Instruction->NativeInstruction->NativeNativeInstruction
//
// NativeInstructions that return a Native
*/
#ifndef Fennel_NativeNativeInstruction_Included
#define Fennel_NativeNativeInstruction_Included

FENNEL_BEGIN_NAMESPACE

#include "fennel/calc/NativeInstruction.h"

template<typename TMPLT>
class NativeNativeInstruction : public NativeInstruction<TMPLT>
{
public:
    explicit
    NativeNativeInstruction(RegisterRef<TMPLT>* result,
                            StandardTypeDescriptorOrdinal nativeType)
        : NativeInstruction<TMPLT>(nativeType),
          mResult(result)
    { }
    explicit
    NativeNativeInstruction(RegisterRef<TMPLT>* result,
                            RegisterRef<TMPLT>* op1,
                            StandardTypeDescriptorOrdinal nativeType)
        : NativeInstruction<TMPLT>(op1, nativeType),
          mResult(result)
    { }
    explicit
    NativeNativeInstruction(RegisterRef<TMPLT>* result,
                            RegisterRef<TMPLT>* op1,
                            RegisterRef<TMPLT>* op2, 
                            StandardTypeDescriptorOrdinal nativeType)
        : NativeInstruction<TMPLT>(op1, op2, nativeType),
          mResult(result)
    { }
    virtual
    ~NativeNativeInstruction() { }

protected:
    RegisterRef<TMPLT>* mResult;
};

template <typename TMPLT>
class NativeAdd : public NativeNativeInstruction<TMPLT>
{
public: 
    explicit
    NativeAdd(RegisterRef<TMPLT>* result,
              RegisterRef<TMPLT>* op1, 
              RegisterRef<TMPLT>* op2,
              StandardTypeDescriptorOrdinal nativeType)
        : NativeNativeInstruction<TMPLT>(result, op1, op2, nativeType)
    { }
    virtual
    ~NativeAdd() { }

    virtual void exec(TProgramCounter& pc) const { 
        pc++;
        if (mOp1->isNull() || mOp2->isNull()) {
            mResult->toNull();
        } else {
            mResult->value(mOp1->value() + mOp2->value());
        }
    }

    const char* longName() const { return "NativeAdd"; }
    const char* shortName() const { return "ADD"; } 
    void describe(string& out, bool values) const {
        describeHelper(out, values, longName(), shortName(), mResult, mOp1, mOp2);
    }
};

template <typename TMPLT>
class NativeSub : public NativeNativeInstruction<TMPLT>
{
public: 
    explicit
    NativeSub(RegisterRef<TMPLT>* result,
              RegisterRef<TMPLT>* op1, 
              RegisterRef<TMPLT>* op2,
              StandardTypeDescriptorOrdinal nativeType)
        : NativeNativeInstruction<TMPLT>(result, op1, op2, nativeType)
    { }
    virtual
    ~NativeSub() { }

    virtual void exec(TProgramCounter& pc) const { 
        pc++;
        if (mOp1->isNull() || mOp2->isNull()) {
            mResult->toNull();
        } else {
            mResult->value(mOp1->value() - mOp2->value());
        }
    }

    const char* longName() const { return "NativeSub"; }
    const char* shortName() const { return "SUB"; }
    void describe(string& out, bool values) const {
        describeHelper(out, values, longName(), shortName(), mResult, mOp1, mOp2);
    }
};

template <typename TMPLT>
class NativeMul : public NativeNativeInstruction<TMPLT>
{
public: 
    explicit
    NativeMul(RegisterRef<TMPLT>* result,
              RegisterRef<TMPLT>* op1,
              RegisterRef<TMPLT>* op2,
              StandardTypeDescriptorOrdinal nativeType)
        : NativeNativeInstruction<TMPLT>(result, op1, op2, nativeType)
    { }
    virtual
    ~NativeMul() { }

    virtual void exec(TProgramCounter& pc) const { 
        pc++;
        if (mOp1->isNull() || mOp2->isNull()) {
            mResult->toNull();
        } else {
            mResult->value(mOp1->value() * mOp2->value());
        }
    }

    const char* longName() const { return "NativeMul"; }
    const char* shortName() const { return "MUL"; }
    void describe(string& out, bool values) const {
        describeHelper(out, values, longName(), shortName(), mResult, mOp1, mOp2);
    }
};

template <typename TMPLT>
class NativeDiv : public NativeNativeInstruction<TMPLT>
{
public: 
    explicit
    NativeDiv(RegisterRef<TMPLT>* result,
              RegisterRef<TMPLT>* op1, 
              RegisterRef<TMPLT>* op2,
              StandardTypeDescriptorOrdinal nativeType)
        : NativeNativeInstruction<TMPLT>(result, op1, op2, nativeType)
    { }

    virtual
    ~NativeDiv() { }

    virtual void exec(TProgramCounter& pc) const { 
        pc++;
        // Nulls propagate as per custom: Joe Celko's SQL For Smarties pg55
        // Could not find in SQL99 spec.
        if (mOp1->isNull() || mOp2->isNull()) {
            mResult->toNull();
        } else {
            TMPLT o2 = mOp2->value(); // encourage into register
            if (o2 == 0) {
                mResult->toNull();
                // SQL99 22.1 SQLState dataexception class 22, division by zero subclass 012
                throw CalcMessage("22012", pc - 1); 
            }
            mResult->value(mOp1->value() / o2);
        }
    }

    const char* longName() const { return "NativeDiv"; } 
    const char* shortName() const { return "DIV"; } 
    void describe(string& out, bool values) const {
        describeHelper(out, values, longName(), shortName(), mResult, mOp1, mOp2);
    }
};

template <typename TMPLT>
class NativeNeg : public NativeNativeInstruction<TMPLT>
{
public: 
    explicit
    NativeNeg(RegisterRef<TMPLT>* result,
              RegisterRef<TMPLT>* op1,
              StandardTypeDescriptorOrdinal nativeType)
        : NativeNativeInstruction<TMPLT>(result, op1, nativeType)
    { }
    virtual
    ~NativeNeg() { }

    virtual void exec(TProgramCounter& pc) const { 
        pc++;
        if (mOp1->isNull()) {
            mResult->toNull();
        } else {
            mResult->value(mOp1->value() * -1);
        }
    }
    const char* longName() const { return "NativeNeg"; }
    const char* shortName() const { return "NEG"; }
    void describe(string& out, bool values) const {
        describeHelper(out, values, longName(), shortName(), mResult, mOp1, mOp2);
    }
};

template <typename TMPLT>
class NativeMove : public NativeNativeInstruction<TMPLT>
{
public: 
    explicit
    NativeMove(RegisterRef<TMPLT>* result,
               RegisterRef<TMPLT>* op1,
               StandardTypeDescriptorOrdinal nativeType)
        : NativeNativeInstruction<TMPLT>(result, op1, nativeType)
    { }
    virtual
    ~NativeMove() { }

    virtual void exec(TProgramCounter& pc) const { 
        pc++;
        if (mOp1->isNull()) {
            mResult->toNull();
        } else {
            mResult->value(mOp1->value());
        }
    }
    const char* longName() const { return "NativeMove"; }
    const char* shortName() const { return "MOVE"; }
    void describe(string& out, bool values) const {
        describeHelper(out, values, longName(), shortName(), mResult, mOp1, mOp2);
    }
};

template <typename TMPLT>
class NativeRef : public NativeNativeInstruction<TMPLT>
{
public: 
    explicit
    NativeRef(RegisterRef<TMPLT>* result,
              RegisterRef<TMPLT>* op1,
              StandardTypeDescriptorOrdinal nativeType)
        : NativeNativeInstruction<TMPLT>(result, op1, nativeType)
    { }
    virtual
    ~NativeRef() { }

    virtual void exec(TProgramCounter& pc) const { 
        pc++;
        mResult->refer(mOp1);
    }
    const char* longName() const { return "NativeRef"; }
    const char* shortName() const { return "REF"; }
    void describe(string& out, bool values) const {
        describeHelper(out, values, longName(), shortName(), mResult, mOp1, mOp2);
    }
};

template <typename TMPLT>
class NativeToNull : public NativeNativeInstruction<TMPLT>
{
public: 
    explicit
    NativeToNull(RegisterRef<TMPLT>* result,
                 StandardTypeDescriptorOrdinal nativeType)
        : NativeNativeInstruction<TMPLT>(result, nativeType)
    { }
    virtual
    ~NativeToNull() { }

    virtual void exec(TProgramCounter& pc) const { 
        pc++;
        mResult->toNull();
    }
    const char* longName() const { return "NativeToNull"; }
    const char* shortName() const { return "TONULL"; }
    void describe(string& out, bool values) const {
        describeHelper(out, values, longName(), shortName(), mResult, mOp1, mOp2);
    }
};

FENNEL_END_NAMESPACE

#endif

// End NativeNativeInstruction.h

