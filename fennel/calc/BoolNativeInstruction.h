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
*/
#ifndef Fennel_BoolNativeInstruction_Included
#define Fennel_BoolNativeInstruction_Included

#include "fennel/calc/NativeInstruction.h"

FENNEL_BEGIN_NAMESPACE

/**
 * Support for operators that return booleans, i.e. comparison operators
 *
 * @author John Kalucki
 */
template<typename TMPLT>
class BoolNativeInstruction : public NativeInstruction<TMPLT>
{
public:
    explicit
    BoolNativeInstruction(RegisterRef<bool>* result,
                          RegisterRef<TMPLT>* op1,
                          StandardTypeDescriptorOrdinal nativeType)
        : NativeInstruction<TMPLT>(op1, nativeType),
          mResult(result)
    { }
    explicit
    BoolNativeInstruction(RegisterRef<bool>* result,
                          RegisterRef<TMPLT>* op1,
                          RegisterRef<TMPLT>* op2, 
                          StandardTypeDescriptorOrdinal nativeType)
        : NativeInstruction<TMPLT>(op1, op2, nativeType),
          mResult(result)
    { }
    virtual
    ~BoolNativeInstruction() { }

    virtual void setCalc(Calculator* calcP) {
        mResult->setCalc(calcP);
        mOp1->setCalc(calcP);
        mOp2->setCalc(calcP);  // note: may be unused if instruction has 1 operand
    }
protected:
    RegisterRef<bool>* mResult;
};

template <typename TMPLT>
class BoolNativeEqual : public BoolNativeInstruction<TMPLT>
{
public: 
    explicit
    BoolNativeEqual(RegisterRef<bool>* result,
                    RegisterRef<TMPLT>* op1, 
                    RegisterRef<TMPLT>* op2,
                    StandardTypeDescriptorOrdinal nativeType)
        : BoolNativeInstruction<TMPLT>(result, op1, op2, nativeType)
    { }
    virtual
    ~BoolNativeEqual() { }

    virtual void exec(TProgramCounter& pc) const { 
        pc++;
        if (mOp1->isNull() || mOp2->isNull()) {
            mResult->toNull();
        } else if (mOp1->value() == mOp2->value()) {
            mResult->value(true);
        } else {
            mResult->value(false);
        }
    }

    const char * longName() const { return "BoolNativeEqual"; }
    const char * shortName() const { return "=="; }
    void describe(string &out, bool values) const {
        describeHelper(out, values, longName(), shortName(), mResult, mOp1, mOp2);
    }
};

template <typename TMPLT>
class BoolNativeNotEqual : public BoolNativeInstruction<TMPLT>
{
public: 
    explicit
    BoolNativeNotEqual(RegisterRef<bool>* result,
                       RegisterRef<TMPLT>* op1, 
                       RegisterRef<TMPLT>* op2,
                       StandardTypeDescriptorOrdinal nativeType)
        : BoolNativeInstruction<TMPLT>(result, op1, op2, nativeType)
    { }
    virtual
    ~BoolNativeNotEqual() { }

    virtual void exec(TProgramCounter& pc) const { 
        pc++;
        if (mOp1->isNull() || mOp2->isNull()) {
            mResult->toNull();
        } else if (mOp1->value() == mOp2->value()) {
            mResult->value(false);
        } else {
            mResult->value(true);
        }
    }

    const char * longName() const { return "BoolNativeNotEqual"; }
    const char * shortName() const { return "!="; }
    void describe(string &out, bool values) const {
        describeHelper(out, values, longName(), shortName(), mResult, mOp1, mOp2);
    }
};

template <typename TMPLT>
class BoolNativeGreater : public BoolNativeInstruction<TMPLT>
{
public: 
    explicit
    BoolNativeGreater(RegisterRef<bool>* result,
                      RegisterRef<TMPLT>* op1, 
                      RegisterRef<TMPLT>* op2,
                      StandardTypeDescriptorOrdinal nativeType)
        : BoolNativeInstruction<TMPLT>(result, op1, op2, nativeType)
    { }
    virtual
    ~BoolNativeGreater() { }

    virtual void exec(TProgramCounter& pc) const { 
        pc++;
        if (mOp1->isNull() || mOp2->isNull()) {
            mResult->toNull();
        } else if (mOp1->value() > mOp2->value()) {
            mResult->value(true);
        } else {
            mResult->value(false);
        }
    }

    const char * longName() const { return "BoolNativeGreater"; }
    const char * shortName() const { return ">"; }
    void describe(string &out, bool values) const {
        describeHelper(out, values, longName(), shortName(), mResult, mOp1, mOp2);
    }
};

template <typename TMPLT>
class BoolNativeGreaterEqual : public BoolNativeInstruction<TMPLT>
{
public: 
    explicit
    BoolNativeGreaterEqual(RegisterRef<bool>* result,
                           RegisterRef<TMPLT>* op1, 
                           RegisterRef<TMPLT>* op2,
                           StandardTypeDescriptorOrdinal nativeType)
        : BoolNativeInstruction<TMPLT>(result, op1, op2, nativeType)
    { }
    virtual
    ~BoolNativeGreaterEqual() { }

    virtual void exec(TProgramCounter& pc) const { 
        pc++;
        if (mOp1->isNull() || mOp2->isNull()) {
            mResult->toNull();
        } else if (mOp1->value() >= mOp2->value()) {
            mResult->value(true);
        } else {
            mResult->value(false);
        }
    }

    const char * longName() const { return "BoolNativeGreaterEqual"; }
    const char * shortName() const { return ">="; }
    void describe(string &out, bool values) const {
        describeHelper(out, values, longName(), shortName(), mResult, mOp1, mOp2);
    }
};

template <typename TMPLT>
class BoolNativeLess : public BoolNativeInstruction<TMPLT>
{
public: 
    explicit
    BoolNativeLess(RegisterRef<bool>* result,
                   RegisterRef<TMPLT>* op1, 
                   RegisterRef<TMPLT>* op2,
                   StandardTypeDescriptorOrdinal nativeType)
        : BoolNativeInstruction<TMPLT>(result, op1, op2, nativeType)
    { }
    virtual
    ~BoolNativeLess() { }

    virtual void exec(TProgramCounter& pc) const { 
        pc++;
        if (mOp1->isNull() || mOp2->isNull()) {
            mResult->toNull();
        } else if (mOp1->value() < mOp2->value()) {
            mResult->value(true);
        } else {
            mResult->value(false);
        }
    }
    const char * longName() const { return "BoolNativeLess"; }
    const char * shortName() const { return "<"; }
    void describe(string &out, bool values) const {
        describeHelper(out, values, longName(), shortName(), mResult, mOp1, mOp2);
    }
};

template <typename TMPLT>
class BoolNativeLessEqual : public BoolNativeInstruction<TMPLT>
{
public: 
    explicit
    BoolNativeLessEqual(RegisterRef<bool>* result,
                        RegisterRef<TMPLT>* op1, 
                        RegisterRef<TMPLT>* op2,
                        StandardTypeDescriptorOrdinal nativeType)
        : BoolNativeInstruction<TMPLT>(result, op1, op2, nativeType)
    { }
    virtual
    ~BoolNativeLessEqual() { }

    virtual void exec(TProgramCounter& pc) const { 
        pc++;
        if (mOp1->isNull() || mOp2->isNull()) {
            mResult->toNull();
        } else if (mOp1->value() <= mOp2->value()) {
            mResult->value(true);
        } else {
            mResult->value(false);
        }
    }

    const char * longName() const { return "BoolNativeLessEqual"; }
    const char * shortName() const { return "<="; }
    void describe(string &out, bool values) const {
        describeHelper(out, values, longName(), shortName(), mResult, mOp1, mOp2);
    }
};

template <typename TMPLT>
class BoolNativeIsNull : public BoolNativeInstruction<TMPLT>
{
public: 
    explicit
    BoolNativeIsNull(RegisterRef<bool>* result,
                     RegisterRef<TMPLT>* op1, 
                     StandardTypeDescriptorOrdinal nativeType)
        : BoolNativeInstruction<TMPLT>(result, op1, nativeType)
    { }
    virtual
    ~BoolNativeIsNull() { }

    virtual void exec(TProgramCounter& pc) const { 
        pc++;
        if (mOp1->isNull()) {
            mResult->value(true);
        } else {
            mResult->value(false);
        }
    }

    const char * longName() const { return "BoolNativeIsNull"; }
    const char * shortName() const { return "NULL"; }
    void describe(string &out, bool values) const {
        describeHelper(out, values, longName(), shortName(), mResult, mOp1, mOp2);
    }
};

template <typename TMPLT>
class BoolNativeIsNotNull : public BoolNativeInstruction<TMPLT>
{
public: 
    explicit
    BoolNativeIsNotNull(RegisterRef<bool>* result,
                        RegisterRef<TMPLT>* op1, 
                        StandardTypeDescriptorOrdinal nativeType)
        : BoolNativeInstruction<TMPLT>(result, op1, nativeType)
    { }
    virtual
    ~BoolNativeIsNotNull() { }

    virtual void exec(TProgramCounter& pc) const { 
        pc++;
        if (mOp1->isNull()) {
            mResult->value(false);
        } else {
            mResult->value(true);
        }
    }

    const char * longName() const { return "BoolNativeIsNotNull"; }
    const char * shortName() const { return "NOTNULL"; }
    void describe(string &out, bool values) const {
        describeHelper(out, values, longName(), shortName(), mResult, mOp1, mOp2);
    }
};


FENNEL_END_NAMESPACE

#endif

// End BoolNativeInstruction.h

