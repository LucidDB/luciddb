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
#ifndef Fennel_BoolPointerInstruction_Included
#define Fennel_BoolPointerInstruction_Included

#include "fennel/calc/PointerInstruction.h"

FENNEL_BEGIN_NAMESPACE

/**
 * Support for operators that return booleans, i.e. comparison operators
 *
 * @author John Kalucki
 */
template<typename PTR_TYPE>
class BoolPointerInstruction : public PointerInstruction
{
public:
    explicit
    BoolPointerInstruction(RegisterRef<bool>* result,
                           RegisterRef<PTR_TYPE>* op1,
                           StandardTypeDescriptorOrdinal pointerType)
        : mResult(result),
          mOp1(op1),
          mOp2(),            // unused
          mPointerType(pointerType)
    { }
    explicit
    BoolPointerInstruction(RegisterRef<bool>* result,
                           RegisterRef<PTR_TYPE>* op1,
                           RegisterRef<PTR_TYPE>* op2, 
                           StandardTypeDescriptorOrdinal pointerType)
        : mResult(result),
          mOp1(op1),
          mOp2(op2),
          mPointerType(pointerType)
    { }
    ~BoolPointerInstruction() { 
        // If (0) to reduce performance impact of template type checking
        if (0) PointerInstruction_NotAPointerType<PTR_TYPE>();
    }

protected:
    RegisterRef<bool>* mResult;
    RegisterRef<PTR_TYPE>* mOp1;
    RegisterRef<PTR_TYPE>* mOp2;
    StandardTypeDescriptorOrdinal mPointerType;

};

template <typename PTR_TYPE>
class BoolPointerEqual : public BoolPointerInstruction<PTR_TYPE>
{
public: 
    explicit
    BoolPointerEqual(RegisterRef<bool>* result,
                     RegisterRef<PTR_TYPE>* op1, 
                     RegisterRef<PTR_TYPE>* op2,
                     StandardTypeDescriptorOrdinal pointerType)
        : BoolPointerInstruction<PTR_TYPE>(result, op1, op2, pointerType)
    { }
    virtual
    ~BoolPointerEqual() { }

    virtual void exec(TProgramCounter& pc) const { 
        pc++;

        if (mOp1->isNull() || mOp2->isNull()) {
            mResult->toNull();
        } else if (mOp1->pointer() == mOp2->pointer()) {
            mResult->value(true);
        } else {
            mResult->value(false);
        }
    }

    const char * longName() const { return "BoolPointerEqual"; }
    const char * shortName() const { return "=="; }
    void describe(string &out, bool values) const {
        describeHelper(out, values, longName(), shortName(), mResult, mOp1, mOp2);
    }
};

template <typename PTR_TYPE>
class BoolPointerNotEqual : public BoolPointerInstruction<PTR_TYPE>
{
public:  
    explicit
    BoolPointerNotEqual(RegisterRef<bool>* result,
                        RegisterRef<PTR_TYPE>* op1, 
                        RegisterRef<PTR_TYPE>* op2,
                        StandardTypeDescriptorOrdinal pointerType)
        : BoolPointerInstruction<PTR_TYPE>(result, op1, op2, pointerType)
    { }
    virtual
    ~BoolPointerNotEqual() { }

    virtual void exec(TProgramCounter& pc) const { 
        pc++;
        if (mOp1->isNull() || mOp2->isNull()) {
            mResult->toNull
                ();
        } else if (mOp1->pointer() == mOp2->pointer()) {
            mResult->value(false);
        } else {
            mResult->value(true);
        }
    }

    const char * longName() const { return "BoolPointerNotEqual"; }
    const char * shortName() const { return "!="; }
    void describe(string &out, bool values) const {
        describeHelper(out, values, longName(), shortName(), mResult, mOp1, mOp2);
    }
};

template <typename PTR_TYPE>
class BoolPointerGreater : public BoolPointerInstruction<PTR_TYPE>
{
public: 
    explicit
    BoolPointerGreater(RegisterRef<bool>* result,
                       RegisterRef<PTR_TYPE>* op1, 
                       RegisterRef<PTR_TYPE>* op2,
                       StandardTypeDescriptorOrdinal pointerType)
        : BoolPointerInstruction<PTR_TYPE>(result, op1, op2, pointerType)
    { }
    virtual
    ~BoolPointerGreater() { }

    virtual void exec(TProgramCounter& pc) const { 
        pc++;
        if (mOp1->isNull() || mOp2->isNull()) {
            mResult->toNull();
        } else if (mOp1->pointer() > mOp2->pointer()) {
            mResult->value(true);
        } else {
            mResult->value(false);
        }
    }

    const char * longName() const { return "BoolPointerGreater"; }
    const char * shortName() const { return ">"; }
    void describe(string &out, bool values) const {
        describeHelper(out, values, longName(), shortName(), mResult, mOp1, mOp2);
    }
};

template <typename PTR_TYPE>
class BoolPointerGreaterEqual : public BoolPointerInstruction<PTR_TYPE>
{
public: 
    explicit
    BoolPointerGreaterEqual(RegisterRef<bool>* result,
                            RegisterRef<PTR_TYPE>* op1, 
                            RegisterRef<PTR_TYPE>* op2,
                            StandardTypeDescriptorOrdinal pointerType)
        : BoolPointerInstruction<PTR_TYPE>(result, op1, op2, pointerType)
    { }
    virtual
    ~BoolPointerGreaterEqual() { }

    virtual void exec(TProgramCounter& pc) const { 
        pc++;
        if (mOp1->isNull() || mOp2->isNull()) {
            mResult->toNull();
        } else if (mOp1->pointer() >= mOp2->pointer()) {
            mResult->value(true);
        } else {
            mResult->value(false);
        }
    }

    const char * longName() const { return "BoolPointerGreaterEqual"; }
    const char * shortName() const { return ">="; }
    void describe(string &out, bool values) const {
        describeHelper(out, values, longName(), shortName(), mResult, mOp1, mOp2);
    }
};

template <typename PTR_TYPE>
class BoolPointerLess : public BoolPointerInstruction<PTR_TYPE>
{
public: 
    explicit
    BoolPointerLess(RegisterRef<bool>* result,
                    RegisterRef<PTR_TYPE>* op1, 
                    RegisterRef<PTR_TYPE>* op2,
                    StandardTypeDescriptorOrdinal pointerType)
        : BoolPointerInstruction<PTR_TYPE>(result, op1, op2, pointerType)
    { }
    virtual
    ~BoolPointerLess() { }

    virtual void exec(TProgramCounter& pc) const { 
        pc++;
        if (mOp1->isNull() || mOp2->isNull()) {
            mResult->toNull();
        } else if (mOp1->pointer() < mOp2->pointer()) {
            mResult->value(true);
        } else {
            mResult->value(false);
        }
    }
    const char * longName() const { return "BoolPointerLess"; }
    const char * shortName() const { return "<"; }
    void describe(string &out, bool values) const {
        describeHelper(out, values, longName(), shortName(), mResult, mOp1, mOp2);
    }
};

template <typename PTR_TYPE>
class BoolPointerLessEqual : public BoolPointerInstruction<PTR_TYPE>
{
public: 
    explicit
    BoolPointerLessEqual(RegisterRef<bool>* result,
                         RegisterRef<PTR_TYPE>* op1, 
                         RegisterRef<PTR_TYPE>* op2,
                         StandardTypeDescriptorOrdinal pointerType)
        : BoolPointerInstruction<PTR_TYPE>(result, op1, op2, pointerType)
    { }
    virtual
    ~BoolPointerLessEqual() { }

    virtual void exec(TProgramCounter& pc) const { 
        pc++;
        if (mOp1->isNull() || mOp2->isNull()) {
            mResult->toNull();
        } else if (mOp1->pointer() <= mOp2->pointer()) {
            mResult->value(true);
        } else {
            mResult->value(false);
        }
    }

    const char * longName() const { return "BoolPointerLessEqual"; }
    const char * shortName() const { return "<="; }
    void describe(string &out, bool values) const {
        describeHelper(out, values, longName(), shortName(), mResult, mOp1, mOp2);
    }
};

template <typename PTR_TYPE>
class BoolPointerIsNull : public BoolPointerInstruction<PTR_TYPE>
{
public: 
    explicit
    BoolPointerIsNull(RegisterRef<bool>* result,
                      RegisterRef<PTR_TYPE>* op1, 
                      StandardTypeDescriptorOrdinal pointerType)
        : BoolPointerInstruction<PTR_TYPE>(result, op1, pointerType)
    { }
    virtual
    ~BoolPointerIsNull() { }

    virtual void exec(TProgramCounter& pc) const { 
        pc++;
        if (mOp1->isNull()) {
            mResult->value(true);
        } else {
            mResult->value(false);
        }
    }

    const char * longName() const { return "BoolPointerIsNull"; }
    const char * shortName() const { return "NULL"; }
    void describe(string &out, bool values) const {
        describeHelper(out, values, longName(), shortName(), mResult, mOp1, mOp2);
    }
};

template <typename PTR_TYPE>
class BoolPointerIsNotNull : public BoolPointerInstruction<PTR_TYPE>
{
public: 
    explicit
    BoolPointerIsNotNull(RegisterRef<bool>* result,
                         RegisterRef<PTR_TYPE>* op1, 
                         StandardTypeDescriptorOrdinal pointerType)
        : BoolPointerInstruction<PTR_TYPE>(result, op1, pointerType)
    { }
    virtual
    ~BoolPointerIsNotNull() { }

    virtual void exec(TProgramCounter& pc) const { 
        pc++;
        if (mOp1->isNull()) {
            mResult->value(false);
        } else {
            mResult->value(true);
        }
    }

    const char * longName() const { return "BoolPointerIsNotNull"; }
    const char * shortName() const { return "NOTNULL"; }
    void describe(string &out, bool values) const {
        describeHelper(out, values, longName(), shortName(), mResult, mOp1, mOp2);
    }
};


FENNEL_END_NAMESPACE

#endif

// End BoolPointerInstruction.h

