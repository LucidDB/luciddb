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
// Bool Instruction
//
// Instruction->Bool
*/
#ifndef Fennel_BoolInstruction_Included
#define Fennel_BoolInstruction_Included

#include "fennel/calc/Instruction.h"

FENNEL_BEGIN_NAMESPACE

class BoolInstruction : public Instruction
{
public:
    explicit
    BoolInstruction(RegisterRef<bool>* result)
        : mResult(result),
          mOp1(),
          mOp2()
    { }
    explicit
    BoolInstruction(RegisterRef<bool>* result, RegisterRef<bool>* op1) 
        : mResult(result),
          mOp1(op1),
          mOp2()
    { }
    explicit
    BoolInstruction(RegisterRef<bool>* &result,
                    RegisterRef<bool>* &op1,
                    RegisterRef<bool>* &op2)
        : mResult(result),
          mOp1(op1),
          mOp2(op2)
    { }

    virtual
    ~BoolInstruction()
    { }

protected:
    RegisterRef<bool>* mResult;
    RegisterRef<bool>* mOp1;
    RegisterRef<bool>* mOp2;
};

class BoolOr : public BoolInstruction
{
public: 
    explicit
    BoolOr(RegisterRef<bool>* result,
           RegisterRef<bool>* op1,
           RegisterRef<bool>* op2)
        : BoolInstruction(result, op1, op2)
    { }

    virtual 
    ~BoolOr() { }

    const char* longName() const;
    const char* shortName() const;
    void describe(string& out, bool values) const;

    virtual void exec(TProgramCounter& pc) const { 
        // SQL99, 6.30, Table 14
        pc++;
        
        if (mOp1->isNull()) {
            if (mOp2->isNull() || mOp2->value() == false) {
                mResult->toNull();
            } else {
                mResult->value(true);
            }
        } else {
            if (mOp2->isNull()) {
                if (mOp1->value() == true) {
                    mResult->value(true);
                } else {
                    mResult->toNull();
                }
            } else {
                if (mOp1->value() == true || mOp2->value() == true) {
                    mResult->value(true);
                } else {
                    mResult->value(false);
                }
            }
        }
    }
};

class BoolAnd : public BoolInstruction
{
public: 
    explicit
    BoolAnd(RegisterRef<bool>* result,
            RegisterRef<bool>* op1,
            RegisterRef<bool>* op2)
        : BoolInstruction(result, op1, op2)
    { }

    ~BoolAnd() { }

    const char* longName() const;
    const char* shortName() const;
    void describe(string& out, bool values) const;

    virtual void exec(TProgramCounter& pc) const {
        // SQL99, 6.30, Table 13
        pc++;
        if (mOp1->isNull()) {
            if (mOp2->isNull() || mOp2->value() == true) {
                mResult->toNull();
            } else {
                mResult->value(false);
            }
        } else {
            if (mOp2->isNull()) {
                if (mOp1->value() == true) {
                    mResult->toNull();
                } else {
                    mResult->value(false);
                }
            } else if (mOp1->value() == true && mOp2->value() == true) {
                mResult->value(true);
            } else {
                mResult->value(false);
            }
        }
    }
};

class BoolNot : public BoolInstruction
{
public: 
    explicit
    BoolNot(RegisterRef<bool>* result,
            RegisterRef<bool>* op1)
        : BoolInstruction(result, op1)
    { }

    ~BoolNot() { }

    const char* longName() const;
    const char* shortName() const;
    void describe(string& out, bool values) const;

    virtual void exec(TProgramCounter& pc) const {
        // SQL99, 6.30, General Rule #2
        pc++;
        if (mOp1->isNull()) {
            mResult->toNull();
        } else if (mOp1->value() == true) {
            mResult->value(false);
        } else {
            mResult->value(true);
        }
    }
};

class BoolMove : public BoolInstruction
{
public:
    explicit
    BoolMove(RegisterRef<bool>* result,
             RegisterRef<bool>* op1)
        : BoolInstruction(result, op1)
    { }

    ~BoolMove() { }

    const char* longName() const;
    const char* shortName() const;
    void describe(string& out, bool values) const;

    virtual void exec(TProgramCounter& pc) const {
        pc++;
        if (mOp1->isNull()) {
            mResult->toNull();
        } else {
            mResult->value(mOp1->value());
        }
    }
};

class BoolIs : public BoolInstruction
{
public:
    explicit
    BoolIs(RegisterRef<bool>* result,
           RegisterRef<bool>* op1,
           RegisterRef<bool>* op2)
        : BoolInstruction(result, op1, op2)
    { }

    ~BoolIs() { }

    const char* longName() const;
    const char* shortName() const;
    void describe(string& out, bool values) const;

    virtual void exec(TProgramCounter& pc) const {
        // SQL99, 6.30, Table 15
        pc++;
        if (mOp1->isNull()) {
            if (mOp2->isNull()) {
                mResult->value(true);
            } else {
                mResult->value(false);
            }
        } else if (mOp2->isNull()) {
            mResult->value(false);
        } else if (mOp1->value() == mOp2->value()) {
            mResult->value(true);
        } else {
            mResult->value(false);
        }
    }
};

class BoolIsNot : public BoolInstruction
{
public:
    explicit
    BoolIsNot(RegisterRef<bool>* result,
              RegisterRef<bool>* op1,
              RegisterRef<bool>* op2)
        : BoolInstruction(result, op1, op2)
    { }

    ~BoolIsNot() { }

    const char* longName() const;
    const char* shortName() const;
    void describe(string& out, bool values) const;

    virtual void exec(TProgramCounter& pc) const {
        // SQL99, 6.30, Table 15
        pc++;
        if (mOp1->isNull()) {
            if (mOp2->isNull()) {
                mResult->value(false);
            } else {
                mResult->value(true);
            }
        } else if (mOp2->isNull()) {
            mResult->value(true);
        } else if (mOp1->value() == mOp2->value()) {
            mResult->value(false);
        } else {
            mResult->value(true);
        }
    }
};


// BoolEqual is not the same as SQL99 boolean IS
class BoolEqual : public BoolInstruction
{
public:
    explicit
    BoolEqual(RegisterRef<bool>* result,
              RegisterRef<bool>* op1,
              RegisterRef<bool>* op2)
        : BoolInstruction(result, op1, op2)
    { }

    ~BoolEqual() { }

    const char* longName() const;
    const char* shortName() const;
    void describe(string& out, bool values) const;

    virtual void exec(TProgramCounter& pc) const {
        // SQL99, 4.6.1 Comparison and Assignment of Booleans
        pc++;
        if (mOp1->isNull() || mOp2->isNull()) {
            mResult->toNull();
        } else {
            if (mOp1->value() == mOp2->value()) {
                mResult->value(true);
            } else {
                mResult->value(false);
            }
        }
    }
};

class BoolNotEqual : public BoolInstruction
{
public:
    explicit
    BoolNotEqual(RegisterRef<bool>* result,
                 RegisterRef<bool>* op1,
                 RegisterRef<bool>* op2)
        : BoolInstruction(result, op1, op2)
    { }

    ~BoolNotEqual() { }

    const char* longName() const;
    const char* shortName() const;
    void describe(string& out, bool values) const;

    virtual void exec(TProgramCounter& pc) const {
        // SQL99, 4.6.1 Comparison and Assignment of Booleans
        pc++;
        if (mOp1->isNull() || mOp2->isNull()) {
            mResult->toNull();
        } else {
            if (mOp1->value() == mOp2->value()) {
                mResult->value(false);
            } else {
                mResult->value(true);
            }
        }
    }
};

class BoolGreater : public BoolInstruction
{
public:
    explicit
    BoolGreater(RegisterRef<bool>* result,
                RegisterRef<bool>* op1,
                RegisterRef<bool>* op2)
        : BoolInstruction(result, op1, op2)
    { }

    ~BoolGreater() { }

    const char* longName() const;
    const char* shortName() const;
    void describe(string& out, bool values) const;

    virtual void exec(TProgramCounter& pc) const {
        // SQL99, 4.6.1 Comparison and Assignment of Booleans
        pc++;
        if (mOp1->isNull() || mOp2->isNull()) {
            mResult->toNull();
        } else {
            if ((mOp1->value() == true) && (mOp2->value() == false)) {
                mResult->value(true);
            } else {
                mResult->value(false);
            }
        }
    }
};

class BoolLess : public BoolInstruction
{
public:
    explicit
    BoolLess(RegisterRef<bool>* result,
             RegisterRef<bool>* op1,
             RegisterRef<bool>* op2)
        : BoolInstruction(result, op1, op2)
    { }

    ~BoolLess() { }

    const char* longName() const;
    const char* shortName() const;
    void describe(string& out, bool values) const;

    virtual void exec(TProgramCounter& pc) const {
        // SQL99, 4.6.1 Comparison and Assignment of Booleans
        pc++;
        if (mOp1->isNull() || mOp2->isNull()) {
            mResult->toNull();
        } else {
            if (mOp1->value() == false && mOp2->value() == true) {
                mResult->value(true);
            } else {
                mResult->value(false);
            }
        }
    }
};

class BoolIsNull : public BoolInstruction
{
public:
    explicit
    BoolIsNull(RegisterRef<bool>* result,
               RegisterRef<bool>* op1)
        : BoolInstruction(result, op1)
    { }

    ~BoolIsNull() { }

    const char* longName() const;
    const char* shortName() const;
    void describe(string& out, bool values) const;

    virtual void exec(TProgramCounter& pc) const {
        pc++;
        if (mOp1->isNull()) {
            mResult->value(true);
        } else {
            mResult->value(false);
        }
    }
};

class BoolIsNotNull : public BoolInstruction
{
public:
    explicit
    BoolIsNotNull(RegisterRef<bool>* result,
                  RegisterRef<bool>* op1)
        : BoolInstruction(result, op1)
    { }

    ~BoolIsNotNull() { }

    const char* longName() const;
    const char* shortName() const;
    void describe(string& out, bool values) const;

    virtual void exec(TProgramCounter& pc) const {
        // SQL99, 4.6.1 Comparison and Assignment of Booleans
        pc++;
        if (mOp1->isNull()) {
            mResult->value(false);
        } else {
            mResult->value(true);
        }
    }
};

class BoolToNull : public BoolInstruction
{
public:
    explicit
    BoolToNull(RegisterRef<bool>* result)
        : BoolInstruction(result)
    { }

    ~BoolToNull() { }

    const char* longName() const;
    const char* shortName() const;
    void describe(string& out, bool values) const;

    virtual void exec(TProgramCounter& pc) const {
        // SQL99, 4.6.1 Comparison and Assignment of Booleans
        pc++;
        mResult->toNull();
    }
};

FENNEL_END_NAMESPACE

#endif

// End BoolInstruction.h

