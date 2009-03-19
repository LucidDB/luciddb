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
// Bool Instruction
//
// Instruction->Bool
*/
#ifndef Fennel_BoolInstruction_Included
#define Fennel_BoolInstruction_Included

#include "fennel/disruptivetech/calc/Instruction.h"

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

    static vector<StandardTypeDescriptorOrdinal>
    regDesc(uint args) {
        vector<StandardTypeDescriptorOrdinal>v;
        uint i;
        for (i = 0; i < args; i++) {
            v.push_back(STANDARD_TYPE_BOOL);
        }
        return v;
    }
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

    static const char* longName();
    static const char* shortName();
    static int numArgs();
    void describe(string& out, bool values) const;

    virtual void exec(TProgramCounter& pc) const {
        // SQL99 Part 2 Section 6.30 Table 14
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

    static InstructionSignature
    signature(StandardTypeDescriptorOrdinal type) {
        vector<StandardTypeDescriptorOrdinal>v(numArgs(), type);
        return InstructionSignature(shortName(), v);
    }

    static Instruction*
    create(InstructionSignature const & sig)
    {
        assert(sig.size() == numArgs());
        assert((sig[0])->type() == STANDARD_TYPE_BOOL);
        assert((sig[1])->type() == STANDARD_TYPE_BOOL);
        assert((sig[2])->type() == STANDARD_TYPE_BOOL);
        return new BoolOr(static_cast<RegisterRef<bool>*> (sig[0]),
                          static_cast<RegisterRef<bool>*> (sig[1]),
                          static_cast<RegisterRef<bool>*> (sig[2]));
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

    static const char* longName();
    static const char* shortName();
    static int numArgs();
    void describe(string& out, bool values) const;

    virtual void exec(TProgramCounter& pc) const {
        // SQL99 Part 2 Section 6.30 Table 13
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

    static InstructionSignature
    signature(StandardTypeDescriptorOrdinal type) {
        vector<StandardTypeDescriptorOrdinal>v(numArgs(), type);
        return InstructionSignature(shortName(), v);
    }

    static Instruction*
    create(InstructionSignature const & sig)
    {
        assert(sig.size() == numArgs());
        assert((sig[0])->type() == STANDARD_TYPE_BOOL);
        assert((sig[1])->type() == STANDARD_TYPE_BOOL);
        assert((sig[2])->type() == STANDARD_TYPE_BOOL);
        return new BoolAnd(static_cast<RegisterRef<bool>*> (sig[0]),
                           static_cast<RegisterRef<bool>*> (sig[1]),
                           static_cast<RegisterRef<bool>*> (sig[2]));
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

    static const char* longName();
    static const char* shortName();
    static int numArgs();
    void describe(string& out, bool values) const;

    virtual void exec(TProgramCounter& pc) const {
        // SQL99 Part 2 Section 6.30 General Rule 2
        pc++;
        if (mOp1->isNull()) {
            mResult->toNull();
        } else if (mOp1->value() == true) {
            mResult->value(false);
        } else {
            mResult->value(true);
        }
    }

    static InstructionSignature
    signature(StandardTypeDescriptorOrdinal type) {
        vector<StandardTypeDescriptorOrdinal>v(numArgs(), type);
        return InstructionSignature(shortName(), v);
    }

    static Instruction*
    create(InstructionSignature const & sig)
    {
        assert(sig.size() == numArgs());
        assert((sig[0])->type() == STANDARD_TYPE_BOOL);
        assert((sig[1])->type() == STANDARD_TYPE_BOOL);
        return new BoolNot(static_cast<RegisterRef<bool>*> (sig[0]),
                           static_cast<RegisterRef<bool>*> (sig[1]));
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

    static const char* longName();
    static const char* shortName();
    static int numArgs();
    void describe(string& out, bool values) const;

    virtual void exec(TProgramCounter& pc) const {
        pc++;
        if (mOp1->isNull()) {
            mResult->toNull();
        } else {
            mResult->value(mOp1->value());
        }
    }

    static InstructionSignature
    signature(StandardTypeDescriptorOrdinal type) {
        vector<StandardTypeDescriptorOrdinal>v(numArgs(), type);
        return InstructionSignature(shortName(), v);
    }

    static Instruction*
    create(InstructionSignature const & sig)
    {
        assert(sig.size() == numArgs());
        assert((sig[0])->type() == STANDARD_TYPE_BOOL);
        assert((sig[1])->type() == STANDARD_TYPE_BOOL);
        return new BoolMove(static_cast<RegisterRef<bool>*> (sig[0]),
                            static_cast<RegisterRef<bool>*> (sig[1]));
    }
};

class BoolRef : public BoolInstruction
{
public:
    explicit
    BoolRef(RegisterRef<bool>* result,
            RegisterRef<bool>* op1)
        : BoolInstruction(result, op1)
    { }

    ~BoolRef() { }

    static const char* longName();
    static const char* shortName();
    static int numArgs();
    void describe(string& out, bool values) const;

    virtual void exec(TProgramCounter& pc) const {
        pc++;
        mResult->refer(mOp1);
    }

    static InstructionSignature
    signature(StandardTypeDescriptorOrdinal type) {
        vector<StandardTypeDescriptorOrdinal>v(numArgs(), type);
        return InstructionSignature(shortName(), v);
    }

    static Instruction*
    create(InstructionSignature const & sig)
    {
        assert(sig.size() == numArgs());
        assert((sig[0])->type() == STANDARD_TYPE_BOOL);
        assert((sig[1])->type() == STANDARD_TYPE_BOOL);
        return new BoolRef(static_cast<RegisterRef<bool>*> (sig[0]),
                           static_cast<RegisterRef<bool>*> (sig[1]));
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

    static const char* longName();
    static const char* shortName();
    static int numArgs();
    void describe(string& out, bool values) const;

    virtual void exec(TProgramCounter& pc) const {
        // SQL99 Part 2 Section 6.30 Table 15
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

    static InstructionSignature
    signature(StandardTypeDescriptorOrdinal type) {
        vector<StandardTypeDescriptorOrdinal>v(numArgs(), type);
        return InstructionSignature(shortName(), v);
    }

    static Instruction*
    create(InstructionSignature const & sig)
    {
        assert(sig.size() == numArgs());
        assert((sig[0])->type() == STANDARD_TYPE_BOOL);
        assert((sig[1])->type() == STANDARD_TYPE_BOOL);
        assert((sig[2])->type() == STANDARD_TYPE_BOOL);
        return new BoolIs(static_cast<RegisterRef<bool>*> (sig[0]),
                          static_cast<RegisterRef<bool>*> (sig[1]),
                          static_cast<RegisterRef<bool>*> (sig[2]));
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

    static const char* longName();
    static const char* shortName();
    static int numArgs();
    void describe(string& out, bool values) const;

    virtual void exec(TProgramCounter& pc) const {
        // SQL99 Part 2 Section 6.30 Table 15
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

    static InstructionSignature
    signature(StandardTypeDescriptorOrdinal type) {
        vector<StandardTypeDescriptorOrdinal>v(numArgs(), type);
        return InstructionSignature(shortName(), v);
    }

    static Instruction*
    create(InstructionSignature const & sig)
    {
        assert(sig.size() == numArgs());
        assert((sig[0])->type() == STANDARD_TYPE_BOOL);
        assert((sig[1])->type() == STANDARD_TYPE_BOOL);
        assert((sig[2])->type() == STANDARD_TYPE_BOOL);
        return new BoolIsNot(static_cast<RegisterRef<bool>*> (sig[0]),
                             static_cast<RegisterRef<bool>*> (sig[1]),
                             static_cast<RegisterRef<bool>*> (sig[2]));
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

    static const char* longName();
    static const char* shortName();
    static int numArgs();
    void describe(string& out, bool values) const;

    virtual void exec(TProgramCounter& pc) const {
        // SQL99 Part 2 Section 4.6.1 Comparison and Assignment of Booleans
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

    static InstructionSignature
    signature(StandardTypeDescriptorOrdinal type) {
        vector<StandardTypeDescriptorOrdinal>v(numArgs(), type);
        return InstructionSignature(shortName(), v);
    }

    static Instruction*
    create(InstructionSignature const & sig)
    {
        assert(sig.size() == numArgs());
        assert((sig[0])->type() == STANDARD_TYPE_BOOL);
        assert((sig[1])->type() == STANDARD_TYPE_BOOL);
        assert((sig[2])->type() == STANDARD_TYPE_BOOL);
        return new BoolEqual(static_cast<RegisterRef<bool>*> (sig[0]),
                             static_cast<RegisterRef<bool>*> (sig[1]),
                             static_cast<RegisterRef<bool>*> (sig[2]));
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

    static const char* longName();
    static const char* shortName();
    static int numArgs();
    void describe(string& out, bool values) const;

    virtual void exec(TProgramCounter& pc) const {
        // SQL99 Part 2 Section 4.6.1 Comparison and Assignment of Booleans
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

    static InstructionSignature
    signature(StandardTypeDescriptorOrdinal type) {
        vector<StandardTypeDescriptorOrdinal>v(numArgs(), type);
        return InstructionSignature(shortName(), v);
    }

    static Instruction*
    create(InstructionSignature const & sig)
    {
        assert(sig.size() == numArgs());
        assert((sig[0])->type() == STANDARD_TYPE_BOOL);
        assert((sig[1])->type() == STANDARD_TYPE_BOOL);
        assert((sig[2])->type() == STANDARD_TYPE_BOOL);
        return new BoolNotEqual(static_cast<RegisterRef<bool>*> (sig[0]),
                                static_cast<RegisterRef<bool>*> (sig[1]),
                                static_cast<RegisterRef<bool>*> (sig[2]));
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

    static const char* longName();
    static const char* shortName();
    static int numArgs();
    void describe(string& out, bool values) const;

    virtual void exec(TProgramCounter& pc) const {
        // SQL99 Part 2 Section 4.6.1 Comparison and Assignment of Booleans
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

    static InstructionSignature
    signature(StandardTypeDescriptorOrdinal type) {
        vector<StandardTypeDescriptorOrdinal>v(numArgs(), type);
        return InstructionSignature(shortName(), v);
    }

    static Instruction*
    create(InstructionSignature const & sig)
    {
        assert(sig.size() == numArgs());
        assert((sig[0])->type() == STANDARD_TYPE_BOOL);
        assert((sig[1])->type() == STANDARD_TYPE_BOOL);
        assert((sig[2])->type() == STANDARD_TYPE_BOOL);
        return new BoolGreater(static_cast<RegisterRef<bool>*> (sig[0]),
                               static_cast<RegisterRef<bool>*> (sig[1]),
                               static_cast<RegisterRef<bool>*> (sig[2]));
    }
};

class BoolGreaterEqual : public BoolInstruction
{
public:
    explicit
    BoolGreaterEqual(RegisterRef<bool>* result,
                     RegisterRef<bool>* op1,
                     RegisterRef<bool>* op2)
        : BoolInstruction(result, op1, op2)
    { }

    ~BoolGreaterEqual() { }

    static const char* longName();
    static const char* shortName();
    static int numArgs();
    void describe(string& out, bool values) const;

    virtual void exec(TProgramCounter& pc) const {
        // SQL99 Part 2 Section 4.6.1 Comparison and Assignment of Booleans
        pc++;
        if (mOp1->isNull() || mOp2->isNull()) {
            mResult->toNull();
        } else {
            bool op1 = mOp1->value();
            bool op2 = mOp2->value();
            if ((op1 == true && op2 == false) ||
                op1 == op2) {
                mResult->value(true);
            } else {
                mResult->value(false);
            }
        }
    }

    static InstructionSignature
    signature(StandardTypeDescriptorOrdinal type) {
        vector<StandardTypeDescriptorOrdinal>v(numArgs(), type);
        return InstructionSignature(shortName(), v);
    }

    static Instruction*
    create(InstructionSignature const & sig)
    {
        assert(sig.size() == numArgs());
        assert((sig[0])->type() == STANDARD_TYPE_BOOL);
        assert((sig[1])->type() == STANDARD_TYPE_BOOL);
        assert((sig[2])->type() == STANDARD_TYPE_BOOL);
        return new BoolGreaterEqual(static_cast<RegisterRef<bool>*> (sig[0]),
                                    static_cast<RegisterRef<bool>*> (sig[1]),
                                    static_cast<RegisterRef<bool>*> (sig[2]));
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

    static const char* longName();
    static const char* shortName();
    static int numArgs();
    void describe(string& out, bool values) const;

    virtual void exec(TProgramCounter& pc) const {
        // SQL99 Part 2 Section 4.6.1 Comparison and Assignment of Booleans
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

    static InstructionSignature
    signature(StandardTypeDescriptorOrdinal type) {
        vector<StandardTypeDescriptorOrdinal>v(numArgs(), type);
        return InstructionSignature(shortName(), v);
    }

    static Instruction*
    create(InstructionSignature const & sig)
    {
        assert(sig.size() == numArgs());
        assert((sig[0])->type() == STANDARD_TYPE_BOOL);
        assert((sig[1])->type() == STANDARD_TYPE_BOOL);
        assert((sig[2])->type() == STANDARD_TYPE_BOOL);
        return new BoolLess(static_cast<RegisterRef<bool>*> (sig[0]),
                            static_cast<RegisterRef<bool>*> (sig[1]),
                            static_cast<RegisterRef<bool>*> (sig[2]));
    }
};

class BoolLessEqual : public BoolInstruction
{
public:
    explicit
    BoolLessEqual(RegisterRef<bool>* result,
                  RegisterRef<bool>* op1,
                  RegisterRef<bool>* op2)
        : BoolInstruction(result, op1, op2)
    { }

    ~BoolLessEqual() { }

    static const char* longName();
    static const char* shortName();
    static int numArgs();
    void describe(string& out, bool values) const;

    virtual void exec(TProgramCounter& pc) const {
        // SQL99 Part 2 Section 4.6.1 Comparison and Assignment of Booleans
        pc++;
        if (mOp1->isNull() || mOp2->isNull()) {
            mResult->toNull();
        } else {
            bool op1 = mOp1->value();
            bool op2 = mOp2->value();
            if ((op1 == false && op2 == true) ||
                op1 == op2) {
                mResult->value(true);
            } else {
                mResult->value(false);
            }
        }
    }

    static InstructionSignature
    signature(StandardTypeDescriptorOrdinal type) {
        vector<StandardTypeDescriptorOrdinal>v(numArgs(), type);
        return InstructionSignature(shortName(), v);
    }

    static Instruction*
    create(InstructionSignature const & sig)
    {
        assert(sig.size() == numArgs());
        assert((sig[0])->type() == STANDARD_TYPE_BOOL);
        assert((sig[1])->type() == STANDARD_TYPE_BOOL);
        assert((sig[2])->type() == STANDARD_TYPE_BOOL);
        return new BoolLessEqual(static_cast<RegisterRef<bool>*> (sig[0]),
                                 static_cast<RegisterRef<bool>*> (sig[1]),
                                 static_cast<RegisterRef<bool>*> (sig[2]));
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

    static const char* longName();
    static const char* shortName();
    static int numArgs();
    void describe(string& out, bool values) const;

    virtual void exec(TProgramCounter& pc) const {
        pc++;
        if (mOp1->isNull()) {
            mResult->value(true);
        } else {
            mResult->value(false);
        }
    }

    static InstructionSignature
    signature(StandardTypeDescriptorOrdinal type) {
        vector<StandardTypeDescriptorOrdinal>v(numArgs(), type);
        return InstructionSignature(shortName(), v);
    }

    static Instruction*
    create(InstructionSignature const & sig)
    {
        assert(sig.size() == numArgs());
        assert((sig[0])->type() == STANDARD_TYPE_BOOL);
        assert((sig[1])->type() == STANDARD_TYPE_BOOL);
        return new BoolIsNull(static_cast<RegisterRef<bool>*> (sig[0]),
                              static_cast<RegisterRef<bool>*> (sig[1]));
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

    static const char* longName();
    static const char* shortName();
    static int numArgs();
    void describe(string& out, bool values) const;

    virtual void exec(TProgramCounter& pc) const {
        // SQL99 Part 2 Section 4.6.1 Comparison and Assignment of Booleans
        pc++;
        if (mOp1->isNull()) {
            mResult->value(false);
        } else {
            mResult->value(true);
        }
    }

    static InstructionSignature
    signature(StandardTypeDescriptorOrdinal type) {
        vector<StandardTypeDescriptorOrdinal>v(numArgs(), type);
        return InstructionSignature(shortName(), v);
    }

    static Instruction*
    create(InstructionSignature const & sig)
    {
        assert(sig.size() == numArgs());
        assert((sig[0])->type() == STANDARD_TYPE_BOOL);
        assert((sig[1])->type() == STANDARD_TYPE_BOOL);
        return new BoolIsNotNull(static_cast<RegisterRef<bool>*> (sig[0]),
                                 static_cast<RegisterRef<bool>*> (sig[1]));
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

    static const char* longName();
    static const char* shortName();
    static int numArgs();
    void describe(string& out, bool values) const;

    virtual void exec(TProgramCounter& pc) const {
        // SQL99 Part 2 Section 4.6.1 Comparison and Assignment of Booleans
        pc++;
        mResult->toNull();
    }

    static InstructionSignature
    signature(StandardTypeDescriptorOrdinal type) {
        vector<StandardTypeDescriptorOrdinal>v(numArgs(), type);
        return InstructionSignature(shortName(), v);
    }

    static Instruction*
    create(InstructionSignature const & sig)
    {
        assert(sig.size() == numArgs());
        assert((sig[0])->type() == STANDARD_TYPE_BOOL);
        return new BoolToNull(static_cast<RegisterRef<bool>*> (sig[0]));
    }
};

class BoolInstructionRegister : InstructionRegister {

    // TODO: Refactor registerTypes to class InstructionRegister
    template < class INSTCLASS2 >
    static void
    registerTypes(vector<StandardTypeDescriptorOrdinal> const & t)
    {
        for (uint i = 0; i < t.size(); i++) {
            StandardTypeDescriptorOrdinal type = t[i];
            InstructionSignature sig = INSTCLASS2::signature(type);
            switch(type) {
#define Fennel_InstructionRegisterSwitch_Bool 1
#include "fennel/disruptivetech/calc/InstructionRegisterSwitch.h"
            default:
                throw std::logic_error("Default InstructionRegister");
            }
        }
    }

public:
    static void
    registerInstructions()
    {
        vector<StandardTypeDescriptorOrdinal> t;
        t.push_back(STANDARD_TYPE_BOOL);

        // Have to do full fennel:: qualification of template
        // arguments below to prevent template argument 'TMPLT', of
        // this encapsulating class, from perverting NativeAdd into
        // NativeAdd<TMPLT> or something like
        // that. Anyway. Fennel::NativeAdd works just fine.
        registerTypes<fennel::BoolOr>(t);
        registerTypes<fennel::BoolAnd>(t);
        registerTypes<fennel::BoolNot>(t);
        registerTypes<fennel::BoolMove>(t);
        registerTypes<fennel::BoolRef>(t);
        registerTypes<fennel::BoolIs>(t);
        registerTypes<fennel::BoolIsNot>(t);
        registerTypes<fennel::BoolEqual>(t);
        registerTypes<fennel::BoolNotEqual>(t);
        registerTypes<fennel::BoolGreater>(t);
        registerTypes<fennel::BoolGreaterEqual>(t);
        registerTypes<fennel::BoolLess>(t);
        registerTypes<fennel::BoolLessEqual>(t);
        registerTypes<fennel::BoolIsNull>(t);
        registerTypes<fennel::BoolIsNotNull>(t);
        registerTypes<fennel::BoolToNull>(t);
    }
};


FENNEL_END_NAMESPACE

#endif

// End BoolInstruction.h

