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
// Jump Instruction
//
// Instruction->Jump
*/
#ifndef Fennel_JumpInstruction_Included
#define Fennel_JumpInstruction_Included

#include "fennel/calc/Instruction.h"

FENNEL_BEGIN_NAMESPACE

class JumpInstruction : public Instruction
{
public:
    explicit
    JumpInstruction(TProgramCounter pc): mJumpTo(pc), mOp() { }
    explicit
    JumpInstruction(TProgramCounter pc, RegisterRef<bool>* op): mJumpTo(pc), mOp(op) { }
    virtual
    ~JumpInstruction() { }

    virtual void setCalc(Calculator* calcP) {
        mOp->setCalc(calcP);    // note: may be unused if instruction has no operands
    }

protected:
    TProgramCounter mJumpTo;
    RegisterRef<bool>* mOp;     // may be unused

    virtual void describeHelper(string &out,
                                bool values,
                                const char* longName,
                                const char* shortName) const;
};

class Jump : public JumpInstruction
{
public: 
    explicit
    Jump(TProgramCounter pc)
        : JumpInstruction(pc)
    { }
    virtual
    ~Jump() { }

    virtual void exec(TProgramCounter& pc) const {
        pc = mJumpTo;
    }

    const char * longName() const;
    const char * shortName() const;
    void describe(string &out, bool values) const;
};

class JumpTrue : public JumpInstruction
{
public: 
    explicit
    JumpTrue(TProgramCounter pc, RegisterRef<bool>* op)
        : JumpInstruction (pc, op) 
    { }
    virtual
    ~JumpTrue() { }

    virtual void exec(TProgramCounter& pc) const { 
        if (!mOp->isNull() && mOp->getV() == true) {
            pc = mJumpTo;
        } else {
            pc++;
        }
    }

    const char * longName() const;
    const char * shortName() const;
    void describe(string &out, bool values) const;
};

class JumpFalse : public JumpInstruction
{
public: 
    explicit
    JumpFalse(TProgramCounter pc, RegisterRef<bool>* op)
        : JumpInstruction (pc, op) 
    { }
    virtual
    ~JumpFalse() { }

    virtual void exec(TProgramCounter& pc) const { 
        if (!mOp->isNull() && mOp->getV() == false) {
            pc = mJumpTo;
        } else {
            pc++;
        }
        
    }

    const char * longName() const;
    const char * shortName() const;
    void describe(string &out, bool values) const;
};

class JumpNull : public JumpInstruction
{
public: 
    explicit
    JumpNull(TProgramCounter pc, RegisterRef<bool>* op)
        : JumpInstruction (pc, op) 
    { }
    virtual
    ~JumpNull() { }

    virtual void exec(TProgramCounter& pc) const { 
        if (mOp->isNull()) {
            pc = mJumpTo;
        } else {
            pc++;
        }
        
    }

    const char * longName() const;
    const char * shortName() const;
    void describe(string &out, bool values) const;
};

class JumpNotNull : public JumpInstruction
{
public: 
    explicit
    JumpNotNull(TProgramCounter pc, RegisterRef<bool>* op)
        : JumpInstruction (pc, op) 
    { }
    virtual
    ~JumpNotNull() { }

    virtual void exec(TProgramCounter& pc) const { 
        if (!mOp->isNull()) {
            pc = mJumpTo;
        } else {
            pc++;
        }
    }

    const char * longName() const;
    const char * shortName() const;
    void describe(string &out, bool values) const;
};

FENNEL_END_NAMESPACE

#endif

// End JumpInstruction.h

