/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005-2009 The Eigenbase Project
// Copyright (C) 2004-2009 SQLstream, Inc.
// Copyright (C) 2009-2009 LucidEra, Inc.
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
*/
#ifndef Fennel_BoolPointerInstruction_Included
#define Fennel_BoolPointerInstruction_Included

#include "fennel/calculator/PointerInstruction.h"

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
    BoolPointerInstruction(
        RegisterRef<bool>* result,
        RegisterRef<PTR_TYPE>* op1,
        StandardTypeDescriptorOrdinal pointerType)
        : mResult(result),
          mOp1(op1),
          mOp2(),            // unused
          mPointerType(pointerType)
    {}
    explicit
    BoolPointerInstruction(
        RegisterRef<bool>* result,
        RegisterRef<PTR_TYPE>* op1,
        RegisterRef<PTR_TYPE>* op2,
        StandardTypeDescriptorOrdinal pointerType)
        : mResult(result),
          mOp1(op1),
          mOp2(op2),
          mPointerType(pointerType)
    {}
    ~BoolPointerInstruction() {
#ifndef __MSVC__
        // If (0) to reduce performance impact of template type checking
        if (0) {
            PointerInstruction_NotAPointerType<PTR_TYPE>();
        }
#endif
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
    BoolPointerEqual(
        RegisterRef<bool>* result,
        RegisterRef<PTR_TYPE>* op1,
        RegisterRef<PTR_TYPE>* op2,
        StandardTypeDescriptorOrdinal pointerType)
        : BoolPointerInstruction<PTR_TYPE>(result, op1, op2, pointerType)
    {}
    virtual
    ~BoolPointerEqual() {}

    virtual void exec(TProgramCounter& pc) const {
        pc++;

        if (BoolPointerInstruction<PTR_TYPE>::mOp1->isNull() ||
            BoolPointerInstruction<PTR_TYPE>::mOp2->isNull()) {
            BoolPointerInstruction<PTR_TYPE>::mResult->toNull();
        } else if (BoolPointerInstruction<PTR_TYPE>::mOp1->pointer() ==
                   BoolPointerInstruction<PTR_TYPE>::mOp2->pointer()) {
            BoolPointerInstruction<PTR_TYPE>::mResult->value(true);
        } else {
            BoolPointerInstruction<PTR_TYPE>::mResult->value(false);
        }
    }

    static const char * longName()
    {
        return "BoolPointerEqual";
    }

    static const char * shortName()
    {
        return "EQ";
    }

    static int numArgs()
    {
        return 3;
    }

    void describe(string& out, bool values) const {
        describeHelper(
            out, values, longName(), shortName(),
            BoolPointerInstruction<PTR_TYPE>::mResult,
            BoolPointerInstruction<PTR_TYPE>::mOp1,
            BoolPointerInstruction<PTR_TYPE>::mOp2);
    }

    static InstructionSignature
    signature(StandardTypeDescriptorOrdinal type) {
        vector<StandardTypeDescriptorOrdinal> v(numArgs(), type);
        v[0] = STANDARD_TYPE_BOOL;
        return InstructionSignature(shortName(), v);
    }

    static Instruction*
    create(InstructionSignature const & sig)
    {
        assert(sig.size() == numArgs());
        return new
            BoolPointerEqual(
                static_cast<RegisterRef<bool>*> (sig[0]),
                static_cast<RegisterRef<PTR_TYPE>*> (sig[1]),
                static_cast<RegisterRef<PTR_TYPE>*> (sig[2]),
                (sig[1])->type());
    }
};

template <typename PTR_TYPE>
class BoolPointerNotEqual : public BoolPointerInstruction<PTR_TYPE>
{
public:
    explicit
    BoolPointerNotEqual(
        RegisterRef<bool>* result,
        RegisterRef<PTR_TYPE>* op1,
        RegisterRef<PTR_TYPE>* op2,
        StandardTypeDescriptorOrdinal pointerType)
        : BoolPointerInstruction<PTR_TYPE>(result, op1, op2, pointerType)
    {}
    virtual
    ~BoolPointerNotEqual() {}

    virtual void exec(TProgramCounter& pc) const {
        pc++;
        if (BoolPointerInstruction<PTR_TYPE>::mOp1->isNull() ||
            BoolPointerInstruction<PTR_TYPE>::mOp2->isNull()) {
            BoolPointerInstruction<PTR_TYPE>::mResult->toNull();
        } else if (BoolPointerInstruction<PTR_TYPE>::mOp1->pointer() ==
                   BoolPointerInstruction<PTR_TYPE>::mOp2->pointer()) {
            BoolPointerInstruction<PTR_TYPE>::mResult->value(false);
        } else {
            BoolPointerInstruction<PTR_TYPE>::mResult->value(true);
        }
    }

    static const char * longName()
    {
        return "BoolPointerNotEqual";
    }

    static const char * shortName()
    {
        return "NE";
    }

    static int numArgs()
    {
        return 3;
    }

    void describe(string& out, bool values) const {
        describeHelper(
            out, values, longName(), shortName(),
            BoolPointerInstruction<PTR_TYPE>::mResult,
            BoolPointerInstruction<PTR_TYPE>::mOp1,
            BoolPointerInstruction<PTR_TYPE>::mOp2);
    }

    static InstructionSignature
    signature(StandardTypeDescriptorOrdinal type) {
        vector<StandardTypeDescriptorOrdinal> v(numArgs(), type);
        v[0] = STANDARD_TYPE_BOOL;
        return InstructionSignature(shortName(), v);
    }

    static Instruction*
    create(InstructionSignature const & sig)
    {
        assert(sig.size() == numArgs());
        return new
            BoolPointerNotEqual(
                static_cast<RegisterRef<bool>*> (sig[0]),
                static_cast<RegisterRef<PTR_TYPE>*> (sig[1]),
                static_cast<RegisterRef<PTR_TYPE>*> (sig[2]),
                (sig[1])->type());
    }
};

template <typename PTR_TYPE>
class BoolPointerGreater : public BoolPointerInstruction<PTR_TYPE>
{
public:
    explicit
    BoolPointerGreater(
        RegisterRef<bool>* result,
        RegisterRef<PTR_TYPE>* op1,
        RegisterRef<PTR_TYPE>* op2,
        StandardTypeDescriptorOrdinal pointerType)
        : BoolPointerInstruction<PTR_TYPE>(result, op1, op2, pointerType)
    {}
    virtual
    ~BoolPointerGreater() {}

    virtual void exec(TProgramCounter& pc) const {
        pc++;
        if (BoolPointerInstruction<PTR_TYPE>::mOp1->isNull() ||
            BoolPointerInstruction<PTR_TYPE>::mOp2->isNull()) {
            BoolPointerInstruction<PTR_TYPE>::mResult->toNull();
        } else if (BoolPointerInstruction<PTR_TYPE>::mOp1->pointer() >
                   BoolPointerInstruction<PTR_TYPE>::mOp2->pointer()) {
            BoolPointerInstruction<PTR_TYPE>::mResult->value(true);
        } else {
            BoolPointerInstruction<PTR_TYPE>::mResult->value(false);
        }
    }

    static const char * longName()
    {
        return "BoolPointerGreater";
    }

    static const char * shortName()
    {
        return "GT";
    }

    static int numArgs()
    {
        return 3;
    }

    void describe(string& out, bool values) const {
        describeHelper(
            out, values, longName(), shortName(),
            BoolPointerInstruction<PTR_TYPE>::mResult,
            BoolPointerInstruction<PTR_TYPE>::mOp1,
            BoolPointerInstruction<PTR_TYPE>::mOp2);
    }

    static InstructionSignature
    signature(StandardTypeDescriptorOrdinal type) {
        vector<StandardTypeDescriptorOrdinal> v(numArgs(), type);
        v[0] = STANDARD_TYPE_BOOL;
        return InstructionSignature(shortName(), v);
    }

    static Instruction*
    create(InstructionSignature const & sig)
    {
        assert(sig.size() == numArgs());
        return new
            BoolPointerGreater(
                static_cast<RegisterRef<bool>*> (sig[0]),
                static_cast<RegisterRef<PTR_TYPE>*> (sig[1]),
                static_cast<RegisterRef<PTR_TYPE>*> (sig[2]),
                (sig[1])->type());
    }
};

template <typename PTR_TYPE>
class BoolPointerGreaterEqual : public BoolPointerInstruction<PTR_TYPE>
{
public:
    explicit
    BoolPointerGreaterEqual(
        RegisterRef<bool>* result,
        RegisterRef<PTR_TYPE>* op1,
        RegisterRef<PTR_TYPE>* op2,
        StandardTypeDescriptorOrdinal pointerType)
        : BoolPointerInstruction<PTR_TYPE>(result, op1, op2, pointerType)
    {}
    virtual
    ~BoolPointerGreaterEqual() {}

    virtual void exec(TProgramCounter& pc) const {
        pc++;
        if (BoolPointerInstruction<PTR_TYPE>::mOp1->isNull() ||
            BoolPointerInstruction<PTR_TYPE>::mOp2->isNull()) {
            BoolPointerInstruction<PTR_TYPE>::mResult->toNull();
        } else if (BoolPointerInstruction<PTR_TYPE>::mOp1->pointer() >=
                   BoolPointerInstruction<PTR_TYPE>::mOp2->pointer()) {
            BoolPointerInstruction<PTR_TYPE>::mResult->value(true);
        } else {
            BoolPointerInstruction<PTR_TYPE>::mResult->value(false);
        }
    }

    static const char * longName()
    {
        return "BoolPointerGreaterEqual";
    }

    static const char * shortName()
    {
        return "GE";
    }

    static int numArgs()
    {
        return 3;
    }

    void describe(string& out, bool values) const {
        describeHelper(
            out, values, longName(), shortName(),
            BoolPointerInstruction<PTR_TYPE>::mResult,
            BoolPointerInstruction<PTR_TYPE>::mOp1,
            BoolPointerInstruction<PTR_TYPE>::mOp2);
    }

    static InstructionSignature
    signature(StandardTypeDescriptorOrdinal type) {
        vector<StandardTypeDescriptorOrdinal> v(numArgs(), type);
        v[0] = STANDARD_TYPE_BOOL;
        return InstructionSignature(shortName(), v);
    }

    static Instruction*
    create(InstructionSignature const & sig)
    {
        assert(sig.size() == numArgs());
        return new
            BoolPointerGreaterEqual(
                static_cast<RegisterRef<bool>*> (sig[0]),
                static_cast<RegisterRef<PTR_TYPE>*> (sig[1]),
                static_cast<RegisterRef<PTR_TYPE>*> (sig[2]),
                (sig[1])->type());
    }
};

template <typename PTR_TYPE>
class BoolPointerLess : public BoolPointerInstruction<PTR_TYPE>
{
public:
    explicit
    BoolPointerLess(
        RegisterRef<bool>* result,
        RegisterRef<PTR_TYPE>* op1,
        RegisterRef<PTR_TYPE>* op2,
        StandardTypeDescriptorOrdinal pointerType)
        : BoolPointerInstruction<PTR_TYPE>(result, op1, op2, pointerType)
    {}
    virtual
    ~BoolPointerLess() {}

    virtual void exec(TProgramCounter& pc) const {
        pc++;
        if (BoolPointerInstruction<PTR_TYPE>::mOp1->isNull() ||
            BoolPointerInstruction<PTR_TYPE>::mOp2->isNull()) {
            BoolPointerInstruction<PTR_TYPE>::mResult->toNull();
        } else if (BoolPointerInstruction<PTR_TYPE>::mOp1->pointer() <
                   BoolPointerInstruction<PTR_TYPE>::mOp2->pointer()) {
            BoolPointerInstruction<PTR_TYPE>::mResult->value(true);
        } else {
            BoolPointerInstruction<PTR_TYPE>::mResult->value(false);
        }
    }

    static const char * longName()
    {
        return "BoolPointerLess";
    }

    static const char * shortName()
    {
        return "LT";
    }

    static int numArgs()
    {
        return 3;
    }

    void describe(string& out, bool values) const {
        describeHelper(
            out, values, longName(), shortName(),
            BoolPointerInstruction<PTR_TYPE>::mResult,
            BoolPointerInstruction<PTR_TYPE>::mOp1,
            BoolPointerInstruction<PTR_TYPE>::mOp2);
    }

    static InstructionSignature
    signature(StandardTypeDescriptorOrdinal type) {
        vector<StandardTypeDescriptorOrdinal> v(numArgs(), type);
        v[0] = STANDARD_TYPE_BOOL;
        return InstructionSignature(shortName(), v);
    }

    static Instruction*
    create(InstructionSignature const & sig)
    {
        assert(sig.size() == numArgs());
        return new
            BoolPointerLess(
                static_cast<RegisterRef<bool>*> (sig[0]),
                static_cast<RegisterRef<PTR_TYPE>*> (sig[1]),
                static_cast<RegisterRef<PTR_TYPE>*> (sig[2]),
                (sig[1])->type());
    }
};

template <typename PTR_TYPE>
class BoolPointerLessEqual : public BoolPointerInstruction<PTR_TYPE>
{
public:
    explicit
    BoolPointerLessEqual(
        RegisterRef<bool>* result,
        RegisterRef<PTR_TYPE>* op1,
        RegisterRef<PTR_TYPE>* op2,
        StandardTypeDescriptorOrdinal pointerType)
        : BoolPointerInstruction<PTR_TYPE>(result, op1, op2, pointerType)
    {}
    virtual
    ~BoolPointerLessEqual() {}

    virtual void exec(TProgramCounter& pc) const {
        pc++;
        if (BoolPointerInstruction<PTR_TYPE>::mOp1->isNull() ||
            BoolPointerInstruction<PTR_TYPE>::mOp2->isNull()) {
            BoolPointerInstruction<PTR_TYPE>::mResult->toNull();
        } else if (BoolPointerInstruction<PTR_TYPE>::mOp1->pointer() <=
                   BoolPointerInstruction<PTR_TYPE>::mOp2->pointer()) {
            BoolPointerInstruction<PTR_TYPE>::mResult->value(true);
        } else {
            BoolPointerInstruction<PTR_TYPE>::mResult->value(false);
        }
    }

    static const char * longName()
    {
        return "BoolPointerLessEqual";
    }

    static const char * shortName()
    {
        return "LE";
    }

    static int numArgs()
    {
        return 3;
    }

    void describe(string& out, bool values) const {
        describeHelper(
            out, values, longName(), shortName(),
            BoolPointerInstruction<PTR_TYPE>::mResult,
            BoolPointerInstruction<PTR_TYPE>::mOp1,
            BoolPointerInstruction<PTR_TYPE>::mOp2);
    }

    static InstructionSignature
    signature(StandardTypeDescriptorOrdinal type) {
        vector<StandardTypeDescriptorOrdinal> v(numArgs(), type);
        v[0] = STANDARD_TYPE_BOOL;
        return InstructionSignature(shortName(), v);
    }

    static Instruction*
    create(InstructionSignature const & sig)
    {
        assert(sig.size() == numArgs());
        return new
            BoolPointerLessEqual(
                static_cast<RegisterRef<bool>*> (sig[0]),
                static_cast<RegisterRef<PTR_TYPE>*> (sig[1]),
                static_cast<RegisterRef<PTR_TYPE>*> (sig[2]),
                (sig[1])->type());
    }
};

template <typename PTR_TYPE>
class BoolPointerIsNull : public BoolPointerInstruction<PTR_TYPE>
{
public:
    explicit
    BoolPointerIsNull(
        RegisterRef<bool>* result,
        RegisterRef<PTR_TYPE>* op1,
        StandardTypeDescriptorOrdinal pointerType)
        : BoolPointerInstruction<PTR_TYPE>(result, op1, pointerType)
    {}
    virtual
    ~BoolPointerIsNull() {}

    virtual void exec(TProgramCounter& pc) const {
        pc++;
        if (BoolPointerInstruction<PTR_TYPE>::mOp1->isNull()) {
            BoolPointerInstruction<PTR_TYPE>::mResult->value(true);
        } else {
            BoolPointerInstruction<PTR_TYPE>::mResult->value(false);
        }
    }

    static const char * longName()
    {
        return "BoolPointerIsNull";
    }

    static const char * shortName()
    {
        return "ISNULL";
    }

    static int numArgs()
    {
        return 2;
    }

    void describe(string& out, bool values) const {
        describeHelper(
            out, values, longName(), shortName(),
            BoolPointerInstruction<PTR_TYPE>::mResult,
            BoolPointerInstruction<PTR_TYPE>::mOp1,
            BoolPointerInstruction<PTR_TYPE>::mOp2);
    }

    static InstructionSignature
    signature(StandardTypeDescriptorOrdinal type) {
        vector<StandardTypeDescriptorOrdinal> v(numArgs(), type);
        v[0] = STANDARD_TYPE_BOOL;
        return InstructionSignature(shortName(), v);
    }

    static Instruction*
    create(InstructionSignature const & sig)
    {
        assert(sig.size() == numArgs());
        return new
            BoolPointerIsNull(
                static_cast<RegisterRef<bool>*> (sig[0]),
                static_cast<RegisterRef<PTR_TYPE>*> (sig[1]),
                (sig[1])->type());
    }
};

template <typename PTR_TYPE>
class BoolPointerIsNotNull : public BoolPointerInstruction<PTR_TYPE>
{
public:
    explicit
    BoolPointerIsNotNull(
        RegisterRef<bool>* result,
        RegisterRef<PTR_TYPE>* op1,
        StandardTypeDescriptorOrdinal pointerType)
        : BoolPointerInstruction<PTR_TYPE>(result, op1, pointerType)
    {}
    virtual
    ~BoolPointerIsNotNull() {}

    virtual void exec(TProgramCounter& pc) const {
        pc++;
        if (BoolPointerInstruction<PTR_TYPE>::mOp1->isNull()) {
            BoolPointerInstruction<PTR_TYPE>::mResult->value(false);
        } else {
            BoolPointerInstruction<PTR_TYPE>::mResult->value(true);
        }
    }

    static const char * longName()
    {
        return "BoolPointerIsNotNull";
    }

    static const char * shortName()
    {
        return "ISNOTNULL";
    }

    static int numArgs()
    {
        return 2;
    }

    void describe(string& out, bool values) const {
        describeHelper(
            out, values, longName(), shortName(),
            BoolPointerInstruction<PTR_TYPE>::mResult,
            BoolPointerInstruction<PTR_TYPE>::mOp1,
            BoolPointerInstruction<PTR_TYPE>::mOp2);
    }

    static InstructionSignature
    signature(StandardTypeDescriptorOrdinal type) {
        vector<StandardTypeDescriptorOrdinal> v(numArgs(), type);
        v[0] = STANDARD_TYPE_BOOL;
        return InstructionSignature(shortName(), v);
    }

    static Instruction*
    create(InstructionSignature const & sig)
    {
        assert(sig.size() == numArgs());
        return new
            BoolPointerIsNotNull(
                static_cast<RegisterRef<bool>*> (sig[0]),
                static_cast<RegisterRef<PTR_TYPE>*> (sig[1]),
                (sig[1])->type());
    }
};

class FENNEL_CALCULATOR_EXPORT BoolPointerInstructionRegister
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
        registerTypes<fennel::BoolPointerEqual>(t);
        registerTypes<fennel::BoolPointerNotEqual>(t);
        registerTypes<fennel::BoolPointerGreater>(t);
        registerTypes<fennel::BoolPointerGreaterEqual>(t);
        registerTypes<fennel::BoolPointerLess>(t);
        registerTypes<fennel::BoolPointerLessEqual>(t);
        registerTypes<fennel::BoolPointerIsNull>(t);
        registerTypes<fennel::BoolPointerIsNotNull>(t);
    }
};

FENNEL_END_NAMESPACE

#endif

// End BoolPointerInstruction.h

