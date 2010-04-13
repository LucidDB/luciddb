/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005 The Eigenbase Project
// Copyright (C) 2004 SQLstream, Inc.
// Copyright (C) 2009 Dynamo BI Corporation
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
#ifndef Fennel_IntegralNativeInstruction_Included
#define Fennel_IntegralNativeInstruction_Included

#include "fennel/calculator/NativeInstruction.h"

FENNEL_BEGIN_NAMESPACE

/**
 * Template for native integral (int, long, enum, etc.) types only
 *
 * Support for bitwise operators, modulus and the like that are not defined
 * on floating point numbers
 */

//
// IntegralNativeInstruction_NotAnIntegralType
//
// Force the use of a (non-pointer) native integral type.
// Note: You cannot use typedefs like int32_t here or the
// built-in names thereof won't work. By using the built-in
// type name, you can support the built-in and typedefs
// built on top.
//
template <class T> class IntegralNativeInstruction_NotAnIntegralType;
template <> class IntegralNativeInstruction_NotAnIntegralType<char> {};
template <> class IntegralNativeInstruction_NotAnIntegralType<short> {};
template <> class IntegralNativeInstruction_NotAnIntegralType<int> {};
template <> class IntegralNativeInstruction_NotAnIntegralType<long> {};
template <> class IntegralNativeInstruction_NotAnIntegralType<long long> {};
template <> class IntegralNativeInstruction_NotAnIntegralType<unsigned char> {};
template <> class IntegralNativeInstruction_NotAnIntegralType<
    unsigned short> {};
template <> class IntegralNativeInstruction_NotAnIntegralType<unsigned int> {};
template <> class IntegralNativeInstruction_NotAnIntegralType<unsigned long> {};
template <> class IntegralNativeInstruction_NotAnIntegralType<
    unsigned long long> {};
template <> class IntegralNativeInstruction_NotAnIntegralType<signed char> {};

template<typename TMPLT>
class IntegralNativeInstruction : public NativeInstruction<TMPLT>
{
public:
    explicit
    IntegralNativeInstruction(
        RegisterRef<TMPLT>* result,
        RegisterRef<TMPLT>* op1,
        StandardTypeDescriptorOrdinal nativeType)
        : NativeInstruction<TMPLT>(op1, nativeType),
          mResult(result)
    {
        assert(StandardTypeDescriptor::isIntegralNative(nativeType));
    }

    explicit
    IntegralNativeInstruction(
        RegisterRef<TMPLT>* result,
        RegisterRef<TMPLT>* op1,
        RegisterRef<TMPLT>* op2,
        StandardTypeDescriptorOrdinal nativeType)
        : NativeInstruction<TMPLT>(op1, op2, nativeType),
          mResult(result)
    {
        assert(StandardTypeDescriptor::isIntegralNative(nativeType));
    }

    ~IntegralNativeInstruction()
    {
        // If (0) to reduce performance impact of template type checking
        if (0) {
            IntegralNativeInstruction_NotAnIntegralType<TMPLT>();
        }
    }

protected:
    RegisterRef<TMPLT>* mResult;
};

template <typename TMPLT>
class IntegralNativeMod : public IntegralNativeInstruction<TMPLT>
{
public:
    explicit
    IntegralNativeMod(
        RegisterRef<TMPLT>* result,
        RegisterRef<TMPLT>* op1,
        RegisterRef<TMPLT>* op2,
        StandardTypeDescriptorOrdinal nativeType)
        : IntegralNativeInstruction<TMPLT>(result, op1, op2, nativeType)
    {}

    virtual
    ~IntegralNativeMod() {}

    virtual void exec(TProgramCounter& pc) const {
        pc++;
        // SQL99 Part 2 Section 6.17 General Rule 10
        if (NativeInstruction<TMPLT>::mOp1->isNull()
            || NativeInstruction<TMPLT>::mOp2->isNull())
        {
            IntegralNativeInstruction<TMPLT>::mResult->toNull();
        } else {
            // encourage into register
            TMPLT o2 = NativeInstruction<TMPLT>::mOp2->value();
            if (o2 == 0) {
                IntegralNativeInstruction<TMPLT>::mResult->toNull();
                // SQL99 22.1 SQLState dataexception class 22,
                // division by zero subclass 012
                throw CalcMessage(
                    SqlState::instance().code22012(), pc - 1);
            }
            IntegralNativeInstruction<TMPLT>::mResult->value(
                NativeInstruction<TMPLT>::mOp1->value() % o2);
        }
    }

    static const char * longName()
    {
        return "IntegralNativeMod";
    }

    static const char * shortName()
    {
        return "MOD";
    }

    static int numArgs()
    {
        return 3;
    }

    void describe(string& out, bool values) const {
        describeHelper(
            out, values, longName(), shortName(),
            IntegralNativeInstruction<TMPLT>::mResult,
            NativeInstruction<TMPLT>::mOp1,
            NativeInstruction<TMPLT>::mOp2);
    }

    static InstructionSignature
    signature(StandardTypeDescriptorOrdinal type) {
        vector<StandardTypeDescriptorOrdinal> v(numArgs(), type);
        return InstructionSignature(shortName(), v);
    }

    static Instruction*
    create(InstructionSignature const & sig)
    {
        assert(sig.size() == numArgs());
        return new IntegralNativeMod(
            static_cast<RegisterRef<TMPLT>*> (sig[0]),
            static_cast<RegisterRef<TMPLT>*> (sig[1]),
            static_cast<RegisterRef<TMPLT>*> (sig[2]),
            (sig[0])->type());
    }
};

template <typename TMPLT>
class IntegralNativeAnd : public IntegralNativeInstruction<TMPLT>
{
public:
    explicit
    IntegralNativeAnd(
        RegisterRef<TMPLT>* result,
        RegisterRef<TMPLT>* op1,
        RegisterRef<TMPLT>* op2,
        StandardTypeDescriptorOrdinal nativeType)
        : IntegralNativeInstruction<TMPLT>(result, op1, op2, nativeType)
    {}

    virtual
    ~IntegralNativeAnd() {}

    virtual void exec(TProgramCounter& pc) const {
        // making up null semantics here
        if (NativeInstruction<TMPLT>::mOp1->isNull()
            || NativeInstruction<TMPLT>::mOp2->isNull())
        {
            IntegralNativeInstruction<TMPLT>::mResult->toNull();
        } else {
            IntegralNativeInstruction<TMPLT>::mResult->value(
                NativeInstruction<TMPLT>::mOp1->value()
                & NativeInstruction<TMPLT>::mOp2->value());
        }
        pc++;
    }

    static const char * longName()
    {
        return "IntegralNativeAnd";
    }

    static const char * shortName()
    {
        return "AND";
    }

    static int numArgs()
    {
        return 3;
    }

    void describe(string& out, bool values) const {
        describeHelper(
            out, values, longName(), shortName(),
            IntegralNativeInstruction<TMPLT>::mResult,
            NativeInstruction<TMPLT>::mOp1,
            NativeInstruction<TMPLT>::mOp2);
    }

    static InstructionSignature
    signature(StandardTypeDescriptorOrdinal type) {
        vector<StandardTypeDescriptorOrdinal> v(numArgs(), type);
        return InstructionSignature(shortName(), v);
    }

    static Instruction*
    create(InstructionSignature const & sig)
    {
        assert(sig.size() == numArgs());
        return new IntegralNativeAnd(
            static_cast<RegisterRef<TMPLT>*> (sig[0]),
            static_cast<RegisterRef<TMPLT>*> (sig[1]),
            static_cast<RegisterRef<TMPLT>*> (sig[2]),
            (sig[0])->type());
    }
};

template <typename TMPLT>
class IntegralNativeOr : public IntegralNativeInstruction<TMPLT>
{
public:
    explicit
    IntegralNativeOr(
        RegisterRef<TMPLT>* result,
        RegisterRef<TMPLT>* op1,
        RegisterRef<TMPLT>* op2,
        StandardTypeDescriptorOrdinal nativeType)
        : IntegralNativeInstruction<TMPLT>(result, op1, op2, nativeType)
    {}
    virtual
    ~IntegralNativeOr() {}

    virtual void exec(TProgramCounter& pc) const {
        pc++;
        // making up null semantics here
        if (NativeInstruction<TMPLT>::mOp1->isNull()
            || NativeInstruction<TMPLT>::mOp2->isNull())
        {
            IntegralNativeInstruction<TMPLT>::mResult->toNull();
        } else {
            IntegralNativeInstruction<TMPLT>::mResult->value(
                NativeInstruction<TMPLT>::mOp1->value() |
                NativeInstruction<TMPLT>::mOp2->value());
        }
    }

    static const char * longName()
    {
        return "IntegralNativeOr";
    }

    static const char * shortName()
    {
        return "OR";
    }

    static int numArgs()
    {
        return 3;
    }

    void describe(string& out, bool values) const {
        describeHelper(
            out, values, longName(), shortName(),
            IntegralNativeInstruction<TMPLT>::mResult,
            NativeInstruction<TMPLT>::mOp1,
            NativeInstruction<TMPLT>::mOp2);
    }

    static InstructionSignature
    signature(StandardTypeDescriptorOrdinal type) {
        vector<StandardTypeDescriptorOrdinal> v(numArgs(), type);
        return InstructionSignature(shortName(), v);
    }

    static Instruction*
    create(InstructionSignature const & sig)
    {
        assert(sig.size() == numArgs());
        return new IntegralNativeOr(
            static_cast<RegisterRef<TMPLT>*> (sig[0]),
            static_cast<RegisterRef<TMPLT>*> (sig[1]),
            static_cast<RegisterRef<TMPLT>*> (sig[2]),
            (sig[0])->type());
    }
};

template <typename TMPLT>
class IntegralNativeShiftLeft : public IntegralNativeInstruction<TMPLT>
{
public:
    explicit
    IntegralNativeShiftLeft(
        RegisterRef<TMPLT>* result,
        RegisterRef<TMPLT>* op1,
        RegisterRef<TMPLT>* op2,
        StandardTypeDescriptorOrdinal nativeType)
        : IntegralNativeInstruction<TMPLT>(result, op1, op2, nativeType)
    {}
    virtual
    ~IntegralNativeShiftLeft() {}

    virtual void exec(TProgramCounter& pc) const {
        pc++;
        // making up null semantics here
        if (NativeInstruction<TMPLT>::mOp1->isNull()
            || NativeInstruction<TMPLT>::mOp2->isNull())
        {
            IntegralNativeInstruction<TMPLT>::mResult->toNull();
        } else {
            IntegralNativeInstruction<TMPLT>::mResult->value(
                NativeInstruction<TMPLT>::mOp1->value()
                << NativeInstruction<TMPLT>::mOp2->value());
        }
    }

    static const char * longName()
    {
        return "IntegralNativeShiftLeft";
    }

    static const char * shortName()
    {
        return "SHFL";
    }

    static int numArgs()
    {
        return 3;
    }

    void describe(string& out, bool values) const {
        describeHelper(
            out, values, longName(), shortName(),
            IntegralNativeInstruction<TMPLT>::mResult,
            NativeInstruction<TMPLT>::mOp1,
            NativeInstruction<TMPLT>::mOp2);
    }

    static InstructionSignature
    signature(StandardTypeDescriptorOrdinal type) {
        vector<StandardTypeDescriptorOrdinal> v(numArgs(), type);
        return InstructionSignature(shortName(), v);
    }

    static Instruction*
    create(InstructionSignature const & sig)
    {
        assert(sig.size() == numArgs());
        return new
            IntegralNativeShiftLeft(
                static_cast<RegisterRef<TMPLT>*> (sig[0]),
                static_cast<RegisterRef<TMPLT>*> (sig[1]),
                static_cast<RegisterRef<TMPLT>*> (sig[2]),
                (sig[0])->type());
    }
};

template <typename TMPLT>
class IntegralNativeShiftRight : public IntegralNativeInstruction<TMPLT>
{
public:
    explicit
    IntegralNativeShiftRight(
        RegisterRef<TMPLT>* result,
        RegisterRef<TMPLT>* op1,
        RegisterRef<TMPLT>* op2,
        StandardTypeDescriptorOrdinal nativeType)
        : IntegralNativeInstruction<TMPLT>(result, op1, op2, nativeType)
    {}
    virtual
    ~IntegralNativeShiftRight() {}

    virtual void exec(TProgramCounter& pc) const {
        pc++;
        // making up null semantics here
        if (NativeInstruction<TMPLT>::mOp1->isNull()
            || NativeInstruction<TMPLT>::mOp2->isNull())
        {
            IntegralNativeInstruction<TMPLT>::mResult->toNull();
        } else {
            IntegralNativeInstruction<TMPLT>::mResult->value(
                NativeInstruction<TMPLT>::mOp1->value()
                >> NativeInstruction<TMPLT>::mOp2->value());
        }
    }

    static const char * longName()
    {
        return "IntegralNativeShiftRight";
    }

    static const char * shortName()
    {
        return "SHFR";
    }

    static int numArgs()
    {
        return 3;
    }

    void describe(string& out, bool values) const {
        describeHelper(
            out, values, longName(), shortName(),
            IntegralNativeInstruction<TMPLT>::mResult,
            NativeInstruction<TMPLT>::mOp1,
            NativeInstruction<TMPLT>::mOp2);
    }

    static InstructionSignature
    signature(StandardTypeDescriptorOrdinal type) {
        vector<StandardTypeDescriptorOrdinal> v(numArgs(), type);
        return InstructionSignature(shortName(), v);
    }

    static Instruction*
    create(InstructionSignature const & sig)
    {
        assert(sig.size() == numArgs());
        return new
            IntegralNativeShiftRight(
                static_cast<RegisterRef<TMPLT>*> (sig[0]),
                static_cast<RegisterRef<TMPLT>*> (sig[1]),
                static_cast<RegisterRef<TMPLT>*> (sig[2]),
                (sig[0])->type());
    }
};

class FENNEL_CALCULATOR_EXPORT IntegralNativeInstructionRegister
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
#define Fennel_InstructionRegisterSwitch_Integral 1
#include "fennel/calculator/InstructionRegisterSwitch.h"
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
        t = InstructionSignature::typeVector(StandardTypeDescriptor::isExact);

        // Have to do full fennel:: qualification of template
        // arguments below to prevent template argument 'TMPLT', of
        // this encapsulating class, from perverting NativeAdd into
        // NativeAdd<TMPLT> or something like
        // that. Anyway. Fennel::NativeAdd works just fine.
        registerTypes<fennel::IntegralNativeMod>(t);
        registerTypes<fennel::IntegralNativeAnd>(t);
        registerTypes<fennel::IntegralNativeOr>(t);
        registerTypes<fennel::IntegralNativeShiftLeft>(t);
        registerTypes<fennel::IntegralNativeShiftRight>(t);
    }
};


FENNEL_END_NAMESPACE

#endif

// End IntegralNativeInstruction.h

