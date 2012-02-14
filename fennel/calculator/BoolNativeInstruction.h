/*
// Licensed to DynamoBI Corporation (DynamoBI) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  DynamoBI licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at

//   http://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
*/
#ifndef Fennel_BoolNativeInstruction_Included
#define Fennel_BoolNativeInstruction_Included

#include "fennel/calculator/NativeInstruction.h"

FENNEL_BEGIN_NAMESPACE

/**
 * Support for operators that return booleans, i.e. comparison operators
 *
 * @author John Kalucki
 */
template<typename TMPLT>
class BoolNativeInstruction
    : public NativeInstruction<TMPLT>
{
public:
    explicit
    BoolNativeInstruction(
        RegisterRef<bool>* result,
        RegisterRef<TMPLT>* op1,
        StandardTypeDescriptorOrdinal nativeType)
        : NativeInstruction<TMPLT>(op1, nativeType),
          mResult(result)
    {}
    explicit
    BoolNativeInstruction(
        RegisterRef<bool>* result,
        RegisterRef<TMPLT>* op1,
        RegisterRef<TMPLT>* op2,
        StandardTypeDescriptorOrdinal nativeType)
        : NativeInstruction<TMPLT>(op1, op2, nativeType),
          mResult(result)
    {}
    virtual
    ~BoolNativeInstruction() {}

protected:
    RegisterRef<bool>* mResult;
};

template <typename TMPLT>
class BoolNativeEqual
    : public BoolNativeInstruction<TMPLT>
{
public:
    explicit
    BoolNativeEqual(
        RegisterRef<bool>* result,
        RegisterRef<TMPLT>* op1,
        RegisterRef<TMPLT>* op2,
        StandardTypeDescriptorOrdinal nativeType)
        : BoolNativeInstruction<TMPLT>(result, op1, op2, nativeType)
    {}
    virtual
    ~BoolNativeEqual() {}

    virtual void exec(TProgramCounter& pc) const {
        pc++;
        if (NativeInstruction<TMPLT>::mOp1->isNull()
            || NativeInstruction<TMPLT>::mOp2->isNull())
        {
            BoolNativeInstruction<TMPLT>::mResult->toNull();
        } else if (NativeInstruction<TMPLT>::mOp1->value()
            == NativeInstruction<TMPLT>::mOp2->value())
        {
            BoolNativeInstruction<TMPLT>::mResult->value(true);
        } else {
            BoolNativeInstruction<TMPLT>::mResult->value(false);
        }
    }

    static const char * longName()
    {
        return "BoolNativeEqual";
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
            BoolNativeInstruction<TMPLT>::mResult,
            NativeInstruction<TMPLT>::mOp1,
            NativeInstruction<TMPLT>::mOp2);
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
            BoolNativeEqual(
                static_cast<RegisterRef<bool>*> (sig[0]),
                static_cast<RegisterRef<TMPLT>*> (sig[1]),
                static_cast<RegisterRef<TMPLT>*> (sig[2]),
                (sig[1])->type());
    }
};

template <typename TMPLT>
class BoolNativeNotEqual : public BoolNativeInstruction<TMPLT>
{
public:
    explicit
    BoolNativeNotEqual(
        RegisterRef<bool>* result,
        RegisterRef<TMPLT>* op1,
        RegisterRef<TMPLT>* op2,
        StandardTypeDescriptorOrdinal nativeType)
        : BoolNativeInstruction<TMPLT>(result, op1, op2, nativeType)
    {}
    virtual
    ~BoolNativeNotEqual() {}

    virtual void exec(TProgramCounter& pc) const {
        pc++;
        if (NativeInstruction<TMPLT>::mOp1->isNull()
            || NativeInstruction<TMPLT>::mOp2->isNull())
        {
            BoolNativeInstruction<TMPLT>::mResult->toNull();
        } else if (NativeInstruction<TMPLT>::mOp1->value()
            == NativeInstruction<TMPLT>::mOp2->value())
        {
            BoolNativeInstruction<TMPLT>::mResult->value(false);
        } else {
            BoolNativeInstruction<TMPLT>::mResult->value(true);
        }
    }

    static const char * longName()
    {
        return "BoolNativeNotEqual";
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
            BoolNativeInstruction<TMPLT>::mResult,
            NativeInstruction<TMPLT>::mOp1,
            NativeInstruction<TMPLT>::mOp2);
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
            BoolNativeNotEqual(
                static_cast<RegisterRef<bool>*> (sig[0]),
                static_cast<RegisterRef<TMPLT>*> (sig[1]),
                static_cast<RegisterRef<TMPLT>*> (sig[2]),
                (sig[1])->type());
    }
};

template <typename TMPLT>
class BoolNativeGreater : public BoolNativeInstruction<TMPLT>
{
public:
    explicit
    BoolNativeGreater(
        RegisterRef<bool>* result,
        RegisterRef<TMPLT>* op1,
        RegisterRef<TMPLT>* op2,
        StandardTypeDescriptorOrdinal nativeType)
        : BoolNativeInstruction<TMPLT>(result, op1, op2, nativeType)
    {}
    virtual
    ~BoolNativeGreater() {}

    virtual void exec(TProgramCounter& pc) const {
        pc++;
        if (NativeInstruction<TMPLT>::mOp1->isNull()
            || NativeInstruction<TMPLT>::mOp2->isNull())
        {
            BoolNativeInstruction<TMPLT>::mResult->toNull();
        } else if (NativeInstruction<TMPLT>::mOp1->value() >
                   NativeInstruction<TMPLT>::mOp2->value())
        {
            BoolNativeInstruction<TMPLT>::mResult->value(true);
        } else {
            BoolNativeInstruction<TMPLT>::mResult->value(false);
        }
    }

    static const char * longName()
    {
        return "BoolNativeGreater";
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
            BoolNativeInstruction<TMPLT>::mResult,
            NativeInstruction<TMPLT>::mOp1,
            NativeInstruction<TMPLT>::mOp2);
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
            BoolNativeGreater(
                static_cast<RegisterRef<bool>*> (sig[0]),
                static_cast<RegisterRef<TMPLT>*> (sig[1]),
                static_cast<RegisterRef<TMPLT>*> (sig[2]),
                (sig[1])->type());
    }
};

template <typename TMPLT>
class BoolNativeGreaterEqual : public BoolNativeInstruction<TMPLT>
{
public:
    explicit
    BoolNativeGreaterEqual(
        RegisterRef<bool>* result,
        RegisterRef<TMPLT>* op1,
        RegisterRef<TMPLT>* op2,
        StandardTypeDescriptorOrdinal nativeType)
        : BoolNativeInstruction<TMPLT>(result, op1, op2, nativeType)
    {}
    virtual
    ~BoolNativeGreaterEqual() {}

    virtual void exec(TProgramCounter& pc) const {
        pc++;
        if (NativeInstruction<TMPLT>::mOp1->isNull()
            || NativeInstruction<TMPLT>::mOp2->isNull())
        {
            BoolNativeInstruction<TMPLT>::mResult->toNull();
        } else if (NativeInstruction<TMPLT>::mOp1->value()
            >= NativeInstruction<TMPLT>::mOp2->value())
        {
            BoolNativeInstruction<TMPLT>::mResult->value(true);
        } else {
            BoolNativeInstruction<TMPLT>::mResult->value(false);
        }
    }

    static const char * longName()
    {
        return "BoolNativeGreaterEqual";
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
            BoolNativeInstruction<TMPLT>::mResult,
            NativeInstruction<TMPLT>::mOp1,
            NativeInstruction<TMPLT>::mOp2);
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
            BoolNativeGreaterEqual(
                static_cast<RegisterRef<bool>*> (sig[0]),
                static_cast<RegisterRef<TMPLT>*> (sig[1]),
                static_cast<RegisterRef<TMPLT>*> (sig[2]),
                (sig[1])->type());
    }
};

template <typename TMPLT>
class BoolNativeLess : public BoolNativeInstruction<TMPLT>
{
public:
    explicit
    BoolNativeLess(
        RegisterRef<bool>* result,
        RegisterRef<TMPLT>* op1,
        RegisterRef<TMPLT>* op2,
        StandardTypeDescriptorOrdinal nativeType)
        : BoolNativeInstruction<TMPLT>(result, op1, op2, nativeType)
    {}
    virtual
    ~BoolNativeLess() {}

    virtual void exec(TProgramCounter& pc) const {
        pc++;
        if (NativeInstruction<TMPLT>::mOp1->isNull()
            || NativeInstruction<TMPLT>::mOp2->isNull())
        {
            BoolNativeInstruction<TMPLT>::mResult->toNull();
        } else if (NativeInstruction<TMPLT>::mOp1->value()
                   < NativeInstruction<TMPLT>::mOp2->value())
        {
            BoolNativeInstruction<TMPLT>::mResult->value(true);
        } else {
            BoolNativeInstruction<TMPLT>::mResult->value(false);
        }
    }

    static const char * longName()
    {
        return "BoolNativeLess";
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
            BoolNativeInstruction<TMPLT>::mResult,
            NativeInstruction<TMPLT>::mOp1,
            NativeInstruction<TMPLT>::mOp2);
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
            BoolNativeLess(
                static_cast<RegisterRef<bool>*> (sig[0]),
                static_cast<RegisterRef<TMPLT>*> (sig[1]),
                static_cast<RegisterRef<TMPLT>*> (sig[2]),
                (sig[1])->type());
    }
};

template <typename TMPLT>
class BoolNativeLessEqual : public BoolNativeInstruction<TMPLT>
{
public:
    explicit
    BoolNativeLessEqual(
        RegisterRef<bool>* result,
        RegisterRef<TMPLT>* op1,
        RegisterRef<TMPLT>* op2,
        StandardTypeDescriptorOrdinal nativeType)
        : BoolNativeInstruction<TMPLT>(result, op1, op2, nativeType)
    {}
    virtual
    ~BoolNativeLessEqual() {}

    virtual void exec(TProgramCounter& pc) const {
        pc++;
        if (NativeInstruction<TMPLT>::mOp1->isNull()
            || NativeInstruction<TMPLT>::mOp2->isNull())
        {
            BoolNativeInstruction<TMPLT>::mResult->toNull();
        } else if (NativeInstruction<TMPLT>::mOp1->value()
            <= NativeInstruction<TMPLT>::mOp2->value())
        {
            BoolNativeInstruction<TMPLT>::mResult->value(true);
        } else {
            BoolNativeInstruction<TMPLT>::mResult->value(false);
        }
    }

    static const char * longName()
    {
        return "BoolNativeLessEqual";
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
            BoolNativeInstruction<TMPLT>::mResult,
            NativeInstruction<TMPLT>::mOp1,
            NativeInstruction<TMPLT>::mOp2);
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
            BoolNativeLessEqual(
                static_cast<RegisterRef<bool>*> (sig[0]),
                static_cast<RegisterRef<TMPLT>*> (sig[1]),
                static_cast<RegisterRef<TMPLT>*> (sig[2]),
                (sig[1])->type());
    }
};

template <typename TMPLT>
class BoolNativeIsNull : public BoolNativeInstruction<TMPLT>
{
public:
    explicit
    BoolNativeIsNull(
        RegisterRef<bool>* result,
        RegisterRef<TMPLT>* op1,
        StandardTypeDescriptorOrdinal nativeType)
        : BoolNativeInstruction<TMPLT>(result, op1, nativeType)
    {}
    virtual
    ~BoolNativeIsNull() {}

    virtual void exec(TProgramCounter& pc) const {
        pc++;
        if (NativeInstruction<TMPLT>::mOp1->isNull()) {
            BoolNativeInstruction<TMPLT>::mResult->value(true);
        } else {
            BoolNativeInstruction<TMPLT>::mResult->value(false);
        }
    }

    static const char * longName()
    {
        return "BoolNativeIsNull";
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
            BoolNativeInstruction<TMPLT>::mResult,
            NativeInstruction<TMPLT>::mOp1,
            NativeInstruction<TMPLT>::mOp2);
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
        return new BoolNativeIsNull(
            static_cast<RegisterRef<bool>*> (sig[0]),
            static_cast<RegisterRef<TMPLT>*> (sig[1]),
            (sig[1])->type());
    }
};

template <typename TMPLT>
class BoolNativeIsNotNull : public BoolNativeInstruction<TMPLT>
{
public:
    explicit
    BoolNativeIsNotNull(
        RegisterRef<bool>* result,
        RegisterRef<TMPLT>* op1,
        StandardTypeDescriptorOrdinal nativeType)
        : BoolNativeInstruction<TMPLT>(result, op1, nativeType)
    {}
    virtual
    ~BoolNativeIsNotNull() {}

    virtual void exec(TProgramCounter& pc) const {
        pc++;
        if (NativeInstruction<TMPLT>::mOp1->isNull()) {
            BoolNativeInstruction<TMPLT>::mResult->value(false);
        } else {
            BoolNativeInstruction<TMPLT>::mResult->value(true);
        }
    }

    static const char * longName()
    {
        return "BoolNativeIsNotNull";
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
            BoolNativeInstruction<TMPLT>::mResult,
            NativeInstruction<TMPLT>::mOp1,
            NativeInstruction<TMPLT>::mOp2);
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
            BoolNativeIsNotNull(
                static_cast<RegisterRef<bool>*> (sig[0]),
                static_cast<RegisterRef<TMPLT>*> (sig[1]),
                (sig[1])->type());
    }
};

class FENNEL_CALCULATOR_EXPORT BoolNativeInstructionRegister
    : InstructionRegister
{

    // TODO: Refactor registerTypes to class InstructionRegister
    template < template <typename> class INSTCLASS2 >
    static void
    registerTypes(vector<StandardTypeDescriptorOrdinal> const &t)
    {
        for (uint i = 0; i < t.size(); i++) {
            StandardTypeDescriptorOrdinal type = t[i];
            // Type <char> below is a placeholder and is ignored.
            InstructionSignature sig = INSTCLASS2<char>::signature(type);
            switch (type) {
#define Fennel_InstructionRegisterSwitch_NativeNotBool 1
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
        t = InstructionSignature::typeVector
            (StandardTypeDescriptor::isNativeNotBool);

        // Have to do full fennel:: qualification of template
        // arguments below to prevent template argument 'TMPLT', of
        // this encapsulating class, from perverting NativeAdd into
        // NativeAdd<TMPLT> or something like
        // that. Anyway. Fennel::NativeAdd works just fine.
        registerTypes<fennel::BoolNativeEqual>(t);
        registerTypes<fennel::BoolNativeNotEqual>(t);
        registerTypes<fennel::BoolNativeGreater>(t);
        registerTypes<fennel::BoolNativeGreaterEqual>(t);
        registerTypes<fennel::BoolNativeLess>(t);
        registerTypes<fennel::BoolNativeLessEqual>(t);
        registerTypes<fennel::BoolNativeIsNull>(t);
        registerTypes<fennel::BoolNativeIsNotNull>(t);
    }
};

FENNEL_END_NAMESPACE

#endif

// End BoolNativeInstruction.h

