/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2004-2007 Disruptive Tech
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
// NativeNativeInstruction
//
// Instruction->NativeInstruction->NativeNativeInstruction
//
// NativeInstructions that return a Native
*/
#ifndef Fennel_NativeNativeInstruction_Included
#define Fennel_NativeNativeInstruction_Included

#include "fennel/disruptivetech/calc/NativeInstruction.h"
#include <math.h>
#include "NoisyArithmetic.h"

FENNEL_BEGIN_NAMESPACE

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
        if (NativeInstruction<TMPLT>::mOp1->isNull() || 
            NativeInstruction<TMPLT>::mOp2->isNull()) {
            // SQL99 Part 2 Section 6.26 General Rule 1
            NativeNativeInstruction<TMPLT>::mResult->toNull();
        } else {
            // set result to NULL in case an exception is raised (TODO:review this!)
            NativeNativeInstruction<TMPLT>::mResult->toNull();
            NativeNativeInstruction<TMPLT>::mResult->
               value( Noisy<TMPLT>::add( pc-1,
                    NativeInstruction<TMPLT>::mOp1->value(),
                    NativeInstruction<TMPLT>::mOp2->value()));
        }
    }

    static const char * longName() { return "NativeAdd"; }
    static const char * shortName() { return "ADD"; } 
    static int numArgs() { return 3; }
    void describe(string& out, bool values) const {
        describeHelper(out, values, longName(), shortName(),
                       NativeNativeInstruction<TMPLT>::mResult, 
                       NativeInstruction<TMPLT>::mOp1, 
                       NativeInstruction<TMPLT>::mOp2);
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
        return new NativeAdd(static_cast<RegisterRef<TMPLT>*> (sig[0]),
                             static_cast<RegisterRef<TMPLT>*> (sig[1]),
                             static_cast<RegisterRef<TMPLT>*> (sig[2]),
                             (sig[0])->type());
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
        if (NativeInstruction<TMPLT>::mOp1->isNull() || 
            NativeInstruction<TMPLT>::mOp2->isNull()) {
            // SQL99 Part 2 Section 6.26 General Rule 1
            NativeNativeInstruction<TMPLT>::mResult->toNull();
        } else {
            // set result to NULL in case an exception is raised (TODO:review this!)
            NativeNativeInstruction<TMPLT>::mResult->toNull();
            NativeNativeInstruction<TMPLT>::mResult->
               value( Noisy<TMPLT>::sub( pc-1,
                    NativeInstruction<TMPLT>::mOp1->value(),
                    NativeInstruction<TMPLT>::mOp2->value()));
        }
    }

    static char const * const longName() { return "NativeSub"; }
    static char const * const shortName() { return "SUB"; }
    static int numArgs() { return 3; }
    void describe(string& out, bool values) const {
        describeHelper(out, values, longName(), shortName(),
                       NativeNativeInstruction<TMPLT>::mResult, 
                       NativeInstruction<TMPLT>::mOp1, 
                       NativeInstruction<TMPLT>::mOp2);
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
        return new NativeSub(static_cast<RegisterRef<TMPLT>*> (sig[0]),
                             static_cast<RegisterRef<TMPLT>*> (sig[1]),
                             static_cast<RegisterRef<TMPLT>*> (sig[2]),
                             (sig[0])->type());
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
        if (NativeInstruction<TMPLT>::mOp1->isNull() || 
            NativeInstruction<TMPLT>::mOp2->isNull()) {
            // SQL99 Part 2 Section 6.26 General Rule 1
            NativeNativeInstruction<TMPLT>::mResult->toNull();
        } else {
            // set result to NULL in case an exception is raised (TODO:review this!)
            NativeNativeInstruction<TMPLT>::mResult->toNull();
            NativeNativeInstruction<TMPLT>::mResult->
               value( Noisy<TMPLT>::mul( pc-1,
                    NativeInstruction<TMPLT>::mOp1->value(),
                    NativeInstruction<TMPLT>::mOp2->value()));
        }
    }

    static char const * const longName() { return "NativeMul"; }
    static char const * const shortName() { return "MUL"; }
    static int numArgs() { return 3; }
    void describe(string& out, bool values) const {
        describeHelper(out, values, longName(), shortName(),
                       NativeNativeInstruction<TMPLT>::mResult, 
                       NativeInstruction<TMPLT>::mOp1, 
                       NativeInstruction<TMPLT>::mOp2);
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
        return new NativeMul(static_cast<RegisterRef<TMPLT>*> (sig[0]),
                             static_cast<RegisterRef<TMPLT>*> (sig[1]),
                             static_cast<RegisterRef<TMPLT>*> (sig[2]),
                             (sig[0])->type());
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
        if (NativeInstruction<TMPLT>::mOp1->isNull() || 
            NativeInstruction<TMPLT>::mOp2->isNull()) {
            // SQL99 Part 2 Section 6.26 General Rule 1
            NativeNativeInstruction<TMPLT>::mResult->toNull();
        } else {
#if 0
  JR 6/07, now thrown in NoisyArithmetic ...
            TMPLT o2 = NativeInstruction<TMPLT>::mOp2->value(); // encourage into register
            if (o2 == 0) {
                // SQL99 Part 2 Section 6.26 General Rule 4
                NativeNativeInstruction<TMPLT>::mResult->toNull();
                // SQL99 Part 2 Section 22.1 SQLState dataexception class 22,
                // division by zero subclass 012
                throw CalcMessage("22012", pc - 1); 
            }
#endif
            // set result to NULL in case an exception is raised (TODO:review this!)
            NativeNativeInstruction<TMPLT>::mResult->toNull();
            NativeNativeInstruction<TMPLT>::mResult->
               value( Noisy<TMPLT>::div( pc-1,
                    NativeInstruction<TMPLT>::mOp1->value(),
                    NativeInstruction<TMPLT>::mOp2->value()));
        }
    }

    static char const * const longName() { return "NativeDiv"; } 
    static char const * const shortName() { return "DIV"; } 
    static int numArgs() { return 3; }
    void describe(string& out, bool values) const {
        describeHelper(out, values, longName(), shortName(),
                       NativeNativeInstruction<TMPLT>::mResult, 
                       NativeInstruction<TMPLT>::mOp1, 
                       NativeInstruction<TMPLT>::mOp2);
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
        return new NativeDiv(static_cast<RegisterRef<TMPLT>*> (sig[0]),
                             static_cast<RegisterRef<TMPLT>*> (sig[1]),
                             static_cast<RegisterRef<TMPLT>*> (sig[2]),
                             (sig[0])->type());
    }
};

// NativeNeg implements monadic arithmetic operator '-' (unary minus)
// See SQL99 Part 2 Section 6.26 General Rule 3.
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
        if (NativeInstruction<TMPLT>::mOp1->isNull()) {
            // SQL99 Part 2 Section 6.26 General Rule 1
            NativeNativeInstruction<TMPLT>::mResult->toNull();
        } else {
            // set result to NULL in case an exception is raised (TODO:review this!)
            NativeNativeInstruction<TMPLT>::mResult->toNull();
            NativeNativeInstruction<TMPLT>::mResult->
               value( Noisy<TMPLT>::neg( pc-1,
                    NativeInstruction<TMPLT>::mOp1->value()));
        }
    }
    static char const * const longName() { return "NativeNeg"; }
    static char const * const shortName() { return "NEG"; }
    static int numArgs() { return 2; }
    void describe(string& out, bool values) const {
        describeHelper(out, values, longName(), shortName(),
                       NativeNativeInstruction<TMPLT>::mResult, 
                       NativeInstruction<TMPLT>::mOp1, 
                       NativeInstruction<TMPLT>::mOp2);
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
        return new NativeNeg(static_cast<RegisterRef<TMPLT>*> (sig[0]),
                             static_cast<RegisterRef<TMPLT>*> (sig[1]),
                             (sig[0])->type());
    }
};

template <class TMPLT>
class NativeRoundHelp {
public:
    static void r(TMPLT& result, TMPLT op1) {
        // no-op
        result = op1;
    }
};

template<>
class NativeRoundHelp<double> {
public:
    static void r(double& result, double op1) {
        // implements round away from zero
        result = round(op1);
    }
};

template<>
class NativeRoundHelp<float> {
public:
    static void r(float& result, float op1) {
        // implements round away from zero
        result = roundf(op1);
    }
};

// See SQL99 Part 2 Section 4.5 Numbers, paragraph 4, for a discussion on
// rounding away from zero.
// NativeRound does a round "away from zero" (e.g. -0.5 -> -1.0)
template <typename TMPLT>
class NativeRound : public NativeNativeInstruction<TMPLT>
{
public: 
    explicit
    NativeRound(RegisterRef<TMPLT>* result,
                RegisterRef<TMPLT>* op1,
                StandardTypeDescriptorOrdinal nativeType)
        : NativeNativeInstruction<TMPLT>(result, op1, nativeType)
    { }
    virtual
    ~NativeRound() { }

    virtual void exec(TProgramCounter& pc) const { 
        pc++;
        if (NativeInstruction<TMPLT>::mOp1->isNull()) {
            NativeNativeInstruction<TMPLT>::mResult->toNull();
        } else {
            // TODO: have not implemented exceptions for this operation
            TMPLT tmp;
            NativeRoundHelp<TMPLT>::r
                 (tmp, NativeInstruction<TMPLT>::mOp1->value());
            NativeNativeInstruction<TMPLT>::mResult->value(tmp);
        }
    }
    static char const * const longName() { return "NativeRound"; }
    static char const * const shortName() { return "ROUND"; }
    static int numArgs() { return 2; }
    void describe(string& out, bool values) const {
        describeHelper(out, values, longName(), shortName(),
                       NativeNativeInstruction<TMPLT>::mResult, 
                       NativeInstruction<TMPLT>::mOp1, 
                       NativeInstruction<TMPLT>::mOp2);
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
        return new NativeRound(static_cast<RegisterRef<TMPLT>*> (sig[0]),
                               static_cast<RegisterRef<TMPLT>*> (sig[1]),
                               (sig[0])->type());
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
        if (NativeInstruction<TMPLT>::mOp1->isNull()) {
            NativeNativeInstruction<TMPLT>::mResult->toNull();
        } else {
            NativeNativeInstruction<TMPLT>::mResult->value
               (NativeInstruction<TMPLT>::mOp1->value());
        }
    }
    static char const * const longName() { return "NativeMove"; }
    static char const * const shortName() { return "MOVE"; }
    static int numArgs() { return 2; }
    void describe(string& out, bool values) const {
        describeHelper(out, values, longName(), shortName(),
                       NativeNativeInstruction<TMPLT>::mResult, 
                       NativeInstruction<TMPLT>::mOp1, 
                       NativeInstruction<TMPLT>::mOp2);
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
        return new NativeMove(static_cast<RegisterRef<TMPLT>*> (sig[0]),
                              static_cast<RegisterRef<TMPLT>*> (sig[1]),
                              (sig[0])->type());
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
        NativeNativeInstruction<TMPLT>::mResult->
            refer(NativeInstruction<TMPLT>::mOp1);
    }
    static char const * const longName() { return "NativeRef"; }
    static char const * const shortName() { return "REF"; }
    static int numArgs() { return 2; }
    void describe(string& out, bool values) const {
        describeHelper(out, values, longName(), shortName(),
                       NativeNativeInstruction<TMPLT>::mResult, 
                       NativeInstruction<TMPLT>::mOp1, 
                       NativeInstruction<TMPLT>::mOp2);
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
        return new NativeRef(static_cast<RegisterRef<TMPLT>*> (sig[0]),
                             static_cast<RegisterRef<TMPLT>*> (sig[1]),
                             (sig[0])->type());
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
        NativeNativeInstruction<TMPLT>::mResult->toNull();
    }
    static char const * const longName() { return "NativeToNull"; }
    static char const * const shortName() { return "TONULL"; }
    static int numArgs() { return 1; }
    void describe(string& out, bool values) const {
        describeHelper(out, values, longName(), shortName(),
                       NativeNativeInstruction<TMPLT>::mResult, 
                       NativeInstruction<TMPLT>::mOp1, 
                       NativeInstruction<TMPLT>::mOp2);
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
        return new NativeToNull(static_cast<RegisterRef<TMPLT>*> (sig[0]),
                                (sig[0])->type());
    }
};

class NativeNativeInstructionRegister : InstructionRegister {

    // TODO: Refactor registerTypes to class InstructionRegister
    template < template <typename> class INSTCLASS2 >
    static void
    registerTypes(vector<StandardTypeDescriptorOrdinal> const & t) {

        for (uint i = 0; i < t.size(); i++) {
            StandardTypeDescriptorOrdinal type = t[i];
            // Type <char> below is a placeholder and is ignored.
            InstructionSignature sig = INSTCLASS2<char>::signature(type);
            switch(type) {
#define Fennel_InstructionRegisterSwitch_NativeNotBool 1
#include "fennel/disruptivetech/calc/InstructionRegisterSwitch.h"
            default:
                throw std::logic_error("Default InstructionRegister");
            }
        }
    }

public:
    static void
    registerInstructions() {

        vector<StandardTypeDescriptorOrdinal> t;
        t = InstructionSignature::typeVector
            (StandardTypeDescriptor::isNativeNotBool);

        // Have to do full fennel:: qualification of template
        // arguments below to prevent template argument 'TMPLT', of
        // this encapsulating class, from perverting NativeAdd into
        // NativeAdd<TMPLT> or something like
        // that. Anyway. Fennel::NativeAdd works just fine.
        registerTypes<fennel::NativeAdd>(t);
        registerTypes<fennel::NativeSub>(t);
        registerTypes<fennel::NativeMul>(t);
        registerTypes<fennel::NativeDiv>(t);
        registerTypes<fennel::NativeNeg>(t);
        registerTypes<fennel::NativeRound>(t);
        registerTypes<fennel::NativeMove>(t);
        registerTypes<fennel::NativeRef>(t);
        registerTypes<fennel::NativeToNull>(t);
    }
};


FENNEL_END_NAMESPACE

#endif

// End NativeNativeInstruction.h
