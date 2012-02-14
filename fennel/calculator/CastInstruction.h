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
#ifndef Fennel_CastInstruction_Included
#define Fennel_CastInstruction_Included

#include "fennel/calculator/NativeInstruction.h"
#include <boost/cast.hpp>

FENNEL_BEGIN_NAMESPACE

/**
 * Instructions that cast between types.
 *
 * @author John Kalucki
 */
template<typename RESULT_T, typename SOURCE_T>
class CastInstruction : public Instruction
{
public:
    explicit
    CastInstruction(
        RegisterRef<RESULT_T>* result,
        RegisterRef<SOURCE_T>* op,
        StandardTypeDescriptorOrdinal resultType,
        StandardTypeDescriptorOrdinal sourceType)
        : mResult(result),
          mOp1(op),
          mOp2(NULL)
    {}

    virtual
    ~CastInstruction() {}

protected:
    RegisterRef<RESULT_T>* mResult;
    RegisterRef<SOURCE_T>* mOp1;
    RegisterRef<SOURCE_T>* mOp2; // may be unused
};

template<typename RESULT_T, typename SOURCE_T>
class CastCast : public CastInstruction<RESULT_T, SOURCE_T>
{
public:
    explicit
    CastCast(
        RegisterRef<RESULT_T>* result,
        RegisterRef<SOURCE_T>* op1,
        StandardTypeDescriptorOrdinal resultType,
        StandardTypeDescriptorOrdinal sourceType)
        : CastInstruction<RESULT_T, SOURCE_T>(
            result, op1, resultType, sourceType)
    {}

    virtual
    ~CastCast() {}

    virtual void exec(TProgramCounter& pc) const {
        // See SQL99 Part 2 Section 6.22 for specification of CAST() operator
        pc++;
        if (CastInstruction<RESULT_T, SOURCE_T>::mOp1->isNull()) {
            // SQL99 Part 2 Section 6.22 General Rule 2.c.
            CastInstruction<RESULT_T, SOURCE_T>::mResult->toNull();
        } else {
            try {
                CastInstruction<RESULT_T, SOURCE_T>::mResult->value(
                    boost::numeric_cast<RESULT_T>(
                        CastInstruction<RESULT_T, SOURCE_T>::mOp1->value()));
            } catch (boost::bad_numeric_cast) {
                // class contains no useful information about what went wrong
                // SQL99 Part 2 Section 6.2 General Rule 6.a.ii, 7.a.ii
                // 22003 - Data Exception -- Numeric Value Out of Range
                throw CalcMessage(
                    SqlState::instance().code22003(), pc - 1);
            }
        }
    }

    static const char* longName()
    {
        return "NativeCast";
    }

    static const char* shortName()
    {
        return "CAST";
    }

    static int numArgs()
    {
        return 2;
    }

    void describe(string& out, bool values) const {
        RegisterRef<char> dummy;
        describeHelper(
            out, values, longName(), shortName(),
            CastInstruction<RESULT_T, SOURCE_T>::mResult,
            CastInstruction<RESULT_T, SOURCE_T>::mOp1,
            CastInstruction<RESULT_T, SOURCE_T>::mOp2);
    }

    static InstructionSignature
    signature(
        StandardTypeDescriptorOrdinal type1,
        StandardTypeDescriptorOrdinal type2)
    {
        vector<StandardTypeDescriptorOrdinal> v;
        v.push_back(type1);
        v.push_back(type2);
        return InstructionSignature(shortName(), v);
    }

    static Instruction*
    create(InstructionSignature const & sig)
    {
        assert(sig.size() == numArgs());
        return new CastCast(
            static_cast<RegisterRef<RESULT_T>*> (sig[0]),
            static_cast<RegisterRef<SOURCE_T>*> (sig[1]),
            (sig[0])->type(),
            (sig[1])->type());
    }
};

#define TTT(a)

class FENNEL_CALCULATOR_EXPORT CastInstructionRegister
    : InstructionRegister
{

    // TODO: Refactor registerTypes to class InstructionRegister
    template < template <typename, typename > class INSTCLASS2 >
    static void
    registerTypes(
        vector<StandardTypeDescriptorOrdinal> const & t1,
        vector<StandardTypeDescriptorOrdinal> const & t2)
    {
        for (uint i = 0; i < t1.size(); i++) {
            for (uint j = 0; j < t2.size(); j++) {
                StandardTypeDescriptorOrdinal type1 = t1[i];
                StandardTypeDescriptorOrdinal type2 = t2[j];
                // Types <char,char> below is a placeholder and is ignored.
                InstructionSignature sig =
                    INSTCLASS2<char, char>::signature(type1, type2);
#include "fennel/calculator/InstructionRegisterSwitchCast.h"
                // Note: Above .h includes a throw std::logic_error if
                // type combination cannot be found.
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
        registerTypes<fennel::CastCast>(t, t);
    }
};


FENNEL_END_NAMESPACE

#endif

// End CastInstruction.h

