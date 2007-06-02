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
*/
#ifndef Fennel_CastInstruction_Included
#define Fennel_CastInstruction_Included

#include "fennel/disruptivetech/calc/NativeInstruction.h"
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
    CastInstruction(RegisterRef<RESULT_T>* result,
                    RegisterRef<SOURCE_T>* op,
                    StandardTypeDescriptorOrdinal resultType,
                    StandardTypeDescriptorOrdinal sourceType)
        : mResult(result),
          mOp1(op),
          mOp2(NULL)
    { }
    virtual
    ~CastInstruction() { }

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
    CastCast(RegisterRef<RESULT_T>* result,
             RegisterRef<SOURCE_T>* op1, 
             StandardTypeDescriptorOrdinal resultType,
             StandardTypeDescriptorOrdinal sourceType)
        : CastInstruction<RESULT_T, SOURCE_T>(result, op1, resultType, sourceType)
    { }
    virtual
    ~CastCast() { }

    virtual void exec(TProgramCounter& pc) const {
        // See SQL99 Part 2 Section 6.22 for specification of CAST() operator
        pc++;
        if (CastInstruction<RESULT_T, SOURCE_T>::mOp1->isNull()) {
            // SQL99 Part 2 Section 6.22 General Rule 2.c.
            CastInstruction<RESULT_T, SOURCE_T>::mResult->toNull();
        } else {
            // TODO: Update Boost library to fix the following
            // TODO: problem: 
            // TODO: See lists.boost.org/MailArchives/boost/msg63700.php
            // TODO: for details. In short, exceptions are thrown
            // TODO: for cases where data is lost and the target is
            // TODO: unsigned, but not thrown for lost data to signed.
            // TODO: Also some problems with approx being cast to exact.

            // HACK: Add some extra tests before numeric_cast to try
            // HACK: to catch some of the more henious and obvious errors.

            SOURCE_T src = CastInstruction<RESULT_T, SOURCE_T>::mOp1->value();
            bool thr = false;
            bool resultSigned = numeric_limits<RESULT_T>::is_signed;
            bool sourceSigned = numeric_limits<SOURCE_T>::is_signed;

            // Note: min() for approx type is the smallest positive
            // number, not the most negative number.
            RESULT_T min = (numeric_limits<RESULT_T>::is_integer ?
                            numeric_limits<RESULT_T>::min() :
                            - numeric_limits<RESULT_T>::max());
            RESULT_T max = numeric_limits<RESULT_T>::max();

            if (resultSigned == sourceSigned) {
                // Both signed or both unsigned (including approx)
                if (max < src || min > src) {
                    thr = true;
                }
            } else if (resultSigned && !sourceSigned) {
                // Unsigned to signed
                if (max < src) {
                    thr = true;
                }
            } else {
                // Signed to unsigned
                // RESULT_T is unsigned, SOURCE is signed
                if (max < src || min > src) {
                    thr = true;
                }
            }
            if (thr) {
                throw CalcMessage("22003", pc - 1);
            }
            // HACK: End. (Phew.)

            try {
                CastInstruction<RESULT_T, SOURCE_T>::mResult->value
                   (boost::numeric_cast<RESULT_T>
                      (CastInstruction<RESULT_T, SOURCE_T>::mOp1->value()));
            }
            catch (boost::bad_numeric_cast) {
                // class contains no useful information about what went wrong
                // SQL99 Part 2 Section 6.2 General Rule 6.a.ii, 7.a.ii
                // 22003 - Data Exception -- Numeric Value Out of Range
                throw CalcMessage("22003", pc - 1);
            }
        }
    }

    static const char* longName() { return "NativeCast"; }
    static const char* shortName() { return "CAST"; }
    static int numArgs() { return 2; }
    void describe(string& out, bool values) const {
        RegisterRef<char> dummy;
        describeHelper(out, values, longName(), shortName(), 
                       CastInstruction<RESULT_T, SOURCE_T>::mResult, 
                       CastInstruction<RESULT_T, SOURCE_T>::mOp1, 
                       CastInstruction<RESULT_T, SOURCE_T>::mOp2);
    }

    static InstructionSignature
    signature(StandardTypeDescriptorOrdinal type1,
              StandardTypeDescriptorOrdinal type2) {
        vector<StandardTypeDescriptorOrdinal>v;
        v.push_back(type1);
        v.push_back(type2);
        return InstructionSignature(shortName(), v);
    }

    static Instruction*
    create(InstructionSignature const & sig)
    {
        assert(sig.size() == numArgs());
        return new CastCast(static_cast<RegisterRef<RESULT_T>*> (sig[0]),
                            static_cast<RegisterRef<SOURCE_T>*> (sig[1]),
                            (sig[0])->type(),
                            (sig[1])->type());
    }
};

#define TTT(a)

class CastInstructionRegister : InstructionRegister {

    // TODO: Refactor registerTypes to class InstructionRegister
    template < template <typename, typename > class INSTCLASS2 >
    static void
    registerTypes(vector<StandardTypeDescriptorOrdinal> const & t1,
                  vector<StandardTypeDescriptorOrdinal> const & t2) {
        for (uint i = 0; i < t1.size(); i++) {
            for (uint j = 0; j < t2.size(); j++) {
                StandardTypeDescriptorOrdinal type1 = t1[i];
                StandardTypeDescriptorOrdinal type2 = t2[j];
                // Types <char,char> below is a placeholder and is ignored.
                InstructionSignature sig =
                    INSTCLASS2<char, char>::signature(type1, type2);
#include "fennel/disruptivetech/calc/InstructionRegisterSwitchCast.h"
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

