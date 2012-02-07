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
#ifndef Fennel_Instruction_Included
#define Fennel_Instruction_Included

#include <string>
#include "fennel/calculator/InstructionSignature.h"
#include "fennel/calculator/RegisterReference.h"
#include "fennel/tuple/StandardTypeDescriptor.h"
#include "fennel/calculator/InstructionFactory.h"
#include "fennel/calculator/CalcMessage.h"

FENNEL_BEGIN_NAMESPACE

using namespace std;


// Instruction
// Not a pure abstract base class, but nearly so.
class FENNEL_CALCULATOR_EXPORT Instruction
{
public:
    explicit
    Instruction() {}

    virtual
    ~Instruction() {}

    virtual void describe(string& out, bool values) const = 0;

    // Subclasses must also implement a create() call described by
    // typedef InstructionCreateFunction. Since this member function
    // must be static (as the object doesn't exist to create it) it
    // cannot also be virtual, due to a limitations of C++ that
    // prevents a "static virtual". Consider this a virtual func() =
    // 0; in the sense that it requires this function to be
    // implemented by each derived concrete class.

protected:
    friend class fennel::Calculator;

    virtual void exec(long &pc) const = 0;

    void describeHelper(
        string &out,
        bool values,
        const char* longName,
        const char* shortName,
        RegisterReference* result,
        RegisterReference* op1,
        RegisterReference* op2) const
    {
        out = longName;
        out += ": ";
        out += result->toString();
        if (values) {
            out += " ( ";
            if (result->isNull()) {
                out += "NULL";
            } else {
                out += result->valueToString();
            }
            out += " ) ";
        }
        out += " = ";

        if (!op2 || !op2->isValid()) {  // if < 2 operands
            out += " ";
            out += shortName;
            out += " ";
        }
        if (op1 && op1->isValid()) {  // 1 or 2 operands
            out += op1->toString();
            if (values) {
                out += " ( ";
                if (op1->isNull()) {
                    out += "NULL";
                } else {
                    out += op1->valueToString();
                }
                out += " ) ";
            }
        }
        if (op2 && op2->isValid()) {  // 2 operands
            out += " ";
            out += shortName;
            out += " ";
            out += op2->toString();
            if (values) {
                out += " ( ";
                if (op2->isNull()) {
                    out += "NULL";
                } else {
                    out += op2->valueToString();
                }
                out += " ) ";
            }
        }
    }
};


//! Provide a method to register Instruction objects into the
//! Instruction Factory
//!
//! Each leaf in the object tree subclasses InstructionRegister
//! and calls ::types for each Instruction.

class FENNEL_CALCULATOR_EXPORT InstructionRegister
{
protected:
    template < typename TYPE1,
               template <typename> class INSTCLASS >
    static void
    registerInstance(StandardTypeDescriptorOrdinal type)
    {
        StringToCreateFn* instMap = InstructionFactory::getInstructionTable();
        (*instMap)[INSTCLASS<TYPE1>::signature(type).compute()] =
            &INSTCLASS<TYPE1>::create;
    }

    template < typename TYPE1,
               typename TYPE2,
               template <typename, typename> class INSTCLASS >
    static void
    registerInstance2(
        StandardTypeDescriptorOrdinal type1,
        StandardTypeDescriptorOrdinal type2)
    {
        StringToCreateFn* instMap = InstructionFactory::getInstructionTable();
        (*instMap)[INSTCLASS<TYPE1,TYPE2>::signature(type1, type2).compute()] =
            &INSTCLASS<TYPE1,TYPE2>::create;
    }

    template < typename IGNOREDDATATYPE, class INSTCLASS >
    static void
    registerInstance(StandardTypeDescriptorOrdinal type)
    {
        StringToCreateFn* instMap = InstructionFactory::getInstructionTable();
        (*instMap)[INSTCLASS::signature(type).compute()] =
            &INSTCLASS::create;
    }
};


FENNEL_END_NAMESPACE

#endif

// End Instruction.h

