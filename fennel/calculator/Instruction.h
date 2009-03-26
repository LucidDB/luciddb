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
//
// Instruction.h
// Include this file if you intend to manipulate instructions directly,
// otherwise include Calculator.h
//
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
class Instruction
{
public:
    explicit
    Instruction () { }

    virtual
    ~Instruction() { }

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

    void describeHelper(string &out,
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

class InstructionRegister {
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
    registerInstance2(StandardTypeDescriptorOrdinal type1,
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

