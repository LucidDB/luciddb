/*
// $Id$
// Fennel is a relational database kernel.
// Copyright (C) 2004-2004 Disruptive Tech
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
// Instruction.h
// Include this file if you intend to manipulate instructions directly, 
// otherwise include Calculator.h
//
*/
#ifndef Fennel_Instruction_Included
#define Fennel_Instruction_Included

#include <string>
#include "fennel/calc/Calculator.h"
#include "fennel/calc/RegisterReference.h"
#include "fennel/tuple/StandardTypeDescriptor.h"

FENNEL_BEGIN_NAMESPACE

using namespace std;

// Instruction
// not a pure abstract base class. should it be?
class Instruction
{
public:
    // setup functions -- can be convenient
    explicit
    Instruction () { }

    virtual
    ~Instruction() { }

    // trace functions -- should be fast
    virtual const char * longName() const = 0;
    virtual const char * shortName() const = 0;
    virtual void describe(string &out, bool values) const = 0;

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




FENNEL_END_NAMESPACE

#endif

// End Instruction.h

