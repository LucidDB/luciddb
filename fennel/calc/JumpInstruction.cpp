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
// Instruction->Jump
*/
#include "fennel/common/CommonPreamble.h"
#include "fennel/calc/Calculator.h"
#include "fennel/calc/JumpInstruction.h"

#include "boost/lexical_cast.hpp"
using boost::lexical_cast;

FENNEL_BEGIN_CPPFILE("$Id$");

void
JumpInstruction::describeHelper(string &out,
                                bool values,
                                const char* longName,
                                const char* shortName) const {
    out = longName;
    out += " To Addr: ";
    out += boost::lexical_cast<std::string>(mJumpTo);
    out += " ";
    out += shortName;
    out += " ";

    if (mOp && mOp->isValid()) {
        out += mOp->toString();
        if (values) {
            out += " ( ";
            if (mOp->isNull()) {
                out += "NULL";
            } else {
                out += mOp->valueToString();
            }
            out += " ) ";
        }
    }
}


const char *
Jump::longName() const
{
    return "Jump";
}
const char *
Jump::shortName() const
{
    return "Jmp";
}
void
Jump::describe(string &out, bool values) const {
    describeHelper(out, values, longName(), shortName());
}

const char *
JumpTrue::longName() const
{
    return "JumpTrue";
}
const char *
JumpTrue::shortName() const
{
    return "JmpT";
}
void
JumpTrue::describe(string &out, bool values) const {
    describeHelper(out, values, longName(), shortName());
}

const char *
JumpFalse::longName() const
{
    return "JumpFalse";
}
const char *
JumpFalse::shortName() const
{
    return "JmpF";
}
void
JumpFalse::describe(string &out, bool values) const {
    describeHelper(out, values, longName(), shortName());
}

const char *
JumpNull::longName() const
{
    return "JumpNull";
}
const char *
JumpNull::shortName() const
{
    return "JmpN";
}
void
JumpNull::describe(string &out, bool values) const {
    describeHelper(out, values, longName(), shortName());
}

const char *
JumpNotNull::longName() const
{
    return "JumpNotNull";
}
const char *
JumpNotNull::shortName() const
{
    return "JmpNN";
}
void
JumpNotNull::describe(string &out, bool values) const {
    describeHelper(out, values, longName(), shortName());
}

FENNEL_END_CPPFILE("$Id$");

// End JumpInstruction.cpp
