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
// Instruction->Jump
*/
#include "fennel/common/CommonPreamble.h"
#include "fennel/disruptivetech/calc/Calculator.h"
#include "fennel/disruptivetech/calc/JumpInstruction.h"

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
Jump::longName()
{
    return "Jump";
}
const char *
Jump::shortName()
{
    return "JMP";
}
int
Jump::numArgs()
{
    return 0;  // PC is not counted
}
void
Jump::describe(string& out, bool values) const {
    describeHelper(out, values, longName(), shortName());
}

const char *
JumpTrue::longName()
{
    return "JumpTrue";
}
const char *
JumpTrue::shortName()
{
    return "JMPT";
}
int
JumpTrue::numArgs()
{
    return 1;  // PC is not counted
}
void
JumpTrue::describe(string& out, bool values) const {
    describeHelper(out, values, longName(), shortName());
}

const char *
JumpFalse::longName()
{
    return "JumpFalse";
}
const char *
JumpFalse::shortName()
{
    return "JMPF";
}
int
JumpFalse::numArgs()
{
    return 1;  // PC is not counted
}
void
JumpFalse::describe(string& out, bool values) const {
    describeHelper(out, values, longName(), shortName());
}

const char *
JumpNull::longName()
{
    return "JumpNull";
}
const char *
JumpNull::shortName()
{
    return "JMPN";
}
int
JumpNull::numArgs()
{
    return 1;  // PC is not counted
}
void
JumpNull::describe(string& out, bool values) const {
    describeHelper(out, values, longName(), shortName());
}

const char *
JumpNotNull::longName()
{
    return "JumpNotNull";
}
const char *
JumpNotNull::shortName()
{
    return "JMPNN";
}
int
JumpNotNull::numArgs()
{
    return 1;  // PC is not counted
}
void
JumpNotNull::describe(string& out, bool values) const {
    describeHelper(out, values, longName(), shortName());
}

FENNEL_END_CPPFILE("$Id$");

// End JumpInstruction.cpp
