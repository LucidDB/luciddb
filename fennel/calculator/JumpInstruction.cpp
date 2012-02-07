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
#include "fennel/common/CommonPreamble.h"
#include "fennel/calculator/Calculator.h"
#include "fennel/calculator/JumpInstruction.h"

#include "boost/lexical_cast.hpp"
using boost::lexical_cast;

FENNEL_BEGIN_CPPFILE("$Id$");

void
JumpInstruction::describeHelper(
    string &out,
    bool values,
    const char* longName,
    const char* shortName) const
{
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
