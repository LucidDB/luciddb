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
#include "fennel/calculator/InstructionSignature.h"

FENNEL_BEGIN_CPPFILE("$Id$");

InstructionSignature::InstructionSignature(
    string const & name)
    : name(name),
      hasRegisters(false),
      hasPc(false)
{
}

InstructionSignature::InstructionSignature(
    string const & name,
    vector<StandardTypeDescriptorOrdinal>
    const &operands)
    : name(name),
      types(operands),
      hasRegisters(false),
      hasPc(false)
{
}

InstructionSignature::InstructionSignature(
    string const & name,
    vector<RegisterReference*> const & operands)
    : name(name),
      registers(operands),
      hasRegisters(true),
      hasPc(false)
{
    registersToTypes();
}

InstructionSignature::InstructionSignature(
    string const & name,
    TProgramCounter pc,
    vector<StandardTypeDescriptorOrdinal>
    const &operands)
    : name(name),
      types(operands),
      hasRegisters(false),
      pc(pc),
      hasPc(true)
{
}

InstructionSignature::InstructionSignature(
    string const & name,
    TProgramCounter pc,
    vector<RegisterReference*> const & operands)
    : name(name),
      registers(operands),
      hasRegisters(true),
      pc(pc),
      hasPc(true)
{
    registersToTypes();
}

string
InstructionSignature::compute() const
{
    ostringstream ostr;
    uint size = types.size();

    ostr << name << "(";
    if (hasPc) {
        ostr << "PC";
        if (size) {
            ostr << ",";
        }
    }
    for (uint i = 0; i < size; i++) {
        if (i > 0) {
            ostr << ",";
        }
        ostr << StandardTypeDescriptor::toString(types[i]);
    }
    ostr << ")";
    return ostr.str();
}

string
InstructionSignature::getName() const
{
    return name;
}

RegisterReference*
InstructionSignature::operator[] (uint index) const
{
    assert(hasRegisters);
    return registers[index];
}

uint
InstructionSignature::size() const
{
    return types.size();
}

TProgramCounter
InstructionSignature::getPc() const
{
    assert(hasPc);
    return pc;
}

vector<StandardTypeDescriptorOrdinal>
InstructionSignature::typeVector(
    bool(*typeFunction)(StandardTypeDescriptorOrdinal))
{
    vector<StandardTypeDescriptorOrdinal> v;
    int iter;
    StandardTypeDescriptorOrdinal iter2;
    assert(STANDARD_TYPE_MIN < STANDARD_TYPE_END_NO_UNICODE);
    assert(STANDARD_TYPE_MIN == STANDARD_TYPE_INT_8);
    assert(STANDARD_TYPE_END_NO_UNICODE == STANDARD_TYPE_VARBINARY + 1);

    for (iter = STANDARD_TYPE_MIN; iter < STANDARD_TYPE_END_NO_UNICODE;
         iter++)
    {
        iter2 = StandardTypeDescriptorOrdinal(iter);
        if (typeFunction(iter2)) {
            v.push_back(iter2);
        }
    }

    return v;
}

void
InstructionSignature::registersToTypes()
{
    for (uint i = 0; i < registers.size(); i++) {
        assert(registers[i] != NULL);
        types.push_back(registers[i]->type());
    }

}

FENNEL_END_CPPFILE("$Id$");

// End InstructionSignature.cpp
