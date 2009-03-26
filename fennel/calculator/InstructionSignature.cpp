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
    vector<StandardTypeDescriptorOrdinal>v;
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
