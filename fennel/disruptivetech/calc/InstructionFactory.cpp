/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2004-2005 Disruptive Tech
// Copyright (C) 2005-2005 The Eigenbase Project
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
#include "fennel/disruptivetech/calc/InstructionFactory.h"
#include "fennel/disruptivetech/calc/InstructionCommon.h"

FENNEL_BEGIN_CPPFILE("$Id$");

StringToCreateFn InstructionFactory::instructionTable;
ExtendedInstructionTable InstructionFactory::extendedTable;

// Create a typical instruction
Instruction*
InstructionFactory::createInstruction(string const & name,
                                      vector<RegisterReference*>& operands)
{
    InstructionSignature signature(name, operands);
    return createInstructionHelper(signature);
}

// Create a Jump instruction
Instruction*
InstructionFactory::createInstruction(string const & name, 
                                      TProgramCounter pc,
                                      RegisterReference* operand)
{
    vector<RegisterReference*>v;
    if (operand) {
        v.push_back(operand);
    }

    InstructionSignature signature(name, pc, v);
    return createInstructionHelper(signature);
}

Instruction*
InstructionFactory::createInstructionHelper(InstructionSignature const &sig)
{
    InstructionCreateFunction createFn = instructionTable[sig.compute()];
    
    if (createFn) {
        return createFn(sig);
    } else {
        throw FennelExcn(sig.compute() + " is not a registered instruction");
    }
}

// Create an Extended instruction
Instruction*
InstructionFactory::createInstruction(string const & name, 
                                      string const & function,
                                      vector<RegisterReference*> & operands)
{
    InstructionSignature signature(function, operands);
    ExtendedInstructionDef* instDef = extendedTable[signature.compute()];
    if (instDef == NULL) {
        return NULL;
    }
    return instDef->createInstruction(operands);
}

string
InstructionFactory::signatures()
{
    ostringstream s("");

    StringToCreateFnIter i = instructionTable.begin();
    StringToCreateFnIter end = instructionTable.end();

    while (i != end) {
        s << (*i).first << endl;
        i++;
    }
    return s.str();
}

string
InstructionFactory::extendedSignatures()
{
    return extendedTable.signatures();
}

void
InstructionFactory::registerInstructions()
{
    BoolInstructionRegister::registerInstructions();
    BoolNativeInstructionRegister::registerInstructions();
    BoolPointerInstructionRegister::registerInstructions();
    CastInstructionRegister::registerInstructions();
    IntegralNativeInstructionRegister::registerInstructions();
    IntegralPointerInstructionRegister::registerInstructions();
    JumpInstructionRegister::registerInstructions();
    NativeNativeInstructionRegister::registerInstructions();
    PointerIntegralInstructionRegister::registerInstructions();
    PointerPointerInstructionRegister::registerInstructions();
    ReturnInstructionRegister::registerInstructions();
}

FENNEL_END_CPPFILE("$Id$");

// End InstructionFactory.cpp
