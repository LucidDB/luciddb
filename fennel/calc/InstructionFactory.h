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
// InstructionFactory
//
*/

#ifndef Fennel_InstructionFactory_Included
#define Fennel_InstructionFactory_Included

#include "fennel/calc/InstructionSignature.h"
#include "fennel/calc/CalcAssemblerException.h"
#include "fennel/calc/RegisterReference.h"
#include "fennel/tuple/StandardTypeDescriptor.h"

FENNEL_BEGIN_NAMESPACE

class ExtendedInstructionTable;

//! Dynamically create Instruction objects given an InstructionDescription
//! description of the desired Instruction.
class InstructionFactory
{
public:
    explicit
    InstructionFactory() {}

    // Create a typical instruction
    static Instruction*
    createInstruction(string const & name,
                      vector<RegisterReference*> & operands); // add const?

    // Create a Jump instruction
    static Instruction*
    createInstruction(string const & name, 
                      TProgramCounter pc,
                      RegisterReference* operand); // add const?

    // Create an Extended instruction
    static Instruction*
    createInstruction(string const & name, 
                      string const & function,
                      vector<RegisterReference*> & operands); // add const?

    // Start-of-world setup
    static void
    registerInstructions();

    static StringToCreateFn* 
    getInstructionTable()
    {
        return &instructionTable;
    }

    // Extended Instruction hooks
    static ExtendedInstructionTable*
    getExtendedInstructionTable()
    {
        return &extendedTable;
    }

private:
    static Instruction*
    createInstructionHelper(InstructionSignature const & sig);

    //! A map to hold regular instructions
    static StringToCreateFn instructionTable;
    //! A map to hold extended instructions
    static ExtendedInstructionTable extendedTable;
};

FENNEL_END_NAMESPACE

#endif

// End InstructionFactory.h
