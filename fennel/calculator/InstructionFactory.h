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
// InstructionFactory
//
*/

#ifndef Fennel_InstructionFactory_Included
#define Fennel_InstructionFactory_Included

#include "fennel/calculator/InstructionSignature.h"
#include "fennel/calculator/CalcAssemblerException.h"
#include "fennel/calculator/RegisterReference.h"
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

    // Creates a typical instruction
    static Instruction*
    createInstruction(
        string const &name,
        vector<RegisterReference*> const &operands);

    // Creates a Jump instruction
    static Instruction*
    createInstruction(
        string const &name,
        TProgramCounter pc,
        RegisterReference* operand); // add const?

    // Creates an Extended instruction
    static Instruction*
    createInstruction(
        string const &name,
        string const &function,
        vector<RegisterReference*> const &operands);

    // debugging & tracing
    static string
    signatures();

    static string
    extendedSignatures();

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
