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
// InstructionArgs.h
// Include this file if you intend to manipulate instructions directly,
// otherwise include Calculator.h
//
*/
#ifndef Fennel_InstructionArgs_Included
#define Fennel_InstructionArgs_Included

#include <string>
#include "fennel/calculator/Calculator.h"
#include "fennel/calculator/RegisterReference.h"
#include "fennel/tuple/StandardTypeDescriptor.h"

FENNEL_BEGIN_NAMESPACE

// InstructionArgs
// A class that can represent all possible arguments to
// an Instruction constructor.
class InstructionArgs
{
public:
    explicit
    InstructionArgs(const vector<RegisterReference*>o)
        : operands(o),
          pcSet(false)
    {
    }

    explicit
    InstructionArgs(
        const vector<RegisterReference*>o,
        TProgramCounter p)
        : operands(o),
          pc(p),
          pcSet(true)
    {
    }

    const TProgramCounter
    getPC()
    {
        assert(pcSet);
        return pc;
    }

    const vector<RegisterReference*>&
    getOperands()
    {
        return operands;
    }

    const RegisterReference*
    operator[] (int i)
    {
        return operands[i];
    }

private:
    vector<RegisterReference*> operands;
    TProgramCounter pc;
    bool pcSet;
};




FENNEL_END_NAMESPACE

#endif

// End InstructionArgs.h

