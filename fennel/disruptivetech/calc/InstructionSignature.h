/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2004-2005 Disruptive Tech
// Copyright (C) 2005-2005 The Eigenbase Project
//
// This program is free software; you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation; either version 2 of the License, or
// (at your option) any later Eigenbase-approved version.
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

#ifndef Fennel_InstructionSignature_Included
#define Fennel_InstructionSignature_Included

#include "fennel/tuple/StandardTypeDescriptor.h"
#include "fennel/disruptivetech/calc/RegisterReference.h"
#include "fennel/disruptivetech/calc/CalcTypedefs.h"
#include <string>
#include <vector>
#include <map>

FENNEL_BEGIN_NAMESPACE

using namespace std;

class InstructionSignature {
public:
    explicit
    InstructionSignature(string const & name);
    explicit
    InstructionSignature(string const & name,
                         vector<StandardTypeDescriptorOrdinal>
                         const &operands);
    explicit
    InstructionSignature(string const & name,
                         vector<RegisterReference*> const & operands);
    explicit
    InstructionSignature(string const & name,
                         TProgramCounter pc,
                         vector<StandardTypeDescriptorOrdinal>
                         const &operands);
    explicit
    InstructionSignature(string const & name,
                         TProgramCounter pc,
                         vector<RegisterReference*> const & operands);

    // TODO: convert this to an explicit conversion
    string compute() const;

    string getName() const;
    RegisterReference* operator[] (uint index) const;
    uint size() const;
    TProgramCounter getPc() const;

    //! Returns a vector that contains all types that match a given
    //! StandardTypeDescriptor::function()
    static vector<StandardTypeDescriptorOrdinal>
    typeVector(bool(*typeFunction)(StandardTypeDescriptorOrdinal));

private:
    string name;
    vector<StandardTypeDescriptorOrdinal> types;
    vector<RegisterReference*> registers;
    bool hasRegisters;
    TProgramCounter pc;
    bool hasPc;
    
    void registersToTypes();
};


class Instruction;

//! InstructionCreateFunction is a pointer to the create()
//! public member function supported by all Instructions.
typedef Instruction*(*InstructionCreateFunction)
    (InstructionSignature const &);

//! A map type to register all regular Instructions. ExtendedInstructions
//! still have their own lookup table.
// TODO: Consider merging the regular & extended tables.
typedef std::map< string, InstructionCreateFunction > StringToCreateFn;

//! StringCreateFnIterator is a STL iterator on the StringToCreateFn table.
typedef 
std::map< string, InstructionCreateFunction >::iterator StringToCreateFnIter;


FENNEL_END_NAMESPACE

#endif

// End InstructionSignature.h

