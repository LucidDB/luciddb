/*
// $Id$
// Fennel is a relational database kernel.
// Copyright (C) 2004-2004 Disruptive Tech
//
// This program is free software; you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation; either version 2 of the License, or
// (at your option) any later version.
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
// InstructionDescription
//
*/

#ifndef Fennel_InstructionDescription_Included
#define Fennel_InstructionDescription_Included

#include "fennel/disruptivetech/calc/Calculator.h"
#include "fennel/disruptivetech/calc/RegisterReference.h"
#include "fennel/tuple/StandardTypeDescriptor.h"
#include <map>

FENNEL_BEGIN_NAMESPACE

class Instruction;

//! A StandardTypeDescriptorOrdinal that allows a level of
//! wildcarding
class RegDesc
{
public:
    enum Groups {
        REGDESC_NONE = 0,
        REGDESC_ANY,
        REGDESC_NATIVE,
        REGDESC_INTEGRAL,
        REGDESC_POINTER,
        REGDESC_ARRAY
    };

    // C'tor for exactly one type
    explicit
    RegDesc(StandardTypeDescriptorOrdinal typeArg) : 
        type(typeArg),
        group(REGDESC_NONE)
    {
    }
   
    // C'tor for a group of types
    explicit
    RegDesc(Groups groupArg) :
        type(STANDARD_TYPE_END),
        group(groupArg)
    {
    }
    
    bool
    match(StandardTypeDescriptorOrdinal m);

private:
    StandardTypeDescriptorOrdinal type;
    Groups group;
};

//! Description of an instruction. (Contrasted with
//! an ExtendedInstruction.)
class InstructionDescription
{
private:
    // TODO: Move to instruction.h?
    //! InstructionCreateFunction is a pointer to the create()
    //! public member function supported by all Instructions.
    typedef Instruction*(*InstructionCreateFunction)
        (vector<RegisterReference*>);
public:
    explicit
    InstructionDescription(const string& nameArg,
                           const vector<RegDesc> registerdescArg,
                           InstructionCreateFunction createFnArg) :
        name(nameArg),
        registerdesc(registerdescArg),
        createFn(createFnArg)
    {
    }

    void setName(string const &s)
    {
        name = s;
    }

    string getName() const
    {
        return name;
    }
    
private:
    string name;
    vector<RegDesc> registerdesc;
    InstructionCreateFunction createFn;
    Instruction* inst;
    TProgramCounter pc;
};


typedef std::map< string, InstructionDescription* > StringToInstDesc;

FENNEL_END_NAMESPACE

#endif

// End InstructionDescription.h
