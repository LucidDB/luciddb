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

#ifndef Fennel_InstructionDescription_Included
#define Fennel_InstructionDescription_Included

#include "fennel/calculator/Calculator.h"
#include "fennel/calculator/RegisterReference.h"
#include "fennel/tuple/StandardTypeDescriptor.h"
#include <map>

FENNEL_BEGIN_NAMESPACE

class Instruction;

//! A StandardTypeDescriptorOrdinal that allows a level of
//! wildcarding
class FENNEL_CALCULATOR_EXPORT RegDesc
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
    RegDesc(StandardTypeDescriptorOrdinal typeArg)
        : type(typeArg),
        group(REGDESC_NONE)
    {
    }

    // C'tor for a group of types
    explicit
    RegDesc(Groups groupArg)
        : type(STANDARD_TYPE_END_NO_UNICODE),
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
class FENNEL_CALCULATOR_EXPORT InstructionDescription
{
private:
    // TODO: Move to instruction.h?
    //! InstructionCreateFunction is a pointer to the create()
    //! public member function supported by all Instructions.
    typedef Instruction *(*InstructionCreateFunction)(
        vector<RegisterReference*> const &);
public:
    explicit
    InstructionDescription(
        string const &nameArg,
        vector<RegDesc> const &registerdescArg,
        InstructionCreateFunction createFnArg)
        : name(nameArg),
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
