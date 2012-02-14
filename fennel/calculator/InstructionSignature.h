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

#ifndef Fennel_InstructionSignature_Included
#define Fennel_InstructionSignature_Included

#include "fennel/tuple/StandardTypeDescriptor.h"
#include "fennel/calculator/RegisterReference.h"
#include "fennel/calculator/CalcTypedefs.h"
#include <string>
#include <vector>
#include <map>

FENNEL_BEGIN_NAMESPACE

using namespace std;

class FENNEL_CALCULATOR_EXPORT InstructionSignature
{
public:
    explicit
    InstructionSignature(string const & name);
    explicit
    InstructionSignature(
        string const & name,
        vector<StandardTypeDescriptorOrdinal>
        const &operands);

    explicit
    InstructionSignature(
        string const & name,
        vector<RegisterReference*> const & operands);

    explicit
    InstructionSignature(
        string const & name,
        TProgramCounter pc,
        vector<StandardTypeDescriptorOrdinal>
        const &operands);

    explicit
    InstructionSignature(
        string const & name,
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

