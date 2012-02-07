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

#ifndef Fennel_ExtendedInstructionTable_Included
#define Fennel_ExtendedInstructionTable_Included

#include "fennel/calculator/ExtendedInstruction.h"
#include <map>

FENNEL_BEGIN_NAMESPACE

using std::string;

//! A singleton mapping of ExtendedInstruction signatures to
//! ExtendedInstruction functors.
class FENNEL_CALCULATOR_EXPORT ExtendedInstructionTable
{
public:
    //! Registers an extended instruction and the functor which implements it.
    template <typename T>
    void add(
        const string &name,
        const vector<StandardTypeDescriptorOrdinal> &parameterTypes,
        T *dummy,
        typename T::Functor functor)
    {
        FunctorExtendedInstructionDef<T> *pDef =
            new FunctorExtendedInstructionDef<T>(
                name,
                parameterTypes,
                functor);
        _defsByName[pDef->getSignature()] = pDef;
    }

    //! Looks up an extended instruction by signature (name + argument types)
    //!
    //! Returns null if instruction not found.
    ExtendedInstructionDef* operator[] (string const &signature) {
        return _defsByName[signature];
    }

    string signatures();

private:
    map<string, ExtendedInstructionDef *> _defsByName;
};


FENNEL_END_NAMESPACE

#endif
// End ExtendedInstructionTable.h
