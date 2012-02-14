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

#ifndef Fennel_ExtendedInstructionContext_Included
#define Fennel_ExtendedInstructionContext_Included

FENNEL_BEGIN_NAMESPACE

//! A abstract base class for Extended Instructions that wish to store context
//! between exec() calls. Typically used to store results of pre-compilation
//! or cache instantiations of library classes, and so forth.
//! An alternate implementation could store context pointers in local variables.
class FENNEL_CALCULATOR_EXPORT ExtendedInstructionContext
{
public:
    explicit
    ExtendedInstructionContext()
    {
    }
    virtual
    ~ExtendedInstructionContext()
    {
    }
};

FENNEL_END_NAMESPACE

#endif
// End ExtendedInstructionContext.h
