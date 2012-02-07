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
#ifndef NoisyArithmetic_Included
#define NoisyArithmetic_Included

#include <stdexcept>
#include "CalcMessage.h"

FENNEL_BEGIN_NAMESPACE

/* ---
Struct for notification that an exception is forthcoming, allows
the callee to take some exception specific action with its opaque
element pData (such as cast it to a RegisterReference and call
toNull()).

Assumptions: if this structure exists the fnCB must not
be NULL.
 --- */
struct TExceptionCBData
{
    void (* fnCB)(SqlStateInfo const &, void *);
    void *pData;
    TExceptionCBData(
        void (* fnTheCB)(SqlStateInfo const &, void *),
        void *pTheData)
    :   fnCB(fnTheCB),
        pData(pTheData)
    {}
};

/* --- */
template <typename TMPL>
    struct Noisy {
        static TMPL add(
            TProgramCounter pc, const TMPL left, const TMPL right,
            TExceptionCBData *pExData)
            throw(CalcMessage);
        static TMPL sub(
            TProgramCounter pc, const TMPL left, const TMPL right,
            TExceptionCBData *pExData)
            throw(CalcMessage);
        static TMPL mul(
            TProgramCounter pc, const TMPL left, const TMPL right,
            TExceptionCBData *pExData) throw(CalcMessage);
        static TMPL div(
            TProgramCounter pc, const TMPL left, const TMPL right,
            TExceptionCBData *pExData)
            throw(CalcMessage);
        static TMPL neg(
            TProgramCounter pc, const TMPL right,
            TExceptionCBData *pExData)
            throw(CalcMessage);
    };

FENNEL_END_NAMESPACE

#endif

// End NoisyArithmetic.h

