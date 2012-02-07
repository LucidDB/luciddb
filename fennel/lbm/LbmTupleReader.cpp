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

#include "fennel/common/CommonPreamble.h"
#include "fennel/lbm/LbmTupleReader.h"
#include "fennel/exec/ExecStreamBufAccessor.h"

FENNEL_BEGIN_CPPFILE("$Id$");

LbmTupleReader::~LbmTupleReader()
{
}

void LbmStreamTupleReader::init(
    SharedExecStreamBufAccessor &pInAccessorInit,
    TupleData &bitmapSegTuple)
{
    pInAccessor = pInAccessorInit;
    pInputTuple = &bitmapSegTuple;
}

ExecStreamResult LbmStreamTupleReader::read(PTupleData &pTupleData)
{
    if (pInAccessor->getState() == EXECBUF_EOS) {
        return EXECRC_EOS;
    }

    // consume the previous input if there was one
    if (pInAccessor->isTupleConsumptionPending()) {
        pInAccessor->consumeTuple();
    }
    if (!pInAccessor->demandData()) {
        return EXECRC_BUF_UNDERFLOW;
    }

    pInAccessor->unmarshalTuple(*pInputTuple);
    return EXECRC_YIELD;
}

void LbmSingleTupleReader::init(TupleData &bitmapSegTuple)
{
    hasTuple = true;
    pInputTuple = &bitmapSegTuple;
}

ExecStreamResult LbmSingleTupleReader::read(PTupleData &pTupleData)
{
    if (!hasTuple) {
        return EXECRC_EOS;
    }
    pTupleData = pInputTuple;
    hasTuple = false;
    return EXECRC_YIELD;
}

FENNEL_END_CPPFILE("$Id$");

// End LbmTupleReader.cpp
