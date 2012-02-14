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
#include "fennel/ftrs/BTreeReadExecStream.h"
#include "fennel/btree/BTreeReader.h"
#include "fennel/exec/ExecStreamBufAccessor.h"

FENNEL_BEGIN_CPPFILE("$Id$");

void BTreeReadExecStream::prepare(BTreeReadExecStreamParams const &params)
{
    BTreeExecStream::prepare(params);
    outputProj.assign(params.outputProj.begin(), params.outputProj.end());
    tupleData.compute(params.outputTupleDesc);
}

void BTreeReadExecStream::getResourceRequirements(
    ExecStreamResourceQuantity &minQuantity,
    ExecStreamResourceQuantity &optQuantity)
{
    BTreeExecStream::getResourceRequirements(minQuantity, optQuantity);

    // one page for BTreeReader
    minQuantity.nCachePages += 1;

    // TODO:  use opt to govern prefetch and come up with a good formula
    optQuantity = minQuantity;
}

void BTreeReadExecStream::open(bool restart)
{
    BTreeExecStream::open(restart);

    if (restart) {
        return;
    }

    // Create the reader here rather than during prepare, in case the btree
    // was dynamically created during stream graph open
    pReader = newReader();
    projAccessor.bind(
        pReader->getTupleAccessorForRead(),
        outputProj);
}

// TODO: When not projecting anything away, we could do producer buffer
// provision instead.  For BTreeCompactNodeAccessor, we can return multiple
// tuples directly by referencing the node data.  For other node accessor
// implementations, we can return single tuples by reference (although that's
// not always a win).

void BTreeReadExecStream::closeImpl()
{
    BTreeExecStream::closeImpl();
}

FENNEL_END_CPPFILE("$Id$");

// End BTreeReadExecStream.cpp
