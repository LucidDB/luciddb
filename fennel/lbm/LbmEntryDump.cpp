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
#include "fennel/lbm/LbmEntryDump.h"

FENNEL_BEGIN_CPPFILE("$Id$");

LbmEntryDump::LbmEntryDump(
    TraceLevel traceLevelInit,
    SharedTraceTarget pTraceTargetInit,
    string nameInit)
    : TraceSource(pTraceTargetInit, nameInit)
{
    traceLevel = traceLevelInit;
}


uint LbmEntryDump::dump(BTreeDescriptor const &treeDescriptor, bool printRID)
{
    uint numTuples = 0;
    TupleData indexTuple;
    TupleDescriptor indexTupleDesc = treeDescriptor.tupleDescriptor;
    SharedBTreeReader pReader = SharedBTreeReader(
        new BTreeReader(treeDescriptor));

    TupleAccessor const& indexTupleAccessor =
        pReader->getTupleAccessorForRead();

    indexTuple.compute(indexTupleDesc);

    ostringstream treeRootPageId;
    treeRootPageId << "RootPageId " << opaqueToInt(treeDescriptor.rootPageId);
    FENNEL_TRACE(traceLevel, treeRootPageId.str());

    if (!pReader->searchFirst()) {
        pReader->endSearch();
    }

    while (pReader->isPositioned()) {
        indexTupleAccessor.unmarshal(indexTuple);
        FENNEL_TRACE(traceLevel, LbmEntry::toString(indexTuple, printRID));
        numTuples ++;

        if (!pReader->searchNext()) {
            pReader->endSearch();
            break;
        }
    }

    return numTuples;
}


FENNEL_END_CPPFILE("$Id$");

// End LbmEntryDump.cpp
