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

#ifndef Fennel_LhxHashTableDump_Included
#define Fennel_LhxHashTableDump_Included

#include "fennel/common/TraceSource.h"
#include "fennel/hashexe/LhxHashTable.h"
#include <stdarg.h>

using namespace std;

FENNEL_BEGIN_NAMESPACE

/**
 * Class to use to dump the content of a LhxHashTable.
 *
 * @author Rushan Chen
 * @version $Id$
 */
class FENNEL_HASHEXE_EXPORT LhxHashTableDump
    : public TraceSource
{
    /**
     * The level at which tracing of cluster dump will be done.
     * I.e., the caller of this object can control the level at
     * which dumps are generated.
     */
    TraceLevel traceLevel;

public:
    explicit LhxHashTableDump(
        TraceLevel traceLevelInit,
        SharedTraceTarget pTraceTargetInit,
        string nameInit)
        : TraceSource(pTraceTargetInit, nameInit)
    {
        traceLevel = traceLevelInit;
    }

    void dump(string traceStr)
    {
        FENNEL_TRACE(traceLevel, traceStr);
    }

    void dump(LhxHashTable &hashTable)
    {
        dump(hashTable.toString());
    }
};

FENNEL_END_NAMESPACE

#endif

// End LhxHashTableDump.h
