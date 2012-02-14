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

#ifndef Fennel_LbmEntryDump_Included
#define Fennel_LbmEntryDump_Included

#include "fennel/common/TraceSource.h"
#include "fennel/tuple/TupleData.h"
#include "fennel/lbm/LbmEntry.h"
#include "fennel/lcs/LcsClusterNode.h"
#include "fennel/btree/BTreeReader.h"
#include <stdarg.h>

using namespace std;

FENNEL_BEGIN_NAMESPACE

/**
 * Class used to dump the contents of a LbmEntry
 *
 * @author Rushan Chen
 * @version $Id$
 */
class FENNEL_LBM_EXPORT LbmEntryDump
    : public TraceSource
{
    static const uint lineLen = 80;

    /**
     * The level at which tracing of cluster dump will be done.
     * I.e., the caller of this object can control the level at
     * which dumps are generated.
     */
    TraceLevel traceLevel;

public:
    explicit LbmEntryDump(
        TraceLevel traceLevelInit,
        SharedTraceTarget pTraceTarget, string name);

    uint dump(BTreeDescriptor const &treeDescriptor, bool printRID = false);
};

FENNEL_END_NAMESPACE

#endif

// End LbmEntryDump.h
