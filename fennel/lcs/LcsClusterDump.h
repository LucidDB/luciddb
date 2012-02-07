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

#ifndef Fennel_LcsClusterDump_Included
#define Fennel_LcsClusterDump_Included

#include "fennel/btree/BTreeDescriptor.h"
#include "fennel/common/TraceSource.h"
#include "fennel/lcs/LcsClusterAccessBase.h"
#include <boost/enable_shared_from_this.hpp>

FENNEL_BEGIN_NAMESPACE

/**
 * Class used to dump the contents of a cluster page using fennel trace
 */
class FENNEL_LCS_EXPORT LcsClusterDump
    : public LcsClusterAccessBase, public TraceSource
{
    /**
     * The level at which tracing of cluster dump will be done.
     * I.e., the caller of this object can control the level at
     * which dumps are generated.
     */
    TraceLevel traceLevel;

    /**
     * Tuple descriptor for the columns in a cluster page
     */
    TupleDescriptor colTupleDesc;

    void callTrace(const char *format, ...);

    PBuffer fprintVal(uint idx, PBuffer pV, uint col);

public:
    explicit LcsClusterDump(
        BTreeDescriptor const &bTreeDescriptor,
        TupleDescriptor const &colTupleDescInit,
        TraceLevel traceLevelInit,
        SharedTraceTarget pTraceTarget,
        std::string name);

    /**
     * Dumps out a cluster page
     *
     * @param pageId pageid of the cluster page
     *
     * @param pHdr pointer to the cluster page
     *
     * @param szBlock number of bytes in the page
     */
    void dump(uint64_t pageId, PConstLcsClusterNode pHdr, uint szBlock);
};


FENNEL_END_NAMESPACE

#endif

// End LcsClusterDump.h
