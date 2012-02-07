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

#ifndef Fennel_ExecStreamDefs_Included
#define Fennel_ExecStreamDefs_Included

#include "fennel/tuple/TupleDescriptor.h"
#include "fennel/segment/SegmentAccessor.h"

FENNEL_BEGIN_NAMESPACE

/**
 * Identifier for an ExecStream within an instance of
 * ExecStreamGraph.
 */
typedef uint ExecStreamId;

enum ExecStreamBufState
{
    EXECBUF_EMPTY,
    EXECBUF_NONEMPTY,
    EXECBUF_UNDERFLOW,
    EXECBUF_OVERFLOW,
    EXECBUF_EOS
};

static std::string ExecStreamBufState_names[] = {
    "EXECBUF_EMPTY",
    "EXECBUF_NONEMPTY",
    "EXECBUF_UNDERFLOW",
    "EXECBUF_OVERFLOW",
    "EXECBUF_EOS"
};

static std::string ExecStreamBufState_names_short[] = {
    "EMP",
    "NEM",
    "UND",
    "OVR",
    "EOS"
};

inline std::ostream & operator<< (std::ostream &os, ExecStreamBufState e)
{
    return os << ExecStreamBufState_names[e];
}

enum ExecStreamBufProvision
{
    BUFPROV_NONE,
    BUFPROV_CONSUMER,
    BUFPROV_PRODUCER,
};

enum ExecStreamResult
{
    EXECRC_BUF_UNDERFLOW,
    EXECRC_BUF_OVERFLOW,
    EXECRC_EOS,
    EXECRC_QUANTUM_EXPIRED,
    EXECRC_YIELD
};

static std::string ExecStreamResult_names[] = {
    "EXECRC_BUF_UNDERFLOW",
    "EXECRC_BUF_OVERFLOW",
    "EXECRC_EOS",
    "EXECRC_QUANTUM_EXPIRED",
    "EXECRC_YIELD"
};

static std::string ExecStreamResult_names_short[] = {
    "UND",
    "OVR",
    "EOS",
    "QNT",
    "YLD"
};

inline std::ostream & operator<< (std::ostream &os, ExecStreamResult e)
{
    return os << ExecStreamResult_names[e];
}

/**
 * ExecStreamQuantum defines the quantum for scheduling of an ExecStream.  The
 * exact interpretation of the specified quantities is stream-dependent.  For
 * example, for nTuplesMax, a filter might count number of input tuples, while
 * a join might count number of tuple comparisons.
 */
struct FENNEL_EXEC_EXPORT ExecStreamQuantum
{
    /**
     * Maximum number of tuples to process per quantum.
     */
    uint nTuplesMax;

    /**
     * Creates a new quantum, initially unlimited.
     */
    explicit ExecStreamQuantum()
    {
        nTuplesMax = MAXU;
    }
};

/**
 * Enumerated type that indicates the nature of a resource requirement setting
 * for an execution stream
 */
enum ExecStreamResourceSettingType {
    /**
     * Setting is accurate
     */
    EXEC_RESOURCE_ACCURATE,
    /**
     * Setting is an estimate, which may or may not be based on up-to-date
     * statistics.  Therefore, if possible, additional resources may be
     * granted to a stream using this setting.
     */
    EXEC_RESOURCE_ESTIMATE,
    /**
     * Setting is unknown and the stream requires maximum resources
     */
    EXEC_RESOURCE_UNBOUNDED
};

/**
 * ExecStreamResourceQuantity quantifies various resources which
 * can be allocated to an ExecStream.
 */
struct FENNEL_EXEC_EXPORT ExecStreamResourceQuantity
{
    /**
     * Number of dedicated threads the stream may request while executing.
     * Non-parallelized streams have 0 for this setting, meaning the only
     * threads which execute them are managed by the scheduler instead.
     */
    uint nThreads;

    /**
     * Number of cache pages the stream may pin while executing.  This includes
     * both scratch pages and I/O pages used for storage access.
     */
    uint nCachePages;

    explicit ExecStreamResourceQuantity()
    {
        nThreads = 0;
        nCachePages = 0;
    }
};

/**
 * Common parameters for instantiating any ExecStream.
 */
struct FENNEL_EXEC_EXPORT ExecStreamParams
{
    /**
     * CacheAccessor to use for any data access.  This will be singular if the
     * stream should not perform data access.
     */
    SharedCacheAccessor pCacheAccessor;

    /**
     * Accessor for segment to use for allocating scratch buffers.  This will
     * be singular if the stream should not use any scratch buffers.
     */
    SegmentAccessor scratchAccessor;

    explicit ExecStreamParams();

    virtual ~ExecStreamParams();
};

FENNEL_END_NAMESPACE

#endif

// End ExecStreamDefs.h
