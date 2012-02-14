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

#ifndef Fennel_ExternalSortExecStream_Included
#define Fennel_ExternalSortExecStream_Included

#include "fennel/exec/ConduitExecStream.h"
#include "fennel/common/FemEnums.h"
#include "fennel/tuple/TupleDescriptor.h"

#include <vector>

FENNEL_BEGIN_NAMESPACE

/**
 * ExternalSortExecStreamParams defines parameters for instantiating an
 * ExternalSortExecStream. Note that when distinctness is DUP_DISCARD, the key
 * should normally be the whole tuple to avoid non-determinism with
 * respect to which tuples are discarded.
 */
struct ExternalSortExecStreamParams : public ConduitExecStreamParams
{
    // TODO:  implement duplicate handling
    /**
     * Mode for dealing with duplicate values.
     */
    Distinctness distinctness;

    /**
     * Segment to use for storing temp pages.
     */
    SharedSegment pTempSegment;

    /**
     * Sort key projection (relative to tupleDesc).
     */
    TupleProjection keyProj;

    // TODO:  generalized collation support
    /**
     * Vector with positions corresponding to those of keyProj; true indicates
     * a descending key column, while false indicates asscending.  If this
     * vector is empty, all columns are assumed to be ascending; otherwise, it
     * must be the same length as keyProj.
     */
    std::vector<bool> descendingKeyColumns;

    /**
     * Whether to materialize one big final run, or return results
     * directly from last merge stage.
     */
    bool storeFinalRun;

    /**
     * Estimate of the number of rows in the sort input.  If MAXU, no stats
     * were available to estimate this value.
     */
    RecordNum estimatedNumRows;

    /**
     * If true, close producers once all input has been read
     */
    bool earlyClose;

    /**
     * The number of leading key columns which are already sorted or
     * partitioned. The XO will sort rows by trailing key columns for each
     * "partition" of rows. If 0, sort the entire input by sortKey.
     */
    uint partitionKeyCount;
};

/**
 * ExternalSortExecStream sorts its input stream according to a parameterized
 * key and returns the sorted data as its output.  The implementation is a
 * standard external sort (degrading stepwise from in-memory quicksort to
 * two-pass merge-sort to multi-pass merge-sort).
 *
 *<p>
 *
 * The actual implementation is in ExternalSortExecStreamImpl.
 *
 * @author John V. Sichi
 * @version $Id$
 */
class FENNEL_SORTER_EXPORT ExternalSortExecStream
    : public ConduitExecStream
{
public:
    /**
     * Factory method.
     *
     * @return new ExternalSortExecStream instance
     */
    static ExternalSortExecStream *newExternalSortExecStream();

    // implement ExecStream
    virtual void prepare(ExternalSortExecStreamParams const &params) = 0;
};

FENNEL_END_NAMESPACE

#endif

// End ExternalSortExecStream.h
