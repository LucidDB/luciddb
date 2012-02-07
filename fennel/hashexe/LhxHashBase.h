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

#ifndef Fennel_LhxHashBase_Included
#define Fennel_LhxHashBase_Included

#include "fennel/segment/SegmentAccessor.h"

using namespace std;

// REVIEW jvs 25-Aug-2006:  This file should be named LhxHashInfo.h, right?

FENNEL_BEGIN_NAMESPACE

enum LhxHashTrim
{
    HASH_TRIM_NONE,
    HASH_TRIM_VARCHAR,
    HASH_TRIM_UNICODE_VARCHAR
};

/**
 * Information common to all hash execution components: join, aggregate.
 *
 * @author Rushan Chen
 * @version $Id$
 */
struct LhxHashInfo
{
    // REVIEW jvs 25-Aug-2006:  This shouldn't be a doxygen comment since
    // it's not associated with any field.  Seems like the second sentence
    // is out-of-date.
    /**
     * Note: need two accessors: one for writing to memory, the other one for
     * writing to the disk. Probably need a list of the second kind, each using
     * one cache page for I/O. See ExternalSorExecStream for models of
     * initializing and using these two accessors.
     */
    /**
     * Accessor for segment used to store runs externally.
     */
    SegmentAccessor externalSegmentAccessor;

    /**
     * Accessor for scratch segment used for building runs in-memory.
     */
    SegmentAccessor memSegmentAccessor;

    /**
     * Cache pages to use by this join. These pages are used for
     * (1) building hash table
     * (2) buffering I/O for writing out to partitions on disk.
     */
    uint numCachePages;

    /**
     * Upper bound on number of cache pages to allocate to slots,
     * which are sensitive to processor cache consciousness.
     */
    uint numCachePagesForSlots;

    // REVIEW jvs 25-Aug-2006:  Instead of ten vectors of types, it's
    // cleaner to create one struct (say LhxHashInputInfo) containing
    // all the types as fields, and then just create one
    // vector<LhxHashInputInfo>.  Access is then info.input[i].useJoinFilter
    // rather than info.useJoinFilter[i].

    // REVIEW jvs 25-Aug-2006:  Why is this named inputDesc?  Isn't
    // it really output?  For example, LhxAggExecStream::prepare
    // sets outputDesc = inputDesc.
    /**
     * Join keys, aggs and data
     * For join,
     * index 0 refers to the orginal(optimizer chosen) probe side
     * index 1 refers to the orginal(optimizer chosen) build side
     * For aggregation,
     * index 0 refers to the input.
     */
    vector<TupleDescriptor> inputDesc;

    vector<TupleProjection> keyProj;

    /*
     * If a key column is varchar type.
     */
    vector< vector<LhxHashTrim> > isKeyColVarChar;

    /*
     * In hash join, if an input can be filtered using bitmap saved from
     * previous round of partitioning.
     */
    vector<bool> useJoinFilter;

    /**
     * Projections of aggs and data fields out of the RHS.
     */
    TupleProjection aggsProj;
    vector<TupleProjection> dataProj;

    /**
     * Estimated number of rows from the inputs.
     */
    vector<RecordNum> numRows;

    /**
     * Key cardinality estimate from the optimizer.
     *
     * It is also used to estimate the size of the hash table(to build partial
     * aggregates) during recursive partitioning for aggregate operations.
     */
    vector<RecordNum> cndKeys;

    /**
     * ExecStream buf accessors.
     */
    vector<SharedExecStreamBufAccessor> streamBufAccessor;

    /**
     * Special hash table properties:
     *
     * filterNull: do not add null keys to hash table
     * In join sementics, nulls do not match; however, in set matching
     * sementics and special comparison semantics("is not distinct from"),
     * nulls are considered equal.
     *
     * removeDuplicate: do not add duplicatekeys to hash table
     * removeDuplicate is only used in set matching joins where
     * inputDesc and keyProj have the same size for both inputs.
     */
    vector<bool> filterNull;
    vector<TupleProjection> filterNullKeyProj;
    vector<bool> removeDuplicate;
};

FENNEL_END_NAMESPACE

#endif

// End LhxHashBase.h
