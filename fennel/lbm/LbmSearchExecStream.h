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

#ifndef Fennel_LbmSearchExecStream_Included
#define Fennel_LbmSearchExecStream_Included

#include "fennel/exec/DynamicParam.h"
#include "fennel/ftrs/BTreePrefetchSearchExecStream.h"

FENNEL_BEGIN_NAMESPACE

/**
 * LbmSearchExecStreamParams defines parameters for instantiating a
 * LbmSearchExecStream
 */
struct LbmSearchExecStreamParams : public BTreePrefetchSearchExecStreamParams
{
    /**
     * Parameter id representing the dynamic parameter used to limit the
     * number of rows to produce on a single execute
     */
    DynamicParamId rowLimitParamId;

    /**
     * Parameter id representing the dynamic parameter used to set the
     * starting rid value for bitmap entries
     */
    DynamicParamId startRidParamId;
};

/**
 * LbmSearchExecStream is the execution stream used for scanning bitmap
 * indexes
 *
 * @author Zelaine Fong
 * @version $Id$
 */
class FENNEL_LBM_EXPORT LbmSearchExecStream
    : public BTreePrefetchSearchExecStream
{
    /**
     * True if the rid key is included in the btree search key.  This will
     * be the case if the entire btree key has search values, and a startRid
     * dynamic parameter is provided.
     */
    bool ridInKey;

    /**
     * Parameter id representing the dynamic parameter used to limit the
     * number of rows to produce on a single execute
     */
    DynamicParamId rowLimitParamId;

    /**
     * True if row limit does not apply
     */
    bool ignoreRowLimit;

    /**
     * Parameter id representing the dynamic parameter used to set the
     * starting rid value for bitmap entries
     */
    DynamicParamId startRidParamId;

    /**
     * Number of rows that can be produced
     */
    RecordNum rowLimit;

    /**
     * Desired starting rid value for bitmap entries
     */
    LcsRid startRid;

    /**
     * Tuple datum used to store dynamic paramter for rowLimit
     */
    TupleDatum rowLimitDatum;

    /**
     * Tuple datum used to store dynamic parameter for startRid
     */
    TupleDatum startRidDatum;

    /**
     * Tuple data used as search key that includes rid
     */
    TupleData ridSearchKeyData;

    /**
     * True if the search key for this stream already has the rid key setup
     * in the descriptor, in the case where the startrid dynamic parameter is
     * used to skip ahead in the btree search
     */
    bool ridKeySetup;

    /**
     * Checks if number of tuples produced has reached limit.  Always returns
     * false if "ignoreRowLimit" parameter is true.
     *
     * @return true if reached row limit
     */
    virtual bool reachedTupleLimit(uint nTuples);

    /**
     * Sets the startrid value in the btree search key, in the case where
     * the rid can be part of the btree search key.
     */
    virtual void setAdditionalKeys();

    /**
     * Sets the lower bound key, taking into account the rid search key.
     *
     * @param buf the buffer containing the lower bound key
     */
    virtual void setLowerBoundKey(PConstBuffer buf);

public:
    virtual void prepare(LbmSearchExecStreamParams const &params);
};

FENNEL_END_NAMESPACE

#endif

// End LbmSearchExecStream.h
