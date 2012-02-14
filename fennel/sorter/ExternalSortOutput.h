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

#ifndef Fennel_ExternalSortOutput_Included
#define Fennel_ExternalSortOutput_Included

#include "fennel/tuple/TupleAccessor.h"
#include "fennel/sorter/ExternalSortSubStream.h"
#include "fennel/exec/ExecStreamDefs.h"

FENNEL_BEGIN_NAMESPACE

class ExternalSortInfo;

/**
 * ExternalSortMerger marshals XO output buffers by fetching from a
 * top-level ExternalSortSubStream.
 */
class FENNEL_SORTER_EXPORT ExternalSortOutput
{
    /**
     * Global information.
     */
    ExternalSortInfo &sortInfo;

    // TODO:  comment or replace
    TupleAccessor tupleAccessor;

    /**
     * Substream from which to fetch.
     */
    ExternalSortSubStream *pSubStream;

    /**
     * Fetch array bound to substream.
     */
    ExternalSortFetchArray *pFetchArray;

    /**
     * 0-based index of next tuple to return from fetch array.
     */
    uint iCurrentTuple;

public:
    explicit ExternalSortOutput(ExternalSortInfo &info);
    virtual ~ExternalSortOutput();

    /**
     * Sets the substream from which to fetch.
     *
     * @param subStream new source
     */
    void setSubStream(ExternalSortSubStream &subStream);

    /**
     * Fetches tuples and writes them to a buffer.
     *
     * @param bufAccessor receives marshalled tuple data
     *
     * @return result
     */
    ExecStreamResult fetch(ExecStreamBufAccessor &bufAccessor);

    /**
     * Releases any resources acquired by this object.
     */
    void releaseResources();
};

FENNEL_END_NAMESPACE

#endif

// End ExternalSortOutput.h
