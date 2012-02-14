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

#ifndef Fennel_ExternalSubStream_Included
#define Fennel_ExternalSubStream_Included

FENNEL_BEGIN_NAMESPACE

class ExternalSortFetchArray;

/**
 * ExternalSortRC defines the internal return code for various
 * operations.
 */
enum ExternalSortRC
{
    /**
     * Operation produced valid results.
     */
    EXTSORT_SUCCESS,

    /**
     * Operation produced no results because end of data was reached.
     */
    EXTSORT_ENDOFDATA,

    /**
     * Operation produced some results but could not continue because
     * output area (e.g. generated run) became full.
     */
    EXTSORT_OVERFLOW,

    /**
     * Operation produced data to continue to the next step in sort operation.
     */
    EXTSORT_YIELD,
};

/**
 * Fetch interface implemented by sorter subcomponents which return
 * intermediate results.
 */
class FENNEL_SORTER_EXPORT ExternalSortSubStream
{
public:
    virtual ~ExternalSortSubStream()
    {
    }

    /**
     * Binds the fetch array which will be used implicitly by
     * subsequent calls to fetch().
     *
     * @return bound fetch array
     */
    virtual ExternalSortFetchArray &bindFetchArray() = 0;

    /**
     * Fetches tuples via the previously bound fetch array.
     *
     * @param nTuplesRequested maximum number of tuples to be returned from
     * fetch (actual count may be less at callee's discretion; this does not
     * indicate end of stream)
     *
     * @return result of fetch (either EXTSORT_ENDOFDATA or EXTSORT_SUCCESS)
     */
    virtual ExternalSortRC fetch(uint nTuplesRequested) = 0;
};

/**
 * Data structure used for array fetch when reading from substreams.
 */
struct ExternalSortFetchArray
{
    /**
     * Array of pointers to marshalled tuple images.
     */
    PBuffer *ppTupleBuffers;

    /**
     * Number of valid entries in ppTupleBuffers.
     */
    uint nTuples;

    /**
     * Creates a new fetch array, initially empty.
     */
    explicit ExternalSortFetchArray()
    {
        ppTupleBuffers = NULL;
        nTuples = 0;
    }
};

/**
 * Maximum number of pointers to return per substream fetch.
 */
const uint EXTSORT_FETCH_ARRAY_SIZE = 100;

FENNEL_END_NAMESPACE

#endif

// End ExternalSortSubStream.h
