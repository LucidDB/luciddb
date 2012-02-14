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

#ifndef Fennel_ExternalSortRunLoader_Included
#define Fennel_ExternalSortRunLoader_Included

#include "fennel/tuple/TupleAccessor.h"
#include "fennel/tuple/TupleProjectionAccessor.h"
#include "fennel/tuple/TupleData.h"
#include "fennel/tuple/TupleDataWithBuffer.h"
#include "fennel/segment/SegPageLock.h"
#include "fennel/sorter/ExternalSortSubStream.h"
#include "fennel/common/TraceSource.h"

#include <vector>

FENNEL_BEGIN_NAMESPACE

class ExternalSortInfo;

/**
 * ExternalSortRunLoader manages the state of a run as it is being loaded and
 * sorted in memory.  As unsorted tuple data is loaded, it is saved in
 * discontiguous memory pages (called data buffers).  In addition, byte
 * pointers to the loaded tuple images are recorded in separately allocated
 * memory pages (called index buffers, and also discontiguous).  When the
 * loader's memory page quota is exhausted, the run is considered full.  Before
 * a run is stored or returned, it is sorted in memory using quicksort.  The
 * quicksort operates on the index buffers (moving pointers only, not data).
 * Each pointer array access during the quicksort requires an indirection
 * computation (first find the right page, and then find the tuple on that
 * page).
 */
class FENNEL_SORTER_EXPORT ExternalSortRunLoader
    : public ExternalSortSubStream, virtual public TraceSource
{

protected:
    /**
     * Global information.
     */
    ExternalSortInfo &sortInfo;

    /**
     * Cache page lock used to allocate pages of scratch memory.
     */
    SegPageLock bufferLock;

    /**
     * Maximum number of memory pages which can be allocated by this loader.
     */
    uint nMemPagesMax;

    /**
     * Array of allocated page buffers which have been recycled but not yet
     * reused.
     */
    std::vector<PBuffer> freeBuffers;

    /**
     * Array of page buffers which have been allocated as index buffers.  These
     * contain arrays of pointers to tuple data stored in separate data
     * buffers.  Order is significant, since an index entry is decomposed into
     * a page and a position on that page.
     */
    std::vector<PBuffer> indexBuffers;

    /**
     * Array of page buffers which have been allocated as data buffers.
     * These store marshalled tuple data.  Order is not significant.
     */
    std::vector<PBuffer> dataBuffers;

    /**
     * Precomputed bit rightshift count for converting a tuple index
     * to a page number (0-based position within indexBuffers array).
     */
    uint indexToPageShift;

    /**
     * Precomputed bitmask for converting a tuple index
     * to a 0-based position in pointer array on containing index page.
     */
    uint indexPageMask;

    /**
     * Pointer to first free byte in current data buffer.
     */
    PBuffer pDataBuffer;

    /**
     * Pointer to end of current data buffer.
     */
    PBuffer pDataBufferEnd;

    /**
     * Pointer to first free byte in current index buffer.
     */
    PBuffer pIndexBuffer;

    /**
     * Pointer to end of current index buffer.
     */
    PBuffer pIndexBufferEnd;

    /**
     * Number of tuples loaded so far.
     */
    uint nTuplesLoaded;

    /**
     * Number of tuples fetched so far.
     */
    uint nTuplesFetched;

    /**
     * partitionKeyData is saved.
     */
    bool partitionKeyInitialized;
    /**
     * Array used to return fetch results.  This gets bound to
     * successive index buffer contents during fetch.
     */
    ExternalSortFetchArray fetchArray;

    // TODO:  comment or replace
    TupleAccessor tupleAccessor;
    TupleAccessor tupleAccessor2;
    TupleProjectionAccessor keyAccessor;
    TupleProjectionAccessor keyAccessor2;
    TupleData keyData;
    TupleData keyData2;
    TupleDataWithBuffer partitionKeyData;

// ----------------------------------------------------------------------
// protected methods
// ----------------------------------------------------------------------

    /**
     * Allocates one page buffer.
     *
     * @return the allocated buffer, or NULL if quota has been reached
     */
    PBuffer allocateBuffer();

    /**
     * Allocates one data buffer.
     *
     * @return whether the buffer was allocated successfully
     */
    bool allocateDataBuffer();

    /**
     * Allocates one index buffer.
     *
     * @return whether the buffer was allocated successfully
     */
    bool allocateIndexBuffer();

    /**
     * Dereferences a pointer array entry during quicksort.
     *
     * @param iTuple 0-based index of tuple pointer to access
     *
     * @return read/write reference to pointer to tuple
     */
    inline PBuffer &getPointerArrayEntry(uint iTuple);

    inline void quickSortSwap(uint l, uint r);
    uint quickSortPartition(uint i, uint j, PBuffer pivot);
    PBuffer quickSortFindPivot(uint l, uint r);
    void quickSort(uint l, uint r);

public:
    /**
     * Flag used only during parallel sort.  When set, this loader
     * has been allocated to a particular thread.  When clear, this
     * loader is available for use by the next thread that needs one.
     * Access to this flag is synchronized in ExternalSortStreamImpl.
     */
    bool runningParallelTask;

    explicit ExternalSortRunLoader(ExternalSortInfo &info);
    virtual ~ExternalSortRunLoader();

    /**
     * Prepares this loader to begin a new run.
     */
    virtual void startRun();

    /**
     * @return whether this loader has been started and not yet fetched
     */
    bool isStarted();

    /**
     * Loads data from buffer into a run.
     *
     * @param bufAccessor buffer from which to read run
     *
     * @return result of load
     */
    virtual ExternalSortRC loadRun(ExecStreamBufAccessor &bufAccessor);

    /**
     * checks for "end of partition".
     * @param bufAccessor buffer from which to read run
     * @param pSrcTuple tuple buffer to check end Of Partition
     * @return true, if "end of partition" detected.
     */
    virtual bool checkEndOfPartition(
        ExecStreamBufAccessor &bufAccessor, PConstBuffer pSrcTuple);

    /**
     * check if the tuple pSrcTuple needs to be skipped from sort operation.
     * @param bufAccessor buffer from which to read run
     * @param pSrcTuple tuple buffer to check if it needs to be skipped
     * @return true, if the tuple is to be skipped. Base class always returns
     *               false.
     */
    virtual bool skipRow(
        ExecStreamBufAccessor &bufAccessor, PConstBuffer pSrcTuple);

    /**
     * Sorts loaded run.
     */
    virtual void sort();

    /**
     * @return number of tuples loaded so far in current run
     */
    uint getLoadedTupleCount();

    /**
     * Releases any resources acquired by this loader.
     */
    void releaseResources();

    // implement ExternalSortSubStream
    virtual ExternalSortFetchArray &bindFetchArray();
    virtual ExternalSortRC fetch(uint nTuplesRequested);
};

FENNEL_END_NAMESPACE

#endif

// End ExternalSortRunLoader.h
