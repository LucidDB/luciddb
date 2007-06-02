/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2004-2007 LucidEra, Inc.
// Copyright (C) 2005-2007 The Eigenbase Project
// Portions Copyright (C) 2004-2007 John V. Sichi
//
// This program is free software; you can redistribute it and/or modify it
// under the terms of the GNU General Public License as published by the Free
// Software Foundation; either version 2 of the License, or (at your option)
// any later version approved by The Eigenbase Project.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program; if not, write to the Free Software
// Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
*/

#ifndef Fennel_ExternalSortRunLoader_Included
#define Fennel_ExternalSortRunLoader_Included

#include "fennel/tuple/TupleAccessor.h"
#include "fennel/tuple/TupleProjectionAccessor.h"
#include "fennel/tuple/TupleData.h"
#include "fennel/segment/SegPageLock.h"
#include "fennel/lucidera/sorter/ExternalSortSubStream.h"

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
class ExternalSortRunLoader : public ExternalSortSubStream
{
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

// ----------------------------------------------------------------------
// private methods
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
    
    inline void quickSortSwap(uint l,uint r);
    uint quickSortPartition(uint i,uint j,PBuffer pivot);
    PBuffer quickSortFindPivot(uint l,uint r);
    void quickSort(uint l,uint r);
    
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
    void startRun();

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
    ExternalSortRC loadRun(ExecStreamBufAccessor &bufAccessor);

    /**
     * Sorts loaded run.
     */
    void sort();

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
