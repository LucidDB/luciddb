/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2004-2005 Red Square, Inc.
// Copyright (C) 2005-2005 The Eigenbase Project
// Portions Copyright (C) 2004-2005 John V. Sichi
//
// This program is free software; you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation; either version 2 of the License, or
// (at your option) any later Eigenbase-approved version.
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

#ifndef Fennel_ExternalSortStreamImpl_Included
#define Fennel_ExternalSortStreamImpl_Included

#include "fennel/redsquare/sorter/ExternalSortStream.h"
#include "fennel/synch/SynchObj.h"
#include "fennel/synch/ThreadPool.h"

#include <boost/scoped_ptr.hpp>

#include "fennel/redsquare/sorter/ExternalSortInfo.h"
#include "fennel/redsquare/sorter/ExternalSortRunLoader.h"
#include "fennel/redsquare/sorter/ExternalSortRunAccessor.h"
#include "fennel/redsquare/sorter/ExternalSortMerger.h"
#include "fennel/redsquare/sorter/ExternalSortOutput.h"
#include "fennel/redsquare/sorter/ExternalSortTask.h"

FENNEL_BEGIN_NAMESPACE

// DEPRECATED
    
class ExternalSortRunLoader;
typedef boost::shared_ptr<ExternalSortRunLoader>
SharedExternalSortRunLoader;

/**
 * ExternalSortStreamImpl implements the ExternalSortStream XO.
 */
class ExternalSortStreamImpl : public ExternalSortStream
{
    friend class ExternalSortTask;

    /**
     * Segment to use for storing runs externally.
     */
    SharedSegment pTempSegment;

    /**
     * Global information shared with subcomponents.
     */
    ExternalSortInfo sortInfo;

    /**
     * Maximum number of parallel threads to use.
     */
    uint nParallel;

    /**
     * Array of helpers used to load, quicksort, and store runs.  This array
     * has size equal to nParallel.  For non-parallel sort (nParallel=1), this
     * means there's just one entry.  For parallel sort, this array acts as an
     * availability queue governed by runLoaderAvailable, runLoaderMutex, and
     * the ExternalSortRunLoader::runningParallelTask flag.
     */
    boost::scoped_array<SharedExternalSortRunLoader> runLoaders;

    /**
     * Thread pool used during parallel sort.
     */
    ThreadPool<ExternalSortTask> threadPool;

    /**
     * Condition variable used to signal availability of
     * entries in runLoaders array.
     */
    LocalCondition runLoaderAvailable;

    /**
     * Synchronization for availability status in runLoaders array.
     */
    StrictMutex runLoaderMutex;
    
    /**
     * Helper used to read final run when stored externally.
     */
    boost::scoped_ptr<ExternalSortRunAccessor> pFinalRunAccessor;

    /**
     * Helper used to merge runs.
     */
    boost::scoped_ptr<ExternalSortMerger> pMerger;

    /**
     * Helper used to write XO output.
     */
    boost::scoped_ptr<ExternalSortOutput> pOutputWriter;

    /**
     * Synchronization for storedRuns and nStoredRuns.
     */
    StrictMutex storedRunMutex;
    
    /**
     * Information on runs stored externally.
     */
    std::vector<ExternalSortStoredRun> storedRuns;

    /**
     * Number of stored runs (almost redundant with storedRuns.size()).
     */
    uint nStoredRuns;

    /**
     * Whether the XO is ready to start writing results.
     */
    bool resultsReady;

    /**
     * Whether to materialize one big final run, or return results
     * directly from last merge stage.
     */
    bool storeFinalRun;

// ----------------------------------------------------------------------
// private methods
// ----------------------------------------------------------------------
    
    /**
     * Performs enough sorting to be able to start returning results
     * (non-parallel version).
     *
     * @return either EXTSORT_ENDOFDATA (empty input) or
     * EXTSORT_SUCCESS (non-empty input)
     */
    ExternalSortRC computeFirstResult();
    
    /**
     * Performs enough sorting to be able to start returning results
     * (parallel version).
     *
     * @return either EXTSORT_ENDOFDATA (empty input) or
     * EXTSORT_SUCCESS (non-empty input)
     */
    ExternalSortRC computeFirstResultParallel();

    /**
     * Sorts one run in memory.
     *
     * @param runLoader loaded run to sort
     */
    void sortRun(ExternalSortRunLoader &runLoader);
    
    /**
     * Stores one run.
     *
     * @param subStream substream whose contents are to be fetched and stored
     * as a run
     */
    void storeRun(ExternalSortSubStream &subStream);

    /**
     * Performs enough merging to be able to start returning results.
     */
    void mergeFirstResult();

    /**
     * Adjusts run order for optimal merging.
     */
    void optimizeRunOrder();

    /**
     * Deletes information on runs after they have been merged.
     *
     * @param iFirstRun 0-based deletion start position in storedRuns
     *
     * @param nRuns number of storedRuns entries to delete
     */
    void deleteStoredRunInfo(uint iFirstRun, uint nRuns);

    /**
     * Reserves one run loader for use during parallel sort, blocking
     * on runLoaderAvailable until one becomes unavailable.
     *
     * @return reserved loader
     */
    ExternalSortRunLoader &reserveRunLoader();

    /**
     * Unreserves one run loader after its contents have been stored,
     * making it available to other threads.
     *
     * @param runLoader loader to unreserve
     */
    void unreserveRunLoader(ExternalSortRunLoader &runLoader);

    /**
     * Releases resources associated with this stream.
     */
    void releaseResources();
    
    // implement TupleStream
    virtual void closeImpl();
    
public:
    // implement TupleStream
    virtual void prepare(ExternalSortStreamParams const &params);
    virtual void open(bool restart);
    virtual TupleDescriptor const &getOutputDesc() const;
    virtual bool writeResultToConsumerBuffer(
        ByteOutputStream &resultOutputStream);
    virtual BufferProvision getResultBufferProvision() const;
    virtual BufferProvision getInputBufferRequirement() const;
    virtual void getResourceRequirements(
        ExecutionStreamResourceQuantity &minQuantity,
        ExecutionStreamResourceQuantity &optQuantity);
    virtual void setResourceAllocation(
        ExecutionStreamResourceQuantity const &quantity);
};

FENNEL_END_NAMESPACE

#endif

// End ExternalSortStreamImpl.h
