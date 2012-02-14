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

#ifndef Fennel_ExternalSortExecStreamImpl_Included
#define Fennel_ExternalSortExecStreamImpl_Included

#include "fennel/sorter/ExternalSortExecStream.h"
#include "fennel/synch/SynchObj.h"
#include "fennel/synch/ThreadPool.h"

#include <boost/scoped_ptr.hpp>

#include "fennel/sorter/ExternalSortInfo.h"
#include "fennel/sorter/ExternalSortRunLoader.h"
#include "fennel/sorter/ExternalSortRunAccessor.h"
#include "fennel/sorter/ExternalSortMerger.h"
#include "fennel/sorter/ExternalSortOutput.h"
#include "fennel/sorter/ExternalSortTask.h"

FENNEL_BEGIN_NAMESPACE

class ExternalSortRunLoader;
typedef boost::shared_ptr<ExternalSortRunLoader>
SharedExternalSortRunLoader;

/**
 * ExternalSortExecStreamImpl implements the ExternalSortExecStream interface.
 *
 * @author John V. Sichi
 * @version $Id$
 */
class FENNEL_SORTER_EXPORT ExternalSortExecStreamImpl
    : public ExternalSortExecStream
{
    friend class ExternalSortTask;

protected:
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
     * Synchronization for storedRuns.
     */
    StrictMutex storedRunMutex;

    /**
     * Information on runs stored externally.
     */
    std::vector<SharedSegStreamAllocation> storedRuns;

    /**
     * Whether the XO is ready to start writing results.
     */
    bool resultsReady;

    /**
     * Whether to materialize one big final run, or return results
     * directly from last merge stage.
     */
    bool storeFinalRun;

    /**
     * Estimate of the number of rows in the sort input.  If < 0, no stats
     * were available to estimate this value.
     */
    int estimatedNumRows;

    /**
     * If true, close producers once all input has been read
     */
    bool earlyClose;

// ----------------------------------------------------------------------
// protected methods
// ----------------------------------------------------------------------

    // TODO jvs 10-Nov-2004:  rework comments
    /**
     * Performs enough sorting to be able to start returning results
     * (non-parallel version).
     */
    virtual ExecStreamResult computeFirstResult();

    /**
     * Performs enough sorting to be able to start returning results
     * (parallel version).
     */
    void computeFirstResultParallel();

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

    /**
     * Reallocates resources for partition sorts.
     */
    void reallocateResources();

    // implement ExecStream
    virtual void closeImpl();

    // Initialize RunLoaders
    virtual void initRunLoaders(bool restart);

    // handle underflow condition
    virtual ExecStreamResult handleUnderflow();

public:
    explicit ExternalSortExecStreamImpl();

    // implement ExecStream
    virtual void prepare(ExternalSortExecStreamParams const &params);
    virtual void open(bool restart);
    virtual ExecStreamResult execute(ExecStreamQuantum const &quantum);
    virtual void getResourceRequirements(
        ExecStreamResourceQuantity &minQuantity,
        ExecStreamResourceQuantity &optQuantity,
        ExecStreamResourceSettingType &optType);
    virtual void setResourceAllocation(
        ExecStreamResourceQuantity &quantity);
};

FENNEL_END_NAMESPACE

#endif

// End ExternalSortExecStreamImpl.h
