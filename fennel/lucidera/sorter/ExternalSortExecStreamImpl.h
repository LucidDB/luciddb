/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2004-2005 LucidEra, Inc.
// Copyright (C) 2005-2005 The Eigenbase Project
// Portions Copyright (C) 2004-2005 John V. Sichi
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

#ifndef Fennel_ExternalSortExecStreamImpl_Included
#define Fennel_ExternalSortExecStreamImpl_Included

#include "fennel/lucidera/sorter/ExternalSortExecStream.h"
#include "fennel/synch/SynchObj.h"
#include "fennel/synch/ThreadPool.h"

#include <boost/scoped_ptr.hpp>

#include "fennel/lucidera/sorter/ExternalSortInfo.h"
#include "fennel/lucidera/sorter/ExternalSortRunLoader.h"
#include "fennel/lucidera/sorter/ExternalSortRunAccessor.h"
#include "fennel/lucidera/sorter/ExternalSortMerger.h"
#include "fennel/lucidera/sorter/ExternalSortOutput.h"
#include "fennel/lucidera/sorter/ExternalSortTask.h"

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
class ExternalSortExecStreamImpl : public ExternalSortExecStream
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
// private methods
// ----------------------------------------------------------------------

    // TODO jvs 10-Nov-2004:  rework comments
    /**
     * Performs enough sorting to be able to start returning results
     * (non-parallel version).
     */
    void computeFirstResult();
    
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
    
    // implement ExecStream
    virtual void closeImpl();
    
public:
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
