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

#include "fennel/common/CommonPreamble.h"
#include "fennel/redsquare/sorter/ExternalSortStreamImpl.h"
#include "fennel/common/ByteOutputStream.h"
#include "fennel/segment/Segment.h"

FENNEL_BEGIN_CPPFILE("$Id$");

ExternalSortStream &ExternalSortStream::newExternalSortStream()
{
    return *new ExternalSortStreamImpl();
}

ExternalSortInfo::ExternalSortInfo()
{
    nSortMemPages = 0;
    nSortMemPagesPerRun = 0;
    cbPage = 0;
}

void ExternalSortStreamImpl::prepare(ExternalSortStreamParams const &params)
{
    SingleInputTupleStream::prepare(params);

    pTempSegment = params.pTempSegment;
    resultsReady = false;
    nStoredRuns = 0;
    nParallel = 1;
    storeFinalRun = params.storeFinalRun;
    
    switch (params.distinctness) {
    case DUP_ALLOW:
        break;
    case DUP_DISCARD:
        // TODO
        permAssert(false);
    case DUP_FAIL:
        // TODO
        permAssert(false);
    }

    TupleDescriptor const &srcRecDef = pInputStream->getOutputDesc();
    sortInfo.keyProj = params.keyProj;
    assert(params.outputTupleDesc == srcRecDef);
    sortInfo.tupleDesc = srcRecDef;
    sortInfo.keyDesc.projectFrom(sortInfo.tupleDesc,params.keyProj);
    sortInfo.cbPage = params.pTempSegment->getFullPageSize();
    sortInfo.memSegmentAccessor = params.scratchAccessor;
    sortInfo.externalSegmentAccessor.pCacheAccessor = params.pCacheAccessor;
    sortInfo.externalSegmentAccessor.pSegment = params.pTempSegment;
    sortInfo.nSortMemPages = 0;
}

void ExternalSortStreamImpl::getResourceRequirements(
    ExecutionStreamResourceQuantity &minQuantity,
    ExecutionStreamResourceQuantity &optQuantity)
{
    ExecutionStream::getResourceRequirements(minQuantity,optQuantity);

    // REVIEW
    minQuantity.nCachePages += 3;
    
    // TODO
    optQuantity = minQuantity;
}

void ExternalSortStreamImpl::setResourceAllocation(
    ExecutionStreamResourceQuantity const &quantity)
{
    // REVIEW
    ExecutionStream::setResourceAllocation(quantity);
    sortInfo.nSortMemPages = quantity.nCachePages;
    nParallel = quantity.nThreads + 1;
}

void ExternalSortStreamImpl::open(bool restart)
{
    if (restart) {
        releaseResources();
    }

    SingleInputTupleStream::open(restart);

    // divvy up available memory by degree of parallelism
    sortInfo.nSortMemPagesPerRun = (sortInfo.nSortMemPages / nParallel);

    // subtract off one page per run for I/O buffering
    assert(sortInfo.nSortMemPagesPerRun > 0);
    sortInfo.nSortMemPagesPerRun--;
    
    // need at least two non-I/O pages per run: one for keys and one for data
    assert(sortInfo.nSortMemPagesPerRun > 1);

    runLoaders.reset(new SharedExternalSortRunLoader[nParallel]);
    for (uint i = 0; i < nParallel; ++i) {
        runLoaders[i].reset(new ExternalSortRunLoader(sortInfo));
    }
    
    pOutputWriter.reset(new ExternalSortOutput(sortInfo));

    for (uint i = 0; i < nParallel; ++i) {
        runLoaders[i]->startRun();
    }
    
    // default to local sort as output obj
    pOutputWriter->setSubStream(*(runLoaders[0]));

    resultsReady = false;
}

TupleDescriptor const &ExternalSortStreamImpl::getOutputDesc() const
{
    return sortInfo.tupleDesc;
}

bool ExternalSortStreamImpl::writeResultToConsumerBuffer(
    ByteOutputStream &resultOutputStream)
{
    ExternalSortRC rc = EXTSORT_SUCCESS;

    for (;;) {
        if (!resultsReady) {
            if (nParallel > 1) {
                rc = computeFirstResultParallel();
            } else {
                rc = computeFirstResult();
            }
            
            if (rc == EXTSORT_ENDOFDATA) {
                return false;
            } else {
                assert(rc == EXTSORT_SUCCESS);
            }
        }

        rc = pOutputWriter->fetch(resultOutputStream);
        switch (rc) {
        case EXTSORT_SUCCESS:
            // in the success case we just return right away - this is
            // where we have data ready to go and we don't need to process
            // anything else
            return true;
        case EXTSORT_ENDOFDATA:
            // we're done
            return false;
        default:
            permAssert(false);
        }

        resultsReady = false;
    }
}

void ExternalSortStreamImpl::closeImpl()
{
    releaseResources();
    SingleInputTupleStream::closeImpl();
}

void ExternalSortStreamImpl::releaseResources()
{
    if (pFinalRunAccessor) {
        pFinalRunAccessor->releaseResources();
    }

    runLoaders.reset();
    pMerger.reset();
    pOutputWriter.reset();
    pFinalRunAccessor.reset();
    storedRuns.clear();
    nStoredRuns = 0;
}

ExecutionStream::BufferProvision 
ExternalSortStreamImpl::getResultBufferProvision() const
{
    return CONSUMER_PROVISION;
}

ExecutionStream::BufferProvision 
ExternalSortStreamImpl::getInputBufferRequirement() const
{
    return PRODUCER_PROVISION;
}

ExternalSortRC ExternalSortStreamImpl::computeFirstResult()
{
    ExternalSortRC rc = EXTSORT_SUCCESS;

    ExternalSortRunLoader &runLoader = *(runLoaders[0]);
    for (;;) {
        runLoader.startRun();
        rc = runLoader.loadRun(*pInputStream);
        switch (rc)
        {
        case EXTSORT_SUCCESS:
            {
                sortRun(runLoader);
                if ((nStoredRuns != 0) || storeFinalRun) {
                    // store last run
                    storeRun(runLoader);
                }
            }
            // NOTE:  fall through
        case EXTSORT_ENDOFDATA:
            {
                mergeFirstResult();

                resultsReady = true;
                return EXTSORT_SUCCESS;
            }

        case EXTSORT_OVERFLOW:
            {
                sortRun(runLoader);
                storeRun(runLoader);
                break;
            }

        default:
            {
                permAssert(false);
            }
        }
    }
    
    return rc;
}

void ExternalSortStreamImpl::storeRun(ExternalSortSubStream &subStream)
{
    FENNEL_TRACE(
        TRACE_FINE,
        "storing run " << nStoredRuns);
                    
    boost::scoped_ptr<ExternalSortRunAccessor> pRunAccessor;
    pRunAccessor.reset(new ExternalSortRunAccessor(sortInfo));
    pRunAccessor->storeRun(subStream);
    
    StrictMutexGuard mutexGuard(storedRunMutex);
    storedRuns.resize(nStoredRuns + 1);
    pRunAccessor->getStoredRun(storedRuns[nStoredRuns]);
    nStoredRuns++;
}

void ExternalSortStreamImpl::mergeFirstResult()
{
    if (nStoredRuns != 0) {
        for (uint i = 0; i < nParallel; i++) {
            runLoaders[i]->releaseResources();
        }

        if (!pMerger) {
            pMerger.reset(new ExternalSortMerger(sortInfo));
            pMerger->initRunAccess();
        }

        uint iFirstRun = nStoredRuns - 1;
        while (iFirstRun > 0) {
            uint nRunsToMerge;

            // REVIEW jvs 13-June-2005:  I had to change this to account for
            // the output buffer needed during merge.  Not sure why it worked
            // in BB?
            uint nMergePages = sortInfo.nSortMemPages - 1;
            if (nStoredRuns <= nMergePages) {
                nRunsToMerge = nStoredRuns;
            } else {
                nRunsToMerge = std::min(
                    nStoredRuns - nMergePages + 1,
                    nMergePages);
            }

            optimizeRunOrder();
            iFirstRun = nStoredRuns - nRunsToMerge;

            FENNEL_TRACE(
                TRACE_FINE,
                "merging from run " << iFirstRun
                << " with run count = " << nRunsToMerge);
            
            if ((iFirstRun > 0) || storeFinalRun) {
                pMerger->startMerge(
                    storedRuns.begin() + iFirstRun,nRunsToMerge,true);
                storeRun(*pMerger);
                deleteStoredRunInfo(iFirstRun,nRunsToMerge);
            } else {
                // REVIEW: we might actually want to delete the runs as we read
                // them here (last param = true).
                pMerger->startMerge(
                    storedRuns.begin() + iFirstRun,nRunsToMerge,false);
            }
        }

        if (nStoredRuns == 1) {
            if (!pFinalRunAccessor) {
                pFinalRunAccessor.reset(new ExternalSortRunAccessor(sortInfo));
            }

            FENNEL_TRACE(
                TRACE_FINE,
                "fetching from final run");
            
            pFinalRunAccessor->initRead();
            // REVIEW:  shouldn't last param be true instead?
            pFinalRunAccessor->startRead(storedRuns[0],false);
            pMerger->releaseResources();
            pOutputWriter->setSubStream(*pFinalRunAccessor);
        } else {
            FENNEL_TRACE(
                TRACE_FINE,
                "fetching from final merge with run count = "
                << nStoredRuns);
            
            pOutputWriter->setSubStream(*pMerger);
        }
    }
}

void ExternalSortStreamImpl::optimizeRunOrder()
{
    uint i = nStoredRuns - 1;
    while ((i > 0)
           && (storedRuns[i].nStoredPages > storedRuns[i-1].nStoredPages))
    {
        std::swap(storedRuns[i],storedRuns[i-1]);
        i--;
    }
}

void ExternalSortStreamImpl::deleteStoredRunInfo(uint iFirstRun,uint nRuns)
{
    StrictMutexGuard mutexGuard(storedRunMutex);
    storedRuns.erase(
        storedRuns.begin() + iFirstRun,
        storedRuns.begin() + iFirstRun + nRuns);
    nStoredRuns -= nRuns;
}

ExternalSortRC ExternalSortStreamImpl::computeFirstResultParallel()
{
    // FIXME jvs 19-June-2005:  ThreadPool needs to propagate excns!

    assert(nParallel > 1);

    // minus one because the main dispatcher thread runs in parallel with the
    // pooled threads
    threadPool.start(nParallel - 1);
    try {
        for (;;) {
            ExternalSortRunLoader &runLoader = reserveRunLoader();
            runLoader.startRun();
            ExternalSortRC rc = runLoader.loadRun(*pInputStream);
            if (rc == EXTSORT_ENDOFDATA) {
                // the entire input has been processed, so we're ready
                // for merge
                unreserveRunLoader(runLoader);
                break;
            }
            // otherwise, schedule a new sort task
            ExternalSortTask task(*this,runLoader);
            threadPool.submitTask(task);
        }
    } catch (...) {
        // REVEW jvs 19-June-2005:  signal a request to expedite cleanup?
        
        // wait for all tasks to clean up
        threadPool.stop();
        throw;
    }

    // wait for all tasks to complete before beginning merge
    threadPool.stop();

    mergeFirstResult();
    resultsReady = true;

    return EXTSORT_SUCCESS;
}

void ExternalSortTask::execute()
{
    sortStream.sortRun(runLoader);
    sortStream.storeRun(runLoader);
    sortStream.unreserveRunLoader(runLoader);
}

void ExternalSortStreamImpl::sortRun(ExternalSortRunLoader &runLoader)
{
    FENNEL_TRACE(
        TRACE_FINE,
        "sorting run with tuple count = "
        << runLoader.getLoadedTupleCount());
    runLoader.sort();
}

ExternalSortRunLoader &ExternalSortStreamImpl::reserveRunLoader()
{
    StrictMutexGuard mutexGuard(runLoaderMutex);
    for (;;) {
        for (uint i = 0; i < nParallel; ++i) {
            ExternalSortRunLoader &runLoader = *(runLoaders[i]);
            if (!runLoader.runningParallelTask) {
                runLoader.runningParallelTask = true;
                return runLoader;
            }
        }
        runLoaderAvailable.wait(mutexGuard);
    }
}

void ExternalSortStreamImpl::unreserveRunLoader(
    ExternalSortRunLoader &runLoader)
{
    StrictMutexGuard mutexGuard(runLoaderMutex);
    runLoader.runningParallelTask = false;
    runLoaderAvailable.notify_all();
}

FENNEL_END_CPPFILE("$Id$");

// End ExternalSortStream.cpp
