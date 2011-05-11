/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005 The Eigenbase Project
// Copyright (C) 2009 SQLstream, Inc.
// Copyright (C) 2004 Dynamo BI Corporation
// Portions Copyright (C) 2004 John V. Sichi
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

#include "fennel/common/CommonPreamble.h"
#include "fennel/sorter/ExternalSortExecStreamImpl.h"
#include "fennel/segment/Segment.h"
#include "fennel/segment/SegStreamAllocation.h"
#include "fennel/exec/ExecStreamGraphImpl.h"
#include "fennel/exec/ExecStreamBufAccessor.h"

FENNEL_BEGIN_CPPFILE("$Id$");

ExternalSortExecStream *ExternalSortExecStream::newExternalSortExecStream()
{
    return new ExternalSortExecStreamImpl();
}

ExternalSortInfo::ExternalSortInfo(ExecStream &streamInit)
    : stream(streamInit)
{
    nSortMemPages = 0;
    nIndexMemPages = 0;
    nSortMemPagesPerRun = 0;
    cbPage = 0;
    partitionKeyCount = 0;
}

int ExternalSortInfo::compareKeys(TupleData const &key1, TupleData const &key2)
{
    int c = keyDesc.compareTuples(key1, key2);
    if (!c) {
        return 0;
    }
    // abs(c) is 1-based column ordinal
    int i = (c > 0) ? c : -c;
    // shift to 0-based
    --i;
    if (descendingKeyColumns[i]) {
        // flip comparison result for DESC
        return -c;
    } else {
        return c;
    }
}

ExternalSortExecStreamImpl::ExternalSortExecStreamImpl()
    : sortInfo(*this)
{
}

void ExternalSortExecStreamImpl::prepare(
    ExternalSortExecStreamParams const &params)
{
    ConduitExecStream::prepare(params);

    pTempSegment = params.pTempSegment;
    resultsReady = false;
    nParallel = 1;
    storeFinalRun = params.storeFinalRun;
    estimatedNumRows = params.estimatedNumRows;
    earlyClose = params.earlyClose;

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

    TupleDescriptor const &srcRecDef = pInAccessor->getTupleDesc();
    sortInfo.keyProj = params.keyProj;
    assert(params.outputTupleDesc == srcRecDef);
    sortInfo.tupleDesc = srcRecDef;
    sortInfo.keyDesc.projectFrom(sortInfo.tupleDesc, params.keyProj);
    for (int i = 0; i < sortInfo.keyProj.size(); i++) {
         FENNEL_TRACE(
             TRACE_FINEST, "Sort Key Column = " << sortInfo.keyProj[i]);
    }
    sortInfo.descendingKeyColumns = params.descendingKeyColumns;
    if (sortInfo.descendingKeyColumns.empty()) {
        // default is all ascending
        sortInfo.descendingKeyColumns.resize(sortInfo.keyProj.size(), false);
    }
    for (int i = 0; i < sortInfo.descendingKeyColumns.size(); i++) {
         FENNEL_TRACE(
             TRACE_FINEST, "Sort Order for Column " << i << " = "
             << sortInfo.descendingKeyColumns[i]);
    }
    sortInfo.cbPage = params.pTempSegment->getFullPageSize();
    sortInfo.memSegmentAccessor = params.scratchAccessor;
    sortInfo.externalSegmentAccessor.pCacheAccessor = params.pCacheAccessor;
    sortInfo.externalSegmentAccessor.pSegment = params.pTempSegment;
    sortInfo.partitionKeyCount = params.partitionKeyCount;
    if (sortInfo.partitionKeyCount < 0
        || sortInfo.partitionKeyCount > sortInfo.keyProj.size())
    {
        sortInfo.partitionKeyCount = 0;
    }
    sortInfo.nSortMemPages = 0;
    sortInfo.nIndexMemPages = 0;
    assert(sortInfo.partitionKeyCount >= 0);
    assert(sortInfo.partitionKeyCount <= sortInfo.keyProj.size());
    if (earlyClose) {
        assert(sortInfo.partitionKeyCount == 0);
    }
}

void ExternalSortExecStreamImpl::getResourceRequirements(
    ExecStreamResourceQuantity &minQuantity,
    ExecStreamResourceQuantity &optQuantity,
    ExecStreamResourceSettingType &optType)
{
    ConduitExecStream::getResourceRequirements(minQuantity, optQuantity);

    // REVIEW
    uint minPages = 3;
    minQuantity.nCachePages += minPages;

    // if no estimated row count is available, request an unbounded amount
    // from the resource governor; otherwise, estimate the number of pages
    // for an in-memory sort
    if (isMAXU(estimatedNumRows)) {
        optType = EXEC_RESOURCE_UNBOUNDED;
    } else {
        // use the average of the min and max rowsizes
        // TODO - use stats to come up with a more accurate average
        RecordNum nPages =
            estimatedNumRows
            * ((pOutAccessor->getScratchTupleAccessor().getMaxByteCount()
                + pOutAccessor->getScratchTupleAccessor().getMinByteCount())
               / 2)
            / sortInfo.memSegmentAccessor.pSegment->getUsablePageSize();
        uint numPages;
        if (nPages >= uint(MAXU)) {
            numPages = uint(MAXU) - 1;
        } else {
            numPages = uint(nPages);
        }
        // make sure the opt is bigger than the min; otherwise, the
        // resource governor won't try to give it extra
        optQuantity.nCachePages += std::max(minPages + 1, numPages);
        optType = EXEC_RESOURCE_ESTIMATE;
    }
}

void ExternalSortExecStreamImpl::setResourceAllocation(
    ExecStreamResourceQuantity &quantity)
{
    ConduitExecStream::setResourceAllocation(quantity);
    sortInfo.nSortMemPages = quantity.nCachePages;
    sortInfo.nIndexMemPages = getCacheConsciousPageRation(
        *(sortInfo.memSegmentAccessor.pCacheAccessor),
        quantity);

    nParallel = quantity.nThreads + 1;

    // NOTE jvs 10-Nov-2004:  parallel sort is currently disabled
    // as an effect of the scheduler-revamp.  We may resurrect it, or
    // we may decide to handle parallelism up at the scheduler level.
    assert(nParallel == 1);
}

void ExternalSortExecStreamImpl::open(bool restart)
{
    if (restart) {
        releaseResources();
    }

    ConduitExecStream::open(restart);

    // divvy up available memory by degree of parallelism
    sortInfo.nSortMemPagesPerRun = (sortInfo.nSortMemPages / nParallel);

    // subtract off one page per run for I/O buffering
    assert(sortInfo.nSortMemPagesPerRun > 0);
    sortInfo.nSortMemPagesPerRun--;

    // need at least two non-I/O pages per run: one for keys and one for data
    assert(sortInfo.nSortMemPagesPerRun > 1);

    // Initialize RunLoaders.
    initRunLoaders(false);

    pOutputWriter.reset(new ExternalSortOutput(sortInfo));

    for (uint i = 0; i < nParallel; ++i) {
        runLoaders[i]->startRun();
    }

    // default to local sort as output obj
    pOutputWriter->setSubStream(*(runLoaders[0]));

    resultsReady = false;
}

void ExternalSortExecStreamImpl::initRunLoaders(bool restart)
{
    if (restart) {
        // if restarting runLoaders, do nothing.
    } else {
        runLoaders.reset(new SharedExternalSortRunLoader[nParallel]);
        for (uint i = 0; i < nParallel; ++i) {
            runLoaders[i].reset(new ExternalSortRunLoader(sortInfo));
            runLoaders[i]->initTraceSource(
               getSharedTraceTarget(),
               getTraceSourceName() + ".runLoader");
        }
    }
}

ExecStreamResult ExternalSortExecStreamImpl::execute(
    ExecStreamQuantum const &quantum)
{
    for (;;) {
        if (!resultsReady) {
            if (pInAccessor->getState() != EXECBUF_EOS) {
                ExecStreamResult rc = precheckConduitBuffers();
                if (rc == EXECRC_BUF_UNDERFLOW) {
                    rc = handleUnderflow();
                }
                if (rc != EXECRC_YIELD) {
                    return rc;
                }
                if (nParallel > 1) {
                    // FIXME
                    computeFirstResultParallel();
                } else {
                    rc = computeFirstResult();
                    if (rc != EXECRC_YIELD) {
                        return rc;
                    }
                }
            } else {
                ExternalSortRunLoader &runLoader = *(runLoaders[0]);
                if (runLoader.isStarted()) {
                    sortRun(runLoader);
                    if (storedRuns.size() || storeFinalRun) {
                        // store last run
                        storeRun(runLoader);
                    }
                }
            }
            mergeFirstResult();

            // close the producers now that we've read all input
            if (earlyClose) {
                ExecStreamGraphImpl &graphImpl =
                    dynamic_cast<ExecStreamGraphImpl&>(getGraph());
                graphImpl.closeProducers(getStreamId());
            }

            resultsReady = true;
        }

        ExecStreamResult rc2 = pOutputWriter->fetch(*pOutAccessor);
        if (rc2 == EXECRC_EOS) {
            if (sortInfo.partitionKeyCount > 0
                && pInAccessor->getState() != EXECBUF_EOS)
            {
                // results have already been produced. Continue loading rows
                // for the next "partition".
                reallocateResources();
                continue;
            }
            pOutAccessor->markEOS();
        }
        return rc2;
    }
}

ExecStreamResult ExternalSortExecStreamImpl::handleUnderflow()
{
    // do nothing just return underflow
    return EXECRC_BUF_UNDERFLOW;
}

void ExternalSortExecStreamImpl::reallocateResources()
{
    initRunLoaders(true);
    for (uint i = 0; i < nParallel; ++i) {
        runLoaders[i]->startRun();
    }

    pOutputWriter.reset(new ExternalSortOutput(sortInfo));
    // default to local sort as output obj
    pOutputWriter->setSubStream(*(runLoaders[0]));
    resultsReady = false;
    pMerger.reset();
    pFinalRunAccessor.reset();
    storedRuns.clear();
}

void ExternalSortExecStreamImpl::closeImpl()
{
    releaseResources();
    ConduitExecStream::closeImpl();
}

void ExternalSortExecStreamImpl::releaseResources()
{
    if (pFinalRunAccessor) {
        pFinalRunAccessor->releaseResources();
    }

    runLoaders.reset();
    pMerger.reset();
    pOutputWriter.reset();
    pFinalRunAccessor.reset();
    storedRuns.clear();
}

ExecStreamResult ExternalSortExecStreamImpl::computeFirstResult()
{
    ExternalSortRunLoader &runLoader = *(runLoaders[0]);
    for (;;) {
        if (!runLoader.isStarted()) {
            runLoader.startRun();
        }
        ExternalSortRC rc = runLoader.loadRun(*pInAccessor);
        if (rc == EXTSORT_YIELD) {
            if (runLoader.getLoadedTupleCount() > 0) {
                sortRun(runLoader);
                if (storedRuns.size() || storeFinalRun) {
                    storeRun(runLoader);
                }
            }
            // saw 'end of partition'. ready to merge all stored sortRuns.
            return EXECRC_YIELD;
        } else if (rc == EXTSORT_OVERFLOW) {
            sortRun(runLoader);
            storeRun(runLoader);
            // now ready to load more rows from input.
        }
        // load more rows from input.
        return EXECRC_BUF_UNDERFLOW;
    }
}

void ExternalSortExecStreamImpl::storeRun(ExternalSortSubStream &subStream)
{
    FENNEL_TRACE(
        TRACE_FINE,
        "storing run " << storedRuns.size());

    boost::scoped_ptr<ExternalSortRunAccessor> pRunAccessor;
    pRunAccessor.reset(new ExternalSortRunAccessor(sortInfo));
    pRunAccessor->storeRun(subStream);

    StrictMutexGuard mutexGuard(storedRunMutex);
    storedRuns.push_back(pRunAccessor->getStoredRun());
}

void ExternalSortExecStreamImpl::mergeFirstResult()
{
    if (storedRuns.size()) {
        for (uint i = 0; i < nParallel; i++) {
            runLoaders[i]->releaseResources();
        }

        if (!pMerger) {
            pMerger.reset(new ExternalSortMerger(sortInfo));
            pMerger->initRunAccess();
        }

        uint iFirstRun = storedRuns.size() - 1;
        while (iFirstRun > 0) {
            uint nRunsToMerge;

            // REVIEW jvs 13-June-2004:  I had to change this to account for
            // the output buffer needed during merge.  Not sure why it worked
            // in BB?
            uint nMergePages = sortInfo.nSortMemPages - 1;
            if (storedRuns.size() <= nMergePages) {
                nRunsToMerge = storedRuns.size();
            } else {
                nRunsToMerge = std::min<uint>(
                    storedRuns.size() - nMergePages + 1,
                    nMergePages);
            }

            optimizeRunOrder();
            iFirstRun = storedRuns.size() - nRunsToMerge;

            FENNEL_TRACE(
                TRACE_FINE,
                "merging from run " << iFirstRun
                << " with run count = " << nRunsToMerge);

            pMerger->startMerge(
                storedRuns.begin() + iFirstRun, nRunsToMerge);
            if ((iFirstRun > 0) || storeFinalRun) {
                storeRun(*pMerger);
                deleteStoredRunInfo(iFirstRun, nRunsToMerge);
            }
        }

        if (storedRuns.size() == 1) {
            if (!pFinalRunAccessor) {
                pFinalRunAccessor.reset(new ExternalSortRunAccessor(sortInfo));
            }

            FENNEL_TRACE(
                TRACE_FINE,
                "fetching from final run");

            pFinalRunAccessor->initRead();
            pFinalRunAccessor->startRead(storedRuns[0]);
            pMerger->releaseResources();
            pOutputWriter->setSubStream(*pFinalRunAccessor);
        } else {
            FENNEL_TRACE(
                TRACE_FINE,
                "fetching from final merge with run count = "
                << storedRuns.size());

            pOutputWriter->setSubStream(*pMerger);
        }
    }
}

void ExternalSortExecStreamImpl::optimizeRunOrder()
{
    uint i = storedRuns.size() - 1;
    while ((i > 0)
           && (storedRuns[i]->getWrittenPageCount()
               > storedRuns[i - 1]->getWrittenPageCount()))
    {
        std::swap(storedRuns[i], storedRuns[i - 1]);
        i--;
    }
}

void ExternalSortExecStreamImpl::deleteStoredRunInfo(uint iFirstRun, uint nRuns)
{
    StrictMutexGuard mutexGuard(storedRunMutex);
    storedRuns.erase(
        storedRuns.begin() + iFirstRun,
        storedRuns.begin() + iFirstRun + nRuns);
}

void ExternalSortExecStreamImpl::computeFirstResultParallel()
{
    // FIXME jvs 19-June-2004:  ThreadPool needs to propagate excns!

    assert(nParallel > 1);

    // minus one because the main dispatcher thread runs in parallel with the
    // pooled threads
    threadPool.start(nParallel - 1);
    try {
        for (;;) {
            ExternalSortRunLoader &runLoader = reserveRunLoader();
            runLoader.startRun();
            // FIXME
#if 0
            ExternalSortRC rc = runLoader.loadRun(*pInputStream);
#else
            ExternalSortRC rc = EXTSORT_ENDOFDATA;
#endif
            if (rc == EXTSORT_ENDOFDATA) {
                // the entire input has been processed, so we're ready
                // for merge
                unreserveRunLoader(runLoader);
                break;
            }
            // otherwise, schedule a new sort task
            // FIXME
#if 0
            ExternalSortTask task(*this,runLoader);
            threadPool.submitTask(task);
#endif
        }
    } catch (...) {
        // REVEW jvs 19-June-2004:  signal a request to expedite cleanup?

        // wait for all tasks to clean up
        threadPool.stop();
        throw;
    }

    // wait for all tasks to complete before beginning merge
    threadPool.stop();
}

void ExternalSortTask::execute()
{
    sortStream.sortRun(runLoader);
    sortStream.storeRun(runLoader);
    sortStream.unreserveRunLoader(runLoader);
}

void ExternalSortExecStreamImpl::sortRun(ExternalSortRunLoader &runLoader)
{
    FENNEL_TRACE(
        TRACE_FINE,
        "sorting run with tuple count = "
        << runLoader.getLoadedTupleCount());
    runLoader.sort();
}

ExternalSortRunLoader &ExternalSortExecStreamImpl::reserveRunLoader()
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

void ExternalSortExecStreamImpl::unreserveRunLoader(
    ExternalSortRunLoader &runLoader)
{
    StrictMutexGuard mutexGuard(runLoaderMutex);
    runLoader.runningParallelTask = false;
    runLoaderAvailable.notify_all();
}

FENNEL_END_CPPFILE("$Id$");

// End ExternalSortExecStreamImpl.cpp
