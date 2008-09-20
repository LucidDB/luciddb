/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2008-2008 The Eigenbase Project
// Copyright (C) 2008-2008 Disruptive Tech
// Copyright (C) 2008-2008 LucidEra, Inc.
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
#include "fennel/exec/ParallelExecStreamScheduler.h"
#include "fennel/exec/ExecStreamGraphImpl.h"
#include "fennel/exec/ExecStream.h"
#include "fennel/exec/ExecStreamBufAccessor.h"
#include "fennel/exec/DoubleBufferExecStream.h"
#include "fennel/exec/ExecStreamEmbryo.h"
#include "fennel/common/AbortExcn.h"
#include "fennel/synch/ThreadTracker.h"

FENNEL_BEGIN_CPPFILE("$Id$");

// TODO jvs 5-Jul-2008:  more tracing

ParallelExecStreamScheduler::ParallelExecStreamScheduler(
    SharedTraceTarget pTraceTarget,
    std::string name,
    ThreadTracker &threadTrackerInit,
    uint degreeOfParallelismInit)
    : TraceSource(pTraceTarget, name),
      ExecStreamScheduler(pTraceTarget, name),
      threadTracker(threadTrackerInit)
{
    degreeOfParallelism = degreeOfParallelismInit;
    assert(degreeOfParallelism > 0);
    threadPool.setThreadTracker(threadTracker);
    mgrState = MGR_STOPPED;
}

ParallelExecStreamScheduler::~ParallelExecStreamScheduler()
{
}

inline void ParallelExecStreamScheduler::alterNeighborInhibition(
    ExecStreamId streamId, int delta)
{
    ExecStreamGraphImpl &graphImpl =
        dynamic_cast<ExecStreamGraphImpl&>(*pGraph);
    ExecStreamGraphImpl::GraphRep const &graphRep = graphImpl.getGraphRep();
    ExecStreamGraphImpl::OutEdgeIterPair outEdges =
        boost::out_edges(streamId, graphRep);
    for (; outEdges.first != outEdges.second; ++(outEdges.first)) {
        ExecStreamGraphImpl::Edge edge = *(outEdges.first);
        ExecStreamId consumer = boost::target(edge, graphRep);
        streamStateMap[consumer].inhibitionCount += delta;
        assert(streamStateMap[consumer].inhibitionCount >= 0);
    }
    ExecStreamGraphImpl::InEdgeIterPair inEdges =
        boost::in_edges(streamId, graphRep);
    for (; inEdges.first != inEdges.second; ++(inEdges.first)) {
        ExecStreamGraphImpl::Edge edge = *(inEdges.first);
        ExecStreamId producer = boost::source(edge, graphRep);
        streamStateMap[producer].inhibitionCount += delta;
        assert(streamStateMap[producer].inhibitionCount >= 0);
    }
}

inline bool ParallelExecStreamScheduler::isInhibited(ExecStreamId streamId)
{
    return streamStateMap[streamId].inhibitionCount > 0;
}

void ParallelExecStreamScheduler::addGraph(
    SharedExecStreamGraph pGraphInit)
{
    assert(!pGraph);
    
    ExecStreamScheduler::addGraph(pGraphInit);
    pGraph = pGraphInit;
}

void ParallelExecStreamScheduler::removeGraph(
    SharedExecStreamGraph pGraphInit)
{
    assert(pGraph == pGraphInit);
    
    pGraph.reset();
    ExecStreamScheduler::removeGraph(pGraphInit);
}

void ParallelExecStreamScheduler::start()
{
    FENNEL_TRACE(TRACE_FINE,"start");
    assert(pGraph->isAcyclic());
    pPendingExcn.reset();

    ExecStreamGraphImpl &graphImpl =
        dynamic_cast<ExecStreamGraphImpl&>(*pGraph);
    ExecStreamGraphImpl::GraphRep const &graphRep = graphImpl.getGraphRep();
    ExecStreamGraphImpl::VertexIterPair vertices = boost::vertices(graphRep);
    while (vertices.first != vertices.second) {
        ExecStreamId streamId = *vertices.first;
        streamStateMap[streamId].state = SS_SLEEPING;
        streamStateMap[streamId].inhibitionCount = 0;
        ++vertices.first;
    }

    vertices = boost::vertices(graphRep);
    while (vertices.first != vertices.second) {
        ExecStreamId streamId = *vertices.first;
        if (!graphImpl.getStreamFromVertex(streamId)) {
            // initially inhibit producers until first call to readStream
            alterNeighborInhibition(streamId, +1);
            ExecStreamGraphImpl::InEdgeIterPair inEdges =
                boost::in_edges(streamId, graphRep);
            for (; inEdges.first != inEdges.second; ++(inEdges.first)) {
                ExecStreamGraphImpl::Edge edge = *(inEdges.first);
                ExecStreamBufAccessor &bufAccessor =
                    graphImpl.getBufAccessorFromEdge(edge);
                bufAccessor.requestProduction();
            }
        }
        ++vertices.first;
    }

    // +1 for the manager task, which will tie up one thread
    threadPool.start(degreeOfParallelism + 1);

    // kick off the manager task
    ParallelExecTask managerTask(*this, NULL);
    mgrState = MGR_RUNNING;
    threadPool.submitTask(managerTask);
}

void ParallelExecStreamScheduler::setRunnable(ExecStream &stream, bool runnable)
{
    permAssert(false);
}

void ParallelExecStreamScheduler::makeRunnable(ExecStream &stream)
{
    permAssert(false);
}

void ParallelExecStreamScheduler::abort(ExecStreamGraph &graph)
{
    StrictMutexGuard mutexGuard(mutex);
    FENNEL_TRACE(TRACE_FINE,"abort requested");

    if (!pPendingExcn) {
        pPendingExcn.reset(new AbortExcn());
    }
    condition.notify_one();
}

void ParallelExecStreamScheduler::stop()
{
    FENNEL_TRACE(TRACE_FINE,"stop");

    StrictMutexGuard mutexGuard(mutex);
    if (mgrState != MGR_STOPPED) {
        mgrState = MGR_STOPPING;
        condition.notify_one();
        while (mgrState != MGR_STOPPED) {
            sentinelCondition.wait(mutexGuard);
        }
    }
    mutexGuard.unlock();

    threadPool.stop();

    // NOTE jvs 10-Aug-2008:  This is how we keep the cloned excn
    // from becoming a memory leak.  It assumes that the caller
    // doesn't invoke pScheduler->stop() until *after* the exception
    // has been completely handled and is no longer referenced.
    pPendingExcn.reset();
    
    completedQueue.clear();
    inhibitedQueue.clear();
}

void ParallelExecStreamScheduler::createBufferProvisionAdapter(
    ExecStreamEmbryo &embryo)
{
    // use double buffering so that producers and consumers can run
    // in parallel
    DoubleBufferExecStreamParams adapterParams;
    embryo.init(
        new DoubleBufferExecStream(),
        adapterParams);
}

void ParallelExecStreamScheduler::executeManager()
{
    // TODO jvs 16-Aug-2008:  RAII
    try {
        tryExecuteManager();
    } catch (...) {
        StrictMutexGuard mutexGuard(mutex);
        mgrState = MGR_STOPPED;
        sentinelCondition.notify_all();
        throw;
    }
    StrictMutexGuard mutexGuard(mutex);
    mgrState = MGR_STOPPED;
    sentinelCondition.notify_all();
}

void ParallelExecStreamScheduler::tryExecuteManager()
{
    FENNEL_TRACE(TRACE_FINE,"manager task starting");
    for (;;) {
        StrictMutexGuard mutexGuard(mutex);
        while (completedQueue.empty() && (mgrState == MGR_RUNNING)
            && !pPendingExcn)
        {
            condition.wait(mutexGuard);
        }
        if (pPendingExcn) {
            return;
        }
        if (mgrState != MGR_RUNNING) {
            return;
        }
        while (!completedQueue.empty()) {
            ParallelExecResult result = completedQueue.front();
            completedQueue.pop_front();
            // don't hold lock while doing expensive state maintenance
            mutexGuard.unlock();
            processCompletedTask(result);
            if (pPendingExcn) {
                return;
            }
            mutexGuard.lock();
        }
    }
}

ExecStreamBufAccessor &ParallelExecStreamScheduler::readStream(
    ExecStream &stream)
{
    FENNEL_TRACE(
        TRACE_FINE,
        "entering readStream " << stream.getName());
    
    ExecStreamId current = stream.getStreamId();
    ExecStreamGraphImpl &graphImpl =
        dynamic_cast<ExecStreamGraphImpl&>(*pGraph);
    ExecStreamGraphImpl::GraphRep const &graphRep = graphImpl.getGraphRep();

    // assert that we're reading from a designated output stream
    assert(boost::out_degree(current,graphRep) == 1);
    ExecStreamGraphImpl::Edge edge =
        *(boost::out_edges(current,graphRep).first);
    ExecStreamBufAccessor &bufAccessor = graphImpl.getBufAccessorFromEdge(edge);
    current = boost::target(edge, graphRep);
    assert(!graphImpl.getStreamFromVertex(current));

    if (bufAccessor.getState() == EXECBUF_EMPTY) {
        bufAccessor.requestProduction();
    } else if (bufAccessor.getState() != EXECBUF_UNDERFLOW) {
        // data or EOS already available
        return bufAccessor;
    }

    // please sir, I'd like some more
    ParallelExecResult result(current, EXECRC_BUF_UNDERFLOW);
    StrictMutexGuard mutexGuard(mutex);
    streamStateMap[current].state = SS_SLEEPING;
    completedQueue.push_back(result);
    condition.notify_one();

    while ((streamStateMap[current].state == SS_SLEEPING) && !pPendingExcn) {
        sentinelCondition.wait(mutexGuard);
    }

    if (pPendingExcn) {
        pPendingExcn->throwSelf();
    }
    
    return bufAccessor;
}

void ParallelExecStreamScheduler::processCompletedTask(
    ParallelExecResult const &result)
{
    ExecStreamId current = result.getStreamId();
    ExecStreamGraphImpl &graphImpl =
        dynamic_cast<ExecStreamGraphImpl&>(*pGraph);
    ExecStreamGraphImpl::GraphRep const &graphRep = graphImpl.getGraphRep();
    
    streamStateMap[current].state = SS_SLEEPING;
    alterNeighborInhibition(current, -1);

    switch (result.getResultCode()) {
    case EXECRC_EOS:
    case EXECRC_BUF_OVERFLOW:
    case EXECRC_BUF_UNDERFLOW:
        {
            ExecStreamGraphImpl::OutEdgeIterPair outEdges =
                boost::out_edges(current, graphRep);
            for (; outEdges.first != outEdges.second; ++(outEdges.first)) {
                ExecStreamGraphImpl::Edge edge = *(outEdges.first);
                ExecStreamBufAccessor &bufAccessor =
                    graphImpl.getBufAccessorFromEdge(edge);
                if (bufAccessor.getState() != EXECBUF_UNDERFLOW) {
                    ExecStreamId consumer = boost::target(edge, graphRep);
                    bool sentinel = addToQueue(consumer);
                    if (sentinel) {
                        if (bufAccessor.getState() != EXECBUF_EMPTY) {
                            signalSentinel(consumer);
                        }
                    }
                }
            }
            ExecStreamGraphImpl::InEdgeIterPair inEdges =
                boost::in_edges(current, graphRep);
            bool sawUnderflow = false;
            for (; inEdges.first != inEdges.second; ++(inEdges.first)) {
                ExecStreamGraphImpl::Edge edge = *(inEdges.first);
                ExecStreamBufAccessor &bufAccessor =
                    graphImpl.getBufAccessorFromEdge(edge);
                if (bufAccessor.getState() == EXECBUF_UNDERFLOW) {
                    ExecStreamId producer = boost::source(edge, graphRep);
                    addToQueue(producer);
                    sawUnderflow = true;
                }
            }
            if (!sawUnderflow &&
                (result.getResultCode() == EXECRC_BUF_UNDERFLOW))
            {
                // sometimes, a stream may return underflow, even though none
                // of its inputs are in underflow state; instead, some are in
                // EOS; we interpret this as a yield request to help keep
                // the stream's state machine logic simpler
                addToQueue(current);
            }
        }
        break;
    case EXECRC_QUANTUM_EXPIRED:
        addToQueue(current);
        break;
    default:
        permAssert(false);
    }

    // REVIEW jvs 4-Jul-2008:  only reschedule inhibited neighbors?
    retryInhibitedQueue();
}

void ParallelExecStreamScheduler::signalSentinel(ExecStreamId sentinelId)
{
    alterNeighborInhibition(sentinelId, +1);
    
    StrictMutexGuard mutexGuard(mutex);
    streamStateMap[sentinelId].state = SS_RUNNING;
    sentinelCondition.notify_all();
}

void ParallelExecStreamScheduler::executeTask(ExecStream &stream)
{
    try {
        tryExecuteTask(stream);
    } catch (std::exception &ex) {
        StrictMutexGuard mutexGuard(mutex);
        if (!pPendingExcn) {
            pPendingExcn.reset(threadTracker.cloneExcn(ex));
        }
        condition.notify_one();
    } catch (...) {
        // REVIEW jvs 22-Jul-2008:  panic instead?
        StrictMutexGuard mutexGuard(mutex);
        if (!pPendingExcn) {
            pPendingExcn.reset(new FennelExcn("Unknown error"));
        }
        condition.notify_one();
    }
}

void ParallelExecStreamScheduler::tryExecuteTask(ExecStream &stream)
{
    ExecStreamQuantum quantum;
    ExecStreamResult rc = executeStream(stream, quantum);
    ParallelExecResult result(stream.getStreamId(), rc);
    
    StrictMutexGuard mutexGuard(mutex);
    completedQueue.push_back(result);
    condition.notify_one();
}

void ParallelExecStreamScheduler::retryInhibitedQueue()
{
    // addToQueue may bounce some back to inhibitedQueue,
    // so process via transitQueue
    transitQueue = inhibitedQueue;
    inhibitedQueue.clear();
    while (!transitQueue.empty()) {
        ExecStreamId inhibitedStreamId = transitQueue.front();
        transitQueue.pop_front();
        streamStateMap[inhibitedStreamId].state = SS_SLEEPING;
        addToQueue(inhibitedStreamId);
    }
}

bool ParallelExecStreamScheduler::addToQueue(ExecStreamId streamId)
{
    if (pPendingExcn) {
        return false;
    }
    switch(streamStateMap[streamId].state) {
    case SS_SLEEPING:
        {
            ExecStreamGraphImpl &graphImpl =
                dynamic_cast<ExecStreamGraphImpl&>(*pGraph);
            SharedExecStream pStream = graphImpl.getStreamFromVertex(streamId);
            if (!pStream) {
                // sentinel node
                return true;
            }
            if (isInhibited(streamId)) {
                streamStateMap[streamId].state = SS_INHIBITED;
                inhibitedQueue.push_back(streamId);
            } else {
                streamStateMap[streamId].state = SS_RUNNING;
                alterNeighborInhibition(streamId, +1);
                ParallelExecTask task(*this, pStream.get());
                threadPool.submitTask(task);
            }
        }
        break;
    case SS_INHIBITED:
    case SS_RUNNING:
        // ignore request
        break;
    default:
        permAssert(false);
    }
    return false;
}

ParallelExecTask::ParallelExecTask(
    ParallelExecStreamScheduler &schedulerInit,
    ExecStream *pStreamInit)
    : scheduler(schedulerInit)
{
    pStream = pStreamInit;
}

void ParallelExecTask::execute()
{
    if (pStream) {
        scheduler.executeTask(*pStream);
    } else {
        scheduler.executeManager();
    }
}

ParallelExecResult::ParallelExecResult(
    ExecStreamId streamIdInit,
    ExecStreamResult rcInit)
{
    streamId = streamIdInit;
    rc = rcInit;
}

FENNEL_END_CPPFILE("$Id$");

// End ParallelExecStreamScheduler.cpp
