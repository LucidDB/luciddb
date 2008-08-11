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
            // mark output nodes as running so that producers will
            // be inhibited except while we're in readStream
            streamStateMap[streamId].state = SS_RUNNING;
            alterNeighborInhibition(streamId, +1);
        }
        ++vertices.first;
    }

    threadPool.start(degreeOfParallelism);
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

// FIXME jvs 6-Aug-2008:  Need to deal with concurrent invocations
// to readStream on different parts of the graph (any plan with UDX)

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

    if ((bufAccessor.getState() != EXECBUF_EMPTY)
        && (bufAccessor.getState() != EXECBUF_UNDERFLOW))
    {
        // data already available
        return bufAccessor;
    }
    
    // mark reader as sleeping so that producers can run
    streamStateMap[current].state = SS_SLEEPING;
    alterNeighborInhibition(current, -1);

    do {
        // get the ball rolling
        addToQueue(stream.getStreamId());

        // in case stream was already in the inhibited queue, we need
        // to reschedule it now, otherwise we'll be waiting forever
        // REVIEW jvs 5-Jul-2008:  only retry this stream, not all?
        retryInhibitedQueue();

        StrictMutexGuard mutexGuard(mutex);
        while (completedQueue.empty()) {
            condition.wait(mutexGuard);
            if (pPendingExcn) {
                // REVIEW jvs 6-Aug-2008:  need to update streamStateMap?
                pPendingExcn->throwSelf();
            }
        }
        while (!completedQueue.empty()) {
            ParallelExecResult result = completedQueue.front();
            completedQueue.pop_front();
            // don't hold lock while doing expensive state maintenance
            mutexGuard.unlock();
            processCompletedTask(result);
            mutexGuard.lock();
            if (pPendingExcn) {
                // REVIEW jvs 6-Aug-2008:  need to update streamStateMap?
                pPendingExcn->throwSelf();
            }
        }
    } while ((bufAccessor.getState() == EXECBUF_EMPTY)
        || (bufAccessor.getState() == EXECBUF_UNDERFLOW)
        || (streamStateMap[current].inhibitionCount != 0));
    
    // inhibit producers while caller is accessing returned data
    streamStateMap[current].state = SS_RUNNING;
    alterNeighborInhibition(current, +1);

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
                    addToQueue(consumer);
                }
            }
            ExecStreamGraphImpl::InEdgeIterPair inEdges =
                boost::in_edges(current, graphRep);
            for (; inEdges.first != inEdges.second; ++(inEdges.first)) {
                ExecStreamGraphImpl::Edge edge = *(inEdges.first);
                ExecStreamBufAccessor &bufAccessor =
                    graphImpl.getBufAccessorFromEdge(edge);
                if (bufAccessor.getState() == EXECBUF_UNDERFLOW) {
                    ExecStreamId producer = boost::source(edge, graphRep);
                    addToQueue(producer);
                }
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
    ParallelExecResult result(stream, rc);
    
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

void ParallelExecStreamScheduler::addToQueue(ExecStreamId streamId)
{
    if (pPendingExcn) {
        return;
    }
    switch(streamStateMap[streamId].state) {
    case SS_SLEEPING:
        {
            ExecStreamGraphImpl &graphImpl =
                dynamic_cast<ExecStreamGraphImpl&>(*pGraph);
            SharedExecStream pStream = graphImpl.getStreamFromVertex(streamId);
            if (!pStream) {
                // sentinel node
                return;
            }
            if (isInhibited(streamId)) {
                streamStateMap[streamId].state = SS_INHIBITED;
                inhibitedQueue.push_back(streamId);
            } else {
                streamStateMap[streamId].state = SS_RUNNING;
                alterNeighborInhibition(streamId, +1);
                ParallelExecTask task(*this, *pStream);
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
}

ParallelExecTask::ParallelExecTask(
    ParallelExecStreamScheduler &schedulerInit,
    ExecStream &streamInit)
    : scheduler(schedulerInit),
      stream(streamInit)
{
}

void ParallelExecTask::execute()
{
    scheduler.executeTask(stream);
}

ParallelExecResult::ParallelExecResult(
    ExecStream &streamInit,
    ExecStreamResult rcInit)
    : stream(streamInit)
{
    rc = rcInit;
}

FENNEL_END_CPPFILE("$Id$");

// End ParallelExecStreamScheduler.cpp
