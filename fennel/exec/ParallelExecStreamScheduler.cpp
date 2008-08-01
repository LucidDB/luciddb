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
    ExecStreamGraphImpl::GraphRep graphRep = graphImpl.getGraphRep();
    ExecStreamGraphImpl::VertexIterPair vertices = boost::vertices(graphRep);
    while (vertices.first != vertices.second) {
        ExecStreamId streamId = *vertices.first;
        if (graphImpl.getStreamFromVertex(streamId)) {
            streamStateMap[streamId] = SS_SLEEPING;
        } else {
            // mark output nodes as running so that producers will
            // be inhibited except while we're in readStream
            streamStateMap[streamId] = SS_RUNNING;
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
    condition.notify_all();
}

void ParallelExecStreamScheduler::stop()
{
    FENNEL_TRACE(TRACE_FINE,"stop");

    threadPool.stop();

    pPendingExcn.reset();
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
    ExecStreamGraphImpl::GraphRep graphRep = graphImpl.getGraphRep();

    // assert that we're reading from a designated output stream
    assert(boost::out_degree(current,graphRep) == 1);
    ExecStreamGraphImpl::Edge edge =
        *(boost::out_edges(current,graphRep).first);
    ExecStreamBufAccessor &bufAccessor = graphImpl.getBufAccessorFromEdge(edge);
    current = boost::target(edge, graphRep);
    assert(!graphImpl.getStreamFromVertex(current));

    // mark reader as sleeping so that producers can run
    StrictMutexGuard mutexGuard(mutex);
    streamStateMap[current] = SS_SLEEPING;

    while (((bufAccessor.getState() == EXECBUF_EMPTY)
               || (bufAccessor.getState() == EXECBUF_UNDERFLOW))
        && !pPendingExcn)
    {
        // get the ball rolling
        addToQueue(stream.getStreamId());

        // in case stream was already in the inhibited queue, we need
        // to reschedule it now, otherwise we'll be waiting forever
        // REVIEW jvs 5-Jul-2008:  only retry this stream, not all?
        retryInhibitedQueue();

        condition.wait(mutexGuard);
    }
    
    // inhibit producers while caller is accessing returned data
    streamStateMap[current] = SS_RUNNING;

    if (pPendingExcn) {
        pPendingExcn->throwSelf();
    }
    
    return bufAccessor;
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
        condition.notify_all();
    } catch (...) {
        // REVIEW jvs 22-Jul-2008:  panic instead?
        StrictMutexGuard mutexGuard(mutex);
        if (!pPendingExcn) {
            pPendingExcn.reset(new FennelExcn("Unknown error"));
        }
        condition.notify_all();
    }
}

void ParallelExecStreamScheduler::tryExecuteTask(ExecStream &stream)
{
    StrictMutexGuard mutexGuard(mutex);
    assert(streamStateMap[stream.getStreamId()] == SS_RUNNABLE);

    if (isInhibited(stream.getStreamId())) {
        streamStateMap[stream.getStreamId()] = SS_INHIBITED;
        inhibitedQueue.push_back(stream.getStreamId());
        return;
    }
    
    streamStateMap[stream.getStreamId()] = SS_RUNNING;
    mutexGuard.unlock();

    ExecStreamQuantum quantum;
    ExecStreamResult rc = executeStream(stream, quantum);
    ExecStreamId current = stream.getStreamId();
    ExecStreamGraphImpl &graphImpl =
        dynamic_cast<ExecStreamGraphImpl&>(*pGraph);
    ExecStreamGraphImpl::GraphRep graphRep = graphImpl.getGraphRep();
    
    mutexGuard.lock();
    streamStateMap[stream.getStreamId()] = SS_SLEEPING;

    bool signalReadStream = false;
    
    switch (rc) {
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
                    SharedExecStream pStream =
                        graphImpl.getStreamFromVertex(consumer);
                    if (!pStream) {
                        signalReadStream = true;
                    }
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
        addToQueue(stream.getStreamId());
        break;
    default:
        permAssert(false);
    }

    // REVIEW jvs 4-Jul-2008:  only reschedule inhibited neighbors?
    retryInhibitedQueue();

    // only notify if graph output is in EOS or overflow
    if (signalReadStream) {
        condition.notify_all();
    }
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
        streamStateMap[inhibitedStreamId] = SS_SLEEPING;
        addToQueue(inhibitedStreamId);
    }
}

bool ParallelExecStreamScheduler::isInhibited(ExecStreamId streamId)
{
    ExecStreamGraphImpl &graphImpl =
        dynamic_cast<ExecStreamGraphImpl&>(*pGraph);
    ExecStreamGraphImpl::GraphRep graphRep = graphImpl.getGraphRep();
    ExecStreamGraphImpl::OutEdgeIterPair outEdges =
        boost::out_edges(streamId, graphRep);
    for (; outEdges.first != outEdges.second; ++(outEdges.first)) {
        ExecStreamGraphImpl::Edge edge = *(outEdges.first);
        ExecStreamId consumer = boost::target(edge, graphRep);
        if (streamStateMap[consumer] == SS_RUNNING) {
            return true;
        }
    }
    ExecStreamGraphImpl::InEdgeIterPair inEdges =
        boost::in_edges(streamId, graphRep);
    for (; inEdges.first != inEdges.second; ++(inEdges.first)) {
        ExecStreamGraphImpl::Edge edge = *(inEdges.first);
        ExecStreamId producer = boost::source(edge, graphRep);
        if (streamStateMap[producer] == SS_RUNNING) {
            return true;
        }
    }
    return false;
}

void ParallelExecStreamScheduler::addToQueue(ExecStreamId streamId)
{
    if (pPendingExcn) {
        return;
    }
    switch(streamStateMap[streamId]) {
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
                // don't bother scheduling if we already know it's
                // inhibited
                streamStateMap[streamId] = SS_INHIBITED;
                inhibitedQueue.push_back(streamId);
            } else {
                streamStateMap[streamId] = SS_RUNNABLE;
                ParallelExecTask task(*this, *pStream);
                threadPool.submitTask(task);
            }
        }
        break;
    case SS_INHIBITED:
    case SS_RUNNING:
    case SS_RUNNABLE:
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

FENNEL_END_CPPFILE("$Id$");

// End ParallelExecStreamScheduler.cpp
