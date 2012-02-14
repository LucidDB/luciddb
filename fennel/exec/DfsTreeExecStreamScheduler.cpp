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

#include "fennel/common/CommonPreamble.h"
#include "fennel/exec/DfsTreeExecStreamScheduler.h"
#include "fennel/exec/ExecStreamGraphImpl.h"
#include "fennel/exec/ExecStream.h"
#include "fennel/exec/ExecStreamBufAccessor.h"
#include "fennel/common/AbortExcn.h"

FENNEL_BEGIN_CPPFILE("$Id$");

DfsTreeExecStreamScheduler::DfsTreeExecStreamScheduler(
    SharedTraceTarget pTraceTargetInit,
    std::string nameInit)
    : TraceSource(pTraceTargetInit, nameInit),
      ExecStreamScheduler(pTraceTargetInit, nameInit)
{
}

DfsTreeExecStreamScheduler::~DfsTreeExecStreamScheduler()
{
}

void DfsTreeExecStreamScheduler::addGraph(SharedExecStreamGraph pGraphInit)
{
    assert(!pGraph);

    ExecStreamScheduler::addGraph(pGraphInit);
    pGraph = pGraphInit;
}

void DfsTreeExecStreamScheduler::removeGraph(SharedExecStreamGraph pGraphInit)
{
    assert(pGraph == pGraphInit);

    pGraph.reset();
    ExecStreamScheduler::removeGraph(pGraphInit);
}

void DfsTreeExecStreamScheduler::start()
{
    FENNEL_TRACE(TRACE_FINE, "start");

    // TODO jvs 2-Jan-2006:  rename this class now that it's no longer
    // restricted to trees; come up with something more generic in case
    // DFS becomes irrelevant also.

    // note: we no longer check that graph is a tree (or forest of trees)
    // since it is now possible to have multiple consumers from a single
    // producer
    assert(pGraph->isAcyclic());
    aborted = false;
}

void DfsTreeExecStreamScheduler::setRunnable(ExecStream &, bool)
{
    permAssert(false);
}

void DfsTreeExecStreamScheduler::abort(ExecStreamGraph &)
{
    FENNEL_TRACE(TRACE_FINE, "abort requested");

    aborted = true;
}

void DfsTreeExecStreamScheduler::checkAbort() const
{
    if (aborted) {
        FENNEL_TRACE(TRACE_FINE, "abort detected");
        throw AbortExcn();
    }
}

void DfsTreeExecStreamScheduler::stop()
{
    FENNEL_TRACE(TRACE_FINE, "stop");

    // nothing to do
    aborted = false;
}

ExecStreamBufAccessor &DfsTreeExecStreamScheduler::readStream(
    ExecStream &stream)
{
    FENNEL_TRACE(
        TRACE_FINE,
        "entering readStream " << stream.getName());

    ExecStreamId current = stream.getStreamId();
    ExecStreamQuantum quantum;

    ExecStreamGraphImpl &graphImpl =
        dynamic_cast<ExecStreamGraphImpl&>(*pGraph);
    ExecStreamGraphImpl::GraphRep const &graphRep = graphImpl.getGraphRep();

    // assert that we're reading from a designated output stream
    assert(boost::out_degree(current, graphRep) == 1);
    assert(!graphImpl.getStreamFromVertex(
        boost::target(
            *(boost::out_edges(current,graphRep).first),
            graphRep)));

    // TODO:  assertions about accessor state/provision

    for (;;) {
        ExecStreamGraphImpl::InEdgeIterPair inEdges =
            boost::in_edges(current, graphRep);
        for (; inEdges.first != inEdges.second; ++(inEdges.first)) {
            ExecStreamGraphImpl::Edge edge = *(inEdges.first);
            ExecStreamBufAccessor &bufAccessor =
                graphImpl.getBufAccessorFromEdge(edge);
            if (bufAccessor.getState() == EXECBUF_UNDERFLOW) {
                // move current upstream
                current = boost::source(edge, graphRep);
                break;
            }
        }
        if (inEdges.first != inEdges.second) {
            // hit EXECBUF_UNDERFLOW
            continue;
        }

        SharedExecStream pStream = graphImpl.getStreamFromVertex(current);
        ExecStreamResult rc = executeStream(*pStream, quantum);

        checkAbort();

        ExecStreamGraphImpl::Edge edge;

        switch (rc) {
        case EXECRC_EOS:
            // find a consumer that is not in EOS state
            if (!findNextConsumer(
                    graphImpl, graphRep, stream, edge, current, EXECBUF_EOS))
            {
                return graphImpl.getBufAccessorFromEdge(edge);
            }
            // if all were in eos, just use the last consumer
            break;
        case EXECRC_BUF_OVERFLOW:
            // find a consumer that is not in underflow state; i.e., not
            // waiting on this producer to continue execution
            if (!findNextConsumer(
                    graphImpl, graphRep, stream, edge, current,
                    EXECBUF_UNDERFLOW))
            {
                return graphImpl.getBufAccessorFromEdge(edge);
            }
            break;
        case EXECRC_BUF_UNDERFLOW:
            // TODO:  assert that at least one input is in state
            // EXECBUF_UNDERFLOW
            break;
        case EXECRC_QUANTUM_EXPIRED:
            break;
        default:
            permAssert(false);
        }
    }
}

bool DfsTreeExecStreamScheduler::findNextConsumer(
    ExecStreamGraphImpl &graphImpl,
    const ExecStreamGraphImpl::GraphRep &graphRep,
    const ExecStream &stream,
    ExecStreamGraphImpl::Edge &edge,
    ExecStreamId &current,
    ExecStreamBufState skipState)
{
    ExecStreamGraphImpl::OutEdgeIterPair outEdges =
        boost::out_edges(current, graphRep);

    bool emptyFound = false;
    // dummy initializations to avoid compiler error
    ExecStreamGraphImpl::Edge emptyEdge = edge;
    ExecStreamId emptyStreamId = current;

    for (; outEdges.first != outEdges.second; ++(outEdges.first)) {
        edge = *(outEdges.first);
        current = boost::target(edge, graphRep);
        if (boost::out_degree(current, graphRep) == 0) {
            // we've hit the output sentinel
            assert(!graphImpl.getStreamFromVertex(current));
            FENNEL_TRACE(
                TRACE_FINE,
                "leaving readStream " << stream.getName());
            return false;
        }

        ExecStreamBufAccessor &bufAccessor =
            graphImpl.getBufAccessorFromEdge(edge);

        // Save the first edge with an empty state that we find, but don't
        // return that as the next consumer.  We want to give priority to
        // streams that have explicity requested data.  So, only return the
        // empty edge consumer if there are no consumers that have explicitly
        // requested data.
        if (bufAccessor.getState() == EXECBUF_EMPTY) {
            if (!emptyFound) {
                emptyFound = true;
                emptyEdge = edge;
                emptyStreamId = current;
            }
            continue;
        }

        if (bufAccessor.getState() != skipState) {
            break;
        }
        assert(!(skipState == EXECBUF_UNDERFLOW
                 && bufAccessor.getState() == EXECBUF_EOS));
    }

    if (outEdges.first == outEdges.second && emptyFound) {
        edge = emptyEdge;
        current = emptyStreamId;
    } else {
        assert(!(skipState == EXECBUF_UNDERFLOW
                 && outEdges.first == outEdges.second));
    }

    return true;
}

FENNEL_END_CPPFILE("$Id$");

// End DfsTreeExecStreamScheduler.cpp
