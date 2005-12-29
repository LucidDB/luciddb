/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2005-2005 Disruptive Tech
// Copyright (C) 2005-2005 LucidEra, Inc.
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
    : ExecStreamScheduler(pTraceTargetInit, nameInit)
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
    FENNEL_TRACE(TRACE_FINE,"start");
    
    ExecStreamGraphImpl &graphImpl =
        dynamic_cast<ExecStreamGraphImpl&>(*pGraph);
    ExecStreamGraphImpl::GraphRep graphRep = graphImpl.getGraphRep();

    // note: we no longer check that graph is a tree (or forest of trees)
    // since it is now possible to have multiple consumers from a single
    // producer
    assert(graphImpl.checkForNoCycles());
    aborted = false;
}

void DfsTreeExecStreamScheduler::makeRunnable(ExecStream &)
{
    permAssert(false);
}

void DfsTreeExecStreamScheduler::abort(ExecStreamGraph &)
{
    FENNEL_TRACE(TRACE_FINE,"abort requested");
    
    aborted = true;
}

void DfsTreeExecStreamScheduler::stop()
{
    FENNEL_TRACE(TRACE_FINE,"stop");
    
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
    ExecStreamGraphImpl::GraphRep graphRep = graphImpl.getGraphRep();

    // assert that we're reading from a designated output stream
    assert(boost::out_degree(current,graphRep) == 1);
    assert(!graphImpl.getStreamFromVertex(
               boost::target(
                   *(boost::out_edges(current,graphRep).first),
                   graphRep)));

    // TODO:  assertions about accessor state/provision

    for (;;) {
        ExecStreamGraphImpl::InEdgeIterPair inEdges =
            boost::in_edges(current,graphRep);
        for (; inEdges.first != inEdges.second; ++(inEdges.first)) {
            ExecStreamGraphImpl::Edge edge = *(inEdges.first);
            ExecStreamBufAccessor &bufAccessor =
                graphImpl.getBufAccessorFromEdge(edge);
            if (bufAccessor.getState() == EXECBUF_UNDERFLOW) {
                // move current upstream
                current = boost::source(edge,graphRep);
                break;
            }
        }
        if (inEdges.first != inEdges.second) {
            // hit EXECBUF_UNDERFLOW
            continue;
        }

        SharedExecStream pStream = graphImpl.getStreamFromVertex(current);
        ExecStreamResult rc = executeStream(*pStream, quantum);

        if (aborted) {
            FENNEL_TRACE(TRACE_FINE,"abort detected");
            throw AbortExcn();
        }

        ExecStreamGraphImpl::Edge edge;

        switch(rc) {
        case EXECRC_EOS:
            // find a consumer that is not in EOS state
            if (!findNextConsumer(graphImpl, graphRep, stream, edge, current,
                                  EXECBUF_EOS)) {
                return graphImpl.getBufAccessorFromEdge(edge);
            } 
            // if all were in eos, just use the last consumer
            break;
        case EXECRC_BUF_OVERFLOW:
            // find a consumer that is not in underflow state; i.e., not
            // waiting on this producer to continue execution
            if (!findNextConsumer(graphImpl, graphRep, stream, edge, current,
                                  EXECBUF_UNDERFLOW)) {
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
        boost::out_edges(current,graphRep);
    for (; outEdges.first != outEdges.second; ++(outEdges.first)) {

        edge = *(outEdges.first);
        current = boost::target(edge,graphRep);
        if (boost::out_degree(current,graphRep) == 0) {
            // we've hit the output sentinel
            assert(!graphImpl.getStreamFromVertex(current));
            FENNEL_TRACE(
                TRACE_FINE,
                "leaving readStream " << stream.getName());
            return false;
        }

        ExecStreamBufAccessor &bufAccessor =
            graphImpl.getBufAccessorFromEdge(edge);
        if (bufAccessor.getState() != skipState) {
            break;
        }
        assert(!(skipState == EXECBUF_UNDERFLOW &&
                    bufAccessor.getState() == EXECBUF_EOS));
    }
    // should be at least one consumer in non-underflow state if looking
    // for a non-underflow consumer
    assert(!(skipState == EXECBUF_UNDERFLOW &&
                outEdges.first == outEdges.second));

    return true;
}

FENNEL_END_CPPFILE("$Id$");

// End DfsTreeExecStreamScheduler.cpp
