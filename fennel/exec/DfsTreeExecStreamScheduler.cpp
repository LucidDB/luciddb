/*
// $Id$
// Fennel is a relational database kernel.
// Copyright (C) 2004-2004 John V. Sichi.
//
// This program is free software; you can redistribute it and/or
// modify it under the terms of the GNU Lesser General Public License
// as published by the Free Software Foundation; either version 2.1
// of the License, or (at your option) any later version.
// 
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Lesser General Public License for more details.
// 
// You should have received a copy of the GNU Lesser General Public License
// along with this program; if not, write to the Free Software
// Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
*/

#include "fennel/common/CommonPreamble.h"
#include "fennel/exec/DfsTreeExecStreamScheduler.h"
#include "fennel/exec/ExecStreamGraphImpl.h"
#include "fennel/exec/ExecStream.h"
#include "fennel/exec/ExecStreamBufAccessor.h"
#include "fennel/common/AbortExcn.h"

FENNEL_BEGIN_CPPFILE("$Id$");

DfsTreeExecStreamScheduler::DfsTreeExecStreamScheduler(
    TraceTarget *pTraceTargetInit,
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
    
    ExecStreamGraphImpl &graphImpl = pGraph->getImpl();
    ExecStreamGraphImpl::GraphRep graphRep = graphImpl.getGraphRep();

    // assert that graph is a tree (or forest of trees)
    ExecStreamGraphImpl::VertexIterPair p = boost::vertices(graphRep);
    for (; p.first != p.second; ++(p.first)) {
        assert(boost::out_degree(*(p.first),graphRep) < 2);
    }

    // TODO:  assert no cycles
    aborted = false;
}

void DfsTreeExecStreamScheduler::makeRunnable(ExecStream &)
{
    permAssert(false);
}

void DfsTreeExecStreamScheduler::abort()
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

    ExecStreamGraphImpl &graphImpl = pGraph->getImpl();
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
            if (bufAccessor.getState() == EXECBUF_NEED_PRODUCTION) {
                // move current upstream
                current = boost::source(edge,graphRep);
                break;
            }
        }
        if (inEdges.first != inEdges.second) {
            // hit EXECBUF_NEED_PRODUCTION
            continue;
        }

        SharedExecStream pStream = graphImpl.getStreamFromVertex(current);
        ExecStreamResult rc = executeStream(*pStream, quantum);

        if (aborted) {
            FENNEL_TRACE(TRACE_FINE,"abort detected");
            throw AbortExcn();
        }

        switch(rc) {
        case EXECRC_EOS:
        case EXECRC_OUTPUT:
            {
                // move current downstream
                assert(boost::out_degree(current,graphRep) == 1);
                ExecStreamGraphImpl::Edge edge =
                    *(boost::out_edges(current,graphRep).first);
                current = boost::target(edge,graphRep);
                if (boost::out_degree(current,graphRep) == 0) {
                    // we've hit the output sentinel
                    assert(!graphImpl.getStreamFromVertex(current));
                    FENNEL_TRACE(
                        TRACE_FINE,
                        "leaving readStream " << stream.getName());
                    return graphImpl.getBufAccessorFromEdge(edge);
                }
            }
            break;
        case EXECRC_NEED_INPUT:
            // TODO:  assert that at least one input is in state
            // EXECBUF_NEED_PRODUCTION
            break;
        case EXECRC_NO_OUTPUT:
            break;
        default:
            permAssert(false);
        }
    }
}

FENNEL_END_CPPFILE("$Id$");

// End DfsTreeExecStreamScheduler.cpp
