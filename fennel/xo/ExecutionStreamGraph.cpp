/*
// $Id$
// Fennel is a relational database kernel.
// Copyright (C) 1999-2004 John V. Sichi.
// Copyright (C) 2003-2004 Disruptive Tech
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
#include "fennel/xo/ExecutionStreamGraphImpl.h"
#include "fennel/xo/TupleStream.h"
#include "fennel/segment/Segment.h"

#include <boost/bind.hpp>
#include <boost/graph/topological_sort.hpp>

FENNEL_BEGIN_CPPFILE("$Id$");

ExecutionStreamGraph::~ExecutionStreamGraph()
{
}

ExecutionStreamGraphImpl::ExecutionStreamGraphImpl()
{
    isPrepared = false;
    isOpen = false;
    doDataflowClose = false;
}

void ExecutionStreamGraphImpl::setTxn(SharedLogicalTxn pTxnInit)
{
    pTxn = pTxnInit;
}

void ExecutionStreamGraphImpl::setScratchSegment(
    SharedSegment pScratchSegmentInit)
{
    pScratchSegment = pScratchSegmentInit;
}

SharedLogicalTxn ExecutionStreamGraphImpl::getTxn()
{
    return pTxn;
}

SharedExecutionStream ExecutionStreamGraphImpl::getStreamFromVertex(
    Vertex vertex)
{
    return boost::get(boost::vertex_data,graphRep)[vertex];
}

void ExecutionStreamGraphImpl::addStream(
    SharedExecutionStream pStream)
{
    assert(pStream->getName().length());
    assert(findStream(pStream->getName()).get()==NULL);
    Vertex streamVertex = boost::add_vertex(graphRep);
    pStream->id = streamVertex;
    pStream->pGraph = this;
    boost::put(boost::vertex_data,graphRep,streamVertex,pStream);
    streamMap[pStream->getName()] = pStream->getStreamId();
    streamOutMap[pStream->getName()] = pStream->getStreamId();
}

void ExecutionStreamGraphImpl::addDataflow(
    ExecutionStreamId producerId,
    ExecutionStreamId consumerId)
{
    boost::add_edge(producerId,consumerId,graphRep);
}

SharedExecutionStream ExecutionStreamGraphImpl::findStream(
    std::string name)
{
    StreamMapConstIter pPair = streamMap.find(name);
    if (pPair == streamMap.end()) {
        SharedExecutionStream nullStream;
        return nullStream;
    } else {
        return getStreamFromVertex(pPair->second);
    }
}

SharedExecutionStream ExecutionStreamGraphImpl::findLastStream(
    std::string name)
{
    StreamMapConstIter pPair = streamOutMap.find(name);
    if (pPair == streamOutMap.end()) {
        SharedExecutionStream nullStream;
        return nullStream;
    } else {
        return getStreamFromVertex(pPair->second);
    }
}

void ExecutionStreamGraphImpl::interposeStream(
    std::string name,
    ExecutionStreamId interposedId)
{
    SharedExecutionStream pLastStream = findLastStream(name);
    assert(pLastStream.get());
    streamOutMap[name] = interposedId;
    addDataflow(
        pLastStream->getStreamId(),
        interposedId);
}

void ExecutionStreamGraphImpl::sortStreams()
{
    std::vector<Vertex> sortedVertices;
    boost::topological_sort(
        graphRep,std::back_inserter(sortedVertices));
    sortedStreams.resize(sortedVertices.size());
    // boost::topological_sort produces an ordering from consumers to
    // producers, but we want the oppposite ordering, hence
    // sortedStreams.rbegin() below
    std::transform(
        sortedVertices.begin(),
        sortedVertices.end(),
        sortedStreams.rbegin(),
        boost::bind(&ExecutionStreamGraphImpl::getStreamFromVertex,this,_1));
}

void ExecutionStreamGraphImpl::prepare()
{
    isPrepared = true;
    sortStreams();
}

void ExecutionStreamGraphImpl::open()
{
    assert(!isOpen);
    isOpen = true;
    needsClose = true;
    
    // proceed in dataflow order (from producers to consumers)
    std::for_each(
        sortedStreams.begin(),
        sortedStreams.end(),
        boost::bind(&ExecutionStream::open,_1,false));
}

void ExecutionStreamGraphImpl::closeImpl()
{
    isOpen = false;
    if (sortedStreams.empty()) {
        // in case prepare was never called
        sortStreams();
    }
    if (doDataflowClose) {
        std::for_each(
            sortedStreams.begin(),
            sortedStreams.end(),
            boost::bind(&ClosableObject::close,_1));
    } else {
        std::for_each(
            sortedStreams.rbegin(),
            sortedStreams.rend(),
            boost::bind(&ClosableObject::close,_1));
    }
    pTxn.reset();

    // release any scratch memory
    if (pScratchSegment) {
        pScratchSegment->deallocatePageRange(NULL_PAGE_ID,NULL_PAGE_ID);
    }
}

uint ExecutionStreamGraphImpl::getInputCount(
    ExecutionStreamId streamId)
{
    Vertex streamVertex = boost::vertices(graphRep).first[streamId];
    return boost::in_degree(streamVertex,graphRep);
}

SharedExecutionStream ExecutionStreamGraphImpl::getStreamInput(
    ExecutionStreamId streamId,
    uint iInput)
{
    Vertex streamVertex = boost::vertices(graphRep).first[streamId];
    Edge inputEdge = boost::in_edges(streamVertex,graphRep).first[iInput];
    Vertex inputVertex = boost::source(inputEdge,graphRep);
    return getStreamFromVertex(inputVertex);
}

SharedExecutionStream ExecutionStreamGraphImpl::getSinkStream()
{
    // the sink comes at the end of the topological sort
    return sortedStreams.back();
}

std::vector<SharedExecutionStream> ExecutionStreamGraphImpl::getSortedStreams()
{
    assert(isPrepared);
    return sortedStreams;
}

void *ExecutionStreamGraphImpl::getInterface()
{
    return static_cast<ExecutionStreamGraph *>(this);
}

char *ExecutionStreamGraphImpl::getInterfaceName()
{
    return "ExecutionStreamGraph";
}

FENNEL_END_CPPFILE("$Id$");

// End ExecutionStreamGraph.cpp
