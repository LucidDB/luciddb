/*
// $Id$
// Fennel is a relational database kernel.
// Copyright (C) 1999-2004 John V. Sichi.
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
#include "fennel/exec/ExecStreamGraphImpl.h"
#include "fennel/exec/ExecStream.h"
#include "fennel/exec/ExecStreamBufAccessor.h"
#include "fennel/exec/ExecStreamScheduler.h"
#include "fennel/segment/Segment.h"

#include <boost/bind.hpp>
#include <boost/graph/topological_sort.hpp>

FENNEL_BEGIN_CPPFILE("$Id$");

SharedExecStreamGraph ExecStreamGraph::newExecStreamGraph()
{
    return SharedExecStreamGraph(
        new ExecStreamGraphImpl(),
        ClosableObjectDestructor());
}

ExecStreamGraph::ExecStreamGraph()
{
    pScheduler = NULL;
}

ExecStreamGraph::~ExecStreamGraph()
{
}

ExecStreamGraphImpl::ExecStreamGraphImpl()
{
    isPrepared = false;
    isOpen = false;
    doDataflowClose = false;
}

void ExecStreamGraphImpl::setTxn(SharedLogicalTxn pTxnInit)
{
    pTxn = pTxnInit;
}

void ExecStreamGraphImpl::setScratchSegment(
    SharedSegment pScratchSegmentInit)
{
    pScratchSegment = pScratchSegmentInit;
}

SharedLogicalTxn ExecStreamGraphImpl::getTxn()
{
    return pTxn;
}

void ExecStreamGraphImpl::addStream(
    SharedExecStream pStream)
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

void ExecStreamGraphImpl::addDataflow(
    ExecStreamId producerId,
    ExecStreamId consumerId)
{
    boost::add_edge(producerId,consumerId,graphRep);
}

void ExecStreamGraphImpl::addOutputDataflow(
    ExecStreamId producerId)
{
    Vertex consumerId = boost::add_vertex(graphRep);
    boost::add_edge(producerId,consumerId,graphRep);
}

void ExecStreamGraphImpl::addInputDataflow(
    ExecStreamId consumerId)
{
    Vertex producerId = boost::add_vertex(graphRep);
    boost::add_edge(producerId,consumerId,graphRep);
}

SharedExecStream ExecStreamGraphImpl::findStream(
    std::string name)
{
    StreamMapConstIter pPair = streamMap.find(name);
    if (pPair == streamMap.end()) {
        SharedExecStream nullStream;
        return nullStream;
    } else {
        return getStreamFromVertex(pPair->second);
    }
}

SharedExecStream ExecStreamGraphImpl::findLastStream(
    std::string name)
{
    StreamMapConstIter pPair = streamOutMap.find(name);
    if (pPair == streamOutMap.end()) {
        SharedExecStream nullStream;
        return nullStream;
    } else {
        return getStreamFromVertex(pPair->second);
    }
}

void ExecStreamGraphImpl::interposeStream(
    std::string name,
    ExecStreamId interposedId)
{
    SharedExecStream pLastStream = findLastStream(name);
    assert(pLastStream.get());
    streamOutMap[name] = interposedId;
    addDataflow(
        pLastStream->getStreamId(),
        interposedId);
}

void ExecStreamGraphImpl::sortStreams()
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
        boost::bind(&ExecStreamGraphImpl::getStreamFromVertex,this,_1));

    // now filter out the null vertices representing inputs and outputs
    sortedStreams.erase(
        std::remove(
            sortedStreams.begin(),sortedStreams.end(),SharedExecStream()),
        sortedStreams.end());
}

void ExecStreamGraphImpl::prepare(ExecStreamScheduler &scheduler)
{
    isPrepared = true;
    sortStreams();
    
    // create buffer accessors for all dataflow edges
    EdgeIterPair edges = boost::edges(graphRep);
    for (; edges.first != edges.second; edges.first++) {
        SharedExecStreamBufAccessor pBufAccessor = scheduler.newBufAccessor();
        boost::put(boost::edge_data,graphRep,*(edges.first),pBufAccessor);
    }

    // bind buffer accessors to streams
    std::for_each(
        sortedStreams.begin(),
        sortedStreams.end(),
        boost::bind(
            &ExecStreamGraphImpl::bindStreamBufAccessors,this,_1));
}

void ExecStreamGraphImpl::bindStreamBufAccessors(SharedExecStream pStream)
{
    std::vector<SharedExecStreamBufAccessor> bufAccessors;
    
    // bind the input buffers
    InEdgeIterPair inEdges = boost::in_edges(
        pStream->getStreamId(),graphRep);
    for (; inEdges.first != inEdges.second; ++(inEdges.first)) {
        SharedExecStreamBufAccessor pBufAccessor =
            getSharedBufAccessorFromEdge(*(inEdges.first));
        bufAccessors.push_back(pBufAccessor);
    }
    pStream->setInputBufAccessors(bufAccessors);
    bufAccessors.clear();

    // bind the output buffers
    OutEdgeIterPair outEdges = boost::out_edges(
        pStream->getStreamId(),graphRep);
    for (; outEdges.first != outEdges.second; ++(outEdges.first)) {
        SharedExecStreamBufAccessor pBufAccessor =
            getSharedBufAccessorFromEdge(*(outEdges.first));
        bufAccessors.push_back(pBufAccessor);
        pBufAccessor->setProvision(pStream->getOutputBufProvision());
    }
    pStream->setOutputBufAccessors(bufAccessors);
}

void ExecStreamGraphImpl::open()
{
    assert(!isOpen);
    isOpen = true;
    needsClose = true;
    
    // clear all buffer accessors
    EdgeIterPair edges = boost::edges(graphRep);
    for (; edges.first != edges.second; edges.first++) {
        ExecStreamBufAccessor &bufAccessor =
            getBufAccessorFromEdge(*(edges.first));
        bufAccessor.clear();
    }
    
    // open streams in dataflow order (from producers to consumers)
    std::for_each(
        sortedStreams.begin(),
        sortedStreams.end(),
        boost::bind(&ExecStreamGraphImpl::openStream,this,_1));
}

void ExecStreamGraphImpl::openStream(SharedExecStream pStream)
{
    // TODO jvs 19-July-2004:  move resource allocation to scheduler,
    // and set quotas based on current cache state; for now just set to
    // minimum for testing
    ExecStreamResourceQuantity minQuantity,optQuantity;
    pStream->getResourceRequirements(minQuantity,optQuantity);
    pStream->setResourceAllocation(minQuantity);

    pStream->open(false);
}

void ExecStreamGraphImpl::closeImpl()
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

uint ExecStreamGraphImpl::getInputCount(
    ExecStreamId streamId)
{
    Vertex streamVertex = boost::vertices(graphRep).first[streamId];
    return boost::in_degree(streamVertex,graphRep);
}

uint ExecStreamGraphImpl::getOutputCount(
    ExecStreamId streamId)
{
    Vertex streamVertex = boost::vertices(graphRep).first[streamId];
    return boost::out_degree(streamVertex,graphRep);
}

SharedExecStream ExecStreamGraphImpl::getStreamInput(
    ExecStreamId streamId,
    uint iInput)
{
    Vertex streamVertex = boost::vertices(graphRep).first[streamId];
    Edge inputEdge = boost::in_edges(streamVertex,graphRep).first[iInput];
    Vertex inputVertex = boost::source(inputEdge,graphRep);
    return getStreamFromVertex(inputVertex);
}

SharedExecStream ExecStreamGraphImpl::getStreamOutput(
    ExecStreamId streamId,
    uint iOutput)
{
    Vertex streamVertex = boost::vertices(graphRep).first[streamId];
    Edge outputEdge = boost::out_edges(streamVertex,graphRep).first[iOutput];
    Vertex outputVertex = boost::target(outputEdge,graphRep);
    return getStreamFromVertex(outputVertex);
}

std::vector<SharedExecStream> ExecStreamGraphImpl::getSortedStreams()
{
    assert(isPrepared);
    return sortedStreams;
}

FENNEL_END_CPPFILE("$Id$");

// End ExecStreamGraph.cpp
