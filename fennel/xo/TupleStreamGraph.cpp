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
#include "fennel/xo/TupleStreamGraphImpl.h"
#include "fennel/xo/TupleStream.h"
#include "fennel/segment/Segment.h"

#include <boost/bind.hpp>
#include <boost/graph/topological_sort.hpp>

FENNEL_BEGIN_CPPFILE("$Id$");

TupleStreamGraph::~TupleStreamGraph()
{
}

SharedTupleStreamGraph TupleStreamGraph::newTupleStreamGraph()
{
    return SharedTupleStreamGraph(
        new TupleStreamGraphImpl(),ClosableObjectDestructor());
}

TupleStreamGraphImpl::TupleStreamGraphImpl()
{
}

void TupleStreamGraphImpl::addStream(
    SharedTupleStream pStream)
{
    Vertex streamVertex = boost::add_vertex(graphRep);
    pStream->id = streamVertex;
    pStream->pGraph = this;
    boost::put(boost::vertex_data,graphRep,streamVertex,pStream);
}

/*
TupleStreamGraphImpl::TupleStreamGraphImpl()
{
    isOpen = false;
}

void TupleStreamGraphImpl::setTxn(SharedLogicalTxn pTxnInit)
{
    pTxn = pTxnInit;
}

void TupleStreamGraphImpl::setScratchSegment(
    SharedSegment pScratchSegmentInit)
{
    pScratchSegment = pScratchSegmentInit;
}

SharedLogicalTxn TupleStreamGraphImpl::getTxn()
{
    return pTxn;
}

SharedTupleStream TupleStreamGraphImpl::getStreamFromVertex(Vertex vertex)
{
    return boost::get(boost::vertex_data,graphRep)[vertex];
}

void TupleStreamGraphImpl::addStream(
    SharedTupleStream pStream)
{
    Vertex streamVertex = boost::add_vertex(graphRep);
    pStream->id = streamVertex;
    pStream->pGraph = this;
    boost::put(boost::vertex_data,graphRep,streamVertex,pStream);
}

void TupleStreamGraphImpl::addDataflow(
    TupleStreamId producerId,
    TupleStreamId consumerId)
{
    boost::add_edge(producerId,consumerId,graphRep);
}

void TupleStreamGraphImpl::sortStreams()
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
        boost::bind(&TupleStreamGraphImpl::getStreamFromVertex,this,_1));
}

void TupleStreamGraphImpl::prepare()
{
    sortStreams();
}

void TupleStreamGraphImpl::open()
{
    assert(!isOpen);
    isOpen = true;
    needsClose = true;
    
    // proceed in dataflow order (from producers to consumers)
    std::for_each(
        sortedStreams.begin(),
        sortedStreams.end(),
        boost::bind(&TupleStream::open,_1,false));
}

void TupleStreamGraphImpl::closeImpl()
{
    isOpen = false;
    if (sortedStreams.empty()) {
        // in case prepare was never called
        sortStreams();
    }
    // proceed in reverse dataflow order (from consumers to producers)
    std::for_each(
        sortedStreams.rbegin(),
        sortedStreams.rend(),
        boost::bind(&ClosableObject::close,_1));
    pTxn.reset();

    // release any scratch memory
    if (pScratchSegment) {
        pScratchSegment->deallocatePageRange(NULL_PAGE_ID,NULL_PAGE_ID);
    }
}

uint TupleStreamGraphImpl::getInputCount(
    TupleStreamId streamId)
{
    Vertex streamVertex = boost::vertices(graphRep).first[streamId];
    return boost::in_degree(streamVertex,graphRep);
}

SharedTupleStream TupleStreamGraphImpl::getStreamInput(
    TupleStreamId streamId,
    uint iInput)
{
    Vertex streamVertex = boost::vertices(graphRep).first[streamId];
    Edge inputEdge = boost::in_edges(streamVertex,graphRep).first[iInput];
    Vertex inputVertex = boost::source(inputEdge,graphRep);
    return getStreamFromVertex(inputVertex);
}

SharedTupleStream TupleStreamGraphImpl::getSinkStream()
{
    // the sink comes at the end of the topological sort
    return sortedStreams.back();
}
*/
FENNEL_END_CPPFILE("$Id$");

// End TupleStreamGraph.cpp
