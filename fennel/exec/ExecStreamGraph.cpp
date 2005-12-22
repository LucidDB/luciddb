/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2005-2005 Disruptive Tech
// Copyright (C) 2005-2005 LucidEra, Inc.
// Portions Copyright (C) 1999-2005 John V. Sichi
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
#include "fennel/exec/ExecStreamGraphImpl.h"
#include "fennel/exec/ExecStream.h"
#include "fennel/exec/ExecStreamBufAccessor.h"
#include "fennel/exec/ExecStreamScheduler.h"
#include "fennel/exec/DynamicParam.h"
#include "fennel/segment/Segment.h"
#include "fennel/exec/DynamicParam.h"

#include <boost/bind.hpp>
#include <boost/graph/strong_components.hpp>
#include <boost/graph/topological_sort.hpp>

FENNEL_BEGIN_CPPFILE("$Id$");

SharedExecStreamGraph ExecStreamGraph::newExecStreamGraph()
{
    return SharedExecStreamGraph(
        new ExecStreamGraphImpl(),
        ClosableObjectDestructor());
}

ExecStreamGraph::ExecStreamGraph()
    :pScheduler(NULL),
     pDynamicParamManager(new DynamicParamManager())
{
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

ExecStreamGraphImpl::Vertex ExecStreamGraphImpl::newVertex()
{
    if (freeVertices.size() > 0) {
        Vertex ret = freeVertices.back();
        freeVertices.pop_back();
        return ret;
    }
    return boost::add_vertex(graphRep);
}

void ExecStreamGraphImpl::freeVertex(Vertex v)
{
    boost::clear_vertex(v, graphRep);
    boost::get(boost::vertex_data, graphRep)[v].reset();
    freeVertices.push_back(v);
}

int ExecStreamGraphImpl::getStreamCount()
{
    return boost::num_vertices(graphRep) - freeVertices.size();
}

ExecStreamGraphImpl::Vertex 
ExecStreamGraphImpl::addVertex(SharedExecStream pStream)
{
    Vertex v = newVertex();
    boost::put(boost::vertex_data, graphRep, v, pStream);
    if (pStream) {
        pStream->id = v;
        pStream->pGraph = this;
        streamMap[pStream->getName()] = pStream->getStreamId();
    }
    return v;
}

void ExecStreamGraphImpl::addStream(
    SharedExecStream pStream)
{
    assert(pStream->getName().length());
    assert(findStream(pStream->getName()).get()==NULL);
    (void) addVertex(pStream);
}

void ExecStreamGraphImpl::removeStream(ExecStreamId id)
{
    Vertex v = boost::vertices(graphRep).first[id];
    SharedExecStream pStream = getStreamFromVertex(v);
    assert(pStream->pGraph == this);
    assert(pStream->id == id);

    streamMap.erase(pStream->getName());
    removeFromStreamOutMap(pStream);
    sortedStreams.clear();              // invalidate list: recreated on demand
    freeVertex(v);
    // stream is now detached from any graph, and not usable.
    pStream->pGraph = 0;
    pStream->id = 0;
}


void ExecStreamGraphImpl::removeFromStreamOutMap(SharedExecStream p)
{
    int outCt = getOutputCount(p->getStreamId());
    if (outCt > 0) {
        std::string name = p->getName();
        // assumes map key pairs <name, index> sort lexicographically, so <name, *> is contiguous.
        EdgeMap::iterator startNameRange =
            streamOutMap.find(std::make_pair(name, 0));
        EdgeMap::iterator endNameRange =
            streamOutMap.find(std::make_pair(name, outCt-1));
        streamOutMap.erase(startNameRange, endNameRange);
    }
}

// Deletes all edges and puts all vertices on the free list;
// almost like removeStream() on all vertices,
// but doesn't affect the ExecStream which no longer belongs to this graph.
void ExecStreamGraphImpl::clear()
{
    VertexIterPair verts = boost::vertices(graphRep);
    while (verts.first != verts.second) {
        Vertex v = *verts.first;
        freeVertex(v);
        ++verts.first;
    }

    streamMap.clear();
    streamOutMap.clear();
    sortedStreams.clear();
    needsClose = isOpen = isPrepared = false;
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
    Vertex consumerId = newVertex();
    boost::add_edge(producerId,consumerId,graphRep);
}

void ExecStreamGraphImpl::addInputDataflow(
    ExecStreamId consumerId)
{
    Vertex producerId = newVertex();
    boost::add_edge(producerId,consumerId,graphRep);
}


int ExecStreamGraphImpl::getDataflowCount()
{
    return boost::num_edges(graphRep);
}


void ExecStreamGraphImpl::mergeFrom(ExecStreamGraph& src)
{
    if (ExecStreamGraphImpl *p = dynamic_cast<ExecStreamGraphImpl*>(&src)) {
        mergeFrom(*p);
        return;
    }
    assert(false);
}

void ExecStreamGraphImpl::mergeFrom(
    ExecStreamGraph& src, std::vector<ExecStreamId>const& nodes)
{
    if (ExecStreamGraphImpl *p = dynamic_cast<ExecStreamGraphImpl*>(&src)) {
        mergeFrom(*p, nodes);
        return;
    }
    assert(false);
}

void ExecStreamGraphImpl::mergeFrom(ExecStreamGraphImpl& src)
{
    // Since the identity of the added graph SRC will be lost, at this time both
    // graphs must be prepared, and must both be open or both be closed.
    assert(isPrepared && src.isPrepared);
    assert(isOpen == src.isOpen);

    // map a source vertex ID to the ID of the copied target vertex 
    std::map<Vertex, Vertex> vmap;

    // copy the nodes (with attached streams)
    VertexIterPair verts = boost::vertices(src.graphRep);
    for (; verts.first != verts.second; ++verts.first) {
        Vertex vsrc = *verts.first;
        SharedExecStream pStream = src.getStreamFromVertex(vsrc);
        Vertex vnew = addVertex(pStream);
        vmap[vsrc] = vnew;
    }

    // copy the edges (with attached buffers, which stay bound to the adjacent streams)
    EdgeIterPair edges = boost::edges(src.graphRep);
    for (; edges.first != edges.second; ++edges.first) {
        Edge esrc = *edges.first;
        SharedExecStreamBufAccessor pBuf = src.getSharedBufAccessorFromEdge(esrc);
        std::pair<Edge, bool> x = boost::add_edge(
            vmap[boost::source(esrc, src.graphRep)], // image of source node
            vmap[boost::target(esrc, src.graphRep)], // image of target node
            pBuf, graphRep);
        assert(x.second);
    }
    src.clear();                        // source is empty
    sortedStreams.clear();              // invalid now
}


// merges a subgraph, viz the induced subgraph of a set of NODES of SRC
void ExecStreamGraphImpl::mergeFrom(
    ExecStreamGraphImpl& src, std::vector<ExecStreamId>const& nodes)
{
    // both graphs must be prepared, and must both be open or both be closed.
    assert(isPrepared && src.isPrepared);
    assert(isOpen == src.isOpen);

    // map a source vertex ID to the ID of the copied target vertex 
    std::map<Vertex, Vertex> vmap;

    // Copy the nodes (with attached streams)
    int nnodes = nodes.size();
    for (int i = 0; i < nnodes; i++) {
        Vertex vsrc = boost::vertices(src.graphRep).first[nodes[i]];
        SharedExecStream pStream = src.getStreamFromVertex(vsrc);
        Vertex vnew = addVertex(pStream);
        vmap[vsrc] = vnew;
    }

    // Copy the internal edges (with attached buffers, which stay bound to the adjacent streams).
    // It suffices to scan the outbound edges. The external edges are abandoned.
    if (nnodes > 1) {                   // (when only 1 node, no internal edges)
        for (int i = 0; i < nnodes; i++) {
            // Find all outbound edges E (U,V) in the source subgraph
            Vertex u = boost::vertices(src.graphRep).first[nodes[i]];
            for (OutEdgeIterPair edges = boost::out_edges(u, src.graphRep);
                 edges.first != edges.second;
                 ++edges.first)
            {
                // an edge e (u, v) in the source graph
                Edge e = *edges.first;
                assert(u == boost::source(e, src.graphRep));
                Vertex v = boost::target(e, src.graphRep);
                // V is in the subgraph iff v is a key in the map vmap[]
                if (vmap.find(v) != vmap.end()) {
                    SharedExecStreamBufAccessor pBuf = src.getSharedBufAccessorFromEdge(e);
                    std::pair<Edge, bool> x = boost::add_edge(vmap[u], vmap[v], pBuf, graphRep);
                    assert(x.second);
                }
            }
        }
    }

    // delete the copied subgraph from SRC
    for (int i = 0; i < nnodes; i++) {
        Vertex v = boost::vertices(src.graphRep).first[nodes[i]];
        SharedExecStream pStream = src.getStreamFromVertex(v);
        src.streamMap.erase(pStream->getName());
        src.removeFromStreamOutMap(pStream);
        src.freeVertex(v);
    }
    src.sortedStreams.clear();          // invalidate
    sortedStreams.clear();              // invalidate
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
    std::string name,
    uint iOutput)
{
    EdgeMap::const_iterator pPair =
        streamOutMap.find(std::make_pair(name, iOutput));
    if (pPair == streamOutMap.end()) {
        return findStream(name);
    } else {
        return getStreamFromVertex(pPair->second);
    }
}

void ExecStreamGraphImpl::interposeStream(
    std::string name,
    uint iOutput,
    ExecStreamId interposedId)
{
    SharedExecStream pLastStream = findLastStream(name, iOutput);
    assert(pLastStream.get());
    streamOutMap[std::make_pair(name, iOutput)] = interposedId;
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
    if (sortedStreams.empty()) {
        // in case removeStream() was called after prepare
        sortStreams();
    }
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

SharedExecStream ExecStreamGraphImpl::getStream(ExecStreamId id)
{
    Vertex v = boost::vertices(graphRep).first[id];
    return getStreamFromVertex(v);
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

ExecStreamGraphImpl::Edge ExecStreamGraphImpl::getInputEdge(
    ExecStreamId streamId,
    uint iInput)
{
    Vertex streamVertex = boost::vertices(graphRep).first[streamId];
    return boost::in_edges(streamVertex,graphRep).first[iInput];
}

SharedExecStream ExecStreamGraphImpl::getStreamInput(
    ExecStreamId streamId,
    uint iInput)
{
    Edge inputEdge = getInputEdge(streamId, iInput);
    Vertex inputVertex = boost::source(inputEdge,graphRep);
    return getStreamFromVertex(inputVertex);
}

SharedExecStreamBufAccessor ExecStreamGraphImpl::getStreamInputAccessor(
    ExecStreamId streamId,
    uint iInput)
{
    Edge inputEdge = getInputEdge(streamId, iInput);
    return getSharedBufAccessorFromEdge(inputEdge);
}

ExecStreamGraphImpl::Edge ExecStreamGraphImpl::getOutputEdge(
    ExecStreamId streamId,
    uint iOutput)
{
    Vertex streamVertex = boost::vertices(graphRep).first[streamId];
    return boost::out_edges(streamVertex,graphRep).first[iOutput];
}

SharedExecStream ExecStreamGraphImpl::getStreamOutput(
    ExecStreamId streamId,
    uint iOutput)
{
    Edge outputEdge = getOutputEdge(streamId, iOutput);
    Vertex outputVertex = boost::target(outputEdge,graphRep);
    return getStreamFromVertex(outputVertex);
}

SharedExecStreamBufAccessor ExecStreamGraphImpl::getStreamOutputAccessor(
    ExecStreamId streamId,
    uint iOutput)
{
    Edge outputEdge = getOutputEdge(streamId, iOutput);
    return getSharedBufAccessorFromEdge(outputEdge);
}

std::vector<SharedExecStream> ExecStreamGraphImpl::getSortedStreams()
{
    assert(isPrepared);
    if (sortedStreams.empty()) {
        sortStreams();
    }
    return sortedStreams;
}

bool ExecStreamGraphImpl::checkForNoCycles()
{
    int numVertices = boost::num_vertices(graphRep);

    // if # strong components is < # vertices, then there must be at least
    // one cycle
    std::vector<int> component(numVertices);
    int nStrongComps = boost::strong_components(graphRep, &component[0]);
    return (nStrongComps >= numVertices);
}

FENNEL_END_CPPFILE("$Id$");

// End ExecStreamGraph.cpp
