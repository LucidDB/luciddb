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

#ifndef Fennel_ExecStreamGraphImpl_Included
#define Fennel_ExecStreamGraphImpl_Included

#include "fennel/exec/ExecStreamGraph.h"

#include <vector>
#include <boost/property_map.hpp>
#include <boost/graph/adjacency_list.hpp>
#include <boost/graph/properties.hpp>

// REVIEW:  can this be pulled into fennel namespace somehow?
namespace boost 
{
enum vertex_data_t { vertex_data };
enum edge_data_t { edge_data };
BOOST_INSTALL_PROPERTY(vertex,data);
BOOST_INSTALL_PROPERTY(edge,data);
}

FENNEL_BEGIN_NAMESPACE

/**
 * ExecStreamGraphImpl is an implementation for the ExecStreamGraph
 * interface based on the boost graph template.
 */
class ExecStreamGraphImpl : virtual public ExecStreamGraph
{
public:
    typedef boost::adjacency_list<
        boost::vecS,
        boost::vecS,
        boost::bidirectionalS,
        boost::property<boost::vertex_data_t,SharedExecStream>,
        boost::property<boost::edge_data_t,SharedExecStreamBufAccessor> >
    GraphRep;

    typedef boost::graph_traits<GraphRep>::vertex_descriptor Vertex;

    typedef boost::graph_traits<GraphRep>::edge_descriptor Edge;

    typedef boost::graph_traits<GraphRep>::vertex_iterator VertexIter;

    typedef boost::graph_traits<GraphRep>::edge_iterator EdgeIter;

    typedef boost::graph_traits<GraphRep>::out_edge_iterator OutEdgeIter;

    typedef boost::graph_traits<GraphRep>::in_edge_iterator InEdgeIter;

    typedef std::pair<VertexIter,VertexIter> VertexIterPair;

    typedef std::pair<EdgeIter,EdgeIter> EdgeIterPair;

    typedef std::pair<OutEdgeIter,OutEdgeIter> OutEdgeIterPair;

    typedef std::pair<InEdgeIter,InEdgeIter> InEdgeIterPair;

protected:

    GraphRep graphRep;
    
    typedef std::map<std::string,ExecStreamId> StreamMap;
    typedef StreamMap::const_iterator StreamMapConstIter;
    typedef std::map<std::pair<std::string, uint>,ExecStreamId> EdgeMap;

    // nested classes for implementing renderGraphviz
    class DotGraphRenderer;
    class DotVertexRenderer;
    class DotEdgeRenderer;

    /**
     * List of freed vertices
     */
    std::vector<Vertex> freeVertices;

    /**
     * Map of name to stream
     */
    StreamMap streamMap;
    
    /**
     * Map of name and output arc to stream output, after add-ons
     */
    EdgeMap streamOutMap;

    /**
     * Result of topologically sorting graph (producers before consumers).
     */
    std::vector<SharedExecStream> sortedStreams;

    /**
     * Transaction being executed.
     */
    SharedLogicalTxn pTxn;

    /**
     * Source for scratch buffers.
     */
    SharedSegment pScratchSegment;

    /**
     * Resource governor
     */
    SharedExecStreamGovernor pResourceGovernor;

    /**
     * Whether this graph is currently open.  Note that this is not quite the
     * opposite of the inherited ClosableObject.needsClose, since a graph
     * needs to be closed before destruction if it has been prepared but never
     * opened.
     */
    bool isOpen;

    /**
     * Whether this graph has been prepared.
     */
    bool isPrepared;

    /**
     * Whether to close this graph in dataflow order (producers to consumers)
     */
    bool doDataflowClose;
    
    virtual void closeImpl();
    virtual void sortStreams();
    virtual void openStream(SharedExecStream pStream);
    virtual void bindStreamBufAccessors(SharedExecStream pStream);
    virtual void mergeFrom(ExecStreamGraphImpl& src);
    virtual void mergeFrom(
        ExecStreamGraphImpl& src, std::vector<ExecStreamId>const& nodes);

    /** frees all nodes and edges: like removeStream() on all streams, but
     * faster */
    virtual void clear();
    /** adds a node */
    virtual Vertex addVertex(SharedExecStream pStream);
    
    // manage the free list
    /** @return an available Vertex, first trying the free list */
    Vertex newVertex();
    /** releases a Vertex to the free list */
    void freeVertex(Vertex);

    /** removes a stream from streamOutMap */
    void removeFromStreamOutMap(SharedExecStream);

    virtual Edge getInputEdge(ExecStreamId stream, uint iInput);
    virtual Edge getOutputEdge(ExecStreamId stream, uint iOutput);

public:
    explicit ExecStreamGraphImpl();
    virtual ~ExecStreamGraphImpl() {}
    
    inline GraphRep const &getGraphRep();
    inline SharedExecStream getStreamFromVertex(Vertex);
    inline SharedExecStreamBufAccessor &getSharedBufAccessorFromEdge(Edge);
    inline ExecStreamBufAccessor &getBufAccessorFromEdge(Edge);

    // implement ExecStreamGraph
    virtual void setTxn(SharedLogicalTxn pTxn);
    virtual void setScratchSegment(
        SharedSegment pScratchSegment);
    virtual void setResourceGovernor(
        SharedExecStreamGovernor pResourceGovernor);
    virtual SharedLogicalTxn getTxn();
    virtual SharedExecStreamGovernor getResourceGovernor();
    virtual void prepare(ExecStreamScheduler &scheduler);
    virtual void open();
    virtual void addStream(SharedExecStream pStream);
    virtual void removeStream(ExecStreamId);
    virtual void addDataflow(
        ExecStreamId producerId,
        ExecStreamId consumerId);
    virtual void addOutputDataflow(
        ExecStreamId producerId);
    virtual void addInputDataflow(
        ExecStreamId consumerId);
    virtual void mergeFrom(ExecStreamGraph& src);
    virtual void mergeFrom(
        ExecStreamGraph& src, std::vector<ExecStreamId>const& nodes);
    virtual SharedExecStream findStream(
        std::string name);
    virtual SharedExecStream findLastStream(
        std::string name,
        uint iOutput);
    virtual void interposeStream(
        std::string name,
        uint iOutput,
        ExecStreamId interposedId);
    virtual SharedExecStream getStream(ExecStreamId id);
    virtual uint getInputCount(
        ExecStreamId streamId);
    virtual uint getOutputCount(
        ExecStreamId streamId);
    virtual SharedExecStream getStreamInput(
        ExecStreamId streamId,
        uint iInput);
    virtual SharedExecStreamBufAccessor getStreamInputAccessor(
        ExecStreamId streamId,
        uint iInput);
    virtual SharedExecStream getStreamOutput(
        ExecStreamId streamId,
        uint iOutput);
    virtual SharedExecStreamBufAccessor getStreamOutputAccessor(
        ExecStreamId streamId,
        uint iOutput);
    virtual std::vector<SharedExecStream> getSortedStreams();
    virtual int getStreamCount();
    virtual int getDataflowCount();
    virtual void renderGraphviz(std::ostream &dotStream);
    virtual bool isAcyclic();
};

inline ExecStreamGraphImpl::GraphRep const &ExecStreamGraphImpl::getGraphRep()
{
    return graphRep;
}

inline SharedExecStream ExecStreamGraphImpl::getStreamFromVertex(
    Vertex vertex)
{
    return boost::get(boost::vertex_data,graphRep)[vertex];
}

inline SharedExecStreamBufAccessor &
    ExecStreamGraphImpl::getSharedBufAccessorFromEdge(
        Edge edge)
{
    return boost::get(boost::edge_data,graphRep)[edge];
}
    
inline ExecStreamBufAccessor &ExecStreamGraphImpl::getBufAccessorFromEdge(
    Edge edge)
{
    return *(getSharedBufAccessorFromEdge(edge));
}

FENNEL_END_NAMESPACE

#endif

// End ExecStreamGraphImpl.h
