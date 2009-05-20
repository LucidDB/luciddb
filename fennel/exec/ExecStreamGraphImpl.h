/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005-2009 The Eigenbase Project
// Copyright (C) 2005-2009 SQLstream, Inc.
// Copyright (C) 2005-2009 LucidEra, Inc.
// Portions Copyright (C) 1999-2009 John V. Sichi
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
#include <boost/graph/filtered_graph.hpp>

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
class FENNEL_EXEC_EXPORT ExecStreamGraphImpl
    : virtual public ExecStreamGraph
{
public:
    typedef boost::adjacency_list<
        boost::vecS,
        boost::vecS,
        boost::bidirectionalS,
        boost::property<boost::vertex_data_t,SharedExecStream>,
        boost::property<
        boost::edge_data_t,SharedExecStreamBufAccessor,
        boost::property<boost::edge_weight_t,int> > >
    FullGraphRep;

    typedef boost::graph_traits<FullGraphRep>::vertex_descriptor Vertex;
    typedef boost::graph_traits<FullGraphRep>::edge_descriptor Edge;

    typedef boost::graph_traits<FullGraphRep>::vertex_iterator FgVertexIter;
    typedef boost::graph_traits<FullGraphRep>::edge_iterator FgEdgeIter;
    typedef boost::graph_traits<FullGraphRep>::out_edge_iterator FgOutEdgeIter;
    typedef boost::graph_traits<FullGraphRep>::in_edge_iterator FgInEdgeIter;
    typedef std::pair<FgVertexIter,FgVertexIter> FgVertexIterPair;
    typedef std::pair<FgEdgeIter,FgEdgeIter> FgEdgeIterPair;
    typedef std::pair<FgOutEdgeIter,FgOutEdgeIter> FgOutEdgeIterPair;
    typedef std::pair<FgInEdgeIter,FgInEdgeIter> FgInEdgeIterPair;

    typedef boost::property_map<FullGraphRep, boost::edge_weight_t>::type
        EdgeWeightMap;

    struct ExplicitEdgePredicate
    {
        EdgeWeightMap weightMap;

        // NOTE jvs 6-Jan-2006:  Lack of keyword "explicit" on constructors
        // here is intentional.

        ExplicitEdgePredicate()
        {
        }

        ExplicitEdgePredicate(EdgeWeightMap weightMapInit)
            : weightMap(weightMapInit)
        {
        }

        bool operator () (Edge const &edge) const
        {
            return boost::get(weightMap, edge) > 0;
        }
    };

    typedef boost::filtered_graph<FullGraphRep, ExplicitEdgePredicate>
        GraphRep;
    typedef boost::graph_traits<GraphRep>::vertex_iterator VertexIter;
    typedef boost::graph_traits<GraphRep>::edge_iterator EdgeIter;
    typedef boost::graph_traits<GraphRep>::out_edge_iterator OutEdgeIter;
    typedef boost::graph_traits<GraphRep>::in_edge_iterator InEdgeIter;
    typedef std::pair<VertexIter,VertexIter> VertexIterPair;
    typedef std::pair<EdgeIter,EdgeIter> EdgeIterPair;
    typedef std::pair<OutEdgeIter,OutEdgeIter> OutEdgeIterPair;
    typedef std::pair<InEdgeIter,InEdgeIter> InEdgeIterPair;


protected:

    // NOTE jvs 8-Jan-2007:  We maintain two boost graphs;
    // graphRep is the "full" graph, including both implicit
    // and explicit dataflow edges; filteredGraph is a subgraph
    // view selecting just the explicit dataflows.  Code which
    // accesses the graph needs to decide which view it wants
    // and use the corresponding iterators.

    FullGraphRep graphRep;

    GraphRep filteredGraph;

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
     * Target for row errors.
     */
    SharedErrorTarget pErrorTarget;

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

    class DynamicParamInfo
    {
    public:
      std::vector<ExecStreamId> readerStreamIds;
      std::vector<ExecStreamId> writerStreamIds;
    };

    /**
     * Information on readers and writers of dynamic parameters.
     */
    std::map<DynamicParamId, DynamicParamInfo> dynamicParamMap;

    /**
     * Whether to allow execution without a real transaction.
     */
    bool allowDummyTxnId;

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
    inline FullGraphRep const &getFullGraphRep();
    inline SharedExecStream getStreamFromVertex(Vertex);
    inline SharedExecStreamBufAccessor &getSharedBufAccessorFromEdge(Edge);
    inline ExecStreamBufAccessor &getBufAccessorFromEdge(Edge);

    // implement ExecStreamGraph
    virtual void setTxn(SharedLogicalTxn pTxn);
    virtual void setErrorTarget(SharedErrorTarget pErrorTarget);
    virtual void setScratchSegment(
        SharedSegment pScratchSegment);
    virtual void setResourceGovernor(
        SharedExecStreamGovernor pResourceGovernor);
    virtual SharedLogicalTxn getTxn();
    virtual TxnId getTxnId();
    virtual void enableDummyTxnId(bool enabled);
    virtual SharedExecStreamGovernor getResourceGovernor();
    virtual void prepare(ExecStreamScheduler &scheduler);
    virtual void open();
    virtual void addStream(SharedExecStream pStream);
    virtual void removeStream(ExecStreamId);
    virtual void addDataflow(
        ExecStreamId producerId,
        ExecStreamId consumerId,
        bool isImplicit = false);
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
    virtual void closeProducers(ExecStreamId streamId);
    virtual void declareDynamicParamWriter(
        ExecStreamId streamId,
        DynamicParamId dynamicParamId);
    virtual void declareDynamicParamReader(
        ExecStreamId streamId,
        DynamicParamId dynamicParamId);
    virtual const std::vector<ExecStreamId> &getDynamicParamWriters(
        DynamicParamId dynamicParamId);
    virtual const std::vector<ExecStreamId> &getDynamicParamReaders(
        DynamicParamId dynamicParamId);
};

inline ExecStreamGraphImpl::GraphRep const &
    ExecStreamGraphImpl::getGraphRep()
{
    return filteredGraph;
}

inline ExecStreamGraphImpl::FullGraphRep const &
    ExecStreamGraphImpl::getFullGraphRep()
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
