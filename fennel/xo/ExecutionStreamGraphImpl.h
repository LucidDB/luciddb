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

#ifndef Fennel_ExecutionStreamGraphImpl_Included
#define Fennel_ExecutionStreamGraphImpl_Included

#include "fennel/xo/ExecutionStreamGraph.h"

#include <vector>
#include <boost/property_map.hpp>
#include <boost/graph/adjacency_list.hpp>
#include <boost/graph/properties.hpp>

// REVIEW:  can this be pulled into fennel namespace somehow?
namespace boost 
{
enum vertex_data_t { vertex_data };
BOOST_INSTALL_PROPERTY(vertex,data);
}

FENNEL_BEGIN_NAMESPACE

/**
 * A ExecutionStreamGraphImpl is a connected, directed graph representing
 * dataflow among ExecutionStreams.
 */
template<class S>
class ExecutionStreamGraphImpl : virtual public ExecutionStreamGraph<S>
{
protected:
    typedef boost::adjacency_list<
        boost::vecS,
        boost::vecS,
        boost::bidirectionalS,
        boost::property<boost::vertex_data_t,S> >
    GraphRep;

    typedef typename boost::graph_traits<GraphRep>::vertex_descriptor Vertex;

    typedef typename boost::graph_traits<GraphRep>::edge_descriptor Edge;

    GraphRep graphRep;
    
    /**
     * Result of topologically sorting graph (producers before consumers).
     */
    std::vector<S> sortedStreams;

    /**
     * Transaction being executed.
     */
    SharedLogicalTxn pTxn;

    /**
     * Source for scratch buffers.
     */
    SharedSegment pScratchSegment;

    /**
     * Whether this graph is currently open.  Note that this is not quite the
     * opposite of the inherited ClosableObject.needsClose, since a graph
     * needs to be closed before destruction if it has been prepared but never
     * opened.
     */
    bool isOpen;
    
    explicit ExecutionStreamGraphImpl()
    {
        isOpen = false;
    }

    S getStreamFromVertex(Vertex vertex)
    {
        return boost::get(boost::vertex_data,graphRep)[vertex];
    }

    void closeImpl()
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

    void sortStreams()
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
                boost::bind(&ExecutionStreamGraphImpl<S>::getStreamFromVertex,
                            this,_1));
    }
    
public:
    virtual ~ExecutionStreamGraphImpl()
    {
    }

    virtual void setTxn(SharedLogicalTxn pTxnInit)
    {
        pTxn = pTxnInit;
    }

    virtual void setScratchSegment(
            SharedSegment pScratchSegmentInit)
    {
        pScratchSegment = pScratchSegmentInit;
    }

    virtual SharedLogicalTxn getTxn()
    {
        return pTxn;
    }

    virtual void prepare()
    {
        sortStreams();
    }

    virtual void open()
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

    virtual void addDataflow(
            ExecutionStreamId producerId,
            ExecutionStreamId consumerId)
    {
        boost::add_edge(producerId,consumerId,graphRep);
    }

    virtual uint getInputCount(
            ExecutionStreamId streamId)
    {
        Vertex streamVertex = boost::vertices(graphRep).first[streamId];
        return boost::in_degree(streamVertex,graphRep);
    }

    virtual S getStreamInput(
            ExecutionStreamId streamId,
            uint iInput)
    {
        Vertex streamVertex = boost::vertices(graphRep).first[streamId];
        Edge inputEdge = boost::in_edges(streamVertex,graphRep).first[iInput];
        Vertex inputVertex = boost::source(inputEdge,graphRep);
        return getStreamFromVertex(inputVertex);
    }

    virtual S getSinkStream()
    {
        // the sink comes at the end of the topological sort
        return sortedStreams.back();
    }
};

FENNEL_END_NAMESPACE

#endif

// End ExecutionStreamGraphImpl.h
