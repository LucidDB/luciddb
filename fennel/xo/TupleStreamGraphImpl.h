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

#ifndef Fennel_TupleStreamGraphImpl_Included
#define Fennel_TupleStreamGraphImpl_Included

#include "fennel/xo/TupleStreamGraph.h"

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
 * A TupleStreamGraphImpl is a connected, directed graph representing dataflow
 * among TupleStreams.  Currently, only trees are supported, so each vertex
 * except the root has exactly one target and zero or more sources.
 */
class TupleStreamGraphImpl : public TupleStreamGraph
{
    friend class TupleStreamGraph;
    
    typedef boost::adjacency_list<
        boost::vecS,
        boost::vecS,
        boost::bidirectionalS,
        boost::property<boost::vertex_data_t,SharedTupleStream> >
    GraphRep;

    typedef boost::graph_traits<GraphRep>::vertex_descriptor Vertex;

    typedef boost::graph_traits<GraphRep>::edge_descriptor Edge;

    GraphRep graphRep;
    
    /**
     * Result of topologically sorting graph (producers before consumers).
     */
    std::vector<SharedTupleStream> sortedStreams;

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
    
    explicit TupleStreamGraphImpl();
    SharedTupleStream getStreamFromVertex(Vertex);
    void closeImpl();
    void sortStreams();
    
public:
    virtual void setTxn(SharedLogicalTxn);
    virtual void setScratchSegment(
        SharedSegment pScratchSegment);
    virtual SharedLogicalTxn getTxn();
    virtual void prepare();
    virtual void open();
    virtual void addStream(
        SharedTupleStream pStream);
    virtual void addDataflow(
        TupleStreamId producerId,
        TupleStreamId consumerId);
    virtual uint getInputCount(
        TupleStreamId streamId);
    virtual SharedTupleStream getStreamInput(
        TupleStreamId streamId,
        uint iInput);
    virtual SharedTupleStream getSinkStream();
};

FENNEL_END_NAMESPACE

#endif

// End TupleStreamGraphImpl.h
