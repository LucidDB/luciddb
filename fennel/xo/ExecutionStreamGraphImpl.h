/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2003-2005 Disruptive Tech
// Copyright (C) 2005-2005 Red Square, Inc.
// Portions Copyright (C) 1999-2005 John V. Sichi
//
// This program is free software; you can redistribute it and/or
// modify it under the terms of the GNU General Public License
// as published by the Free Software Foundation; either version 2
// of the License, or (at your option) any later Eigenbase-approved version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program; if not, write to the Free Software
// Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307  USA
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
class ExecutionStreamGraphImpl : virtual public ExecutionStreamGraph
{
protected:
    typedef boost::adjacency_list<
        boost::vecS,
        boost::vecS,
        boost::bidirectionalS,
        boost::property<boost::vertex_data_t,SharedExecutionStream> >
    GraphRep;

    typedef boost::graph_traits<GraphRep>::vertex_descriptor Vertex;

    typedef boost::graph_traits<GraphRep>::edge_descriptor Edge;

    GraphRep graphRep;
    
    typedef std::map<std::string,ExecutionStreamId> StreamMap;
    typedef StreamMap::const_iterator StreamMapConstIter;
    
    /**
     * Map of name to stream
     */
    StreamMap streamMap;
    
    /**
     * Map of name to stream output, after add-ons
     */
    StreamMap streamOutMap;

    /**
     * Result of topologically sorting graph (producers before consumers).
     */
    std::vector<SharedExecutionStream> sortedStreams;

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

    /**
     * Whether this graph has been prepared.
     */
    bool isPrepared;

    /**
     * Whether to close this graph in dataflow order (producers to consumers)
     */
    bool doDataflowClose;
    
    explicit ExecutionStreamGraphImpl();
    virtual SharedExecutionStream getStreamFromVertex(Vertex);
    virtual void closeImpl();
    virtual void sortStreams();
    virtual void openStream(SharedExecutionStream pStream);
    
public:
    virtual ~ExecutionStreamGraphImpl() {}
    
    virtual void setTxn(SharedLogicalTxn);
    virtual void setScratchSegment(
        SharedSegment pScratchSegment);
    virtual SharedLogicalTxn getTxn();
    virtual void prepare();
    virtual void open();
    virtual void addStream(
        SharedExecutionStream pStream);
    virtual void addDataflow(
        ExecutionStreamId producerId,
        ExecutionStreamId consumerId);
    virtual SharedExecutionStream findStream(
            std::string name);
    virtual SharedExecutionStream findLastStream(
            std::string name);
    virtual void interposeStream(
        std::string name,
        ExecutionStreamId interposedId);
    virtual uint getInputCount(
        ExecutionStreamId streamId);
    virtual SharedExecutionStream getStreamInput(
        ExecutionStreamId streamId,
        uint iInput);
    virtual SharedExecutionStream getSinkStream();
    virtual std::vector<SharedExecutionStream> getSortedStreams();
};

FENNEL_END_NAMESPACE

#endif

// End ExecutionStreamGraphImpl.h
