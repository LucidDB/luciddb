/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2005-2005 Disruptive Tech
// Copyright (C) 2005-2005 Red Square, Inc.
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

#ifndef Fennel_ExecStreamGraph_Included
#define Fennel_ExecStreamGraph_Included

#include "fennel/common/ClosableObject.h"
#include "fennel/disruptivetech/calc/DynamicParam.h"

#include <vector>
#include <boost/utility.hpp>

FENNEL_BEGIN_NAMESPACE

/**
 * Identifier for an ExecStream relative to an instance of
 * ExecStreamGraph.
 */
typedef uint ExecStreamId;

class ExecStreamGraphImpl;
class ExecStreamScheduler;

/**
 * An ExecStreamGraph is a directed graph representing dataflow
 * among ExecStreams.  For more information, see ExecStreamDesign.
 *
 * <p>
 * A stream is always a node is a stream graph, but over its lifetime it
 * may be moved from one graph to another. Specifically, it may be
 * prepared in one graph (see ExecStreamGraphEmbro), executed in another,
 * and finally closed & deleted in a third graph. 
 *
 * <p>
 *
 * A stream has a permanent, unique name. These names are used later to find the streams.
 * When a stream is added to a graph it is assigned an ExecStreamId. This
 * identifier is later used to work with the stream. If the stream is moved to another
 * graph, it obtains a new ExecStreamId.
 */
class ExecStreamGraph
    : public boost::noncopyable,
        public ClosableObject
{
    friend class ExecStreamScheduler;
    
protected:
    /**
     * A Scheduler responsible for executing streams in this graph.
     * (Can be null if the current graph is only building streams, not executing
     * them.) Note that we don't use a weak_ptr for this because it needs to be
     * accessed frequently during execution, and the extra locking overhead
     * would be frivolous.
     */
    ExecStreamScheduler *pScheduler;

    /**
     * Manager that handles dynamic parameters for this graph
     */
    DynamicParamManager dynamicParamManager;
        
    explicit ExecStreamGraph();
    
public:
    /**
     * Constructs a new ExecStreamGraph.
     *
     * @return new graph
     */
    static SharedExecStreamGraph newExecStreamGraph();
    
    virtual ~ExecStreamGraph();

    /**
     * @return pointer to executing scheduler, or null if there is none.
     */
    inline ExecStreamScheduler *getScheduler() const;
    
    /**
     * @return reference to the DynamicParamManager
     */
    inline DynamicParamManager &getDynamicParamManager();

    /**
     * Sets the transaction within which this graph should execute.
     * The transaction is reset whenever the graph is closed.
     *
     * @param pTxn transaction
     */
    virtual void setTxn(
        SharedLogicalTxn pTxn) = 0;

    /**
     * Sets the ScratchSegment from which this graph's streams should
     * allocate memory buffers.
     *
     * @param pScratchSegment scratch segment
     */
    virtual void setScratchSegment(
        SharedSegment pScratchSegment) = 0;

    /**
     * @return the transaction within which this graph is executing
     */
    virtual SharedLogicalTxn getTxn() = 0;

    /**
     * Adds a stream to this graph.
     *
     * @param pStream stream to add
     */
    virtual void addStream(
        SharedExecStream pStream) = 0;

    /**
     * Removes a stream from the graph: deletes the edges,
     * and puts the vertex on a free list to be reallocated.
     * Does not free the ExecStream or its ExecStreamBufAccessors.
     */
    virtual void removeStream(ExecStreamId) = 0;

    /**
     * Defines a dataflow relationship between two streams in this graph.
     *
     * @param producerId ID of producer stream in this graph
     *
     * @param consumerId ID of consumer stream in this graph
     */
    virtual void addDataflow(
        ExecStreamId producerId,
        ExecStreamId consumerId) = 0;

    /**
     * Defines a dataflow representing external output produced by this graph.
     *
     * @param producerId ID of producer stream in this graph
     */
    virtual void addOutputDataflow(
        ExecStreamId producerId) = 0;

    /**
     * Defines a dataflow representing external input consumed by this graph.
     *
     * @param consumerId ID of consumer stream in this graph
     */
    virtual void addInputDataflow(
        ExecStreamId consumerId) = 0;

    /**
     * Adds all the vertices and edges from another graph.
     * Assumes the graphs are disjoint, and that both have been prepared.
     * The two graphs are both open, or else both closed.
     * @param src the other graph, which is left empty.
     */
    virtual void mergeFrom(ExecStreamGraph& src) = 0;

    /**
     * Finds a stream by name.
     *
     * @param name name of stream to find
     *
     * @return stream found
     */
    virtual SharedExecStream findStream(
        std::string name) = 0;
    
    /**
     * Finds last stream known for name. May be original stream or an adapter.
     *
     * @param name name of stream to find
     *
     * @param iOutput ordinal of output arc
     *
     * @return stream found
     */
    virtual SharedExecStream findLastStream(
        std::string name,
        uint iOutput) = 0;
    
    /**
     * Interposes an adapter stream. In the process, creates a dataflow 
     * from last stream associated with name to the adapter stream.
     *
     * @param name name of stream to adapt
     *
     * @param iOutput ordinal of output of stream
     *
     * @param ID of adapter stream within this graph
     *
     */
    virtual void interposeStream(
        std::string name,
        uint iOutput,
        ExecStreamId interposedId) = 0;

    /**
     * Prepares this graph for execution.  Only called once (before first open)
     * after all streams and dataflows have been defined.
     *
     * @param scheduler ExecStreamScheduler which will execute this graph
     */
    virtual void prepare(ExecStreamScheduler &scheduler) = 0;

    /**
     * Opens execution on this graph.  A graph may be repeatedly closed
     * and then reopened.
     */
    virtual void open() = 0;

    /**
     * Determines number of input flows consumed by a stream.
     *
     * @param streamId ID of stream
     *
     * @return input count
     */
    virtual uint getInputCount(
        ExecStreamId streamId) = 0;
    
    /**
     * Determines number of output flows produced by a stream.
     *
     * @param streamId ID of stream
     *
     * @return output count
     */
    virtual uint getOutputCount(
        ExecStreamId streamId) = 0;
    
    /**
     * Accesses a stream's input.
     *
     * @param streamId ID of stream
     *
     * @param iInput 0-based input flow ordinal
     *
     * @return upstream producer
     */
    virtual SharedExecStream getStreamInput(
        ExecStreamId streamId,
        uint iInput) = 0;

    /**
     * Accesses a stream's input accessor.
     *
     * @param streamId ID of stream
     *
     * @param iInput 0-based input flow ordinal
     *
     * @return accessor used by upstream producer
     */
    virtual SharedExecStreamBufAccessor getStreamInputAccessor(
        ExecStreamId streamId,
        uint iInput) = 0;

    /**
     * Accesses a stream's output.
     *
     * @param streamId ID of stream
     *
     * @param iInput 0-based output flow ordinal
     *
     * @return downstream consumer
     */
    virtual SharedExecStream getStreamOutput(
        ExecStreamId streamId,
        uint iOutput) = 0;

    /**
     * Accesses a stream's output accessor.
     *
     * @param streamId ID of stream
     *
     * @param iInput 0-based output flow ordinal
     *
     * @return accessor used by downstream consumer
     */
    virtual SharedExecStreamBufAccessor getStreamOutputAccessor(
        ExecStreamId streamId,
        uint iOutput) = 0;

    /**
     * Gets streams, sorted topologically. Can only be called after prepare.
     *
     * @return vector of sorted streams
     */
    virtual std::vector<SharedExecStream> getSortedStreams() = 0;
};

inline ExecStreamScheduler *ExecStreamGraph::getScheduler() const
{
    return pScheduler;
}

inline DynamicParamManager &ExecStreamGraph::getDynamicParamManager()
{
    return dynamicParamManager;
}

FENNEL_END_NAMESPACE

#endif

// End ExecStreamGraph.h
