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

#ifndef Fennel_ExecStreamGraph_Included
#define Fennel_ExecStreamGraph_Included

#include "fennel/common/ClosableObject.h"

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
 *
 * When a stream is added to a graph it is assigned an ExecStreamId. The
 * identifier is later used to work with the stream.
 *
 * <p>
 * 
 * In addition, streams are required to have unique names when they are added
 * to the graph.  These names are used later to find the streams.
 */
class ExecStreamGraph
    : public boost::noncopyable,
        public ClosableObject
{
    friend class ExecStreamScheduler;
    
protected:
    /**
     * Scheduler responsible for executing streams in this graph.  Note that we
     * don't use a weak_ptr for this because it needs to be accessed frequently
     * during execution, and the extra locking overhead would be frivolous.
     */
    ExecStreamScheduler *pScheduler;
    
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
     * @return reference to executing scheduler
     */
    inline ExecStreamScheduler &getScheduler() const;
    
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
     * @return stream found
     */
    virtual SharedExecStream findLastStream(
        std::string name) = 0;
    
    /**
     * Interposes an adapter stream. In the process, creates a dataflow 
     * from last stream associated with name to the adapter stream.
     *
     * @param name name of stream to adapt
     *
     * @param ID of adapter stream within this graph
     *
     */
    virtual void interposeStream(
        std::string name,
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
     * Gets streams, sorted topologically. Can only be called after prepare.
     *
     * @return vector of sorted streams
     */
    virtual std::vector<SharedExecStream> getSortedStreams() = 0;
};

inline ExecStreamScheduler &ExecStreamGraph::getScheduler() const
{
    assert(pScheduler);
    return *pScheduler;
}

FENNEL_END_NAMESPACE

#endif

// End ExecStreamGraph.h
