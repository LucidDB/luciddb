/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005 The Eigenbase Project
// Copyright (C) 2005 SQLstream, Inc.
// Copyright (C) 2005 Dynamo BI Corporation
// Portions Copyright (C) 1999 John V. Sichi
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
 * A stream has a permanent, unique name. These names are used later to find
 * the streams.  When a stream is added to a graph it is assigned an
 * ExecStreamId. This identifier is later used to work with the stream. If the
 * stream is moved to another graph, it obtains a new ExecStreamId.
 */
class FENNEL_EXEC_EXPORT ExecStreamGraph
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
    SharedDynamicParamManager pDynamicParamManager;

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
     * @return reference to the DynamicParamManager for this graph.
     */
    inline SharedDynamicParamManager getDynamicParamManager();

    /**
     * Sets the transaction within which this graph should execute.
     * The transaction is reset whenever the graph is closed.
     *
     * @param pTxn transaction
     */
    virtual void setTxn(
        SharedLogicalTxn pTxn) = 0;

    /**
     * Sets the ErrorTarget to which this graph's streams should
     * send row errors.
     *
     * @param pErrorTarget error target
     */
    virtual void setErrorTarget(
        SharedErrorTarget pErrorTarget) = 0;

    /**
     * Sets the ScratchSegment from which this graph's streams should
     * allocate memory buffers.
     *
     * @param pScratchSegment scratch segment
     */
    virtual void setScratchSegment(
        SharedSegment pScratchSegment) = 0;

    /**
     * Sets the global exec stream governor
     *
     * @param pResourceGovernor exec stream governor
     */
    virtual void setResourceGovernor(
        SharedExecStreamGovernor pResourceGovernor) = 0;

    /**
     * @return the transaction within which this graph is executing
     */
    virtual SharedLogicalTxn getTxn() = 0;

    /**
     * @return the transaction ID for this graph
     */
    virtual TxnId getTxnId() = 0;

    /**
     * Controls whether it is OK to call getTxnId without first
     * calling setTxn.  Normally, this is a bad idea (since
     * in that case getTxnId will return FIRST_TXN_ID as a dummy,
     * which could lead to concurrency problems), but for
     * non-transactional unit tests, this can be useful.
     * Default is disabled.
     *
     * @param enabled whether dummy txn ID's are enabled
     */
    virtual void enableDummyTxnId(bool enabled) = 0;

    /**
     * @return exec stream governor
     */
    virtual SharedExecStreamGovernor getResourceGovernor() = 0;

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
     *
     * @param isImplicit false (the default) if the edge represents
     * direct dataflow; true if the edge represents an implicit
     * dataflow dependency
     */
    virtual void addDataflow(
        ExecStreamId producerId,
        ExecStreamId consumerId,
        bool isImplicit = false) = 0;

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
     * Adds a subgraph, taken (removed) from another graph.  (Slower than
     * mergeFrom(ExecStreamGraph&), which merges its entire source).  Assumes
     * the graphs are disjoint, and that both have been prepared.  The two
     * graphs are both open, or else both closed.  @param src the source graph
     * @param nodes identifies source nodes.
     */
    virtual void mergeFrom(
        ExecStreamGraph& src,
        std::vector<ExecStreamId> const& nodes) = 0;

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
     * @param interposedId ID of adapter stream within this graph
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
     * Translates a stream ID to a stream pointer.
     *
     * @param id ID of a stream in this graph
     *
     * @return shared pointer to the stream
     */
    virtual SharedExecStream getStream(ExecStreamId id) = 0;

    /**
     * Determines number of explicit input flows consumed by a stream.
     *
     * @param streamId ID of stream
     *
     * @return input count
     */
    virtual uint getInputCount(
        ExecStreamId streamId) = 0;

    /**
     * Determines number of explicit output flows produced by a stream.
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
     * @param iInput 0-based input explicit flow ordinal
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
     * @param iInput 0-based input explicit flow ordinal
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
     * @param iOutput 0-based output explicit flow ordinal
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
     * @param iOutput 0-based output explicit flow ordinal
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

    /**
     * @return the number of streams in the graph; can only be called after
     * prepare.
     */
    virtual int getStreamCount() = 0;

    /**
     * @return the number of dataflows (edges) in the graph; can only be called
     * after prepare.
     */
    virtual int getDataflowCount() = 0;

    /**
     * Renders the graph in the .dot format defined by
     * <a href="http://www.graphviz.org">Graphviz</a>.
     *
     * @param dotStream ostream on which to write .dot representation
     */
    virtual void renderGraphviz(std::ostream &dotStream) = 0;

    /**
     * @return true if graph has no cycles
     */
    virtual bool isAcyclic() = 0;

    /**
     * Closes the producers of a stream with a given id.
     *
     * @param streamId stream id of the stream whose producers will be closed
     */
    virtual void closeProducers(ExecStreamId streamId) = 0;

    /**
     * Declares that a given stream writes a given dynamic parameter.
     *
     * @param streamId Stream id
     * @param dynamicParamId Dynamic parameter id
     */
    virtual void declareDynamicParamWriter(
        ExecStreamId streamId,
        DynamicParamId dynamicParamId) = 0;

    /**
     * Declares that a given stream reads a given dynamic parameter.
     *
     * @param streamId Stream id
     * @param dynamicParamId Dynamic parameter id
     */
    virtual void declareDynamicParamReader(
        ExecStreamId streamId,
        DynamicParamId dynamicParamId) = 0;

    /**
     * Returns a list of stream ids that write a given dynamic parameter.
     *
     * @param dynamicParamId Dynamic parameter id
     * @return List of ids of streams that write the parameter
     */
    virtual const std::vector<ExecStreamId> &getDynamicParamWriters(
        DynamicParamId dynamicParamId) = 0;


    /**
     * Returns a list of stream ids that read a given dynamic parameter.
     *
     * @param dynamicParamId Dynamic parameter id
     * @return List of ids of streams that read the parameter
     */
    virtual const std::vector<ExecStreamId> &getDynamicParamReaders(
        DynamicParamId dynamicParamId) = 0;

};

inline ExecStreamScheduler *ExecStreamGraph::getScheduler() const
{
    return pScheduler;
}

inline SharedDynamicParamManager ExecStreamGraph::getDynamicParamManager()
{
    return pDynamicParamManager;
}

FENNEL_END_NAMESPACE

#endif

// End ExecStreamGraph.h
