/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005-2009 The Eigenbase Project
// Copyright (C) 2005-2009 SQLstream, Inc.
// Copyright (C) 2005-2009 LucidEra, Inc.
// Portions Copyright (C) 2004-2009 John V. Sichi
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

#ifndef Fennel_ExecStreamScheduler_Included
#define Fennel_ExecStreamScheduler_Included

#include "fennel/exec/ExecStream.h"
#include "fennel/common/TraceSource.h"

#include <boost/utility.hpp>

FENNEL_BEGIN_NAMESPACE

/**
 * ExecStreamScheduler defines an abstract base for controlling the scheduling
 * of execution streams.  A scheduler determines which execution streams to run
 * and in what order.  For more information, see SchedulerDesign.
 *
 * @author John V. Sichi
 * @version $Id$
 */
class FENNEL_EXEC_EXPORT ExecStreamScheduler
    : public boost::noncopyable,
        public virtual TraceSource
{
protected:
    bool tracingFine;

    /**
     * Constructs a new ExecStreamScheduler.
     *
     * @param pTraceTarget the TraceTarget to which messages will be sent,
     * or NULL to disable tracing entirely
     *
     * @param name the name to use for tracing this scheduler
     */
    explicit ExecStreamScheduler(
        SharedTraceTarget pTraceTarget,
        std::string name);

    /**
     * Executes one stream, performing tracing if enabled.
     *
     * @param stream stream to execute
     *
     * @param quantum quantum controlling stream execution
     *
     * @return result of executing stream
     */
    inline ExecStreamResult executeStream(
        ExecStream &stream,
        ExecStreamQuantum const &quantum);

    /**
     * Traces before execution of a stream.
     *
     * @param stream stream about to be executed
     *
     * @param quantum quantum controlling stream execution
     */
    virtual void tracePreExecution(
        ExecStream &stream,
        ExecStreamQuantum const &quantum);

    /**
     * Traces after execution of a stream.
     *
     * @param stream stream which was just executed
     *
     * @param rc result code returned by stream
     */
    virtual void tracePostExecution(
        ExecStream &stream,
        ExecStreamResult rc);

    /**
     * Traces the states of the input and output buffers adjacent
     * to a stream.
     *
     * @param stream stream whose buffers are to be traced
     *
     * @param inputTupleTraceLevel trace level at which tuple contents
     * of input buffers are to be traced
     *
     * @param outputTupleTraceLevel trace level at which tuple contents
     * of output buffers are to be traced
     */
    virtual void traceStreamBuffers(
        ExecStream &stream,
        TraceLevel inputTupleTraceLevel,
        TraceLevel outputTupleTraceLevel);

public:
    virtual ~ExecStreamScheduler();

    /**
     * Traces the contents of a stream buffer.
     *
     * @param stream stream whose buffer is being traced
     *
     * @param bufAccessor accessor for stream buffer
     *
     * @param traceLevel level at which contents should be traced
     */
    virtual void traceStreamBufferContents(
        ExecStream &stream,
        ExecStreamBufAccessor &bufAccessor,
        TraceLevel traceLevel);

    /**
     * Adds a graph to be scheduled.  Some implementations may require all
     * graphs to be added before scheduler is started; others may allow graphs
     * to be added at any time.
     *
     * @param pGraph the graph to be scheduled
     */
    virtual void addGraph(SharedExecStreamGraph pGraph);

    /**
     * Removes a graph currently being scheduled.  Some implementations may
     * disallow graph removal except when scheduler is stopped; others
     * may disallow graph removal altogether.
     *
     * @param pGraph the graph currently being scheduled
     */
    virtual void removeGraph(SharedExecStreamGraph pGraph);

    /**
     * Starts this scheduler, preparing it to execute streams.
     */
    virtual void start() = 0;

    /**
     * Requests that a specific stream be considered for execution.
     *
     * @param stream the stream to make runnable
     *
     * @deprecated use setRunnable
     */
    inline void makeRunnable(ExecStream &stream);

    /**
     * Sets whether that a specific stream should be considered for execution.
     *
     * @param stream the stream to make runnable
     */
    virtual void setRunnable(
        ExecStream &stream,
        bool runnable) = 0;

    /**
     * Asynchronously aborts execution of any scheduled streams contained by a
     * particular graph and prevents further scheduling.  Returns immediately,
     * not waiting for abort request to be fully processed.
     *
     * @param graph graph to abort; must be one of the graphs
     * associated with this scheduler
     */
    virtual void abort(ExecStreamGraph &graph) = 0;

    /**
     * Checks whether there is an abort request for this
     * scheduler, and if so, throws an AbortExcn.
     */
    virtual void checkAbort() const;

    /**
     * Shuts down this scheduler, preventing any further streams from
     * being scheduled.
     */
    virtual void stop() = 0;

    /**
     * Creates a new ExecStreamBufAccessor suitable for use with
     * this scheduler.
     *
     * @return new buffer accessor
     */
    virtual SharedExecStreamBufAccessor newBufAccessor();

    /**
     * Creates a new adapter stream capable of buffering the output
     * of a stream with BUFPROV_CONSUMER for use as input to a stream
     * with BUFPROV_PRODUCER.  Default implementation is
     * ScratchBufferExecStream.  Caller is responsible for filling
     * in generic ExecStreamParams after return.
     *
     * @param embryo receives new adapter stream
     */
    virtual void createBufferProvisionAdapter(
        ExecStreamEmbryo &embryo);

    /**
     * Creates a new adapter stream capable of copying the output
     * of a stream with BUFPROV_PRODUCER into the input of a stream
     * with BUFPROV_CONSUMER.  Default implementation
     * is CopyExecStream.  Caller is responsible for filling in
     * generic ExecStreamParams after return.
     *
     * @param embryo receives new adapter stream
     */
    virtual void createCopyProvisionAdapter(
        ExecStreamEmbryo &embryo);

    /**
     * Reads data from a stream, first performing any scheduling necessary
     * to make output available.
     *
     * @param stream the stream from which to read
     *
     * @return accessor for output data buffer
     */
    virtual ExecStreamBufAccessor &readStream(
        ExecStream &stream) = 0;

    /**
     * @return the degree of parallelism implemented by this scheduler,
     * or 1 for a non-parallel scheduler
     */
    virtual uint getDegreeOfParallelism();
};

inline ExecStreamResult ExecStreamScheduler::executeStream(
    ExecStream &stream,
    ExecStreamQuantum const &quantum)
{
    if (tracingFine) {
        tracePreExecution(stream, quantum);
        ExecStreamResult rc = stream.execute(quantum);
        tracePostExecution(stream, rc);
        return rc;
    } else {
        return stream.execute(quantum);
    }
}

inline void ExecStreamScheduler::makeRunnable(
    ExecStream &stream)
{
    setRunnable(stream, true);
}

FENNEL_END_NAMESPACE

#endif

// End ExecStreamScheduler.h
