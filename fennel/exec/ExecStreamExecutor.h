/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005 The Eigenbase Project
// Copyright (C) 2005 SQLstream, Inc.
// Copyright (C) 2005 Dynamo BI Corporation
// Portions Copyright (C) 2004 John V. Sichi
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

#ifndef Fennel_ExecStreamExecutor_Included
#define Fennel_ExecStreamExecutor_Included

#include "fennel/exec/ExecStream.h"
#include "fennel/common/TraceSource.h"
#include <boost/utility.hpp>

FENNEL_BEGIN_NAMESPACE

/**
 * ExecStreamExecutor is a base class which can execute an ExecStream with
 * special tracing. Class ExecStreamScheduler extends it with other features
 * necessary to a scheduler.
 *
 * @author Marc Berkowitz
 * @version $Id$
 */
class FENNEL_EXEC_EXPORT ExecStreamExecutor
    : public virtual boost::noncopyable, public virtual TraceSource
{
protected:
    bool tracingFine;       // if TRACE_FINE or better, cached at initialization

    /**
     * Constructs a new ExecStreamScheduler.
     *
     * @param pTraceTarget the TraceTarget to which messages will be sent,
     * or NULL to disable tracing entirely
     *
     * @param name the name to use for tracing this scheduler
     */
    explicit ExecStreamExecutor(
        SharedTraceTarget pTraceTarget,
        std::string name);

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
    virtual ~ExecStreamExecutor();

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
};

inline ExecStreamResult ExecStreamExecutor::executeStream(
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


FENNEL_END_NAMESPACE
#endif
// End ExecStreamExecutor.h


