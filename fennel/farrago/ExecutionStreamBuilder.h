/*
// $Id$
// Fennel is a relational database kernel.
// Copyright (C) 1999-2004 John V. Sichi.
// Copyright (C) 2003-2004 Disruptive Tech
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

#ifndef Fennel_ExecutionStreamBuilder_Included
#define Fennel_ExecutionStreamBuilder_Included

#include "fennel/common/ClosableObject.h"
#include "fennel/farrago/ExecutionStreamFactory.h"
#include "fennel/farrago/Fem.h"
#include "fennel/xo/ExecutionStream.h"

#include <boost/utility.hpp>

FENNEL_BEGIN_NAMESPACE

/**
 * ExecutionStreamBuilder builds a prepared ExecutionStreamGraph from its 
 * Java representation.  It builds a graph in three phases:
 *
 * <ul>
 *   <li>First, it builds the streams</li>
 *   <li>Then, it builds the dataflows</li>
 *   <li>Lastly, it prepares the graph and streams</li>
 * </ul>
 *
 * <p><b>Cache.</b>
 * A new scratch segment is allocated by the builder and is shared between 
 * the graph and the its streams.
 *
 * <p><b>Tracing.</b>
 * <ul>
 *   <li>All streams are TraceSource objects, and are assigned a trace 
 *     name of: <code>xo.<i>streamName</i></code>  Depending on a the 
 *     TraceTarget, this typically corresponds to a trace property like 
 *     <code>org.eigenbase.fennel.xo.<i>streamName</i></code>
 *   <li>If tracing is turned on for a stream, a tracing stream is 
 *     appended to the stream, to monitor its output.  Tracing streams are
 *     appended during the stream building phase.  They are named: 
 *     <code><i>streamName</i>.tracer</code>
 *   <li>Certain types of streams may not support tracing.
 * </ul>
 *
 * <p><b>Buffer Provisioning.</b>
 * Provisioning adapters are special streams interposed between two other 
 * streams when the producer's result provisioning does not meet the 
 * consumer's input requirements.  They are interposed during the dataflow 
 * phase.  They are named: <code><i>producerName</i>.provisioner</code>
 *
 * <p><b>Interposition.</b>
 * When tracers or provisioning adapters are appended to a stream, they 
 * consume the original stream's output and produce a new output.  To make 
 * the appended streams work transparently, the chain of streams is 
 * treated as a single unit.  Subsequent access to the stream's output is 
 * available through the graph by finding the "last" stream registered 
 * under the original stream's name.
 */
class ExecutionStreamBuilder : public boost::noncopyable
{
    /**
     * Database to be accessed by stream.
     */
    SharedDatabase pDatabase;

    /**
     * Factory for creating ExecutionStream objects.
     */
    ExecutionStreamFactory &streamFactory;
    
    /**
     * Graph of stream nodes being built up.
     */
    SharedExecutionStreamGraph pGraph;

    typedef std::map<std::string,ExecutionStreamParts> StreamMap;
    typedef StreamMap::const_iterator StreamMapConstIter;

    /**
     * Streams to be linked and prepared, mapped by name
     */
    StreamMap allStreamParts;

    /**
     * Allocate a stream based on stream definition, add the stream to a 
     * graph and remember how to prepare the stream. Interposes tracing
     * stream, if applicable, as xo.<i>stream</i>.
     */
    void buildStream(
        ProxyExecutionStreamDef &);

    /**
     * Add dataflows between a stream and its inputs. Interposes
     * provisioning adapters as required.
     *
     * @param streamDef corresponding Java stream definition being converted
     */
    void buildStreamInputs(
        ProxyExecutionStreamDef &streamDef);

    /**
     * If tracing is supported for stream, appends a tracing stream. Also 
     * sets up dataflows and inserts a provisioning adapter if necessary.
     *
     * @param name name of stream to add tracing for
     */
    void addTracingStream(
        const std::string &name);

    /**
     * Get the name of the trace source to use based on a stream name.
     *
     * @param streamName name of stream being traced
     *
     * @return corresponding trace source name
     */
    std::string getTraceName(
        const std::string &streamName);

    /**
     * Ensures that a producer is capable of the specified buffer 
     * provisioning requirements. If producer is not capable, an adapter
     * stream is appended to supply the required buffer provisioning. 
     *
     * <p>The "producer" may be a single stream or may be a chain of 
     * streams. In either case, the adapter is appended to the end of the 
     * group under the name of the original stream. It is named according
     * to the last stream: <code><i>lastName</i>.provisioner</code>
     *
     * @param name name of original stream
     *
     * @param requiredDataFlow buffer provisioning requirement
     */
    void addAdapterFor(
        const std::string &name,
        ExecutionStream::BufferProvision requiredDataFlow);

    /**
     * Registers a newly created, unprepared stream with the builder and 
     * adds it to the graph.
     */
    void saveStreamParts(
        ExecutionStreamParts &);
    
    /**
     * Lookup a registered stream. The stream *must* be registered.
     */
    ExecutionStreamParts getStreamParts(
        const std::string &name);
    
    /**
     * Append an add-on stream, such as a tracing stream or adapter stream,
     * which masks the output of the original stream.
     */
    void interposeStream(
        const std::string &name,
        ExecutionStreamId interposedId);

    /**
     * Add dataflow to graph, from one stream's output, after add-ons, to
     * another stream.
     *
     * @param source name of source stream
     *
     * @param target name of target stream
     */
    void addDataflow(
        const std::string &source,
        const std::string &target);
    
public:
    /**
     * Create a new ExecutionStreamBuilder.
     *
     * @param pDatabase database to be accessed by streams
     *
     * @param pTableWriterFactory factory for creating TableWriters
     *
     * @param pGraph graph to be built up with stream implementations
     */
    explicit ExecutionStreamBuilder(
        SharedDatabase pDatabase,
        ExecutionStreamFactory &streamFactory,
        SharedExecutionStreamGraph pGraph);

    /**
     * Main builder entry point.
     *
     * @param streamDef Java representation for collection of stream
     * definitions
     */
    void buildStreamGraph(ProxyCmdPrepareExecutionStreamGraph &cmd);
};

FENNEL_END_NAMESPACE

#endif

// End ExecutionStreamBuilder.h
