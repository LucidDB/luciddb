/*
// Licensed to DynamoBI Corporation (DynamoBI) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  DynamoBI licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at

//   http://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
*/

#ifndef Fennel_ExecStreamBuilder_Included
#define Fennel_ExecStreamBuilder_Included

#include "fennel/common/ClosableObject.h"
#include "fennel/farrago/ExecStreamFactory.h"
#include "fennel/farrago/Fem.h"
#include "fennel/exec/ExecStream.h"
#include "fennel/exec/ExecStreamGraphEmbryo.h"

#include <boost/utility.hpp>

FENNEL_BEGIN_NAMESPACE

/**
 * ExecStreamBuilder builds a prepared ExecStreamGraph from its
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
 * the graph and its streams.
 *
 * <p><b>Tracing.</b>
 *   All streams are assigned a trace
 *     name of: <code>xo.<i>streamName</i></code>  Depending on a the
 *     TraceTarget, this typically corresponds to a trace property like
 *     <code>org.eigenbase.fennel.xo.<i>streamName</i></code>
 *
 * <p><b>Buffer Provisioning.</b>
 * Provisioning adapters are special streams interposed between two other
 * streams when the producer's result provisioning does not meet the
 * consumer's input requirements.  They are interposed during the dataflow
 * phase.  They are named: <code><i>producerName</i>.provisioner</code>
 *
 * <p><b>Interposition.</b>
 * When provisioning adapters are appended to a stream, they
 * consume the original stream's output and produce a new output.  To make
 * the appended streams work transparently, the chain of streams is
 * treated as a single unit.  Subsequent access to the stream's output is
 * available through the graph by finding the "last" stream registered
 * under the original stream's name.
 */
class FENNEL_FARRAGO_EXPORT ExecStreamBuilder
    : public boost::noncopyable
{
    /**
     * Embryo for graph being built up.
     */
    ExecStreamGraphEmbryo &graphEmbryo;

    /**
     * Factory for creating ExecStream objects.
     */
    ExecStreamFactory &streamFactory;

    /**
     * Allocates a stream based on stream definition, adds the stream to a
     * graph and records how to prepare the stream.
     */
    void buildStream(
        ProxyExecutionStreamDef &);

    /**
     * Adds dataflows between a stream and its inputs, in the case where
     * the source input has only one output. Interposes provisioning adapters
     * as required.
     *
     * @param streamDef corresponding Java stream definition being converted
     */
    void buildStreamInputs(
        ProxyExecutionStreamDef &streamDef);

    /**
     * @return number of explicit dataflow outputs from a stream
     */
    int getExplicitOutputCount(
        ProxyExecutionStreamDef &streamDef);

    /**
     * Adds dataflows between a stream and its outputs, preserving order in
     * the case where a stream has multiple outputs.
     *
     * @param streamDef corresponding Java stream definition being converted
     */
    void buildStreamOutputs(
        ProxyExecutionStreamDef &streamDef);

public:
    /**
     * Creates a new ExecStreamBuilder.
     *
     * @param graphEmbryo embryo for graph to be built
     *
     * @param streamFactory factory for creating streams
     */
    explicit ExecStreamBuilder(
        ExecStreamGraphEmbryo &graphEmbryo,
        ExecStreamFactory &streamFactory);

    virtual ~ExecStreamBuilder();

    /**
     * Main builder entry point.
     *
     * @param cmd Java representation for command containing collection of
     * stream definitions
     *
     * @param assumeOutputFromSinks if true, sinks in the graph are
     * assumed to be dataflow output nodes; if false, sinks in the
     * graph are not treated specially
     */
    void buildStreamGraph(
        ProxyCmdPrepareExecutionStreamGraph &cmd,
        bool assumeOutputFromSinks);
};

FENNEL_END_NAMESPACE

#endif

// End ExecStreamBuilder.h
