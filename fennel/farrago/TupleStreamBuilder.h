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

#ifndef Fennel_TupleStreamBuilder_Included
#define Fennel_TupleStreamBuilder_Included

#include "fennel/farrago/Fem.h"
#include "fennel/common/ClosableObject.h"
#include "fennel/xo/TupleStream.h"
#include "fennel/farrago/CmdInterpreter.h"
#include "fennel/farrago/ExecutionStreamFactory.h"

#include <boost/utility.hpp>

FENNEL_BEGIN_NAMESPACE

class BTreeStreamParams;
class BTreeReadTupleStreamParams;
class BTreeScanParams;
class BTreeSearchParams;
class TableIndexWriterParams;
class TableWriterStreamParams;
class JavaTupleStreamParams;
class TupleDescriptor;
class TupleProjection;
class StoredTypeDescriptorFactory;

/**
 * TupleStreamBuilder builds a TupleStreamGraph from its Java representation.
 * It implements FemVisitor by converting each subclass of ProxyTupleStreamDef
 * into the appropriate TupleStream implementation.
 */
class TupleStreamBuilder : public boost::noncopyable, public FemVisitor
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
    SharedTupleStreamGraph pGraph;

    /**
     * Private graph, for sorting streams
     */
    SharedExecutionStreamGraph pSortingGraph;

    typedef std::map<std::string,ExecutionStreamFactors> StreamMap;
    typedef StreamMap::const_iterator StreamMapConstIter;

    /**
     * Streams to be linked and prepared, mapped by name
     */
    StreamMap streams;

    /**
     * Allocate a stream based on stream definition, add the stream to a 
     * graph and remember how to prepare the stream. Interposes tracing
     * stream, if applicable, as xo.<i>stream</i>.
     */
    void visitStream(
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
     * Monitor a stream's output by appending a tracing stream. 
     *
     * @param name name of stream to add tracing for
     *
     * @param traceName name of tracing stream
     */
    void addTracingStream(
        std::string &name,
        std::string &traceName);

    /**
     * Modify a stream's dataflow, as required, to meet the provisioning
     * requirements. An adapter stream may be added to the graph as 
     * <i>base</i>.provisioner
     *
     * @param name name of stream
     *
     * @param base base name to use for adapter
     *
     * @param requiredDataFlow provisioning requirement
     */
    void addAdapterFor(
        std::string &name,
        std::string &base,
        TupleStream::BufferProvision requiredDataFlow);

    /**
     * Call addAdapter using stream name as base name 
     */
    void addAdapterFor(
        std::string &name,
        TupleStream::BufferProvision requiredDataFlow);


    /**
     * Register a newly created, unprepared stream with the builder and add
     * it to the graph. 
     */
    void registerStream(
        ExecutionStreamFactors &);
    
    /**
     * Lookup a registered stream. The stream *must* be registered.
     */
    ExecutionStreamFactors lookupStream(
        std::string &name);
    
    /**
     * Append an add-on stream, such as a tracing stream or adapter stream,
     * which masks the output of the original stream.
     */
    void interposeStream(
        std::string &name,
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
        std::string &source,
        std::string &target);
    
public:
    /**
     * Create a new TupleStreamBuilder.
     *
     * @param pDatabase database to be accessed by streams
     *
     * @param pTableWriterFactory factory for creating TableWriters
     *
     * @param pGraph graph to be built up with stream implementations
     */
    explicit TupleStreamBuilder(
        SharedDatabase pDatabase,
        ExecutionStreamFactory &streamFactory,
        SharedTupleStreamGraph pGraph);

    /**
     * Main builder entry point.
     *
     * @param streamDef Java representation for collection of stream
     * definitions; the corresponding TupleStream objects will be
     * retrievable by name from the graph passed to the constructor
     */
    void buildStreamGraph(ProxyCmdPrepareExecutionStreamGraph &cmd);
};

FENNEL_END_NAMESPACE

#endif

// End TupleStreamBuilder.h
