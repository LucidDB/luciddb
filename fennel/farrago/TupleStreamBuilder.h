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
#include "fennel/common/Distinctness.h"
#include "fennel/xo/TupleStream.h"
#include "fennel/farrago/CmdInterpreter.h"

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
     * Stream returned by current invocation visit method.  Have to do it this
     * way since visit returns void; means we have to be careful about
     * recursion order.
     */
    SharedTupleStream pChildStream;

    /**
     * Copy of params used for building last child.  Kinda ugly.
     */
    TupleStreamParams childParams;

    void visitStream(
        ProxyExecutionStreamDef &,
        TupleStream::BufferProvision requiredDataflow);

    // REVIEW:  some of this is generic logic which should be moved
    // down into an abstract builder in the xo library
    void addTracingStream(std::string);
    void addAdapterFor(TupleStream::BufferProvision);
    void addAdapter(TupleStream &adapter);

    /**
     * Add a new stream to the graph, recursively build its inputs, and set up
     * the corresponding dataflow edges.
     *
     * @param pStream newly allocated TupleStream implementation
     *
     * @param streamDef corresponding Java stream definition being converted
     */
    void buildStreamInputs(
        ExecutionStream *pStream,
        ProxyExecutionStreamDef &streamDef);

public:
    /**
     * Create a new TupleStreamBuilder.
     *
     * @param pDatabase database to be accessed by streams
     *
     * @param pTableWriterFactory factory for creating TableWriters
     *
     * @param pGraph graph to be built up with stream implementations
     *
     * @param pStreamHandle handle to the stream being built
     */
    explicit TupleStreamBuilder(
        SharedDatabase pDatabase,
        ExecutionStreamFactory &streamFactory,
        SharedTupleStreamGraph pGraph);

    /**
     * Main builder entry point.
     *
     * @param streamDef Java representation for top of tree of stream
     * definitions; the corresponding TupleStream will end up as the sink
     * stream in the graph passed to the constructor
     */
    void buildStreamGraph(ProxyExecutionStreamDef &streamDef);
};

FENNEL_END_NAMESPACE

#endif

// End TupleStreamBuilder.h
