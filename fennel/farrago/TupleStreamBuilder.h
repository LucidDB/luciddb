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
     * Graph of stream nodes being built up.
     */
    SharedTupleStreamGraph pGraph;

    /**
     * Factory for creating TableWriters.
     */
    SharedTableWriterFactory pTableWriterFactory;

    /**
     * Stream returned by current invocation visit method.  Have to do it this
     * way since visit returns void; means we have to be careful about
     * recursion order.
     */
    SharedTupleStream pChildStream;

    /**
     * Accessor for ScratchSegment available to all streams.
     */
    SegmentAccessor scratchAccessor;

    /**
     * Copy of params used for building last child.  Kinda ugly.
     */
    TupleStreamParams childParams;

    /**
     * Handle to the stream being built.
     */
    CmdInterpreter::StreamHandle *pStreamHandle;
    
    void visitStream(
        ProxyExecutionStreamDef &,
        TupleStream::BufferProvision requiredDataflow);

    // REVIEW:  some of this is generic logic which should be moved
    // down into an abstract builder in the xo library
    void addTracingStream(std::string);
    void addAdapterFor(TupleStream::BufferProvision);
    void addAdapter(TupleStream &adapter);

    // Per-stream overrides for FemVisitor; add new stream types here
    virtual void visit(ProxyIndexScanDef &);
    virtual void visit(ProxyIndexSearchDef &);
    virtual void visit(ProxyJavaTupleStreamDef &);
    virtual void visit(ProxyTableInserterDef &);
    virtual void visit(ProxyTableDeleterDef &);
    virtual void visit(ProxyTableUpdaterDef &);
    virtual void visit(ProxySortingStreamDef &);
    virtual void visit(ProxyBufferingTupleStreamDef &);
    virtual void visit(ProxyIndexLoaderDef &);
    virtual void visit(ProxyCartesianProductStreamDef &);

    // helpers for above visitors

    void readTupleStreamParams(
        TupleStreamParams &,
        ProxyTupleStreamDef &);
    
    void readBTreeStreamParams(
        BTreeStreamParams &,
        ProxyIndexAccessorDef &);
    
    void readBTreeReadTupleStreamParams(
        BTreeReadTupleStreamParams &,
        ProxyIndexScanDef &);
    
    void readIndexWriterParams(
        TableIndexWriterParams &,
        ProxyIndexWriterDef &);

    void readTableWriterStreamParams(
        TableWriterStreamParams &,
        ProxyTableWriterDef &);

    static Distinctness parseDistinctness(std::string s);

    /**
     * Add a new stream to the graph, recursively build its inputs, and set up
     * the corresponding dataflow edges.
     *
     * @param pStream newly allocated TupleStream implementation
     *
     * @param streamDef corresponding Java stream definition being converted
     */
    void buildStreamInputs(
        TupleStream *pStream,
        ProxyExecutionStreamDef &streamDef);

    /**
     * Decide whether cache quotas should actually be enforced.  By default
     * they are only for a DEBUG build, but this can be overridden by setting
     * trace level net.sf.farrago.fennel.xo.quota to FINE.
     */
    bool shouldEnforceCacheQuotas();
    
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
        SharedTableWriterFactory pTableWriterFactory,
        SharedTupleStreamGraph pGraph,
        CmdInterpreter::StreamHandle *pStreamHandle);

    /**
     * Main builder entry point.
     *
     * @param streamDef Java representation for top of tree of stream
     * definitions; the corresponding TupleStream will end up as the sink
     * stream in the graph passed to the constructor
     */
    void buildStreamGraph(ProxyExecutionStreamDef &streamDef);

    // Some static utilities which are also used in non-stream contexts.  TODO:
    // move somewhere more appropriate.

    /**
     * Read the Java representation of a TupleDescriptor.
     *
     * @param tupleDesc target TupleDescriptor
     *
     * @param javaTupleDesc Java proxy representation
     *
     * @param typeFactory factory for resolving type ordinals
     */
    static void readTupleDescriptor(
        TupleDescriptor &tupleDesc,
        ProxyTupleDescriptor &javaTupleDesc,
        StoredTypeDescriptorFactory const &typeFactory);

    /**
     * Read the Java representation of a TupleProjection
     *
     * @param tupleProj target TupleProjection
     *
     * @param pJavaTupleProj Java representation
     */
    static void readTupleProjection(
        TupleProjection &tupleProj,
        SharedProxyTupleProjection pJavaTupleProj);
};

FENNEL_END_NAMESPACE

#endif

// End TupleStreamBuilder.h
