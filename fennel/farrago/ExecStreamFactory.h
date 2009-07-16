/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005-2009 The Eigenbase Project
// Copyright (C) 2003-2009 SQLstream, Inc.
// Copyright (C) 2005-2009 LucidEra, Inc.
// Portions Copyright (C) 1999-2009 John V. Sichi
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

#ifndef Fennel_ExecStreamFactory_Included
#define Fennel_ExecStreamFactory_Included

#include "fennel/farrago/Fem.h"
#include "fennel/common/ClosableObject.h"
#include "fennel/exec/ExecStream.h"
#include "fennel/exec/ExecStreamEmbryo.h"
#include "fennel/farrago/CmdInterpreter.h"

#include <boost/utility.hpp>

FENNEL_BEGIN_NAMESPACE

class BarrierExecStreamParams;
class BTreeExecStreamParams;
class BTreePrefetchSearchExecStreamParams;
class BTreeReadExecStreamParams;
class BTreeScanExecStreamParams;
class BTreeSearchExecStreamParams;
class FtrsTableIndexWriterParams;
class FtrsTableWriterExecStreamParams;
class JavaExecStreamParams;
class TupleDescriptor;
class TupleProjection;
class StoredTypeDescriptorFactory;
class SingleOutputExecStreamParams;
class SortedAggExecStreamParams;

/**
 * ExecStreamFactory builds an ExecStreamEmbryo from the
 * Java representation of a stream definition.
 *
 * NOTE: this class is not thread-safe
 */
class FENNEL_FARRAGO_EXPORT ExecStreamFactory
    : public boost::noncopyable, virtual public FemVisitor
{
protected:
    /**
     * Database to be accessed by stream.
     */
    SharedDatabase pDatabase;

    /**
     * Factory for creating TableWriters.
     */
    SharedFtrsTableWriterFactory pTableWriterFactory;

    /**
     * Handle to the stream graph being built.
     */
    CmdInterpreter::StreamGraphHandle *pStreamGraphHandle;

    /**
     * Accessor for ScratchSegment available to all streams.
     */
    SegmentAccessor scratchAccessor;

    /**
     * Value set by visit functions
     */
    ExecStreamEmbryo embryo;

    /**
     * Embryo for graph being built.
     */
    ExecStreamGraphEmbryo *pGraphEmbryo;

    /**
     * Subfactories for extending factory behavior.
     */
    std::vector<SharedExecStreamSubFactory> subFactories;

    /**
     * Dispatches to the correct visitor method; only called when
     * no subfactory wants to handle the stream.
     */
    virtual void invokeVisit(
        ProxyExecutionStreamDef &);

    // Per-stream overrides for FemVisitor; add new stream types here
    virtual void visit(ProxyBarrierStreamDef &);
    virtual void visit(ProxyBufferingTupleStreamDef &);
    virtual void visit(ProxyBufferReaderStreamDef &);
    virtual void visit(ProxyBufferWriterStreamDef &);
    virtual void visit(ProxyCartesianProductStreamDef &);
    virtual void visit(ProxyIndexLoaderDef &);
    virtual void visit(ProxyIndexScanDef &);
    virtual void visit(ProxyIndexSearchDef &);
    virtual void visit(ProxyJavaSinkStreamDef &);
    virtual void visit(ProxyJavaTransformStreamDef &);
    virtual void visit(ProxyMergeStreamDef &);
    virtual void visit(ProxyMockTupleStreamDef &);
    virtual void visit(ProxyTableDeleterDef &);
    virtual void visit(ProxyTableInserterDef &);
    virtual void visit(ProxyTableUpdaterDef &);
    virtual void visit(ProxySortedAggStreamDef &);
    virtual void visit(ProxySortingStreamDef &);
    virtual void visit(ProxySplitterStreamDef &);
    virtual void visit(ProxyValuesStreamDef &);
    virtual void visit(ProxyReshapeStreamDef &);
    virtual void visit(ProxyNestedLoopJoinStreamDef &);
    virtual void visit(ProxyBernoulliSamplingStreamDef &);
    virtual void visit(ProxyCalcTupleStreamDef &streamDef);
    virtual void visit(ProxyCorrelationJoinStreamDef &streamDef);
    virtual void visit(ProxyCollectTupleStreamDef &streamDef);
    virtual void visit(ProxyUncollectTupleStreamDef &streamDef);
    virtual void visit(ProxyFlatFileTupleStreamDef &streamDef);
    virtual void visit(ProxyLhxJoinStreamDef &streamDef);
    virtual void visit(ProxyLhxAggStreamDef &streamDef);

    void implementSortWithBTree(ProxySortingStreamDef &streamDef);

public:
    explicit ExecStreamFactory(
        SharedDatabase pDatabase,
        SharedFtrsTableWriterFactory pTableWriterFactory,
        CmdInterpreter::StreamGraphHandle *pStreamGraphHandle);

    void setGraphEmbryo(
        ExecStreamGraphEmbryo &graphEmbryo);

    void setScratchAccessor(SegmentAccessor &scratchAccessor);

    void addSubFactory(SharedExecStreamSubFactory pSubFactory);

    SharedDatabase getDatabase();

    /**
     * Reads the Java representation of an ExecStream.
     */
    virtual ExecStreamEmbryo const &visitStream(
        ProxyExecutionStreamDef &);

    // helpers for subfactories

    char readCharParam(const std::string &val);

    /** makes a TupleDescriptor from its proxy definition */
    void readTupleDescriptor(
        TupleDescriptor& desc, const SharedProxyTupleDescriptor def);

    /**
     * Creates a private scratch segment for an ExecStream.  This must be used
     * if the ExecStream provides early release of its scratch buffers
     * (otherwise it will release scratch buffers for the entire graph, leading
     * to quite bad things).  This method must be called AFTER one of the
     * standard readXyzStreamParams methods below, since those methods set up
     * the default global scratch segment.
     *
     * @param params params in which to set the private scratch segment
     */
    void createPrivateScratchSegment(ExecStreamParams &params);

    void createQuotaAccessors(ExecStreamParams &params);

    void readExecStreamParams(
        ExecStreamParams &,
        ProxyExecutionStreamDef &);

    void readTupleStreamParams(
        SingleOutputExecStreamParams &,
        ProxyTupleStreamDef &);

    void initBTreePrefetchSearchParams(
        BTreePrefetchSearchExecStreamParams &,
        ProxyIndexSearchDef &);

    void readBTreeStreamParams(
        BTreeExecStreamParams &,
        ProxyIndexAccessorDef &);

    void readBTreeParams(
        BTreeParams &,
        ProxyIndexAccessorDef &);

    void readBTreeSearchStreamParams(
        BTreeSearchExecStreamParams &,
        ProxyIndexSearchDef &);

    void readAggStreamParams(
        SortedAggExecStreamParams &,
        ProxyAggStreamDef &);

    void readBTreeReadStreamParams(
        BTreeReadExecStreamParams &,
        ProxyIndexScanDef &);

    void readIndexWriterParams(
        FtrsTableIndexWriterParams &,
        ProxyIndexWriterDef &);

    void readTableWriterStreamParams(
        FtrsTableWriterExecStreamParams &,
        ProxyTableWriterDef &);

    void readBarrierDynamicParams(
        BarrierExecStreamParams &,
        ProxyBarrierStreamDef &);

    void readColumnList(
        ProxyFlatFileTupleStreamDef &streamDef,
        std::vector<std::string> &names);

    DynamicParamId readDynamicParamId(const int val);
};

class FENNEL_FARRAGO_EXPORT ExecStreamSubFactory
    : public boost::noncopyable
{
public:
    virtual ~ExecStreamSubFactory();

    /**
     * Reads the Java representation of an ExecStream.
     *
     * @param factory controlling factory
     *
     * @param streamDef stream definition to be read
     *
     * @param embryo receives the partially initialized stream
     *
     * @return whether stream was created
     */
    virtual bool createStream(
        ExecStreamFactory &factory,
        ProxyExecutionStreamDef &streamDef,
        ExecStreamEmbryo &embryo) = 0;
};

FENNEL_END_NAMESPACE

#endif

// End ExecStreamFactory.h
