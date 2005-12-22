/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2003-2005 Disruptive Tech
// Copyright (C) 2005-2005 LucidEra, Inc.
// Portions Copyright (C) 1999-2005 John V. Sichi
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

class BTreeExecStreamParams;
class BTreeReadExecStreamParams;
class BTreeScanExecStreamParams;
class BTreeSearcExecStreamhParams;
class FtrsTableIndexWriterParams;
class FtrsTableWriterExecStreamParams;
class JavaExecStreamParams;
class TupleDescriptor;
class TupleProjection;
class StoredTypeDescriptorFactory;
class SingleOutputExecStreamParams;

/**
 * ExecStreamFactory builds an ExecStreamEmbryo from the  
 * Java representation of a stream definition.
 *
 * NOTE: this class is not thread-safe
 */
class ExecStreamFactory
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
    virtual void visit(ProxyIndexScanDef &);
    virtual void visit(ProxyIndexSearchDef &);
    virtual void visit(ProxyJavaTupleStreamDef &);
    virtual void visit(ProxyJavaSinkStreamDef &);
    virtual void visit(ProxyTableInserterDef &);
    virtual void visit(ProxyTableDeleterDef &);
    virtual void visit(ProxyTableUpdaterDef &);
    virtual void visit(ProxySortingStreamDef &);
    virtual void visit(ProxyBufferingTupleStreamDef &);
    virtual void visit(ProxyIndexLoaderDef &);
    virtual void visit(ProxyCartesianProductStreamDef &);
    virtual void visit(ProxyMergeStreamDef &);
    virtual void visit(ProxyMockTupleStreamDef &);
    virtual void visit(ProxyAggStreamDef &);
    virtual void visit(ProxySplitterStreamDef &);
    virtual void visit(ProxyBarrierStreamDef &);

    // helpers for above visitors

    void readBTreeReadStreamParams(
        BTreeReadExecStreamParams &,
        ProxyIndexScanDef &);
    
    void readIndexWriterParams(
        FtrsTableIndexWriterParams &,
        ProxyIndexWriterDef &);

    void readTableWriterStreamParams(
        FtrsTableWriterExecStreamParams &,
        ProxyTableWriterDef &);

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
     * Decides whether cache quotas should actually be enforced.  By default
     * they are only for a DEBUG build, but this can be overridden by setting
     * trace level net.sf.fennel.xo.quota to FINE.
     */
    bool shouldEnforceCacheQuotas();
    
    /**
     * Reads the Java representation of an ExecStream.
     */
    virtual ExecStreamEmbryo const &visitStream(
        ProxyExecutionStreamDef &);

    // helpers for subfactories
    
    /** makes a TupleDescriptor from its proxy definition */
    void readTupleDescriptor(TupleDescriptor& desc, const SharedProxyTupleDescriptor def);

    void createQuotaAccessors(ExecStreamParams &params);
    
    void readExecStreamParams(
        ExecStreamParams &,
        ProxyExecutionStreamDef &);
    
    void readTupleStreamParams(
        SingleOutputExecStreamParams &,
        ProxyTupleStreamDef &);

    void readBTreeStreamParams(
        BTreeExecStreamParams &,
        ProxyIndexAccessorDef &);    
};

class ExecStreamSubFactory : public boost::noncopyable
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
