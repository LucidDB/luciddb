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
    virtual void visit(ProxyTableInserterDef &);
    virtual void visit(ProxyTableDeleterDef &);
    virtual void visit(ProxyTableUpdaterDef &);
    virtual void visit(ProxySortingStreamDef &);
    virtual void visit(ProxyBufferingTupleStreamDef &);
    virtual void visit(ProxyIndexLoaderDef &);
    virtual void visit(ProxyCartesianProductStreamDef &);
    virtual void visit(ProxyMockTupleStreamDef &);

    // helpers for above visitors

    void readBTreeStreamParams(
        BTreeExecStreamParams &,
        ProxyIndexAccessorDef &);
    
    void readBTreeReadStreamParams(
        BTreeReadExecStreamParams &,
        ProxyIndexScanDef &);
    
    void readIndexWriterParams(
        FtrsTableIndexWriterParams &,
        ProxyIndexWriterDef &);

    void readTableWriterStreamParams(
        FtrsTableWriterExecStreamParams &,
        ProxyTableWriterDef &);

    void copyAdapterParams(
        ExecStreamParams &adapterParams,
        ExecStreamParams const &params);

    /**
     * Decides whether cache quotas should actually be enforced.  By default
     * they are only for a DEBUG build, but this can be overridden by setting
     * trace level net.sf.fennel.xo.quota to FINE.
     */
    bool shouldEnforceCacheQuotas();
    
public:
    explicit ExecStreamFactory(
        SharedDatabase pDatabase,
        SharedFtrsTableWriterFactory pTableWriterFactory,
        CmdInterpreter::StreamGraphHandle *pStreamGraphHandle);

    void setScratchAccessor(SegmentAccessor &scratchAccessor);

    void addSubFactory(SharedExecStreamSubFactory pSubFactory);

    SharedDatabase getDatabase();

    // override JniProxyVisitor
    virtual void *getLeafPtr();
    const char *getLeafTypeName();

    /**
     * Reads the Java representation of an ExecStream.
     */
    virtual ExecStreamEmbryo const &visitStream(
        ProxyExecutionStreamDef &);

    const ExecStreamEmbryo &newConsumerToProducerProvisionAdapter(
        std::string &name,
        ExecStreamParams const &params);
    
    const ExecStreamEmbryo &newProducerToConsumerProvisionAdapter(
        std::string &name,
        ExecStreamParams const &params);

    // helpers for subfactories
    
    void createQuotaAccessors(ExecStreamParams &params);
    
    void readExecStreamParams(
        ExecStreamParams &,
        ProxyExecutionStreamDef &);
    
    void readTupleStreamParams(
        SingleOutputExecStreamParams &,
        ProxyTupleStreamDef &);
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
