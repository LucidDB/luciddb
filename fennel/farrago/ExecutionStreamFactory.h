/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2003-2005 Disruptive Tech
// Copyright (C) 2005-2005 Red Square, Inc.
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

#ifndef Fennel_ExecutionStreamFactory_Included
#define Fennel_ExecutionStreamFactory_Included

#include "fennel/farrago/Fem.h"
#include "fennel/common/ClosableObject.h"
#include "fennel/xo/ExecutionStream.h"
#include "fennel/farrago/CmdInterpreter.h"

#include <boost/bind.hpp>
#include <boost/function.hpp>
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

// DEPRECATED
    
/**
 * ExecutionStreamParts represents components required to construct and
 * prepare an ExecutionStream. It is designated as the "owner" of stream
 * parameters, because it needs to hold them until the stream is prepared. 
 * Accordingly, it protects the parameters with a shared_ptr. However it is
 * the caller's responsibility to ensure the stream itself is protected.
 *
 * <p>
 *
 * Example usage:
 *
 * <code>
 * 
 * ExecutionStreamParts parts;
 * StreamSubClass *pStream;
 * StreamSubClassParams *pParams;
 *
 * parts.setParts(pStream,pParams);
 * SharedExecutionStream pSharedStream(parts.getStream());
 * parts.prepareStream();
 *
 * </code>
 */
class ExecutionStreamParts
{
    /**
     * Unprotected pointer to execution stream
     */
    ExecutionStream *pStream;

    /**
     * Protected pointer to stream parameters
     */
    SharedExecutionStreamParams sharedParams;

    /**
     * Bound function for preparing stream
     */
    boost::function<void ()> function;

    /**
     * Specifies type of tracing compatible for the stream
     */
    int traceType;
    
public:
    /**
     * Specifies that tracing is supported for stream
     */
    static const int TRACE_TYPE_NONE = 0;

    /**
     * Specifies that the stream supports tuple stream tracing
     */
    static const int TRACE_TYPE_TUPLE_STREAM = TRACE_TYPE_NONE+1;

    /**
     * Saves reference to stream and parameters for preparing it.
     * Supported trace type defaults to TRACE_TYPE_TUPLE_STREAM.
     *
     * @param pStream newly allocated ExecutionStream implementation
     *
     * @param pParams newly allocated ExecutionStreamParameters implementation
     */
    template<class S, class P>
    void setParts(S *pStreamInit, P *pParams) {
        pStream = pStreamInit;
        sharedParams = SharedExecutionStreamParams(pParams);
        function = boost::bind(&S::prepare, pStreamInit, *pParams);
        traceType = TRACE_TYPE_TUPLE_STREAM;
    }

    /**
     * Sets the type of tracing supported for this stream
     */
    void setTraceType(int traceType) { this->traceType = traceType; }

    /**
     * Gets the original, unprotected stream
     */
    ExecutionStream *getStream() const { return pStream; }
    
    /**
     * Gets parameters.
     */
    ExecutionStreamParams &getParams() const { return *sharedParams; }

    /**
     * Gets the type of tracing supported for this stream
     */
    int getTraceType() { return traceType; }

    /**
     * Prepares stream. Subclasses overload the prepare function with
     * specialized parameters. So calling the correct version of prepare
     * requires both the correct stream type and the correct parameter type.
     */
    void prepareStream() { function(); }
};

class ExecutionStreamSubFactory;
typedef boost::shared_ptr<ExecutionStreamSubFactory>
    SharedExecutionStreamSubFactory;

/**
 * ExecutionStreamFactory builds an ExecutionStreamParts from the  
 * Java representation of a stream definition. The parts are later 
 * used to connect and prepare an ExecutionStream.
 *
 * NOTE: this class is not thread-safe
 */
class ExecutionStreamFactory
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
    SharedTableWriterFactory pTableWriterFactory;

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
    ExecutionStreamParts parts;

    /**
     * Subfactories for extending factory behavior.
     */
    std::vector<SharedExecutionStreamSubFactory> subFactories;

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

    /**
     * Decides whether cache quotas should actually be enforced.  By default
     * they are only for a DEBUG build, but this can be overridden by setting
     * trace level net.sf.fennel.xo.quota to FINE.
     */
    bool shouldEnforceCacheQuotas();
    
public:
    explicit ExecutionStreamFactory(
        SharedDatabase pDatabase,
        SharedTableWriterFactory pTableWriterFactory,
        CmdInterpreter::StreamGraphHandle *pStreamGraphHandle);

    virtual ~ExecutionStreamFactory()
        {}

    void setScratchAccessor(SegmentAccessor &scratchAccessor);

    void addSubFactory(SharedExecutionStreamSubFactory pSubFactory);

    SharedDatabase getDatabase();

    /**
     * Reads the Java representation of an ExecutionStream.
     */
    virtual const ExecutionStreamParts &visitStream(
        ProxyExecutionStreamDef &);

    virtual const ExecutionStreamParts &newTracingStream(
        std::string &name,
        ExecutionStreamParts &parts);

    const ExecutionStreamParts &newConsumerToProducerProvisionAdapter(
        std::string &name,
        ExecutionStreamParams &params);
    
    const ExecutionStreamParts &newProducerToConsumerProvisionAdapter(
        std::string &name,
        ExecutionStreamParams &params);

    // helpers for subfactories
    
    void createQuotaAccessors(ExecutionStreamParams &params);
    
    void readExecutionStreamParams(
        ExecutionStreamParams &,
        ProxyExecutionStreamDef &);
    
    void readTupleStreamParams(
        TupleStreamParams &,
        ProxyTupleStreamDef &);
};

class ExecutionStreamSubFactory : public boost::noncopyable
{
public:
    virtual ~ExecutionStreamSubFactory()
    {
    }
    
    /**
     * Reads the Java representation of an ExecutionStream.
     *
     * @param factory controlling factory
     *
     * @param streamDef stream definition to be read
     *
     * @param parts receives the partially initialized stream
     *
     * @return whether stream was created
     */
    virtual bool createStream(
        ExecutionStreamFactory &factory,
        ProxyExecutionStreamDef &streamDef,
        ExecutionStreamParts &parts) = 0;
};

FENNEL_END_NAMESPACE

#endif

// End ExecutionStreamFactory.h
