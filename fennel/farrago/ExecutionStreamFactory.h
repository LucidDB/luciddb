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

#ifndef Fennel_ExecutionStreamFactory_Included
#define Fennel_ExecutionStreamFactory_Included

#include "fennel/farrago/Fem.h"
#include "fennel/common/ClosableObject.h"
#include "fennel/common/Distinctness.h"
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

/**
 * ExecutionStreamFactors represents components required to construct and
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
 * ExecutionStreamFactors factors;
 * StreamSubClass *pStream;
 * StreamSubClassParams *pParams;
 *
 * factors.setFactors(pStream,pParams);
 * SharedExecutionStream pSharedStream(factors.getStream());
 * factors.prepareStream();
 *
 * </code>
 */
class ExecutionStreamFactors
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

public:
    /**
     * Saves reference to stream and parameters for preparing it.
     *
     * @param pStream newly allocated ExecutionStream implementation
     *
     * @param pParams newly allocated ExecutionStreamParameters implementation
     */
    template<class S, class P>
    void setFactors(S *pStreamInit, P *pParams) {
        pStream = pStreamInit;
        sharedParams = SharedExecutionStreamParams(pParams);
        function = boost::bind(&S::prepare, pStreamInit, *pParams);
    }

    /**
     * Gets the original, unprotected stream
     */
    ExecutionStream *getStream() const;
    
    /**
     * Gets parameters
     */
    ExecutionStreamParams &getParams() const;

    /**
     * Prepares stream. Subclasses overload the prepare function with
     * specialized parameters. So calling the correct version of prepare
     * requires both the correct stream type and the correct parameter type.
     */
    void prepareStream();
};

/**
 * ExecutionStreamFactory builds an ExecutionStreamFactors from the  
 * Java representation of a stream definition. The factors are later 
 * used to connect and prepare an ExecutionStream.
 *
 * REVIEW: should this library be thread safe?
 */
class ExecutionStreamFactory : public boost::noncopyable, public FemVisitor
{
    /**
     * Database to be accessed by stream.
     */
    SharedDatabase pDatabase;

    /**
     * Factory for creating TableWriters.
     */
    SharedTableWriterFactory pTableWriterFactory;

    /**
     * Handle to the stream being built.
     */
    CmdInterpreter::StreamHandle *pStreamHandle;

    /**
     * Accessor for ScratchSegment available to all streams.
     */
    SegmentAccessor scratchAccessor;

    /**
     * Value set by visit functions
     */
    ExecutionStreamFactors factors;

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
     * Decide whether cache quotas should actually be enforced.  By default
     * they are only for a DEBUG build, but this can be overridden by setting
     * trace level net.sf.farrago.fennel.xo.quota to FINE.
     */
    bool shouldEnforceCacheQuotas();

public:
    ExecutionStreamFactory(
        SharedDatabase pDatabase,
        SharedTableWriterFactory pTableWriterFactory,
        CmdInterpreter::StreamHandle *pStreamHandle);

    void setScratchAccessor(SegmentAccessor &scratchAccessor);

    /**
     * Read the Java representation of an ExecutionStream
     */
    const ExecutionStreamFactors &visitStream(
        ProxyExecutionStreamDef &);

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

// End ExecutionStreamFactory.h
