/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005-2009 The Eigenbase Project
// Copyright (C) 2005-2009 SQLstream, Inc.
// Copyright (C) 2005-2009 LucidEra, Inc.
// Portions Copyright (C) 2004-2009 John V. Sichi
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

#ifndef Fennel_ExecStream_Included
#define Fennel_ExecStream_Included

#include "fennel/exec/ExecStreamDefs.h"
#include "fennel/exec/ErrorSource.h"
#include "fennel/common/ClosableObject.h"
#include "fennel/common/TraceSource.h"
#include "fennel/tuple/TupleDescriptor.h"
#include "fennel/tuple/TupleFormat.h"

#include <boost/utility.hpp>
#include <vector>

FENNEL_BEGIN_NAMESPACE

/**
 * ExecStream defines an abstract base for all execution objects which
 * process streams of data.  For more information, see ExecStreamDesign.
 *
 * @author John V. Sichi
 * @version $Id$
 */
class FENNEL_EXEC_EXPORT ExecStream
    : public boost::noncopyable,
        virtual public ClosableObject,
        virtual public TraceSource,
        virtual public ErrorSource
{
    friend class ExecStreamGraphImpl;
protected:

    /**
     * Whether this stream is currently open.  Note that this is not quite the
     * opposite of the inherited ClosableObject.needsClose, since a stream
     * needs to be closed before destruction if it has been prepared but never
     * opened.
     */
    bool isOpen;

    /**
     * Dataflow graph containing this stream.  Note that we don't use
     * a weak_ptr for this because it needs to be accessed frequently during
     * execution, and the extra locking overhead would be frivolous.
     */
    ExecStreamGraph *pGraph;

    /**
     * Identifier for this stream; local to its containing graph.
     */
    ExecStreamId id;

    /**
     * Name of stream, as known by optimizer
     */
    std::string name;

    /**
     * The dynamic parameter manager available to this stream. (Obtained at
     * prepare() time. Keep a shared pointer in case the stream is reassigned to
     * another graph for execution; cf ExecStreamGraph::mergeFrom())
     */
    SharedDynamicParamManager pDynamicParamManager;


    /**
     * The transaction embracing the stream. Obtained at open() time; but not
     * released at close() time, to allow TableWriters to replay a txn. Keep a
     * shared pointer in case the stream is reassigned to another graph for
     * execution; cf ExecStreamGraph::mergeFrom())
     */
    SharedLogicalTxn pTxn;


    /**
     * Resource quantities currently allocated to this stream.
     */
    ExecStreamResourceQuantity resourceAllocation;

    /**
     * CacheAccessor used for quota tracking.
     */
    SharedCacheAccessor pQuotaAccessor;

    /**
     * CacheAccessor used for scratch page quota tracking.
     */
    SharedCacheAccessor pScratchQuotaAccessor;

    /**
     * Constructor.  Note that derived class constructors must never take any
     * parameters in order to support deserialization.  See notes on method
     * prepare() for more information.
     */
    explicit ExecStream();

    // interface methods below are protected because they should only be called
    // indirectly via ExecStreamGraph interface

    /**
     * Implements ClosableObject.  ExecStream implementations may
     * override this to release any resources acquired while open.
     */
    virtual void closeImpl();

public:
    /**
     * @return true if the stream can be closed early
     */
    virtual bool canEarlyClose();

    /**
     * @return reference to containing graph
     */
    inline ExecStreamGraph &getGraph() const;

    /**
     * @return the identifier for this stream within containing graph
     */
    inline ExecStreamId getStreamId() const;

    /**
     * Initializes the buffer accessors for inputs to this stream.  This
     * method is only ever called once, before prepare.
     *
     * @param inAccessors buffer accessors ordered by input stream
     */
    virtual void setInputBufAccessors(
        std::vector<SharedExecStreamBufAccessor> const &inAccessors) = 0;

    /**
     * Initializes the buffer accessors for outputs from this stream.  This
     * method is only ever called once, before prepare.
     *
     * @param outAccessors buffer accessors ordered by output stream
     */
    virtual void setOutputBufAccessors(
        std::vector<SharedExecStreamBufAccessor> const &outAccessors) = 0;

    /**
     * Prepares this stream for execution.  A precondition is that input
     * streams must already be defined and prepared.  As an effect of this
     * call, the tuple shape should be defined for all output buffers and
     * remain unchanged for the lifetime of the stream.  This method is only
     * ever called once, before the first open.  Although this method is
     * virtual, derived classes may choose to define an overloaded version
     * instead with a specialized covariant parameter class.
     *
     * @param params instance of stream parameterization class which should be
     * used to prepare this stream
     */
    virtual void prepare(ExecStreamParams const &params);

    /**
     * Determines resource requirements for this stream.  Default implementation
     * declares zero resource requirements.
     *
     * @param minQuantity receives the minimum resource quantity
     * needed by this stream in order to execute
     *
     * @param optQuantity receives the resource quantity
     * needed by this stream in order to execute optimally
     *
     * @param optType Receives the value indicating the accuracy of the
     * optQuantity parameter.  This parameter is optional and defaults to
     * EXEC_RESOURCE_ACCURATE if omitted.  If the optimum setting is an
     * estimate or no value can be specified (e.g., due to lack of statistics),
     * then this parameter needs to be used to indicate a non-accurate
     * optimum resource setting.
     */
    virtual void getResourceRequirements(
        ExecStreamResourceQuantity &minQuantity,
        ExecStreamResourceQuantity &optQuantity,
        ExecStreamResourceSettingType &optType);

    virtual void getResourceRequirements(
        ExecStreamResourceQuantity &minQuantity,
        ExecStreamResourceQuantity &optQuantity);

    /**
     * Sets current resource allocation for this stream.  If called while the
     * stream is open, this indicates a request for the stream to dynamically
     * adjust its memory usage.  If the stream is incapable of honoring
     * the request, it should update quantity with the actual amounts still
     * in use.
     *
     * @param quantity allocated resource quantity
     */
    virtual void setResourceAllocation(
        ExecStreamResourceQuantity &quantity);

    /**
     * Opens this stream, acquiring any resources needed in order to be able to
     * fetch data.  A precondition is that input streams
     * must already be opened.  A stream can be closed and reopened.
     *
     * @param restart if true, the stream must be already open, and should
     * reset itself to start from the beginning of its result set
     */
    virtual void open(bool restart);

    /**
     * Sets unique name of this stream.
     */
    virtual void setName(std::string const &);

    /**
     * @return the name of this stream, as known by the optimizer
     */
    virtual std::string const &getName() const;

    /**
     * Executes this stream.
     *
     * @param quantum governs the maximum amount of execution to perform
     *
     * @return code indicating reason execution ceased
     */
    virtual ExecStreamResult execute(ExecStreamQuantum const &quantum) = 0;

    /**
     * Queries whether this stream's implementation may block when execute()
     * is called.  For accurate scheduling, non-blocking implementations
     * are preferred; the scheduler must be aware of the potential for
     * blocking so that it can allocate extra threads accordingly.
     *
     * @return whether stream may block; default is false
     */
    virtual bool mayBlock() const;

    /**
     * Checks whether there is an abort request for this stream's
     * scheduler.  Normally, streams don't need to check this,
     * since the scheduler services abort requests in between
     * quanta.  However, streams which enter long-running loops
     * need to check for themselves.  If an abort is scheduled,
     * this method will throw an AbortExcn automatically.
     */
    virtual void checkAbort() const;

    /**
     * Queries the BufferProvision which this stream is capable of when
     * producing tuples.
     *
     * @return supported model; default is BUFPROV_NONE
     */
    virtual ExecStreamBufProvision getOutputBufProvision() const;

    /**
     * Queries the BufferProvision to which this stream needs its
     * output to be converted, if any.
     *
     * @return required conversion; default is BUFPROV_NONE
     */
    virtual ExecStreamBufProvision getOutputBufConversion() const;

    /**
     * Queries the BufferProvision which this stream requires of its inputs when
     * consuming their tuples.
     *
     * @return required model; default is BUFPROV_NONE
     */
    virtual ExecStreamBufProvision getInputBufProvision() const;
};

inline ExecStreamId ExecStream::getStreamId() const
{
    return id;
}

inline ExecStreamGraph &ExecStream::getGraph() const
{
    assert(pGraph);
    return *pGraph;
}

FENNEL_END_NAMESPACE

#endif

// End ExecStream.h
