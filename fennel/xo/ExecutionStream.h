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

#ifndef Fennel_ExecutionStream_Included
#define Fennel_ExecutionStream_Included

#include "fennel/common/TraceSource.h"
#include "fennel/tuple/TupleDescriptor.h"
#include "fennel/tuple/TupleFormat.h"
#include "fennel/xo/ExecutionStreamGraph.h"
#include "fennel/xo/ExecutionStreamResourceQuantity.h"
#include "fennel/segment/SegmentAccessor.h"

FENNEL_BEGIN_NAMESPACE

class ExecutionStreamFactory;

/**
 * Common parameters for instantiating any ExecutionStream.
 */
struct ExecutionStreamParams 
{
    virtual ~ExecutionStreamParams();
    
    /**
     * CacheAccessor to use for any data access.  This will be singular if the
     * stream should not perform data access.
     */
    SharedCacheAccessor pCacheAccessor;

    /**
     * Accessor for segment to use for allocating scratch buffers.  This will
     * be singular if the stream should not use any scratch buffers.
     */
    SegmentAccessor scratchAccessor;

    // TODO jvs 27-May-2005:  use this for sanity checking in all relevant XO's
    /**
     * Descriptor for tuples in this stream's output.
     */
    TupleDescriptor outputTupleDesc;

    /**
     * Whether ExecutionStream should enforce resource quotas.
     */
    bool enforceQuotas;
};

/**
 * ExecutionStream is an abstract base class for all stream execution objects
 * (also known as XO's).  An ExecutionStream produces tuples according to a
 * fixed tuple descriptor.  Dataflow takes place in batches of tuples.
 */
class ExecutionStream
    : virtual public ClosableObject, virtual public TraceSource
{
    friend class ExecutionStreamGraphImpl;
protected:

    /**
     * Whether this stream is currently open.  Note that this is not quite the
     * opposite of the inherited ClosableObject.needsClose, since a stream
     * needs to be closed before destruction if it has been prepared but never
     * opened.
     */
    bool isOpen;

    // REVIEW:  make this a weak_ptr?
    /**
     * Dataflow graph containing this stream.
     */
    ExecutionStreamGraph *pGraph;
    
    /**
     * Identifier for this stream.
     */
    ExecutionStreamId id;

    /**
     * Name of stream, as known by optimizer
     */
    std::string name;

    /**
     * Resource quantities currently allocated to this stream.
     */
    ExecutionStreamResourceQuantity resourceAllocation;

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
    explicit ExecutionStream();

    // interface methods below are protected because they should only be called
    // indirectly via ExecutionStreamGraph interface
    
    /**
     * Implements ClosableObject.  ExecutionStream implementations must
     * override this to release any resources acquired while open.
     */
    virtual void closeImpl();
    
    /**
     * Utility method for accessing a child input stream.
     *
     * @param ordinal 0-based ordinal of child
     *
     * @return reference to child
     */
    virtual SharedExecutionStream getStreamInput(uint ordinal);
        
public:
    virtual ~ExecutionStream();
    
    /**
     * Enumeration of supported dataflow buffer provision
     * capabilities/requirements.
     */
    enum BufferProvision {
        /**
         * The dataflow in question is not defined.
         */
        NO_PROVISION,

        /**
         * The consumer provides a ByteOutputStream into which the producer
         * writes tuples via writeResultToConsumerBuffer.
         */
        CONSUMER_PROVISION,

        /**
         * The producer returns a ByteInputStream from which the consumer reads
         * tuples.
         */
        PRODUCER_PROVISION,

        /**
         * Either model is supported for the dataflow in question.
         */
        PRODUCER_OR_CONSUMER_PROVISION
    };
    
    /**
     * Prepares this stream for execution.  A precondition is that input streams
     * must already be defined and prepared.  As an effect of this call, the
     * output tuple descriptor should be defined and remain unchanged for the
     * lifetime of the stream.  This method is only ever called once, before the
     * first open.  Although this method is virtual, derived classes may choose
     * to define an overloaded version instead with a specialized
     * covariant parameter class.
     *
     * @param params instance of stream parameterization class which should be
     * used to prepare this stream
     */
    virtual void prepare(ExecutionStreamParams const &params);

    /**
     * Determines resource requirements for this stream.  Default implementation
     * declares zero resource requirements.
     *
     * @param minQuantity receives the minimum resource quantity
     * needed by this stream in order to execute
     *
     * @param optQuantity receives the resource quantity
     * needed by this stream in order to execute optimally
     */
    virtual void getResourceRequirements(
        ExecutionStreamResourceQuantity &minQuantity,
        ExecutionStreamResourceQuantity &optQuantity);

    /**
     * Sets current resource allocation for this stream.
     *
     * @param quantity allocated resource quantity
     */
    virtual void setResourceAllocation(
        ExecutionStreamResourceQuantity const &quantity);
        
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
     * @return the identifier for this stream within its graph
     */
    virtual ExecutionStreamId getStreamId() const;
    
    /**
     * Sets unique name of this stream for testing.
     *
     * TODO: factor this out into testing harness
     */
    virtual void setName(std::string const &);
        
    /**
     * @return the name of this stream, as known by the optimizer
     */
    virtual std::string const &getName() const;
        
    /**
     * @return TupleDescriptor for tuples produced by this stream.
     */
    virtual TupleDescriptor const &getOutputDesc() const = 0;

    /**
     * @return the format of the tuples produced by this stream; default is
     * TUPLE_FORMAT_STANDARD
     */
    virtual TupleFormat getOutputFormat() const;
    
    // TODO: throttling interface to limit how many tuples are produced?
    
    /**
     * Obtains a ByteInputStream which can produce result tuple data.  Can
     * only be called while the stream is open.  The caller will fetch data by
     * reading bytes from the result stream, which produces them on demand.
     * The returned byte data consists of contiguously marshalled tuples.
     * Marshalled tuples are aligned and never straddle buffer boundaries, so
     * it is safe for the caller to use getReadPointer/consumeReadPointer.
     *
     *<p>
     *
     * Once the returned stream's end is reached, there are no more results
     * (calling getProducerResultStream again will not change this).  But
     * REVIEW: maybe it would be useful to be able to switch streams and
     * continue?
     *
     *<p>
     *
     * It is illegal to call this method on a stream which returns
     * CONSUMER_PROVISION for getResultBufferProvision().  See
     * ConsumerToProducerProvisionAdapter.
     *
     * @return result stream
     */
    virtual ByteInputStream &getProducerResultStream();
    
    /**
     * Requests that a stream produce results and write as many of them as will
     * fit into the current buffer of a ByteOutputStream.  This can only be
     * called while the stream is open, and its usage is mutually exclusive
     * with usage of getProducerResultStream (but the data representation is
     * the same).
     *
     *<p>
     *
     * It is illegal to call this method on a stream which returns
     * PRODUCER_PROVISION for getResultBufferProvision().  See
     * ProducerToConsumerProvisionAdapter.
     *
     * @return true if at least one tuple was written; false if no more results
     */
    virtual bool writeResultToConsumerBuffer(
        ByteOutputStream &resultOutputStream);

    /**
     * Queries the BufferProvision which this stream is capable of when
     * producing tuples.  If CONSUMER_PROVISION, getProducerResultStream is not
     * supported.  If PRODUCER_PROVISION, writeResultToConsumerBuffer is not
     * supported.  If PRODUCER_OR_CONSUMER_PROVISION, consumers can call
     * either.  NO_PROVISION is an illegal return value in this context.
     *
     * @return supported model
     */
    virtual BufferProvision getResultBufferProvision() const = 0;

    // REVIEW:  need to discriminate by input ordinal?
    /**
     * Queries the BufferProvision which this stream requires of its inputs when
     * consuming their tuples.  If CONSUMER_PROVISION or PRODUCER_PROVISION,
     * inputs must support the corresponding model.  If
     * PRODUCER_OR_CONSUMER_PROVISION, inputs can be of either model.  If
     * NO_PROVISION, stream takes no inputs (this is the default).
     *
     * @return required model
     */
    virtual BufferProvision getInputBufferRequirement() const;

    /**
     * Gets a pointer which can be dynamic_cast to the ExecutionStream subclass
     * known to implement this stream (said class must override this method).
     * This is like an extremely unsafe version of dynamic_cast, except that it
     * also takes care of digging through adapters.  Don't use this unless
     * you're absolutely sure you know what you're doing.
     *
     * @return the implementation object, or NULL if this stream doesn't
     * like your prying
     */
    virtual ExecutionStream *getImpl();
};

FENNEL_END_NAMESPACE

#endif

// End ExecutionStream.h
