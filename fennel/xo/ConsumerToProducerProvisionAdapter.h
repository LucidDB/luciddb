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

#ifndef Fennel_ConsumerToProducerProvisionAdapter_Included
#define Fennel_ConsumerToProducerProvisionAdapter_Included

#include "fennel/xo/SingleInputTupleStream.h"
#include "fennel/segment/SegPageLock.h"
#include "fennel/common/ByteInputStream.h"

FENNEL_BEGIN_NAMESPACE

/**
 * ConsumerToProducerProvisionAdapter is an adapter for converting the output
 * of a CONSUMER_PROVISION producer for use by a PRODUCER_PROVISION consumer.
 * The implementation works by allocating a scratch buffer, calling the
 * producer's writeResultToConsumerBuffer implementation to fill it, and
 * returning a stream which reads from it.
 */
class ConsumerToProducerProvisionAdapter
    : public SingleInputTupleStream, private ByteInputStream
{
    /**
     * Accessor for scratch segment.
     */
    SegmentAccessor scratchAccessor;
    
    /**
     * Lock on scratch buffer.
     */
    SegPageLock bufferLock;

    /**
     * Stream for writing to scratch buffer.
     */
    SharedByteArrayOutputStream pBufferOutputStream;

protected:
    virtual void closeImpl();
    
public:
    // implement TupleStream
    virtual void prepare(TupleStreamParams const &params);
    virtual void getResourceRequirements(
        ExecutionStreamResourceQuantity &minQuantity,
        ExecutionStreamResourceQuantity &optQuantity);
    virtual void open(bool restart);
    virtual ByteInputStream &getProducerResultStream();
    virtual BufferProvision getResultBufferProvision() const;
    virtual BufferProvision getInputBufferRequirement() const;
    
    // implement ByteInputStream
    virtual void readNextBuffer();
    virtual ExecutionStream *getImpl();
};

FENNEL_END_NAMESPACE

#endif

// End ConsumerToProducerProvisionAdapter.h
