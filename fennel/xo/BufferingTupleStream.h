/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2005-2005 Disruptive Tech
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

#ifndef Fennel_BufferingTupleStream_Included
#define Fennel_BufferingTupleStream_Included

#include "fennel/xo/SingleInputTupleStream.h"
#include "fennel/segment/SegStream.h"

FENNEL_BEGIN_NAMESPACE

/**
 * BufferingTupleStreamParams defines parameters for instantiating a
 * BufferingTupleStream.
 *
 *<p>
 *
 * TODO:  support usage of a SpillOutputStream.
 */
struct BufferingTupleStreamParams : public TupleStreamParams
{
    /**
     * If true, buffer contents are preserved rather than deleted as they are
     * read.  This allows open(restart=true) to be used to perform multiple
     * iterations over the buffer.
     *
     *<p>
     *
     * TODO: support "tee" on the first pass.
     */
    bool multipass;
};

/**
 * BufferingTupleStream buffers an underlying input stream.  The first read
 * request causes all input tuples to be pumped out into a buffer, after which
 * the original input stream is closed and tuples are returned from the buffer.
 */
class BufferingTupleStream : public SingleInputTupleStream
{
    SegmentAccessor bufferSegmentAccessor;
    SharedSegOutputStream pByteOutputStream;
    SharedSegInputStream pByteInputStream;
    TupleDescriptor tupleDesc;
    PageId firstPageId;
    bool multipass;
    SegStreamPosition restartPos;

    void destroyBuffer();
    void openBufferForRead(bool destroy);
    
public:
    void prepare(BufferingTupleStreamParams const &params);
    virtual void getResourceRequirements(
        ExecutionStreamResourceQuantity &minQuantity,
        ExecutionStreamResourceQuantity &optQuantity);
    virtual void open(bool restart);
    virtual TupleDescriptor const &getOutputDesc() const;
    virtual ByteInputStream &getProducerResultStream();
    virtual void closeImpl();
    virtual BufferProvision getResultBufferProvision() const;
    virtual BufferProvision getInputBufferRequirement() const;
};

FENNEL_END_NAMESPACE

#endif

// End BufferingTupleStream.h
