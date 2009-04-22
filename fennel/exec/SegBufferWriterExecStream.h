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

#ifndef Fennel_SegBufferWriterExecStream_Included
#define Fennel_SegBufferWriterExecStream_Included

#include "fennel/tuple/TupleData.h"
#include "fennel/exec/DiffluenceExecStream.h"
#include <boost/scoped_array.hpp>

FENNEL_BEGIN_NAMESPACE

/**
 * SegBufferWriterExecStreamParams defines parameters for instantiating a
 * SegBufferWriterExecStream.
 */
struct FENNEL_EXEC_EXPORT SegBufferWriterExecStreamParams
    : public DiffluenceExecStreamParams
{
    /**
     * Id of the dynamic parameter used to keep a reference count of the
     * number of active readers of the buffered input
     */
    DynamicParamId readerRefCountParamId;
};

/**
 * SegBufferWriterExecStream reads its input stream and writes the data to
 * a buffer so it can be read by one or more SegBufferReaderExecStreams.
 * It does not write the buffered data to its output stream.  When this stream
 * has completed buffering its input, it will pass the first pageId of the
 * buffered data through its output stream to any reader streams that are
 * ready to start processing data.
 *
 * <p>
 * The writer and reader stream instances all share a dynamic parameter. The
 * dynamic parameter is created by this stream and read by the reader streams.
 * The parameter is a reference counter used to keep track of the number of
 * active readers, which this stream uses to determine if it can destroy the
 * buffered data.
 *
 * @author Zelaine Fong
 * @version $Id$
 */
class FENNEL_EXEC_EXPORT SegBufferWriterExecStream
    : public DiffluenceExecStream
{
    SegmentAccessor bufferSegmentAccessor;
    SharedSegBufferWriter pSegBufferWriter;
    DynamicParamId readerRefCountParamId;
    PageId firstBufferPageId;
    TupleData outputTuple;
    boost::scoped_array<FixedBuffer> outputTupleBuffer;
    uint outputBufSize;
    std::vector<bool> outputWritten;
    uint nOutputsWritten;

    /**
     * Reads and returns the current value of the reader reference count
     * stored in a dynamic parameter.
     *
     * @return the reader reference count
     */
    int64_t readReaderRefCount();

public:
    virtual bool canEarlyClose();
    virtual void prepare(SegBufferWriterExecStreamParams const &params);
    virtual void getResourceRequirements(
        ExecStreamResourceQuantity &minQuantity,
        ExecStreamResourceQuantity &optQuantity);
    virtual void open(bool restart);
    virtual ExecStreamResult execute(ExecStreamQuantum const &quantum);
    virtual void closeImpl();
    virtual ExecStreamBufProvision getInputBufProvision() const;
    virtual ExecStreamBufProvision getOutputBufProvision() const;
};

FENNEL_END_NAMESPACE

#endif

// End SegBufferWriterExecStream.h
