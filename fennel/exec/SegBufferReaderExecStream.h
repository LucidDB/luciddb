/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005 The Eigenbase Project
// Copyright (C) 2005 SQLstream, Inc.
// Copyright (C) 2005 Dynamo BI Corporation
// Portions Copyright (C) 2004 John V. Sichi
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

#ifndef Fennel_SegBufferReaderExecStream_Included
#define Fennel_SegBufferReaderExecStream_Included

#include "fennel/tuple/TupleData.h"
#include "fennel/exec/ConduitExecStream.h"

FENNEL_BEGIN_NAMESPACE

/**
 * SegBufferReaderExecStreamParams defines parameters for instantiating a
 * SegBufferReaderExecStream.
 */
struct FENNEL_EXEC_EXPORT SegBufferReaderExecStreamParams
    : public ConduitExecStreamParams
{
    /**
     * Id of the dynamic parameter used to keep a reference count of the
     * number of active readers of the buffered input
     */
    DynamicParamId readerRefCountParamId;
};

/**
 * SegBufferReaderExecStream reads the buffered input written by
 * a SegBufferWriterExecStream.  It waits until the writer stream has
 * completed buffering its input before it attempts to read it.  It then
 * writes the data to its output stream.
 *
 * <p>
 * The first buffer pageId written by the writer stream will be passed to
 * this stream's input stream, once the data has been buffered.
 *
 * <p>
 * The stream shares a dynamic parameter with its corresponding
 * SegBufferWriterExecStream.  The parameter is a reference counter that's
 * incremented when this stream is opened, and decremented when it's closed.
 * SegBufferWriterExecStream uses this reference counter to determine when
 * it's safe to destroy the buffered data.
 *
 * @author Zelaine Fong
 * @version $Id$
 */
class FENNEL_EXEC_EXPORT SegBufferReaderExecStream
    : public ConduitExecStream
{
    SegmentAccessor bufferSegmentAccessor;
    SharedSegBufferReader pSegBufferReader;
    DynamicParamId readerRefCountParamId;
    PageId firstBufferPageId;
    TupleData inputTuple;
    bool paramIncremented;

public:
    virtual void prepare(SegBufferReaderExecStreamParams const &params);
    virtual void getResourceRequirements(
        ExecStreamResourceQuantity &minQuantity,
        ExecStreamResourceQuantity &optQuantity);
    virtual void open(bool restart);
    virtual ExecStreamResult execute(ExecStreamQuantum const &quantum);
    virtual void closeImpl();
    virtual ExecStreamBufProvision getOutputBufProvision() const;
};

FENNEL_END_NAMESPACE

#endif

// End SegBufferReaderExecStream.h
