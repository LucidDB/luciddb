/*
// Licensed to DynamoBI Corporation (DynamoBI) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  DynamoBI licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at

//   http://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
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
