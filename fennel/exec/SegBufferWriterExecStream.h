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
    bool paramCreated;

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
