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

#ifndef Fennel_SegBufferWriter_Included
#define Fennel_SegBufferWriter_Included

#include "fennel/common/ClosableObject.h"
#include "fennel/segment/SegStream.h"
#include "fennel/segment/SegmentAccessor.h"
#include "fennel/exec/ExecStreamDefs.h"

FENNEL_BEGIN_NAMESPACE

/**
 * SegBufferWriter is a helper class that reads an input stream and writes
 * the data into a buffer.
 *
 * @author John V. Sichi
 * @version $Id$
 */
class FENNEL_EXEC_EXPORT SegBufferWriter
    : public ClosableObject
{
    SharedExecStreamBufAccessor pInAccessor;
    SegmentAccessor bufferSegmentAccessor;
    SharedSegOutputStream pByteOutputStream;
    PageId firstPageId;

    /**
     * True if the buffer pages written should be destroyed on close.
     * Otherwise, it's the responsibility of the caller to destroy the
     * buffer pages.
     */
    bool destroyOnClose;

public:
    /**
     * Creates a shared pointer to a new SegBufferWriter object.
     *
     * @param pInAccessor the input stream that will be read
     * @param bufferSegmentAccessor the segment accessor that will be used
     * to write the buffered data
     * @param destroyOnClose true if the data written needs to be destroyed
     * when the object is destroyed; otherwise, it's the responsibility of
     * the caller to destroy the buffer pages
     */
    static SharedSegBufferWriter newSegBufferWriter(
        SharedExecStreamBufAccessor &pInAccessor,
        SegmentAccessor const &bufferSegmentAccessor,
        bool destroyOnClose);

    /**
     * Creates a new SegBufferWriter object.
     *
     * @param pInAccessorInit the input stream that will be read
     * @param bufferSegmentAccessorInit the segment accessor that will be used
     * to write the buffered data
     * @param destroyOnCloseInit true if the data written needs to be destroyed
     * when the object is destroyed; otherwise, it's the responsibility of
     * the caller to destroy the buffer pages
     */
    explicit SegBufferWriter(
        SharedExecStreamBufAccessor &pInAccessorInit,
        SegmentAccessor const &bufferSegmentAccessorInit,
        bool destroyOnCloseInit);

    /**
     * Reads data from the input stream and buffers it.
     */
    ExecStreamResult write();

    /*
     * @return the pageId of the first buffer page, once writes have been
     * initiated
     */
    PageId getFirstPageId();

    // implement ClosableObject
    virtual void closeImpl();
};

FENNEL_END_NAMESPACE

#endif

// End SegBufferWriter.h
