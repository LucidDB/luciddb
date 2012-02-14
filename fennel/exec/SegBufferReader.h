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

#ifndef Fennel_SegBufferReader_Included
#define Fennel_SegBufferReader_Included

#include "fennel/common/ClosableObject.h"
#include "fennel/segment/SegStream.h"
#include "fennel/segment/SegmentAccessor.h"
#include "fennel/exec/ExecStreamDefs.h"

FENNEL_BEGIN_NAMESPACE

/**
 * SegBufferReader is a helper class that reads data that has previously been
 * buffered, and writes the data to its output stream.
 *
 * @author John V. Sichi
 * @version $Id$
 */
class FENNEL_EXEC_EXPORT SegBufferReader
    : public ClosableObject
{
    SharedExecStreamBufAccessor pOutAccessor;
    SegmentAccessor bufferSegmentAccessor;
    SharedSegInputStream pByteInputStream;
    PageId firstPageId;
    SegStreamPosition restartPos;
    uint cbLastRead;

public:
    /**
     * Creates a shared pointer to a new SegBufferReader object.
     *
     * @param pOutAccessor the output stream that will be written
     * @param bufferSegmentAccessor the segment accessor that will be used
     * to read the buffered data
     * @param firstPageId the pageId of the first buffer page
     */
    static SharedSegBufferReader newSegBufferReader(
        SharedExecStreamBufAccessor &pOutAccessor,
        SegmentAccessor const &bufferSegmentAccessor,
        PageId firstPageId);

    /**
     * Creates a new SegBufferReader object.
     *
     * @param pOutAccessorInit the output stream that will be written
     * @param bufferSegmentAccessorInit the segment accessor that will be used
     * to read the buffered data
     * @param firstPageIdInit the pageId of the first buffer page
     */
    explicit SegBufferReader(
        SharedExecStreamBufAccessor &pOutAccessorInit,
        SegmentAccessor const &bufferSegmentAccessorInit,
        PageId firstPageIdInit);

    /**
     * Initiates reads of the buffered data, beginning at the first page.
     *
     * @param destroy if true, destroy the buffered pages after they've been
     * read
     */
    void open(bool destroy);

    /**
     * Reads the buffered data.
     *
     * @return the execution stream result code indicating the state of the
     * data written to the output stream; EXECRC_BUF_OVERFLOW if data was
     * successfully written or EXECRC_EOS if all data has been written
     */
    ExecStreamResult read();

    // implement ClosableObject
    virtual void closeImpl();
};

FENNEL_END_NAMESPACE

#endif

// End SegBufferReader.h
