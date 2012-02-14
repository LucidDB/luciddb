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

#ifndef Fennel_SpillOutputStream_Included
#define Fennel_SpillOutputStream_Included

#include "fennel/common/ByteOutputStream.h"
#include "fennel/segment/SegPageLock.h"

FENNEL_BEGIN_NAMESPACE

class LogicalTxn;

/**
 * SpillOutputStream implements the ByteOutputStream interface by
 * starting with writes to a cache scratch page.  If this overflows, the
 * contents are spilled to a new SegOutputStream to which all further writes
 * are directed.
 */
class FENNEL_SEGMENT_EXPORT SpillOutputStream
    : public ByteOutputStream
{
    /**
     * Factory to use for creating spill segment if necessary.
     */
    SharedSegmentFactory pSegmentFactory;

    /**
     * CacheAccessor to use for accessing spill segment if necessary.
     */
    SharedCacheAccessor pCacheAccessor;

    /**
     * Spill segment output stream.
     */
    SharedSegOutputStream pSegOutputStream;

    /**
     * Total number of bytes locked in current page.
     */
    uint cbBuffer;

    /**
     * Accessor for scratch segment.
     */
    SegmentAccessor scratchAccessor;

    /**
     * Page lock on scratch page for short streams.
     */
    SegPageLock scratchPageLock;

    /**
     * Filename to assign to spill segment.
     */
    std::string spillFileName;

    // implement the ByteOutputStream interface
    virtual void flushBuffer(uint cbRequested);
    virtual void closeImpl();

    void spill();
    void updatePage();

    explicit SpillOutputStream(
        SharedSegmentFactory,
        SharedCacheAccessor,
        std::string);

public:
    /**
     * Creates a new SpillOutputStream.
     *
     * @param pSegmentFactory the SegmentFactory to use if the output stream
     * spills
     *
     * @param pCacheAccessor the CacheAccessor to use
     *
     * @param spillFileName filename to assign to spill segment
     */
    static SharedSpillOutputStream newSpillOutputStream(
        SharedSegmentFactory pSegmentFactory,
        SharedCacheAccessor pCacheAccessor,
        std::string spillFileName);

    virtual ~SpillOutputStream();

    /**
     * Obtains a ByteInputStream suitable for accessing the contents of this
     * SpillOutputStream.  If spill has already occurred, then this is a
     * SegInputStream, otherwise a ByteArrayInputStream.
     *
     * @param seekPosition if SEEK_STREAM_BEGIN (the default), the input stream
     * is initially positioned before the first byte of stream data; otherwise,
     * after the last byte
     *
     * @return new ByteInputStream
     */
    SharedByteInputStream getInputStream(
        SeekPosition seekPosition = SEEK_STREAM_BEGIN);

    /**
     * Obtains a reference to the underlying segment if this stream has
     * spilled.
     *
     * @return the segment, or a singular pointer if the stream has not spilled
     */
    SharedSegment getSegment();

    /**
     * Obtains a reference to the underlying SegOutputStream if this stream has
     * spilled.
     *
     * @return the stream, or a singular pointer if the stream has not spilled
     */
    SharedSegOutputStream getSegOutputStream();

    // override ByteOutputStream
    virtual void setWriteLatency(WriteLatency writeLatency);
};

FENNEL_END_NAMESPACE

#endif

// End SpillOutputStream.h
