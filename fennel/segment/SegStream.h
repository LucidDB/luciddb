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

#ifndef Fennel_SegStream_Included
#define Fennel_SegStream_Included

#include "fennel/common/ByteStream.h"
#include "fennel/segment/SegPageLock.h"
#include "fennel/common/PseudoUuid.h"

FENNEL_BEGIN_NAMESPACE

// NOTE:  read comments on struct StoredNode before modifying
// SegStreamNode

/**
 * Header stored on each page of a SegStream.
 */
struct FENNEL_SEGMENT_EXPORT SegStreamNode
    : public StoredNode
{
    static const MagicNumber MAGIC_NUMBER = 0x99f28198d53750a5LL;

    uint cbData;
};

/**
 * Additional header information stored on each page of SegStreams
 * for which CRC's are requested.
 */
struct FENNEL_SEGMENT_EXPORT SegStreamCrc
{
    /**
     * The PageId of this page relative to the segment storing the stream.
     */
    PageId pageId;

    /**
     * The UUID of the online instance that wrote this page.
     */
    PseudoUuid onlineUuid;

    /**
     * CRC computed for this page.
     */
    uint64_t checksum;
};

typedef SegNodeLock<SegStreamNode> SegStreamLock;

/**
 * Memento for a position within a SegStream.
 */
struct FENNEL_SEGMENT_EXPORT SegStreamPosition
{
    /**
     * Physical position.
     */
    SegByteId segByteId;

    /**
     * Logical position.
     */
    FileSize cbOffset;
};

/**
 * SegStream is a common base for SegInputStream and SegOutputStream.
 */
class FENNEL_SEGMENT_EXPORT SegStream
    : virtual public ByteStream
{
protected:
    /**
     * Accessor for segment containing stream data.
     */
    SegmentAccessor segmentAccessor;

    /**
     * Lock held on current page, if any.
     */
    SegStreamLock pageLock;

    /**
     * Number of bytes in page header.
     */
    uint cbPageHeader;

    // implement the ClosableObject interface
    virtual void closeImpl();

    explicit SegStream(SegmentAccessor const &,uint cbExtraHeader);
public:

    /**
     * Obtains the current stream position.
     *
     * @param pos receives the position
     */
    virtual void getSegPos(SegStreamPosition &pos) = 0;

    /**
     * @return segment accessed by this stream
     */
    SharedSegment getSegment() const;

    /**
     * @return segment accessor used by this stream
     */
    SegmentAccessor const &getSegmentAccessor() const;
};

/**
 * SegStreamMarker refines ByteStreamMarker with a physical stream
 * position, allowing for random-access mark/reset.
 */
class FENNEL_SEGMENT_EXPORT SegStreamMarker
    : public ByteStreamMarker
{
    friend class SegInputStream;

    /**
     * Position for random-access mark/reset.
     */
    SegStreamPosition segPos;

    explicit SegStreamMarker(SegStream const &segStream);

    // implement ByteStreamMarker
    virtual FileSize getOffset() const;
};

FENNEL_END_NAMESPACE

#endif

// End SegStream.h
