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

#ifndef Fennel_SegInputStream_Included
#define Fennel_SegInputStream_Included

#include "fennel/segment/SegStream.h"
#include "fennel/common/ByteInputStream.h"
#include "fennel/segment/SegPageIter.h"

FENNEL_BEGIN_NAMESPACE

/**
 * SegInputStream implements the ByteInputStream interface by reading data
 * previously written via a SegOutputStream.  seekBackward is supported, but
 * only if the underlying segment supports CONSECUTIVE_ALLOCATION or
 * LINEAR_ALLOCATION.
 */
class FENNEL_SEGMENT_EXPORT SegInputStream
    : public SegStream, public ByteInputStream
{
protected:
    SegPageIter pageIter;

    /**
     * PageId of current page.
     */
    PageId currPageId;

    /**
     * Whether to deallocate stream pages when no longer needed.
     */
    bool shouldDeallocate;

    // implement the ByteInputStream interface
    virtual void readNextBuffer();
    virtual void readPrevBuffer();
    virtual void closeImpl();

    explicit SegInputStream(
        SegmentAccessor const &, PageId, uint cbExtraHeader = 0);

    virtual void lockBuffer();

public:
    /**
     * Creates a new SegInputStream, positioned
     * at beginning-of-stream.
     *
     * @param segmentAccessor accessor for the segment containing the stream
     * data
     *
     * @param beginPageId the first page of stream data; if the default
     * FIRST_LINEAR_PAGE_ID is passed, the segment must support
     * LINEAR_ALLOCATION, and the stream starts at the first page of the
     * segment; if NULL_PAGE_ID is passed, an empty stream is returned
     *
     * @return shared_ptr to new SegInputStream
     */
    static SharedSegInputStream newSegInputStream(
        SegmentAccessor const &segmentAccessor,
        PageId beginPageId = FIRST_LINEAR_PAGE_ID);

    /**
     * Requests that prefetch be performed in anticipation of forward scan.
     */
    void startPrefetch();

    /**
     * Disables prefetch.
     */
    void endPrefetch();

    /**
     * Requests that pages be deallocated after the stream is done reading
     * them.  If in effect, any unread pages will also be deallocated when
     * the stream is closed.  Note that usage of deallocation may cause other
     * methods such as seekSegPos and seekBackward to fail.
     *
     * @param shouldDeallocate whether to deallocate pages
     */
    void setDeallocate(bool shouldDeallocate);

    /**
     * @return true if currently set to deallocate pages as they are read
     */
    bool isDeallocating();

    // implement the SegStream interface
    virtual void getSegPos(SegStreamPosition &pos);

    /**
     * Seeks to a previously recorded stream position.
     *
     * @param pos the new position, previously returned by getSegPos
     */
    void seekSegPos(SegStreamPosition const &pos);

    // override ByteInputStream
    virtual SharedByteStreamMarker newMarker();

    // override ByteInputStream
    virtual void mark(ByteStreamMarker &marker);

    // override ByteInputStream
    virtual void reset(ByteStreamMarker const &marker);
};

FENNEL_END_NAMESPACE

#endif

// End SegInputStream.h
