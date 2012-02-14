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

#ifndef Fennel_LinearDeviceSegment_Included
#define Fennel_LinearDeviceSegment_Included

#include "fennel/segment/Segment.h"
#include <vector>

FENNEL_BEGIN_NAMESPACE

/**
 * LinearDeviceSegmentParams defines initialization parameters for
 * LinearDeviceSegment.
 */
struct FENNEL_SEGMENT_EXPORT LinearDeviceSegmentParams
{
    /**
     * BlockId of the first page in the segment; the
     * owning device must already be registered with the cache.
     */
    BlockId firstBlockId;

    /**
     * Minimum number of pages in segment.  If the device isn't big enough, it
     * is automatically extended when the segment is created.
     */
    BlockNum nPagesMin;

    /**
     * Number of pages considered allocated in segment on construction.  Must
     * be less than nPagesMax (or MAXU, in which case all pages are considered
     * allocated).
     */
    BlockNum nPagesAllocated;

    /**
     * When the device needs to be extended to satisfy an allocation, the number
     * of pages by which to extend it.  If 0, no auto-extension is performed.
     */
    BlockNum nPagesIncrement;

    /**
     * Maximum number of pages in the segment; the device will not be extended
     * beyond this.  If MAXU, segment size is limited only by disk space.
     */
    BlockNum nPagesMax;

    explicit LinearDeviceSegmentParams();
};

/**
 * LinearDeviceSegment is an implementation of Segment in terms of a contiguous
 * range of pages of a single underlying RandomAccessDevice.  See <a
 * href="structSegmentDesign.html#LinearDeviceSegment">the design docs</a> for
 * more detail.
 *
 *<p>
 *
 * Individual deallocation of pages of a LinearDeviceSegment is not supported.
 * deallocatePageRange is supported when endPageId is NULL_PAGE_ID.
 * This does not affect the size of the underlying device (REVIEW: maybe
 * it should?)
 */
class FENNEL_SEGMENT_EXPORT LinearDeviceSegment
    : public Segment
{
    friend class SegmentFactory;

    SharedRandomAccessDevice pDevice;
    BlockId firstBlockId;
    BlockNum nPagesMax, nPagesAllocated, nPagesIncrement, nPagesExtended;

    explicit LinearDeviceSegment(
        SharedCache cache,
        LinearDeviceSegmentParams const &);

    BlockNum getAvailableDevicePages() const;

public:
    virtual ~LinearDeviceSegment();

    DeviceId getDeviceId() const;

    // implementation of Segment interface

    virtual BlockId translatePageId(PageId);
    virtual PageId translateBlockId(BlockId);
    virtual PageId allocatePageId(PageOwnerId ownerId);
    virtual void deallocatePageRange(PageId startPageId, PageId endPageId);
    virtual bool isPageIdAllocated(PageId pageId);
    virtual BlockNum getAllocatedSizeInPages();
    virtual BlockNum getNumPagesOccupiedHighWater();
    virtual BlockNum getNumPagesExtended();
    virtual PageId getPageSuccessor(PageId pageId);
    virtual void setPageSuccessor(PageId pageId, PageId successorId);
    virtual AllocationOrder getAllocationOrder() const;
    virtual bool ensureAllocatedSize(BlockNum nPages);
    virtual void delegatedCheckpoint(
        Segment &delegatingSegment,
        CheckpointType checkpointType);
};

FENNEL_END_NAMESPACE

#endif

// End LinearDeviceSegment.h
