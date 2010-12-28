/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005 The Eigenbase Project
// Copyright (C) 2005 SQLstream, Inc.
// Copyright (C) 2005 Dynamo BI Corporation
// Portions Copyright (C) 1999 John V. Sichi
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
