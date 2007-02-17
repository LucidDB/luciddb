/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2005-2005 Disruptive Tech
// Copyright (C) 2005-2005 LucidEra, Inc.
// Portions Copyright (C) 1999-2005 John V. Sichi
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

#ifndef Fennel_RandomAllocationSegment_Included
#define Fennel_RandomAllocationSegment_Included

#include "fennel/segment/RandomAllocationSegmentBase.h"

FENNEL_BEGIN_NAMESPACE

struct ExtentAllocationNode;

/**
 * RandomAllocationSegment refines RandomAllocationSegmentBase, defining an
 * ExtentAllocationNode where each page entry within the segment is unversioned.
 * The ExtentAllocationNodes are stored in the segment itself.
 */
class RandomAllocationSegment : public RandomAllocationSegmentBase
{
    // implement RandomAllocationSegmentBase
    virtual PageId getSegAllocPageIdForWrite(PageId origSegAllocPageId);
    virtual PageId getExtAllocPageIdForWrite(ExtentNum extentNum);
    virtual PageId allocateFromExtent(ExtentNum extentNum, PageOwnerId ownerId);
    virtual void format();
    virtual void formatPageExtents(
        SegmentAllocationNode &segAllocNode,
        ExtentNum &extentNum);
    virtual PageId allocateFromNewExtent(
        ExtentNum extentNum,
        PageOwnerId ownerId);
    virtual void freePageEntry(ExtentNum extentNum, BlockNum iPageInExtent);
    virtual PageOwnerId getPageOwnerId(
        ExtentNum extentNum,
        BlockNum iPageInExtent);
    virtual PageId getSegAllocPageIdForRead(
        PageId origSegAllocPageId,
        SharedSegment &allocNodeSegment);
    virtual PageId getExtAllocPageIdForRead(
        ExtentNum extentNum,
        SharedSegment &allocNodeSegment);

public:
    explicit RandomAllocationSegment(SharedSegment delegateSegment);

    // implementation of Segment interface
    virtual PageId allocatePageId(PageOwnerId ownerId);
    virtual PageId getPageSuccessor(PageId pageId);
    virtual void setPageSuccessor(PageId pageId, PageId successorId);
    virtual void deallocatePageRange(PageId startPageId, PageId endPageId);
};

FENNEL_END_NAMESPACE

#endif

// End RandomAllocationSegment.h
