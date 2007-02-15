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

#include "fennel/common/CommonPreamble.h"
#include "fennel/segment/RandomAllocationSegmentBaseImpl.h"
#include "fennel/segment/RandomAllocationSegmentImpl.h"

FENNEL_BEGIN_CPPFILE("$Id$");

RandomAllocationSegment::RandomAllocationSegment(
    SharedSegment delegateSegment)
    : RandomAllocationSegmentBase(delegateSegment)
{
    nPagesPerExtent = (getUsablePageSize()-sizeof(ExtentAllocationNode))
        / sizeof(PageEntry);

    // + 1 is for SegAllocNode itself
    nPagesPerSegAlloc = nPagesPerExtent*nExtentsPerSegAlloc + 1;
}

void RandomAllocationSegment::format()
{
    formatFromSegment(shared_from_this());
}

void RandomAllocationSegment::formatPageExtents(
    SegmentAllocationNode &segAllocNode,
    ExtentNum &extentNum)
{
    formatPageExtentsTemplate<
            ExtentAllocationNode,
            ExtentAllocLock,
            PageEntry>(
        segAllocNode,
        shared_from_this(),
        extentNum);
}

PageId RandomAllocationSegment::allocatePageId(PageOwnerId ownerId)
{
    return allocatePageIdFromSegment(ownerId, shared_from_this());
}

PageId RandomAllocationSegment::getSegAllocPageIdForWrite(
    PageId origSegAllocPageId)
{
    return origSegAllocPageId;
}

PageId RandomAllocationSegment::getExtAllocPageIdForWrite(ExtentNum extentNum)
{
    return getExtentAllocPageId(extentNum);
}

PageId RandomAllocationSegment::getSegAllocPageIdForRead(
    PageId origSegAllocPageId,
    SharedSegment &allocNodeSegment)
{
    allocNodeSegment = shared_from_this();
    return origSegAllocPageId;
}

PageId RandomAllocationSegment::getExtAllocPageIdForRead(
    ExtentNum extentNum,
    SharedSegment &allocNodeSegment)
{
    allocNodeSegment = shared_from_this();
    return getExtentAllocPageId(extentNum);
}

PageId RandomAllocationSegment::allocateFromNewExtent(
    ExtentNum extentNum,
    PageOwnerId ownerId)
{
    return
        allocateFromNewExtentTemplate<
                ExtentAllocationNode,
                ExtentAllocLock,
                PageEntry>(
            extentNum,
            ownerId,
            shared_from_this());
}

PageId RandomAllocationSegment::allocateFromExtent(
    ExtentNum extentNum,
    PageOwnerId ownerId)
{
    return
        allocateFromExtentTemplate<
                ExtentAllocationNode,
                ExtentAllocLock,
                PageEntry>(
            extentNum,
            ownerId,
            shared_from_this());
}

void RandomAllocationSegment::deallocatePageRange(
    PageId startPageId,
    PageId endPageId)
{
    deallocatePageRangeFromSegment(startPageId, endPageId, shared_from_this());
}

void RandomAllocationSegment::freePageEntry(
    ExtentNum extentNum,
    BlockNum iPageInExtent)
{
    freePageEntryTemplate<
            ExtentAllocationNode,
            ExtentAllocLock,
            PageEntry>(
        extentNum,
        iPageInExtent,
        shared_from_this());
}

PageId RandomAllocationSegment::getPageSuccessor(PageId pageId)
{
    return
        getPageSuccessorTemplate<ExtentAllocationNode, ExtentAllocLock>(pageId);
}

void RandomAllocationSegment::setPageSuccessor(
    PageId pageId,
    PageId successorId)
{
    setPageSuccessorTemplate<ExtentAllocationNode, ExtentAllocLock>(
        pageId,
        successorId,
        shared_from_this());
}

PageOwnerId RandomAllocationSegment::getPageOwnerId(
    ExtentNum extentNum,
    BlockNum iPageInExtent)
{
    return
        getPageOwnerIdTemplate<
                ExtentAllocationNode,
                ExtentAllocLock>(
            extentNum,
            iPageInExtent);
}

FENNEL_END_CPPFILE("$Id$");

// End RandomAllocationSegment.cpp
