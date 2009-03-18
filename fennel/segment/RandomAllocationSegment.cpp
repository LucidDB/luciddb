/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005-2007 The Eigenbase Project
// Copyright (C) 2005-2007 Disruptive Tech
// Copyright (C) 2005-2007 LucidEra, Inc.
// Portions Copyright (C) 1999-2007 John V. Sichi
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
    nPagesPerExtent = (getUsablePageSize() - sizeof(ExtentAllocationNode))
        / sizeof(PageEntry);

    // + 1 is for SegAllocNode itself
    nPagesPerSegAlloc = nPagesPerExtent*nExtentsPerSegAlloc + 1;
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
        extentNum);
}

PageId RandomAllocationSegment::allocatePageId(PageOwnerId ownerId)
{
    return allocatePageIdFromSegment(ownerId, getTracingSegment());
}

PageId RandomAllocationSegment::getSegAllocPageIdForWrite(
    PageId origSegAllocPageId)
{
    return origSegAllocPageId;
}

void RandomAllocationSegment::undoSegAllocPageWrite(PageId segAllocPageId)
{
}

PageId RandomAllocationSegment::getExtAllocPageIdForWrite(ExtentNum extentNum)
{
    return getExtentAllocPageId(extentNum);
}

PageId RandomAllocationSegment::getSegAllocPageIdForRead(
    PageId origSegAllocPageId,
    SharedSegment &allocNodeSegment)
{
    allocNodeSegment = getTracingSegment();
    return origSegAllocPageId;
}

PageId RandomAllocationSegment::getExtAllocPageIdForRead(
    ExtentNum extentNum,
    SharedSegment &allocNodeSegment)
{
    allocNodeSegment = getTracingSegment();
    return getExtentAllocPageId(extentNum);
}

void RandomAllocationSegment::getPageEntryCopy(
    PageId pageId,
    PageEntry &pageEntryCopy,
    bool isAllocated,
    bool thisSegment)
{
    getPageEntryCopyTemplate<ExtentAllocationNode, ExtentAllocLock, PageEntry>(
        pageId,
        pageEntryCopy,
        isAllocated,
        thisSegment);
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
            getTracingSegment());
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
            getTracingSegment());
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
        iPageInExtent);
}

PageId RandomAllocationSegment::getPageSuccessor(PageId pageId)
{
    PageEntry pageEntry;

    getPageEntryCopy(pageId, pageEntry, true, true);
    return pageEntry.successorId;
}

void RandomAllocationSegment::setPageSuccessor(
    PageId pageId,
    PageId successorId)
{
    setPageSuccessorTemplate<ExtentAllocationNode, ExtentAllocLock>(
        pageId,
        successorId,
        getTracingSegment());
}

PageOwnerId RandomAllocationSegment::getPageOwnerId(
    PageId pageId,
    bool thisSegment)
{
    return getPageOwnerIdTemplate<PageEntry>(pageId, thisSegment);
}

FENNEL_END_CPPFILE("$Id$");

// End RandomAllocationSegment.cpp
