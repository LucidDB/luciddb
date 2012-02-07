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
