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
#include "fennel/segment/VersionedRandomAllocationSegmentImpl.h"

FENNEL_BEGIN_CPPFILE("$Id$");

VersionedRandomAllocationSegment::VersionedRandomAllocationSegment(
    SharedSegment delegateSegment,
    SharedSegment pTempSegmentInit)
    : RandomAllocationSegmentBase(delegateSegment)
{
    nPagesPerExtent =
        (getUsablePageSize() - sizeof(VersionedExtentAllocationNode))
        / sizeof(VersionedPageEntry);

    // + 1 is for SegAllocNode itself
    nPagesPerSegAlloc = nPagesPerExtent*nExtentsPerSegAlloc + 1;

    pTempSegment = pTempSegmentInit;
    mapPages = true;
}

void VersionedRandomAllocationSegment::format()
{
    // Format the pages directly in the permanent segment
    mapPages = false;
    formatFromSegment(shared_from_this());
    mapPages = true;
}

void VersionedRandomAllocationSegment::formatPageExtents(
    SegmentAllocationNode &segAllocNode,
    ExtentNum &extentNum)
{
    SharedSegment allocNodeSegment;
    if (mapPages) {
        allocNodeSegment = pTempSegment;
    } else {
        allocNodeSegment = shared_from_this();
    }

    formatPageExtentsTemplate<
            VersionedExtentAllocationNode,
            VersionedExtentAllocLock,
            VersionedPageEntry>(
        segAllocNode,
        allocNodeSegment,
        extentNum);
}

PageId VersionedRandomAllocationSegment::allocatePageId(PageOwnerId ownerId)
{
    return allocatePageIdFromSegment(ownerId, pTempSegment);
}

PageId VersionedRandomAllocationSegment::getSegAllocPageIdForWrite(
    PageId origSegAllocPageId)
{
    // Note that there may be cases where we are accessing a
    // SegmentAllocationNode for write, even though we don't actually update
    // it.  This occurs when we're trying to allocate a new page.  We need
    // to exclusively lock the the node and access it for write in case there
    // is a free page available in an extent.  As a result, we may unnecessarily
    // increment the updateCount on these pages, resulting in those pages
    // never getting removed from the temp space.  However, that shouldn't
    // waste too much space because in comparison to the number of extent
    // allocation node pages, the number of SegmentAllocationNodes is small.

    if (mapPages) {
        return getTempAllocNodePage<SegAllocLock>(origSegAllocPageId);
    } else {
        return origSegAllocPageId;
    }
}

PageId VersionedRandomAllocationSegment::getSegAllocPageIdForRead(
    PageId origSegAllocPageId,
    SharedSegment &allocNodeSegment)
{
    if (mapPages) {
        return findAllocPageIdForRead(origSegAllocPageId, allocNodeSegment);
    } else {
        return origSegAllocPageId;
    }
}

PageId VersionedRandomAllocationSegment::getExtAllocPageIdForRead(
    ExtentNum extentNum,
    SharedSegment &allocNodeSegment)
{
    if (mapPages) {
        return
            findAllocPageIdForRead(
                getExtentAllocPageId(extentNum),
                allocNodeSegment);
    } else {
        allocNodeSegment = shared_from_this();
        return getExtentAllocPageId(extentNum);
    }
}

PageId VersionedRandomAllocationSegment::findAllocPageIdForRead(
    PageId origAllocNodePageId,
    SharedSegment &allocNodeSegment)
{
    SXMutexSharedGuard mapGuard(mutex);

    // If the allocation node corresponding to the desired page has been
    // modified, it will be in our map.  If so, retrieve the pageId
    // corresponding to the modified allocation node, and access that
    // page from the temporary segment.  Otherwise, access the allocation
    // node from permanent storage.
    //
    // Note that once we've determined that a page has not been updated, we
    // pass the original pageId back and release the mutex guarding the page
    // map.  By doing so, it's possible that the caller will end up reading an
    // older copy of the allocation node.  If that is not acceptable to the
    // caller, this method should not be used.

    PageId tempAllocNodePageId;
    NodeMapConstIter iter = allocationNodeMap.find(origAllocNodePageId);
    if (iter == allocationNodeMap.end()) {
        tempAllocNodePageId = origAllocNodePageId;
        allocNodeSegment = shared_from_this();
    } else {
        tempAllocNodePageId = iter->second->tempPageId;
        allocNodeSegment = pTempSegment;
    }

    return tempAllocNodePageId;
}

PageId VersionedRandomAllocationSegment::allocateFromNewExtent(
    ExtentNum extentNum,
    PageOwnerId ownerId)
{
    return
        allocateFromNewExtentTemplate<
                VersionedExtentAllocationNode,
                VersionedExtentAllocLock,
                VersionedPageEntry>(
            extentNum,
            ownerId,
            pTempSegment);
}

PageId VersionedRandomAllocationSegment::allocateFromExtent(
    ExtentNum extentNum,
    PageOwnerId ownerId)
{
    return
        allocateFromExtentTemplate<
                VersionedExtentAllocationNode,
                VersionedExtentAllocLock,
                VersionedPageEntry>(
            extentNum,
            ownerId,
            pTempSegment);
}

void VersionedRandomAllocationSegment::deallocatePageRange(
    PageId startPageId,
    PageId endPageId)
{
    deallocatePageRangeFromSegment(startPageId, endPageId, pTempSegment);
}

void VersionedRandomAllocationSegment::freePageEntry(
    ExtentNum extentNum,
    BlockNum iPageInExtent)
{
    freePageEntryTemplate<
            VersionedExtentAllocationNode,
            VersionedExtentAllocLock,
            VersionedPageEntry>(
        extentNum,
        iPageInExtent,
        pTempSegment);
}

void VersionedRandomAllocationSegment::markPageEntryUnused(
    PageEntry &pageEntry)
{
    RandomAllocationSegmentBase::markPageEntryUnused(pageEntry);

    VersionedPageEntry versionedPageEntry =
        reinterpret_cast<VersionedPageEntry &>(pageEntry);
    versionedPageEntry.versionChainPageId = NULL_PAGE_ID;
    versionedPageEntry.allocationCsn = NULL_TXN_ID;
}

PageId VersionedRandomAllocationSegment::getPageSuccessor(PageId pageId)
{
    return 
        getPageSuccessorTemplate<
                VersionedExtentAllocationNode,
                VersionedExtentAllocLock>(
            pageId);
}

void VersionedRandomAllocationSegment::setPageSuccessor(
    PageId pageId, PageId successorId)
{
    setPageSuccessorTemplate<
            VersionedExtentAllocationNode,
            VersionedExtentAllocLock>(
        pageId,
        successorId,
        pTempSegment);
}

PageId VersionedRandomAllocationSegment::getExtAllocPageIdForWrite(
    ExtentNum extentNum)
{
    if (mapPages) {
        return
            getTempAllocNodePage<VersionedExtentAllocLock>(
                getExtentAllocPageId(extentNum));
    } else {
        return getExtentAllocPageId(extentNum);
    }
}

PageOwnerId VersionedRandomAllocationSegment::getPageOwnerId(
    ExtentNum extentNum,
    BlockNum iPageInExtent)
{
    return
        getPageOwnerIdTemplate<
                VersionedExtentAllocationNode,
                VersionedExtentAllocLock>(
            extentNum,
            iPageInExtent);
}

void VersionedRandomAllocationSegment::getPageEntryCopy(
    PageId pageId,
    VersionedPageEntry &pageEntryCopy)
{
    assert(isPageIdAllocated(pageId));

    ExtentNum extentNum;
    BlockNum iPageInExtent;
    uint iSegAlloc;
    splitPageId(pageId, iSegAlloc, extentNum, iPageInExtent);
    assert(iPageInExtent);

    SharedSegment allocNodeSegment;
    PageId extentPageId = getExtAllocPageIdForRead(extentNum, allocNodeSegment);

    SegmentAccessor segAccessor(allocNodeSegment, pCache);
    VersionedExtentAllocLock extentAllocLock(segAccessor);
    extentAllocLock.lockShared(extentPageId);
    VersionedExtentAllocationNode const &extentNode =
        extentAllocLock.getNodeForRead();

    VersionedPageEntry const &pageEntry =
        extentNode.getPageEntry(iPageInExtent);
    assert(pageEntry.ownerId != UNALLOCATED_PAGE_OWNER_ID);

    pageEntryCopy = pageEntry;
}

void VersionedRandomAllocationSegment::initPageEntry(
    PageId pageId,
    PageId versionChainId,
    TxnId allocationCsn)
{
    assert(isPageIdAllocated(pageId));

    ExtentNum extentNum;
    BlockNum iPageInExtent;
    uint iSegAlloc;
    splitPageId(pageId, iSegAlloc, extentNum, iPageInExtent);
    assert(iPageInExtent);

    SegmentAccessor segAccessor(pTempSegment, pCache);
    VersionedExtentAllocLock extentAllocLock(segAccessor);
    extentAllocLock.lockExclusive(getExtAllocPageIdForWrite(extentNum));
    VersionedExtentAllocationNode &extentNode =
        extentAllocLock.getNodeForWrite();
    VersionedPageEntry &pageEntry =
        extentNode.getPageEntry(iPageInExtent);
    assert(pageEntry.ownerId != UNALLOCATED_PAGE_OWNER_ID);
    pageEntry.versionChainPageId = versionChainId;
    if (allocationCsn != NULL_TXN_ID) {
        pageEntry.allocationCsn = allocationCsn;
    }
}

void VersionedRandomAllocationSegment::chainPageEntries(
    PageId pageId,
    PageId versionChainId,
    PageId successorId)
{
    assert(isPageIdAllocated(pageId));

    ExtentNum extentNum;
    BlockNum iPageInExtent;
    uint iSegAlloc;
    splitPageId(pageId, iSegAlloc, extentNum, iPageInExtent);
    assert(iPageInExtent);

    SegmentAccessor segAccessor(pTempSegment, pCache);
    VersionedExtentAllocLock extentAllocLock(segAccessor);
    extentAllocLock.lockExclusive(getExtAllocPageIdForWrite(extentNum));
    VersionedExtentAllocationNode &extentNode =
        extentAllocLock.getNodeForWrite();
    VersionedPageEntry &pageEntry =
        extentNode.getPageEntry(iPageInExtent);
    assert(pageEntry.ownerId != UNALLOCATED_PAGE_OWNER_ID);
    if (successorId != NULL_PAGE_ID) {
        pageEntry.successorId = successorId;
    }
    pageEntry.versionChainPageId = versionChainId;
}

void VersionedRandomAllocationSegment::updatePageEntry(
    PageId pageId,
    uint updateCount,
    uint allocationCount,
    int netAllocations,
    TxnId commitCsn,
    PageOwnerId ownerId,
    bool commit)
{
    // Don't check if the pageId is allocated because it might not be if
    // we've deallocated the page.
 
    ExtentNum extentNum;
    BlockNum iPageInExtent;
    uint iSegAlloc;
    splitPageId(pageId, iSegAlloc, extentNum, iPageInExtent);
    assert(iPageInExtent);

    SXMutexExclusiveGuard mapGuard(mutex);

    // Update the SegmentAllocationNode page
    updateExtentEntry(
        iSegAlloc,
        extentNum,
        allocationCount,
        netAllocations,
        commit);

    // Update the extent allocation page, copying the contents from the
    // temporary page in the case of a commit and vice versa for a rollback.
    
    PageId extentPageId = getExtentAllocPageId(extentNum);
    NodeMapConstIter iter = allocationNodeMap.find(extentPageId);
    permAssert(iter != allocationNodeMap.end());
    SharedModifiedAllocationNode pModNode = iter->second;

    if (commit) {
        copyPageEntryFromTemp(
            extentPageId,
            pModNode->tempPageId,
            iPageInExtent,
            commitCsn,
            ownerId);
    } else {
        copyPageEntryToTemp(extentPageId, pModNode->tempPageId, iPageInExtent);
    }

    pModNode->updateCount -= updateCount;
    if (pModNode->updateCount == 0) {
        freeTempPage(extentPageId, pModNode->tempPageId);
    }
}

void VersionedRandomAllocationSegment::copyPageEntryFromTemp(
    PageId origPageId,
    PageId tempPageId,
    BlockNum iPageInExtent,
    TxnId commitCsn,
    PageOwnerId ownerId)
{
    SegmentAccessor tempAccessor(pTempSegment, pCache);
    VersionedExtentAllocLock tempAllocLock(tempAccessor);
    tempAllocLock.lockExclusive(tempPageId);
    VersionedExtentAllocationNode &tempExtentNode =
        tempAllocLock.getNodeForWrite();
    VersionedPageEntry &tempPageEntry =
        tempExtentNode.getPageEntry(iPageInExtent);

    SegmentAccessor selfAccessor(shared_from_this(), pCache);
    VersionedExtentAllocLock extentAllocLock(selfAccessor);
    extentAllocLock.lockExclusive(origPageId);
    VersionedExtentAllocationNode &extentNode =
        extentAllocLock.getNodeForWrite();
    VersionedPageEntry &pageEntry =
        extentNode.getPageEntry(iPageInExtent);

    // Update the temp page entry's csn if this is a new page allocation.
    // We need to update the temp entry because we may still need to use that
    // temp page.
    if (commitCsn != NULL_TXN_ID) {
        tempPageEntry.allocationCsn = commitCsn;
        tempPageEntry.ownerId = ownerId;
    }
    pageEntry = tempPageEntry;
}

void VersionedRandomAllocationSegment::copyPageEntryToTemp(
    PageId origPageId,
    PageId tempPageId,
    BlockNum iPageInExtent)
{
    SegmentAccessor tempAccessor(pTempSegment, pCache);
    VersionedExtentAllocLock tempAllocLock(tempAccessor);
    tempAllocLock.lockExclusive(tempPageId);
    VersionedExtentAllocationNode &tempExtentNode =
        tempAllocLock.getNodeForWrite();
    VersionedPageEntry &tempPageEntry =
        tempExtentNode.getPageEntry(iPageInExtent);

    SegmentAccessor selfAccessor(shared_from_this(), pCache);
    VersionedExtentAllocLock extentAllocLock(selfAccessor);
    extentAllocLock.lockShared(origPageId);
    VersionedExtentAllocationNode const &extentNode =
        extentAllocLock.getNodeForRead();
    VersionedPageEntry const &pageEntry =
        extentNode.getPageEntry(iPageInExtent);

    tempPageEntry = pageEntry;
}

void VersionedRandomAllocationSegment::updateExtentEntry(
    uint iSegAlloc,
    ExtentNum extentNum,
    uint allocationCount,
    int netAllocations,
    bool commit)
{
    // If page allocations and/or deallocations were made to the page, we need
    // to update the SegmentAllocationNode

    if (allocationCount) {
        PageId segAllocPageId = getSegAllocPageId(iSegAlloc);
        NodeMapConstIter iter = allocationNodeMap.find(segAllocPageId);
        permAssert(iter != allocationNodeMap.end());
        SharedModifiedAllocationNode pModNode = iter->second;

        // Initialize the SegmentAllocationNode if it hasn't been allocated
        // yet
        SegmentAccessor selfAccessor(shared_from_this(), pCache);
        SegAllocLock newSegAllocLock(selfAccessor);
        newSegAllocLock.lockExclusive(segAllocPageId);
        if (!newSegAllocLock.checkMagicNumber()) {
            newSegAllocLock.setMagicNumber();
            SegmentAllocationNode &newNode = newSegAllocLock.getNodeForWrite();
            newNode.nPagesPerExtent = nPagesPerExtent;
            newNode.nExtents = 0;
            newNode.nextSegAllocPageId = NULL_PAGE_ID;
            newSegAllocLock.unlock();
        }

        // Update the permanent page if we're committing.  Otherwise, update
        // the temporary page, reverting the allocations/deallocations.
        SharedSegment allocNodeSegment;
        PageId segPageId;
        if (commit) {
            allocNodeSegment = shared_from_this();
            segPageId = segAllocPageId;
        } else {
            allocNodeSegment = pTempSegment;
            segPageId = pModNode->tempPageId;
        }

        SegmentAccessor segAccessor(allocNodeSegment, pCache);
        SegAllocLock segAllocLock(segAccessor);
        segAllocLock.lockExclusive(segPageId);
        SegmentAllocationNode &segAllocNode = segAllocLock.getNodeForWrite();

        // Allocate new extents if the one we're going to be updating hasn't
        // been allocated yet.  Turn off page mapping so the updates will
        // be made on the permanent pages.
        ExtentNum relativeExtentNum = extentNum % nExtentsPerSegAlloc;
        if (segAllocNode.nExtents < relativeExtentNum + 1) {
            ExtentNum startExtentNum =
                segAllocNode.nExtents + nExtentsPerSegAlloc * iSegAlloc;
            mapPages = false;
            segAllocNode.nExtents = relativeExtentNum + 1;
            formatPageExtentsTemplate<
                    VersionedExtentAllocationNode,
                    VersionedExtentAllocLock,
                    VersionedPageEntry>(
                segAllocNode,
                shared_from_this(),
                startExtentNum);
            mapPages = true;
        }

        SegmentAllocationNode::ExtentEntry &extentEntry =
            segAllocNode.getExtentEntry(relativeExtentNum);
        if (commit) {
            extentEntry.nUnallocatedPages -= netAllocations;
        } else {
            extentEntry.nUnallocatedPages += netAllocations;
        }

        pModNode->updateCount -= allocationCount;
        if (pModNode->updateCount == 0) {
            // Unlock the SegementAllocationNode, in case it's the one we're
            // going to be updating for the deallocation
            segAllocLock.unlock();
            freeTempPage(segAllocPageId, pModNode->tempPageId);
        }
    }
}

void VersionedRandomAllocationSegment::freeTempPage(
    PageId origAllocNodePageId,
    PageId tempAllocNodePageId)
{
    pTempSegment->deallocatePageRange(tempAllocNodePageId, tempAllocNodePageId);
    allocationNodeMap.erase(origAllocNodePageId);
}

FENNEL_END_CPPFILE("$Id$");

// End VersionedRandomAllocationSegment.cpp
