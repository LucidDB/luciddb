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
}

void VersionedRandomAllocationSegment::formatPageExtents(
    SegmentAllocationNode &segAllocNode,
    ExtentNum &extentNum)
{
    formatPageExtentsTemplate<
            VersionedExtentAllocationNode,
            VersionedExtentAllocLock,
            VersionedPageEntry>(
        segAllocNode,
        extentNum);
}

PageId VersionedRandomAllocationSegment::allocatePageId(PageOwnerId ownerId)
{
    return allocatePageIdFromSegment(ownerId, pTempSegment);
}

PageId VersionedRandomAllocationSegment::getSegAllocPageIdForWrite(
    PageId origSegAllocPageId)
{
    return getTempAllocNodePage<SegAllocLock>(origSegAllocPageId);
}

void VersionedRandomAllocationSegment::undoSegAllocPageWrite(
    PageId segAllocPageId)
{
    SXMutexExclusiveGuard mapGuard(mapMutex);

    NodeMapConstIter iter = allocationNodeMap.find(segAllocPageId);
    assert(iter != allocationNodeMap.end());
    SharedModifiedAllocationNode pModAllocNode = iter->second;
    pModAllocNode->updateCount--;
}

PageId VersionedRandomAllocationSegment::getSegAllocPageIdForRead(
    PageId origSegAllocPageId,
    SharedSegment &allocNodeSegment)
{
    return findAllocPageIdForRead(origSegAllocPageId, allocNodeSegment);
}

PageId VersionedRandomAllocationSegment::getExtAllocPageIdForRead(
    ExtentNum extentNum,
    SharedSegment &allocNodeSegment)
{
    return
        findAllocPageIdForRead(
            getExtentAllocPageId(extentNum),
            allocNodeSegment);
}

PageId VersionedRandomAllocationSegment::findAllocPageIdForRead(
    PageId origAllocNodePageId,
    SharedSegment &allocNodeSegment)
{
    // If the allocation node corresponding to the desired page has been
    // modified, it will be in our map.  If so, retrieve the pageId
    // corresponding to the modified allocation node, and access that
    // page from the temporary segment.  Otherwise, access the allocation
    // node from permanent storage.
    //
    // Note that this method assumes that the caller has already acquired a
    // shared mutex on the allocationNodeMap.

    PageId tempAllocNodePageId;
    NodeMapConstIter iter = allocationNodeMap.find(origAllocNodePageId);
    if (iter == allocationNodeMap.end()) {
        tempAllocNodePageId = origAllocNodePageId;
        allocNodeSegment = getTracingSegment();
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
    assert(startPageId == endPageId);

    if (startPageId == NULL_PAGE_ID) {
        format();
    } else {

        // Acquire the exclusive mutex to prevent another thread from trying
        // to do the actual free of the same page, if it's an old page.
        SXMutexExclusiveGuard deallocationGuard(deallocationMutex);

        // Simply mark the page as deallocation-deferred.  The actual
        // deallocation will be done by calls to deallocateOldPages().
        deferDeallocation(startPageId);
    }
}

void VersionedRandomAllocationSegment::deferDeallocation(PageId pageId)
{
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
    VersionedPageEntry &pageEntry = extentNode.getPageEntry(iPageInExtent);
    assert(
        pageEntry.ownerId != UNALLOCATED_PAGE_OWNER_ID &&
        !isDeallocatedPageOwnerId(pageEntry.ownerId));
    // Set the deallocation txnId to an arbitrary value, for now.  It will
    // get overwritten with a real txnId at commit time.
    pageEntry.ownerId = makeDeallocatedPageOwnerId(TxnId(0));
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
        iPageInExtent);
}

void VersionedRandomAllocationSegment::markPageEntryUnused(
    PageEntry &pageEntry)
{
    RandomAllocationSegmentBase::markPageEntryUnused(pageEntry);

    VersionedPageEntry *pVersionedPageEntry =
        reinterpret_cast<VersionedPageEntry *>(&pageEntry);
    pVersionedPageEntry->versionChainPageId = NULL_PAGE_ID;
    pVersionedPageEntry->allocationCsn = NULL_TXN_ID;
}

PageId VersionedRandomAllocationSegment::getPageSuccessor(PageId pageId)
{
    VersionedPageEntry pageEntry;

    getLatestPageEntryCopy(pageId, pageEntry);
    return pageEntry.successorId;
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

bool VersionedRandomAllocationSegment::isPageIdAllocated(PageId pageId)
{
    return testPageId(pageId, true, false);
}

bool VersionedRandomAllocationSegment::isPageIdAllocateCommitted(PageId pageId)
{
    return testPageId(pageId, true, true);
}

bool VersionedRandomAllocationSegment::isPageIdValid(PageId pageId)
{
    return testPageId(pageId, false, false);
}

PageId VersionedRandomAllocationSegment::getExtAllocPageIdForWrite(
    ExtentNum extentNum)
{
    return
        getTempAllocNodePage<VersionedExtentAllocLock>(
            getExtentAllocPageId(extentNum));
}

PageOwnerId VersionedRandomAllocationSegment::getPageOwnerId(
    PageId pageId,
    bool thisSegment)
{
    return getPageOwnerIdTemplate<VersionedPageEntry>(pageId, thisSegment);
}

void VersionedRandomAllocationSegment::getPageEntryCopy(
    PageId pageId,
    PageEntry &pageEntryCopy,
    bool isAllocated,
    bool thisSegment)
{
    // We need to get a copy of the page entry rather than a reference
    // because the page entry may originate from a temporary page, which
    // can be freed by another thread.  By holding the mutex on the
    // allocationNodeMap while we're retrieivng the copy, we're ensured that
    // the page cannot be freed until we exit this method.
    SXMutexSharedGuard mapGuard(mapMutex);

    VersionedPageEntry *pVersionedPageEntry =
        reinterpret_cast<VersionedPageEntry *>(&pageEntryCopy);
    getPageEntryCopyTemplate<
            VersionedExtentAllocationNode,
            VersionedExtentAllocLock,
            VersionedPageEntry>(
        pageId,
        *pVersionedPageEntry,
        isAllocated,
        thisSegment);
}

void VersionedRandomAllocationSegment::getLatestPageEntryCopy(
    PageId pageId,
    VersionedPageEntry &pageEntryCopy)
{
    getPageEntryCopy(pageId, pageEntryCopy, true, false);
}

void VersionedRandomAllocationSegment::getCommittedPageEntryCopy(
    PageId pageId,
    VersionedPageEntry &pageEntryCopy)
{
    getPageEntryCopy(pageId, pageEntryCopy, true, true);
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
    if (successorId != NULL_PAGE_ID) {
        pageEntry.successorId = successorId;
    }
    assert(versionChainId != NULL_PAGE_ID);
    pageEntry.versionChainPageId = versionChainId;
}

void VersionedRandomAllocationSegment::updateAllocNodes(
    ModifiedPageEntryMap const &modifiedPageEntryMap,
    TxnId commitCsn,
    bool commit)
{
    SXMutexExclusiveGuard mapGuard(mapMutex);

    for (ModifiedPageEntryMapIter iter = modifiedPageEntryMap.begin();
        iter != modifiedPageEntryMap.end();
        iter++)
    {
        PageId pageId = iter->first;
        SharedModifiedPageEntry pModEntry = iter->second;

        assert(isPageIdAllocated(pageId));
 
        ExtentNum extentNum;
        BlockNum iPageInExtent;
        uint iSegAlloc;
        splitPageId(pageId, iSegAlloc, extentNum, iPageInExtent);
        assert(iPageInExtent);

        // First make sure the page/extent entry we're going to be updating
        // has been allocated
        allocateAllocNodes(iSegAlloc, NULL_PAGE_ID, extentNum);

        // If we're reducing the free page count, update the extent entry
        // first.  Otherwise, update the page entry first.
        if ((commit &&
               pModEntry->lastModType == ModifiedPageEntry::ALLOCATED) ||
           (!commit &&
               pModEntry->lastModType == ModifiedPageEntry::DEALLOCATED))
        {
            updateExtentEntry(
                iSegAlloc,
                extentNum,
                pModEntry->allocationCount,
                commit);
            updatePageEntry(
                pageId,
                extentNum,
                iPageInExtent,
                pModEntry,
                commitCsn,
                commit);
        } else if (
            (!commit &&
                pModEntry->lastModType == ModifiedPageEntry::ALLOCATED) ||
            (commit &&
                pModEntry->lastModType == ModifiedPageEntry::DEALLOCATED))
        {
            updatePageEntry(
                pageId,
                extentNum,
                iPageInExtent,
                pModEntry,
                commitCsn,
                commit);
            updateExtentEntry(
                iSegAlloc,
                extentNum,
                pModEntry->allocationCount,
                commit);
        } else {
            updatePageEntry(
                pageId,
                extentNum,
                iPageInExtent,
                pModEntry,
                commitCsn,
                commit);
        }
    }

    // Deallocate any temp allocation node pages that no longer contain
    // any uncommitted updates
    ModifiedAllocationNodeMap::iterator iter = allocationNodeMap.begin();
    while (iter != allocationNodeMap.end()) {
        SharedModifiedAllocationNode pModNode = iter->second;
        if (pModNode->updateCount == 0) {
            PageId pageId = iter->first;
            iter++;
            freeTempPage(pageId, pModNode->tempPageId);
            allocationNodeMap.erase(pageId);
        } else {
            iter++;
        }
    }
}

void VersionedRandomAllocationSegment::updatePageEntry(
    PageId pageId,
    ExtentNum extentNum,
    uint iPageInExtent,
    SharedModifiedPageEntry pModEntry,
    TxnId commitCsn,
    bool commit)
{
    // Update the extent allocation page, copying the contents from the
    // temporary page in the case of a commit and vice versa for a rollback.
    
    PageId extentPageId = getExtentAllocPageId(extentNum);
    NodeMapConstIter iter = allocationNodeMap.find(extentPageId);
    assert(iter != allocationNodeMap.end());
    SharedModifiedAllocationNode pModNode = iter->second;

    if (commit) {
        copyPageEntryFromTemp(
            pageId,
            extentPageId,
            pModNode->tempPageId,
            iPageInExtent,
            pModEntry->lastModType,
            commitCsn,
            pModEntry->ownerId);
    } else {
        copyPageEntryToTemp(extentPageId, pModNode->tempPageId, iPageInExtent);
    }

    pModNode->updateCount -= pModEntry->updateCount;
}

void VersionedRandomAllocationSegment::copyPageEntryFromTemp(
    PageId pageId,
    PageId origPageId,
    PageId tempPageId,
    BlockNum iPageInExtent,
    ModifiedPageEntry::ModType lastModType,
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

    SegmentAccessor selfAccessor(getTracingSegment(), pCache);
    VersionedExtentAllocLock extentAllocLock(selfAccessor);
    extentAllocLock.lockExclusive(origPageId);
    VersionedExtentAllocationNode &extentNode =
        extentAllocLock.getNodeForWrite();
    VersionedPageEntry &pageEntry =
        extentNode.getPageEntry(iPageInExtent);

    // Update the temp page entry's csn and ownerId if this is a new page
    // allocation.  We need to update the temp entry because we may still
    // need to use that temp page.
    if (lastModType == ModifiedPageEntry::ALLOCATED) {
        assert(tempPageEntry.ownerId == UNCOMMITTED_PAGE_OWNER_ID);
        tempPageEntry.allocationCsn = commitCsn;
        tempPageEntry.ownerId = ownerId;
    } else if (lastModType == ModifiedPageEntry::DEALLOCATED) {
        assert(isDeallocatedPageOwnerId(tempPageEntry.ownerId));
        tempPageEntry.ownerId = makeDeallocatedPageOwnerId(commitCsn);
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

    SegmentAccessor selfAccessor(getTracingSegment(), pCache);
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
    bool commit)
{
    // If the page was newly allocated, we need to update the
    // SegmentAllocationNode

    if (allocationCount) {

        // Update the permanent page if we're committing.  Otherwise, update
        // the temporary page, reverting the allocations/deallocations.
        PageId segAllocPageId = getSegAllocPageId(iSegAlloc);
        NodeMapConstIter iter = allocationNodeMap.find(segAllocPageId);
        assert(iter != allocationNodeMap.end());
        SharedModifiedAllocationNode pModNode = iter->second;
        SharedSegment allocNodeSegment;
        PageId segPageId;
        if (commit) {
            allocNodeSegment = getTracingSegment();
            segPageId = segAllocPageId;
        } else {
            allocNodeSegment = pTempSegment;
            segPageId = pModNode->tempPageId;
        }

        SegmentAccessor segAccessor(allocNodeSegment, pCache);
        SegAllocLock segAllocLock(segAccessor);
        segAllocLock.lockExclusive(segPageId);
        SegmentAllocationNode &segAllocNode = segAllocLock.getNodeForWrite();

        ExtentNum relativeExtentNum = extentNum % nExtentsPerSegAlloc;
        SegmentAllocationNode::ExtentEntry &extentEntry =
            segAllocNode.getExtentEntry(relativeExtentNum);
        if (commit) {
            extentEntry.nUnallocatedPages -= allocationCount;
        } else {
            extentEntry.nUnallocatedPages += allocationCount;
        }

        pModNode->updateCount -= allocationCount;
    }
}

void VersionedRandomAllocationSegment::allocateAllocNodes(
    uint iSegAlloc,
    PageId nextPageId,
    ExtentNum extentNum)
{
    SegmentAccessor selfAccessor(getTracingSegment(), pCache);
    SegAllocLock segAllocLock(selfAccessor);
    PageId segAllocPageId = getSegAllocPageId(iSegAlloc);
    segAllocLock.lockExclusive(segAllocPageId);
    if (segAllocLock.checkMagicNumber()) {

        // If the SegmentAllocationNode has already been allocated and this
        // is the first call to this method, check if we need to allocate
        // VersionedExtentAllocationNodes.  Otherwise, set the
        // nextSegAllocPageId.

        SegmentAllocationNode &node = segAllocLock.getNodeForWrite();
        if (nextPageId == NULL_PAGE_ID) {
            allocateExtAllocNodes(node, iSegAlloc, extentNum);
        } else {
            node.nextSegAllocPageId = nextPageId;
        }
    } else {

        // Allocate a new page and then recursively call this method to set
        // the nextSegAllocPageId on the predecessor SegmentAllocationNode
        // to the newly allocated page, allocating that SegmentAllocationNode
        // if it also hasn't been allocated.  If this is the first call to this
        // method, check if we need to allocate
        // VersionedExtentAllocationNodes.

        permAssert(iSegAlloc >= 1);
        segAllocLock.setMagicNumber();
        SegmentAllocationNode &newNode = segAllocLock.getNodeForWrite();
        // -1 for the extent allocation node itself
        newNode.nPagesPerExtent = nPagesPerExtent - 1;
        newNode.nExtents = 0;
        newNode.nextSegAllocPageId = nextPageId;
        allocateAllocNodes(iSegAlloc - 1, segAllocPageId, extentNum);
        if (nextPageId == NULL_PAGE_ID) {
            allocateExtAllocNodes(newNode, iSegAlloc, extentNum);
        }
    }
}

void VersionedRandomAllocationSegment::allocateExtAllocNodes(
    SegmentAllocationNode &segAllocNode,
    uint iSegAlloc,
    ExtentNum extentNum)
{
    // Allocate new extents if the one we're going to be updating hasn't
    // been allocated yet.  Turn off page mapping so the updates will
    // be made on the permanent pages.
    ExtentNum relativeExtentNum = extentNum % nExtentsPerSegAlloc;
    if (segAllocNode.nExtents < relativeExtentNum + 1) {
        ExtentNum startExtentNum =
            segAllocNode.nExtents + nExtentsPerSegAlloc * iSegAlloc;
        segAllocNode.nExtents = relativeExtentNum + 1;
        formatPageExtentsTemplate<
                VersionedExtentAllocationNode,
                VersionedExtentAllocLock,
                VersionedPageEntry>(
            segAllocNode,
            startExtentNum);
    }
}

bool VersionedRandomAllocationSegment::validateFreePageCount(PageId pageId)
{
    ExtentNum extentNum;
    BlockNum iPageInExtent;
    uint iSegAlloc;
    splitPageId(pageId, iSegAlloc, extentNum, iPageInExtent);
    assert(iPageInExtent);

    PageId segAllocPageId = getSegAllocPageId(iSegAlloc);
    SegmentAccessor selfAccessor(getTracingSegment(), pCache);
    SegAllocLock segAllocLock(selfAccessor);
    segAllocLock.lockShared(segAllocPageId);
    SegmentAllocationNode const &segAllocNode = segAllocLock.getNodeForRead();

    ExtentNum relativeExtentNum = extentNum % nExtentsPerSegAlloc;
    SegmentAllocationNode::ExtentEntry const &extentEntry =
        segAllocNode.getExtentEntry(relativeExtentNum);

    VersionedExtentAllocLock extentAllocLock(selfAccessor);
    PageId extentPageId = getExtentAllocPageId(extentNum);
    extentAllocLock.lockExclusive(extentPageId);
    VersionedExtentAllocationNode const &extentNode =
        extentAllocLock.getNodeForRead();
    uint freePageCount = 0;
    for (uint i = 0; i < nPagesPerExtent; i++) {
        VersionedPageEntry const &pageEntry = extentNode.getPageEntry(i);
        if (pageEntry.ownerId == UNALLOCATED_PAGE_OWNER_ID) {
            freePageCount++;
        }
    }

    bool rc = (freePageCount == extentEntry.nUnallocatedPages);
    return rc;
}

void VersionedRandomAllocationSegment::freeTempPage(
    PageId origAllocNodePageId,
    PageId tempAllocNodePageId)
{
    pTempSegment->deallocatePageRange(tempAllocNodePageId, tempAllocNodePageId);
    allocationNodeMap.erase(origAllocNodePageId);
}

BlockNum VersionedRandomAllocationSegment::getAllocatedSizeInPages()
{
    // Since we will be accessing SegmentAllocationNode pages, we need to
    // acquire a mutex on the allocationNodeMap.
    SXMutexSharedGuard mapGuard(mapMutex);

    return RandomAllocationSegmentBase::getAllocatedSizeInPages();
}

bool VersionedRandomAllocationSegment::getOldPageIds(
    uint &iSegAlloc,
    ExtentNum &extentNum,
    TxnId oldestActiveTxnId,
    uint numPages,
    PageSet &oldPageSet)
{
    uint numOldPages = 0;
    SegmentAccessor selfAccessor(getTracingSegment(), pCache);
    SegAllocLock segAllocLock(selfAccessor);

    while (numOldPages < numPages) {

        PageId segAllocPageId = getSegAllocPageId(iSegAlloc);
        segAllocLock.lockShared(segAllocPageId);
        SegmentAllocationNode const &segAllocNode =
            segAllocLock.getNodeForRead();
        ExtentNum relativeExtentNum = extentNum % nExtentsPerSegAlloc;

        for (uint i = relativeExtentNum; i < segAllocNode.nExtents;
            i++, extentNum++)
        {
            if (numOldPages >= numPages) {
                // Wait until we've reached an extent boundary before
                // returning
                return true;
            }

            SegmentAllocationNode::ExtentEntry const &extentEntry =
                segAllocNode.getExtentEntry(i);
            if (extentEntry.nUnallocatedPages == nPagesPerExtent - 1) {
                continue;
            }

            VersionedExtentAllocLock extentAllocLock(selfAccessor);
            extentAllocLock.lockShared(getExtentAllocPageId(extentNum));
            VersionedExtentAllocationNode const &extentNode =
                extentAllocLock.getNodeForRead();

            // Start at pageEntry 1 to skip past the extent header page,
            // which we can never deallocate
            for (uint j = 1; j < nPagesPerExtent; j++) {

                VersionedPageEntry const &pageEntry = 
                    extentNode.getPageEntry(j);
                if (pageEntry.ownerId == UNALLOCATED_PAGE_OWNER_ID) {
                    continue;
                }

                // Map the pageEntry to its pageId
                PageId pageId = getLinearPageId(makePageNum(extentNum, j));

                // Skip over pages that have no snapshots
                if (pageEntry.versionChainPageId == pageId) {
                    continue;
                }

                // Only consider deallocation-deferred pages if there are no
                // longer any active txns that might be accessing the pages
                if ((!isDeallocatedPageOwnerId(pageEntry.ownerId) &&
                        pageEntry.allocationCsn < oldestActiveTxnId) ||
                    (isDeallocatedPageOwnerId(pageEntry.ownerId) &&
                        getDeallocatedTxnId(pageEntry.ownerId) <
                            oldestActiveTxnId))
                {
                    ++numOldPages;
                    oldPageSet.insert(oldPageSet.end(), pageId);
                }
            }
        }
        ++iSegAlloc;
        if (segAllocNode.nextSegAllocPageId == NULL_PAGE_ID) {
            return false;
        }
    }

    return true;
}

void VersionedRandomAllocationSegment::deallocateOldPages(
    PageSet const &oldPageSet,
    TxnId oldestActiveTxnId)
{
    SXMutexExclusiveGuard mapGuard(mapMutex);
    SXMutexExclusiveGuard deallocationGuard(deallocationMutex);

    std::hash_set<PageId> deallocatedPageSet;
    for (PageSetConstIter pageIter = oldPageSet.begin();
        pageIter != oldPageSet.end(); pageIter++)
    {
        PageId pageId = *pageIter;

        // Skip over pages that have already been deallocated while walking
        // through the page chain of a previous page
        if (deallocatedPageSet.find(pageId) != deallocatedPageSet.end()) {
            continue;
        }
        // Skip over pages that may have been deallocated by another
        // thread calling deallocateOldPages().
        if (!isPageIdAllocateCommitted(pageId)) {
            deallocatedPageSet.insert(pageId);
            continue;
        }

        // Determine the txnId corresponding to the oldest page in the page
        // chain that can be deallocated.  If no pages can be deallocated,
        // then skip the page.  If the page is marked as deallocation-deferred,
        // deallocate the entire page chain.
        PageId anchorPageId;
        bool deallocateChain;
        TxnId deallocationCsn =
            getOldestTxnId(
                pageId,
                oldestActiveTxnId,
                anchorPageId,
                deallocatedPageSet,
                deallocateChain);
        if (deallocateChain) {
            deallocateEntirePageChain(
                pageId,
                oldestActiveTxnId,
                deallocatedPageSet);
            continue;
        }
        if (deallocationCsn == NULL_TXN_ID) {
            continue;
        }

        // Deallocate all pages following the anchor that are older than
        // deallocationCsn
        deallocatePageChain(anchorPageId, deallocationCsn, deallocatedPageSet);
    }
}

TxnId VersionedRandomAllocationSegment::getOldestTxnId(
    PageId pageId,
    TxnId oldestActiveTxnId,
    PageId &anchorPageId,
    std::hash_set<PageId> &deallocatedPageSet,
    bool &deallocateChain)
{
    // Loop through the page chain, looking for the anchor page, and the
    // second, newest old page.  The second, newest old page will be the newest
    // page that can be deallocated.  Note that we can't deallocate the newest
    // old page because it's still being referenced by active txns.

    PageId chainPageId = pageId;
    TxnId anchorCsn = NULL_TXN_ID;
    TxnId newestOldCsn = NULL_TXN_ID;
    TxnId nextNewestOldCsn = NULL_TXN_ID;
    anchorPageId = NULL_PAGE_ID;
    PageId newestOldPageId = NULL_PAGE_ID;
    PageId nextNewestOldPageId = NULL_PAGE_ID;

    deallocateChain = false;
    do {
        if (deallocatedPageSet.find(chainPageId) != deallocatedPageSet.end()) {
            return NULL_TXN_ID;
        }
        assert(isPageIdAllocateCommitted(chainPageId));

        ExtentNum extentNum;
        BlockNum iPageInExtent;
        uint iSegAlloc;
        splitPageId(chainPageId, iSegAlloc, extentNum, iPageInExtent);
        assert(iPageInExtent);
        
        SegmentAccessor selfAccessor(getTracingSegment(), pCache);
        PageId extentPageId = getExtentAllocPageId(extentNum);
        VersionedExtentAllocLock extentAllocLock(selfAccessor);
        extentAllocLock.lockShared(extentPageId);
        VersionedExtentAllocationNode const &extentNode =
            extentAllocLock.getNodeForRead();
        VersionedPageEntry const &pageEntry =
            extentNode.getPageEntry(iPageInExtent);
        assert(pageEntry.ownerId != UNCOMMITTED_PAGE_OWNER_ID);

        // If the page is marked as deallocation-deferred, need to deallocate
        // the entire page chain, if the oldest active txn is newer than the
        // txn that marked the page.  Otherwise, wait until the active txns
        // referencing those pages have committed.
        if (isDeallocatedPageOwnerId(pageEntry.ownerId)) {
            if (getDeallocatedTxnId(pageEntry.ownerId) < oldestActiveTxnId) {
                deallocateChain = true;
            } else {
                skipDeferredDeallocations(pageId, deallocatedPageSet);
            }
            return NULL_TXN_ID;
        }

        if (anchorCsn == NULL_TXN_ID ||
            pageEntry.allocationCsn < anchorCsn) 
        {
            anchorCsn = pageEntry.allocationCsn;
            anchorPageId = chainPageId;
        }
        if (pageEntry.allocationCsn < oldestActiveTxnId) {
            if ((newestOldCsn == NULL_TXN_ID ||
                pageEntry.allocationCsn > newestOldCsn))
            {
                if (newestOldCsn != NULL_TXN_ID) {
                    assert(
                        nextNewestOldCsn == NULL_TXN_ID ||
                        nextNewestOldCsn < newestOldCsn);
                    nextNewestOldCsn = newestOldCsn;
                    nextNewestOldPageId = newestOldPageId;
                }
                newestOldCsn = pageEntry.allocationCsn;
                newestOldPageId = chainPageId;

            // It's possible to have to have two page entries with the same
            // csn if a page is truncated and then versioned within the same
            // transaction.
            } else if ((pageEntry.allocationCsn > nextNewestOldCsn) ||
                (nextNewestOldCsn == NULL_TXN_ID &&
                    pageEntry.allocationCsn != newestOldCsn))
            {
                nextNewestOldCsn = pageEntry.allocationCsn;
                nextNewestOldPageId = chainPageId;
            }
        }
        assert(pageEntry.versionChainPageId != NULL_PAGE_ID);
        chainPageId = pageEntry.versionChainPageId;
    } while (chainPageId != pageId);

    // At least one page in the chain has to be old
    assert(newestOldPageId != NULL_PAGE_ID);
    assert(anchorPageId != NULL_PAGE_ID);

    // If there is no next newest old page, then there's nothing to deallocate
    // in the page chain.  Add the pages we know are old to the
    // deallocatedPageSet so we can directly skip over them if we encounter
    // them again.
    if (nextNewestOldPageId == anchorPageId ||
        nextNewestOldPageId == NULL_PAGE_ID)
    {
        deallocatedPageSet.insert(anchorPageId);
        deallocatedPageSet.insert(pageId);
        deallocatedPageSet.insert(newestOldPageId);
        return NULL_TXN_ID;
    }

    // Set the deallocationCsn so only the next newest old page and any
    // pages older than it will be deallocated
    TxnId deallocationCsn = nextNewestOldCsn + 1;
    assert(deallocationCsn < oldestActiveTxnId);

    return deallocationCsn;
}

void VersionedRandomAllocationSegment::deallocateEntirePageChain(
    PageId pageId,
    TxnId oldestActiveTxnId,
    std::hash_set<PageId> &deallocatedPageSet)
{
    PageId chainPageId = pageId;
    VersionedPageEntry pageEntry;
    do {
        getCommittedPageEntryCopy(chainPageId, pageEntry);

        // All pages in the chain should be marked as deallocation-deferred
        // since we mark them atomically. They also must be old.
        assert(isDeallocatedPageOwnerId(pageEntry.ownerId));
        assert(getDeallocatedTxnId(pageEntry.ownerId) < oldestActiveTxnId);
        assert(pageEntry.allocationCsn < oldestActiveTxnId);

        deallocateSinglePage(chainPageId, deallocatedPageSet);
        chainPageId = pageEntry.versionChainPageId;
    } while (chainPageId != pageId);
}

void VersionedRandomAllocationSegment::deallocateSinglePage(
    PageId pageId,
    std::hash_set<PageId> &deallocatedPageSet)
{
    // Discard the page from the cache
    BlockId blockId = DelegatingSegment::translatePageId(pageId);
    SegmentAccessor selfAccessor(getTracingSegment(), pCache);
    selfAccessor.pCacheAccessor->discardPage(blockId);

    RandomAllocationSegmentBase::deallocatePageRange(pageId, pageId);

    ExtentNum extentNum;
    BlockNum iPageInExtent;
    uint iSegAlloc;
    splitPageId(pageId, iSegAlloc, extentNum, iPageInExtent);
    assert(iPageInExtent);

    // Reflect the changes in the temporary page entry, if it exists
    PageId extentPageId = getExtentAllocPageId(extentNum);
    NodeMapConstIter iter = allocationNodeMap.find(extentPageId);
    if (iter != allocationNodeMap.end()) {
        copyPageEntryToTemp(
            extentPageId,
            iter->second->tempPageId,
            iPageInExtent);
    }

    // Reflect the changes in the temporary extent entry, if it exists
    PageId segAllocPageId = getSegAllocPageId(iSegAlloc);
    iter = allocationNodeMap.find(segAllocPageId);
    if (iter != allocationNodeMap.end()) {
        PageId tempSegAllocNodePageId = iter->second->tempPageId;
        SegmentAccessor segAccessor(pTempSegment, pCache);
        SegAllocLock tempSegAllocLock(segAccessor);
        tempSegAllocLock.lockExclusive(tempSegAllocNodePageId);
        SegmentAllocationNode &tempSegAllocNode =
            tempSegAllocLock.getNodeForWrite();
        ExtentNum relativeExtentNum = extentNum % nExtentsPerSegAlloc;
        SegmentAllocationNode::ExtentEntry &tempExtentEntry =
            tempSegAllocNode.getExtentEntry(relativeExtentNum);
        tempExtentEntry.nUnallocatedPages++;
    }

    deallocatedPageSet.insert(pageId);
}

void VersionedRandomAllocationSegment::deallocatePageChain(
    PageId anchorPageId,
    TxnId deallocationCsn,
    std::hash_set<PageId> &deallocatedPageSet)
{
    ExtentNum extentNum;
    BlockNum iPrevPageInExtent;
    uint iSegAlloc;
    splitPageId(anchorPageId, iSegAlloc, extentNum, iPrevPageInExtent);
    assert(iPrevPageInExtent);

    SegmentAccessor selfAccessor(getTracingSegment(), pCache);
    PageId prevExtentPageId = getExtentAllocPageId(extentNum);
    VersionedExtentAllocLock prevExtentAllocLock(selfAccessor);
    prevExtentAllocLock.lockExclusive(prevExtentPageId);
    VersionedExtentAllocationNode &prevExtentNode =
        prevExtentAllocLock.getNodeForWrite();
    VersionedPageEntry *pPrevPageEntry =
        &(prevExtentNode.getPageEntry(iPrevPageInExtent));
    assert(
        pPrevPageEntry->ownerId != UNALLOCATED_PAGE_OWNER_ID &&
        !isDeallocatedPageOwnerId(pPrevPageEntry->ownerId));
    
    // See if the page is in the process of being marked
    // deallocation-deferred.  If it is, then don't deallocate any of the
    // pages in the page chain, even if they are old.  We'll wait until
    // the deallocation-deferral is actually committed before deallocating
    // them.
    if (uncommittedDeallocation(
        anchorPageId,
        prevExtentPageId,
        iPrevPageInExtent,
        deallocatedPageSet))
    {
        return;
    }

    bool needsUpdate = false;
    PageId nextPageId = pPrevPageEntry->versionChainPageId;
    do {
        VersionedPageEntry pageEntry;
        ExtentNum extentNum;
        BlockNum iPageInExtent;
        uint iSegAlloc;
        splitPageId(nextPageId, iSegAlloc, extentNum, iPageInExtent);
        assert(iPageInExtent);
        PageId extentPageId = getExtentAllocPageId(extentNum);
        getCommittedPageEntryCopy(nextPageId, pageEntry);

        if (pageEntry.allocationCsn < deallocationCsn) {
            
            // Deallocate the page entry
            deallocateSinglePage(nextPageId, deallocatedPageSet);
            nextPageId = pageEntry.versionChainPageId;
            pPrevPageEntry->versionChainPageId = nextPageId;
            needsUpdate = true;

        } else {
            // Reflect the changes made in the previous page entry
            // in the temporary page entry, if it exists
            if (needsUpdate) {
                updateTempPageEntry(
                    prevExtentPageId,
                    iPrevPageInExtent,
                    *pPrevPageEntry);
            }
            needsUpdate = false;

            // Lock the page entry that's now the previous entry
            prevExtentPageId = extentPageId;
            prevExtentAllocLock.lockExclusive(prevExtentPageId);
            VersionedExtentAllocationNode &extentNode =
                prevExtentAllocLock.getNodeForWrite();
            pPrevPageEntry = &(extentNode.getPageEntry(iPageInExtent));
            iPrevPageInExtent = iPageInExtent;

            nextPageId = pageEntry.versionChainPageId;
        }
    } while (nextPageId != anchorPageId);
}

bool VersionedRandomAllocationSegment::uncommittedDeallocation(
    PageId anchorPageId,
    PageId extentPageId,
    BlockNum iPageInExtent,
    std::hash_set<PageId> &deallocatedPageSet)
{
    // See if the page entry corresponding to the anchor is marked as
    // deallocation-deferred in the temporary page entry.  If it is, then
    // that means the txn doing the deallocation has not committed yet.

    NodeMapConstIter iter = allocationNodeMap.find(extentPageId);
    if (iter == allocationNodeMap.end()) {
        return false;
    }

    PageId tempExtentPageId = iter->second->tempPageId;
    SegmentAccessor segAccessor(pTempSegment, pCache);
    VersionedExtentAllocLock tempExtAllocLock(segAccessor);
        tempExtAllocLock.lockShared(tempExtentPageId);
    VersionedExtentAllocationNode const &tempExtentNode =
        tempExtAllocLock.getNodeForRead();
    VersionedPageEntry const &tempPageEntry =
        tempExtentNode.getPageEntry(iPageInExtent);
    if (!isDeallocatedPageOwnerId(tempPageEntry.ownerId)) {
        return false;
    }

    skipDeferredDeallocations(anchorPageId, deallocatedPageSet);
    return true;
}

void VersionedRandomAllocationSegment::skipDeferredDeallocations(
    PageId pageId,
    std::hash_set<PageId> &deallocatedPageSet)
{
    // Add all the pages in the chain to the deallocated page set so we'll 
    // skip over them.  All the other pages in the chain should also be
    // marked as deallocation-deferred.
    PageId chainPageId = pageId;
    VersionedPageEntry pageEntry;
    do {
        deallocatedPageSet.insert(chainPageId);
        getCommittedPageEntryCopy(chainPageId, pageEntry);
        assert(isDeallocatedPageOwnerId(pageEntry.ownerId));
        chainPageId = pageEntry.versionChainPageId;
    } while (chainPageId != pageId);
}

bool VersionedRandomAllocationSegment::validatePageChain(PageId anchorPageId)
{
    PageId chainPageId = anchorPageId;
    VersionedPageEntry pageEntry;
    do {
        getCommittedPageEntryCopy(chainPageId, pageEntry);
        chainPageId = pageEntry.versionChainPageId;
    } while (chainPageId != anchorPageId);

    return true;
}

void VersionedRandomAllocationSegment::updateTempPageEntry(
    PageId extentPageId,
    BlockNum iPageInExtent,
    VersionedPageEntry const &pageEntry)
{
    NodeMapConstIter iter = allocationNodeMap.find(extentPageId);
    if (iter != allocationNodeMap.end()) {
        PageId tempExtentPageId = iter->second->tempPageId;
        SegmentAccessor segAccessor(pTempSegment, pCache);
        VersionedExtentAllocLock tempExtAllocLock(segAccessor);
            tempExtAllocLock.lockExclusive(tempExtentPageId);
        VersionedExtentAllocationNode &tempExtentNode =
            tempExtAllocLock.getNodeForWrite();
        VersionedPageEntry &tempPageEntry =
            tempExtentNode.getPageEntry(iPageInExtent);
        tempPageEntry = pageEntry;
    }
}

FENNEL_END_CPPFILE("$Id$");

// End VersionedRandomAllocationSegment.cpp
