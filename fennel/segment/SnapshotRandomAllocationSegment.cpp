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
#include "fennel/cache/PagePredicate.h"
#include "fennel/segment/SnapshotRandomAllocationSegment.h"
#include "fennel/segment/SegmentFactory.h"
#include "fennel/segment/SegmentAccessor.h"

FENNEL_BEGIN_CPPFILE("$Id$");

SnapshotRandomAllocationSegment::SnapshotRandomAllocationSegment(
    SharedSegment delegateSegment,
    SharedSegment versionedSegment,
    TxnId snapshotCsnInit)
    : DelegatingSegment(delegateSegment)
{
    pVersionedRandomSegment =
        SegmentFactory::dynamicCast<VersionedRandomAllocationSegment *>(
            versionedSegment);
    assert(pVersionedRandomSegment);

    snapshotCsn = snapshotCsnInit;
    needPageFlush = false;
    forceCacheUnmap = false;
}

TxnId SnapshotRandomAllocationSegment::getSnapshotCsn()
{
    return snapshotCsn;
}

BlockId SnapshotRandomAllocationSegment::translatePageId(PageId pageId)
{
    PageId snapshotId = getSnapshotId(pageId);
    return DelegatingSegment::translatePageId(snapshotId);
}

PageId SnapshotRandomAllocationSegment::getSnapshotId(PageId pageId)
{
    StrictMutexGuard mutexGuard(snapshotPageMapMutex);

    // If possible, use the mapping we've previously cached
    PageMapConstIter pSnapshotPageId = snapshotPageMap.find(pageId);
    if (pSnapshotPageId != snapshotPageMap.end()) {
        return pSnapshotPageId->second;
    }

    VersionedPageEntry pageEntry;
    pVersionedRandomSegment->getLatestPageEntryCopy(pageId, pageEntry);
    // Handle the special case where there's no chain
    if (pageEntry.versionChainPageId == pageId) {
        snapshotPageMap[pageId] = pageId;
        return pageId;
    }

    // If we have to walk through the page chain, then we need to be starting
    // from the anchor.  Note that there's no need to acquire the deallocation
    // mutex while walking through the page chain looking for the appropriate
    // snapshot page because we always start at the anchor and walk from
    // newer pages to older pages.  Therefore, we should never try reading
    // the pageEntry for an older page that's going to be deallocated.
    assert(pageId == getAnchorPageId(pageId));
    PageId chainPageId = pageEntry.versionChainPageId;
    do {
        pVersionedRandomSegment->getLatestPageEntryCopy(chainPageId, pageEntry);
        if (snapshotCsn >= pageEntry.allocationCsn) {
            // only consider uncommitted pageEntry's if they correspond to
            // the current txn
            if ((pageEntry.ownerId == UNCOMMITTED_PAGE_OWNER_ID &&
                    snapshotCsn == pageEntry.allocationCsn) ||
                pageEntry.ownerId != UNCOMMITTED_PAGE_OWNER_ID)
            {
                snapshotPageMap[pageId] = chainPageId;
                return chainPageId;
            }
        }
        // permAssert to prevent an infinite loop
        permAssert(chainPageId != pageId);
        chainPageId = pageEntry.versionChainPageId;
    } while (true);
}

PageId SnapshotRandomAllocationSegment::getPageSuccessor(PageId pageId)
{
    PageId snapshotId = getSnapshotId(pageId);
    return DelegatingSegment::getPageSuccessor(snapshotId);
}

void SnapshotRandomAllocationSegment::setPageSuccessor(
    PageId pageId, PageId successorId)
{
    // The successor should be set in the latest page version.  The
    // pageId passed in may correspond to the anchor.
    PageId snapshotId = getSnapshotId(pageId);
    DelegatingSegment::setPageSuccessor(snapshotId, successorId);

    SXMutexExclusiveGuard mapGuard(modPageMapMutex);
    incrPageUpdateCount(
        snapshotId,
        ANON_PAGE_OWNER_ID,
        ModifiedPageEntry::MODIFIED);
}

void SnapshotRandomAllocationSegment::incrPageUpdateCount(
    PageId pageId,
    PageOwnerId ownerId,
    ModifiedPageEntry::ModType modType)
{
    assert(modPageMapMutex.isLocked(LOCKMODE_X));

    needPageFlush = true;

    // Add an entry into the map if it's not there yet.  Otherwise, increment
    // the count for the existing entry.
    ModifiedPageEntryMapIter iter = modPageEntriesMap.find(pageId);
    SharedModifiedPageEntry pModPageEntry;
    if (iter == modPageEntriesMap.end()) {
        pModPageEntry = SharedModifiedPageEntry(new ModifiedPageEntry());
        pModPageEntry->updateCount = 0;
        pModPageEntry->allocationCount = 0;
        pModPageEntry->lastModType = modType;
        pModPageEntry->ownerId = ownerId;
    } else {
        pModPageEntry = iter->second;
        // Once we've deallocated a page, we should not be reusing the
        // pageId
        assert(pModPageEntry->lastModType != ModifiedPageEntry::DEALLOCATED);
    }

    // Update counts corresponding to updates to the SegmentAllocationNode
    if (modType == ModifiedPageEntry::ALLOCATED) {
        pModPageEntry->allocationCount++;
        assert(pModPageEntry->allocationCount <= 1);
        pModPageEntry->lastModType = modType;
        pModPageEntry->ownerId = ownerId;
    } else if (modType == ModifiedPageEntry::DEALLOCATED) {
        pModPageEntry->lastModType = modType;
    }

    // Update count corresponding to the VersionedExtentAllocationNode
    pModPageEntry->updateCount++;

    if (iter == modPageEntriesMap.end()) {
        modPageEntriesMap.insert(
            ModifiedPageEntryMap::value_type(pageId, pModPageEntry));
    }
}

PageId SnapshotRandomAllocationSegment::getAnchorPageId(PageId snapshotId)
{
    // Acquire the deallocation mutex to prevent the page chain from being
    // modified while we're walking through it
    SXMutexSharedGuard(pVersionedRandomSegment->getDeallocationMutex());

    // Walk the page chain to find the anchor, looking for the entry with
    // the minimum allocationCsn.
    PageId chainPageId = snapshotId;
    VersionedPageEntry pageEntry;
    PageId anchorPageId = NULL_PAGE_ID;
    TxnId minCsn = NULL_TXN_ID;
    do {
        pVersionedRandomSegment->getLatestPageEntryCopy(chainPageId, pageEntry);
        if (chainPageId == snapshotId || pageEntry.allocationCsn < minCsn) {
            minCsn = pageEntry.allocationCsn;
            anchorPageId = chainPageId;
        } else if (pageEntry.allocationCsn > minCsn) {
            break;
        }
        chainPageId = pageEntry.versionChainPageId;
    } while (chainPageId != snapshotId);

    return anchorPageId;
}

PageId SnapshotRandomAllocationSegment::allocatePageId(PageOwnerId ownerId)
{
    SXMutexExclusiveGuard mapGuard(modPageMapMutex);

    PageId pageId =
        DelegatingSegment::allocatePageId(UNCOMMITTED_PAGE_OWNER_ID);
    incrPageUpdateCount(pageId, ownerId, ModifiedPageEntry::ALLOCATED);
    pVersionedRandomSegment->initPageEntry(
        pageId,
        pageId,
        snapshotCsn);
    incrPageUpdateCount(
        pageId,
        ANON_PAGE_OWNER_ID,
        ModifiedPageEntry::MODIFIED);
    return pageId;
}

void SnapshotRandomAllocationSegment::deallocatePageRange(
    PageId startPageId,
    PageId endPageId)
{
    permAssert(startPageId != NULL_PAGE_ID);
    permAssert(startPageId == endPageId);

    SXMutexExclusiveGuard mapGuard(modPageMapMutex);
    StrictMutexGuard mutexGuard(snapshotPageMapMutex);

    // Mark the pages in the page chain as deallocation-deferred.  The actual
    // deallocation of these pages will be done by an ALTER SYSTEM DEALLOCATE
    // OLD.

    // Note that we cannot discard snapshot pages from cache because they
    // really haven't been freed yet and still may be referenced by other
    // threads.  The pages will be removed from the cache when they are
    // actually freed.
    
    PageId chainPageId = startPageId;
    VersionedPageEntry pageEntry;
    do {
        pVersionedRandomSegment->getLatestPageEntryCopy(chainPageId, pageEntry);
        DelegatingSegment::deallocatePageRange(chainPageId, chainPageId);
        incrPageUpdateCount(
            chainPageId,
            ANON_PAGE_OWNER_ID,
            ModifiedPageEntry::DEALLOCATED);
        snapshotPageMap.erase(chainPageId);

        chainPageId = pageEntry.versionChainPageId;
    } while (chainPageId != startPageId);
}

PageId SnapshotRandomAllocationSegment::updatePage(
    PageId pageId,
    bool needsTranslation)
{
    PageId anchorPageId;
    PageId snapshotId;
    PageOwnerId ownerId;

    // If the snapshot page is newly allocated, then we can update the page
    // in-place.  The page we would have updated should be the page chained
    // from the anchor, which corresponds to the latest version of the page.
    // That, in turn, should correspond to the snapshot we would have read.

    if (needsTranslation) {
        assert(pageId == getAnchorPageId(pageId));
        VersionedPageEntry pageEntry;
        pVersionedRandomSegment->getLatestPageEntryCopy(pageId, pageEntry);
        assert(pageEntry.versionChainPageId == getSnapshotId(pageId));
        if (isPageNewlyAllocated(pageEntry.versionChainPageId)) {
            return NULL_PAGE_ID;
        }

        anchorPageId = pageId;
        snapshotId = pageEntry.versionChainPageId;
        ownerId = pageEntry.ownerId;

    } else {
        if (isPageNewlyAllocated(pageId)) {
            return NULL_PAGE_ID;
        }
        VersionedPageEntry pageEntry;
        anchorPageId = getAnchorPageId(pageId);
        pVersionedRandomSegment->getLatestPageEntryCopy(
            anchorPageId,
            pageEntry);
        assert(pageEntry.versionChainPageId == pageId);
        assert(pageId == getSnapshotId(anchorPageId));

        snapshotId = pageId;
        ownerId = pageEntry.ownerId;
    }

    // Otherwise, this is the first time we're modifying the page.
    // Allocate a new page and chain it into the existing version chain.
    // Also set the successor page on the new page to the successor set
    // on the current snapshot page.

    PageId newPageId = allocatePageId(ownerId);

    SXMutexExclusiveGuard mapGuard(modPageMapMutex);

    VersionedPageEntry pageEntry;
    pVersionedRandomSegment->getLatestPageEntryCopy(snapshotId, pageEntry);
    chainPageEntries(
        newPageId,
        snapshotId,
        pageEntry.successorId);
    chainPageEntries(anchorPageId, newPageId, NULL_PAGE_ID);

    // Store a mapping of the new page to itself so when we later need to
    // update that page, our cached mapping will tell us to directly
    // access that page.
    StrictMutexGuard mutexGuard(snapshotPageMapMutex);
    snapshotPageMap[newPageId] = newPageId;
    snapshotPageMap[anchorPageId] = newPageId;
    return newPageId;
}

void SnapshotRandomAllocationSegment::chainPageEntries(
    PageId pageId,
    PageId versionChainPageId,
    PageId successorId)
{
    pVersionedRandomSegment->chainPageEntries(
        pageId,
        versionChainPageId,
        successorId);
    incrPageUpdateCount(
        pageId,
        ANON_PAGE_OWNER_ID,
        ModifiedPageEntry::MODIFIED);
}

bool SnapshotRandomAllocationSegment::isPageNewlyAllocated(PageId pageId)
{
    SXMutexSharedGuard mapGuard(modPageMapMutex);

    ModifiedPageEntryMapIter iter = modPageEntriesMap.find(pageId);
    if (iter != modPageEntriesMap.end()) {
        SharedModifiedPageEntry pModPageEntry = iter->second;
        if (pModPageEntry->lastModType == ModifiedPageEntry::ALLOCATED) {
            return true;
        }
    }
    return false;
}

void SnapshotRandomAllocationSegment::commitChanges(TxnId commitCsn)
{
    SXMutexExclusiveGuard mapGuard(modPageMapMutex);

    pVersionedRandomSegment->updateAllocNodes(
        modPageEntriesMap,
        commitCsn,
        true,
        getTracingSegment());
    modPageEntriesMap.clear();
    snapshotPageMap.clear(); 
}

void SnapshotRandomAllocationSegment::rollbackChanges()
{
    SXMutexExclusiveGuard mapGuard(modPageMapMutex);

    pVersionedRandomSegment->updateAllocNodes(
        modPageEntriesMap,
        NULL_TXN_ID,
        false,
        getTracingSegment());
    modPageEntriesMap.clear();
    snapshotPageMap.clear(); 
}

MappedPageListener *SnapshotRandomAllocationSegment::getMappedPageListener(
    BlockId blockId)
{
    PageId snapshotId = translateBlockId(blockId);
    if (isPageNewlyAllocated(snapshotId)) {
        return this;
    } else {
        return pVersionedRandomSegment;
    }
}

void SnapshotRandomAllocationSegment::setForceCacheUnmap()
{
    forceCacheUnmap = true;
}

void SnapshotRandomAllocationSegment::delegatedCheckpoint(
    Segment &delegatingSegment,
    CheckpointType checkpointType)
{
    // Execute the checkpoint only if we have dirty pages or if we're in a
    // mode where we need to execute the checkpoint to discard old entries from
    // the cache.
    //
    // Note that we need to define this method to avoid calling
    // delegatedCheckpoint on the underlying VersionedRandomAllocationSegment.
    if (needPageFlush ||
        (checkpointType == CHECKPOINT_FLUSH_AND_UNMAP && forceCacheUnmap))
    {
        MappedPageListenerPredicate pagePredicate(delegatingSegment);
        pCache->checkpointPages(pagePredicate,checkpointType);
        needPageFlush = false;
    }
}

MappedPageListener
*SnapshotRandomAllocationSegment::notifyAfterPageCheckpointFlush(
    CachePage &page)
{
    return pVersionedRandomSegment;
}

bool SnapshotRandomAllocationSegment::canFlushPage(CachePage &page)
{
    // We can always flush snapshot pages since dirty snapshot pages are
    // always new so we don't have to worry about flushing corresponding
    // log pages
    return true;
}

void SnapshotRandomAllocationSegment::notifyPageDirty(
    CachePage &page,
    bool bDataValid)
{
    // Since snapshot pages are always new, we only need to call notifyPageDirty
    // when the page is first allocated.  That is indicated by bDataValid being
    // false.  bDataValid will be passed in as true in the case where we're
    // copying an existing page into a newly allocated snapshot page.
    // Since we've already called notifyPageDirty on the newly allocated
    // snapshot page, there's no need to call it again.
    if (!bDataValid) {
        DelegatingSegment::notifyPageDirty(page, bDataValid);
    }
}

void SnapshotRandomAllocationSegment::versionPage(
    PageId destAnchorPageId,
    PageId srcAnchorPageId)
{
    assert(destAnchorPageId == getAnchorPageId(destAnchorPageId));
    assert(srcAnchorPageId == getAnchorPageId(srcAnchorPageId));

    VersionedPageEntry pageEntry;
    pVersionedRandomSegment->getLatestPageEntryCopy(
        destAnchorPageId,
        pageEntry);
    TxnId largestDestCsn = pageEntry.allocationCsn;
    if (pageEntry.versionChainPageId != destAnchorPageId) {
        pVersionedRandomSegment->getLatestPageEntryCopy(
            pageEntry.versionChainPageId,
            pageEntry);
        largestDestCsn = pageEntry.allocationCsn;
    }

    SXMutexExclusiveGuard mapGuard(modPageMapMutex);

    // Link the destination anchor to the page following the source anchor,
    // and then link the source anchor to the original page following the 
    // destination anchor.
    pVersionedRandomSegment->getLatestPageEntryCopy(
        srcAnchorPageId,
        pageEntry);
    assert(pageEntry.allocationCsn >= largestDestCsn);
    PageId pageIdAfterSrcAnchor = pageEntry.versionChainPageId;
    pVersionedRandomSegment->getLatestPageEntryCopy(
        destAnchorPageId,
        pageEntry);
    PageId pageIdAfterDestAnchor = pageEntry.versionChainPageId;

    chainPageEntries(destAnchorPageId, pageIdAfterSrcAnchor, NULL_PAGE_ID);
    chainPageEntries(srcAnchorPageId, pageIdAfterDestAnchor, NULL_PAGE_ID);

    StrictMutexGuard mutexGuard(snapshotPageMapMutex);
    snapshotPageMap[destAnchorPageId] = pageIdAfterSrcAnchor;
}

bool SnapshotRandomAllocationSegment::isWriteVersioned()
{
    return true;
}

FENNEL_END_CPPFILE("$Id$");

// End SnapshotRandomAllocationSegment.cpp
