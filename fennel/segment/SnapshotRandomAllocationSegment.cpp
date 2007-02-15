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
#include "fennel/cache/PagePredicate.h"
#include "fennel/segment/SnapshotRandomAllocationSegment.h"
#include "fennel/segment/SegmentFactory.h"

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
    snapshotCsn = snapshotCsnInit;
}

BlockId SnapshotRandomAllocationSegment::translatePageId(PageId pageId)
{
    PageId snapshotId = getSnapshotId(pageId);
    return DelegatingSegment::translatePageId(snapshotId);
}

PageId SnapshotRandomAllocationSegment::getSnapshotId(PageId pageId)
{
    VersionedPageEntry pageEntry;
    pVersionedRandomSegment->getPageEntryCopy(pageId, pageEntry);
    // Handle the special case where there's no chain
    if (pageEntry.versionChainPageId == pageId) {
        return pageId;
    }

    PageId chainPageId = pageEntry.versionChainPageId;
    do {
        pVersionedRandomSegment->getPageEntryCopy(chainPageId, pageEntry);
        if (snapshotCsn >= pageEntry.allocationCsn) {
            // only consider uncommitted pageEntry's if they correspond to
            // the current xact
            if ((pageEntry.ownerId == UNCOMMITTED_PAGE_OWNER_ID &&
                    snapshotCsn == pageEntry.allocationCsn) ||
                pageEntry.ownerId != UNCOMMITTED_PAGE_OWNER_ID)
            {
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
    PageId snapshotId = getSnapshotId(pageId);
    DelegatingSegment::setPageSuccessor(snapshotId, successorId);

    SXMutexExclusiveGuard mapGuard(mutex);
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
    // Add an entry into the map if it's not there yet.  Otherwise, increment
    // the count for the existing entry.
    SnapshotMapConstIter iter = snapshotPageMap.find(pageId);
    SharedModifiedPageEntry pModPageEntry;
    if (iter == snapshotPageMap.end()) {
        pModPageEntry = SharedModifiedPageEntry(new ModifiedPageEntry());
        pModPageEntry->updateCount = 0;
        pModPageEntry->netAllocations = 0;
        pModPageEntry->allocationCount = 0;
        pModPageEntry->ownerId = ownerId;
    } else {
        pModPageEntry = iter->second;
    }

    // Update counts corresponding to updates to the SegmentAllocationNode
    if (modType == ModifiedPageEntry::ALLOCATED) {
        pModPageEntry->netAllocations++;
        pModPageEntry->allocationCount++;
    } else if (modType == ModifiedPageEntry::DEALLOCATED) {
        pModPageEntry->netAllocations--;
        pModPageEntry->allocationCount++;
    }
    permAssert(
        pModPageEntry->netAllocations >= -1 &&
        pModPageEntry->netAllocations <= 1);

    // Update count corresponding to the VersionedExtentAllocationNode
    pModPageEntry->updateCount++;

    snapshotPageMap.insert(SnapshotPageMap::value_type(pageId, pModPageEntry));
}

PageId SnapshotRandomAllocationSegment::translateBlockId(BlockId blockId)
{
    PageId snapshotId = DelegatingSegment::translateBlockId(blockId);
    return getAnchorPageId(snapshotId);
}

PageId SnapshotRandomAllocationSegment::getAnchorPageId(PageId snapshotId)
{
    // Walk the page chain to find the anchor, looking for the entry with
    // the minimum allocationCsn.
    PageId chainPageId = snapshotId;
    VersionedPageEntry pageEntry;
    PageId anchorPageId = NULL_PAGE_ID;
    TxnId minCsn = NULL_TXN_ID;
    do {
        pVersionedRandomSegment->getPageEntryCopy(chainPageId, pageEntry);
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
    SXMutexExclusiveGuard mapGuard(mutex);

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

    // We should only be calling deallocate on anchor pages.
    // Once we've confirmed the page is an anchor, deallocate all pages
    // chained from that anchor.
    assert(getAnchorPageId(startPageId) == startPageId);

    SXMutexExclusiveGuard mapGuard(mutex);

    PageId chainPageId = startPageId;
    VersionedPageEntry pageEntry;
    do {
        pVersionedRandomSegment->getPageEntryCopy(chainPageId, pageEntry);
        DelegatingSegment::deallocatePageRange(
            pageEntry.versionChainPageId,
            pageEntry.versionChainPageId);
        incrPageUpdateCount(
            pageEntry.versionChainPageId,
            ANON_PAGE_OWNER_ID,
            ModifiedPageEntry::DEALLOCATED);
        chainPageId = pageEntry.versionChainPageId;
    } while (chainPageId != startPageId);
}

PageId SnapshotRandomAllocationSegment::updatePage(PageId pageId)
{
    assert(pageId == getAnchorPageId(pageId));

    // If the snapshot page is newly allocated, then we can update the page
    // in-place.  The page we would have updated should be the page chained
    // from the anchor, which corresponds to the latest version of the page.
    // That, in turn, should correspond to the snapshot we would have read.
    
    VersionedPageEntry pageEntry;
    pVersionedRandomSegment->getPageEntryCopy(pageId, pageEntry);
    assert(pageEntry.versionChainPageId == getSnapshotId(pageId));
    if (isPageNewlyAllocated(pageEntry.versionChainPageId)) {
        return NULL_PAGE_ID;
    }

    // Otherwise, this is the first time we're modifying the page.
    // Allocate a new page and chain it into the existing version chain.

    PageId newPageId = allocatePageId(pageEntry.ownerId);
    pVersionedRandomSegment->chainPageEntries(
        newPageId,
        pageEntry.versionChainPageId,
        pageEntry.successorId);

    SXMutexExclusiveGuard mapGuard(mutex);
    incrPageUpdateCount(
        newPageId,
        ANON_PAGE_OWNER_ID,
        ModifiedPageEntry::MODIFIED);
    pVersionedRandomSegment->chainPageEntries(pageId, newPageId, NULL_PAGE_ID);
    incrPageUpdateCount(
        pageId,
        ANON_PAGE_OWNER_ID,
        ModifiedPageEntry::MODIFIED);

    return newPageId;
}

bool SnapshotRandomAllocationSegment::isPageNewlyAllocated(PageId pageId)
{
    SXMutexSharedGuard mapGuard(mutex);

    SnapshotMapConstIter iter =
        snapshotPageMap.find(pageId);
    if (iter != snapshotPageMap.end()) {
        SharedModifiedPageEntry pModPageEntry = iter->second;
        if (pModPageEntry->netAllocations == 1) {
            return true;
        }
    }
    return false;
}

void SnapshotRandomAllocationSegment::commitChanges(TxnId commitCsn)
{
    SXMutexExclusiveGuard mapGuard(mutex);

    for (SnapshotMapConstIter iter = snapshotPageMap.begin();
        iter != snapshotPageMap.end();
        iter++)
    {
        SharedModifiedPageEntry pModEntry = iter->second;
        pVersionedRandomSegment->updatePageEntry(
            iter->first,
            pModEntry->updateCount,
            pModEntry->allocationCount,
            pModEntry->netAllocations,
            pModEntry->netAllocations ? commitCsn : NULL_TXN_ID,
            pModEntry->ownerId,
            true);
    }
    snapshotPageMap.clear();
}

void SnapshotRandomAllocationSegment::rollbackChanges()
{
    SXMutexExclusiveGuard mapGuard(mutex);

    for (SnapshotMapConstIter iter = snapshotPageMap.begin();
        iter != snapshotPageMap.end();
        iter++)
    {
        SharedModifiedPageEntry pModEntry = iter->second;
        pVersionedRandomSegment->updatePageEntry(
            iter->first,
            pModEntry->updateCount,
            pModEntry->allocationCount,
            pModEntry->netAllocations,
            NULL_TXN_ID,
            ANON_PAGE_OWNER_ID,
            false);
    }
    snapshotPageMap.clear();
}

MappedPageListener *SnapshotRandomAllocationSegment::getMappedPageListener(
    PageId pageId)
{
    if (isPageNewlyAllocated(pageId)) {
        return this;
    } else {
        return pVersionedRandomSegment;
    }
}

void SnapshotRandomAllocationSegment::delegatedCheckpoint(
    Segment &delegatingSegment,
    CheckpointType checkpointType)
{
    // Need to match on either the snapshot segment itself or the underlying
    // versioned random segment.  The former will match data pages while the
    // latter will match allocation node pages.
    MappedUnionPageListenerPredicate
        pagePredicate(delegatingSegment, *pVersionedRandomSegment);
    pCache->checkpointPages(pagePredicate, checkpointType);
}

FENNEL_END_CPPFILE("$Id$");

// End SnapshotRandomAllocationSegment.cpp
