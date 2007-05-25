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

#ifndef Fennel_SnapshotRandomAllocationSegment_Included
#define Fennel_SnapshotRandomAllocationSegment_Included

#include "fennel/synch/SXMutex.h"
#include "fennel/synch/SynchObj.h"
#include "fennel/segment/DelegatingSegment.h"
#include "fennel/segment/VersionedRandomAllocationSegment.h"

#include <boost/shared_ptr.hpp>

FENNEL_BEGIN_NAMESPACE

/**
 * SnapshotRandomAllocationSegment implements a random allocation segment that
 * provides a consistent view of the data in the segment based on a specified
 * point in time.  All pages read are as of that specified point in time.  If
 * a page is updated, and this is the first time the page is being updated, a
 * new copy of the page is created and used for the remaining lifetime of the
 * segment.
 *
 * <p>This segment delegates much of its work to its underlying
 * VersionedRandomAllocationSegment.  That segment is responsible for the
 * actual underlying data storage.
 */
class SnapshotRandomAllocationSegment : public DelegatingSegment
{
    /**
     * Keeps track of the number of modifications made to the page entry and
     * extent entry corresponding to a page
     */
    ModifiedPageEntryMap modPageEntries;

    /**
     * Maintains a mapping between a pageId and the snapshot page for this
     * segment
     */
    PageMap snapshotPageMap;

    /**
     * Mutex that ensures only a single thread is modifying snapshotPageMap.
     * It also ensures that the map is in sync with the allocations and
     * deallocations in the underlying VersionedRandomAllocationSegment.
     */
    SXMutex modPageMapMutex;

    /**
     * Mutex protecting the snapshotPageMap
     */
    StrictMutex snapshotPageMapMutex;

    /**
     * Underlying segment that provides versioning of pages
     */
    VersionedRandomAllocationSegment *pVersionedRandomSegment;

    /**
     * The commit sequence number used to determine which pages to read
     */
    TxnId snapshotCsn;

    /**
     * If true, some snapshot page has been modified and therefore pages
     * need to be flushed during a checkpoint call
     */
    bool needPageFlush;

    /**
     * If true, always issue the checkpoint to remove entries from the cache
     * when flushing and unmapping cache entries.  Otherwise, only checkpoint
     * pages if at least one page was dirtied.
     */
    bool forceCacheUnmap;

    /**
     * Retrieves the pageId corresponding to this segment's snapshot of a
     * specified page
     *
     * @param pageId pageId of the desired page
     *
     * @return snapshot pageId corresponding to the desired pageId
     */
    PageId getSnapshotId(PageId pageId);

    /**
     * Increments the counters that keep track of the number of modifications
     * made to a page entry as well as allocations/deallocations.
     *
     * @param pageId the pageId of the page whose counters are being
     * incremented
     *
     * @param ownerId the ownerId that will be set on the page entry when it's
     * committed; only used in the case of a page allocation
     *
     * @param modType type of modification made to the page entry (i.e.,
     * allocated, deallocated, or modified)
     */
    void incrPageUpdateCount(
        PageId pageId,
        PageOwnerId ownerId,
        ModifiedPageEntry::ModType modType);

    /**
     * Retrieves the pageId of the anchor page corresponding to a snapshot page
     *
     * @param snapshotId the pageId of the snapshot page
     *
     * @return pageId of the anchor page
     */
    PageId getAnchorPageId(PageId snapshotId);

    /**
     * Determines whether a page corresponds to a newly allocated one
     *
     * @param pageId pageId of the page in question
     *
     * @return true if the page is newly allocated; false otherwise
     */
    bool isPageNewlyAllocated(PageId pageId);

    /**
     * Chains one page to another.  Also may set the successorId of the first
     * page.
     *
     * @param pageId the pageId of the page that the second parameter will be
     * chained to
     *
     * @param versionChainId the pageId of the page to be chained to the
     * first parameter
     *
     * @param successorId if not set to NULL_PAGE_ID, the successorId of the
     * pageEntry corresponding to the first parameter is set
     */
    void chainPageEntries(
        PageId pageId,
        PageId versionChainId,
        PageId successorId);

public:
    explicit SnapshotRandomAllocationSegment(
        SharedSegment delegateSegment,
        SharedSegment versionedSegment,
        TxnId snapshotCsnInit);

    /**
     * Commit page entry changes 
     *
     * @param commitCsn sequence number to write into pageEntry's on commit
     */
    void commitChanges(TxnId commitCsn);

    /**
     * Rollback page entry changes
     */
    void rollbackChanges();

    /**
     * Adds a source page chain into a destination page chain.  All pages in
     * the source chain must be newer than the pages in the destination chain.
     *
     * @param destAnchorPageId pageId of the anchor page of the destination
     * page chain
     *
     * @param srcArchorPageid pageId of the anchor page of the source page chain
     */
    void versionPage(PageId destAnchorPageId, PageId srcAnchorPageId);

    /**
     * Indicates that a checkpoint should be always be executed, even if no
     * pages are dirty when flushing and unmapping cache entries.  This is
     * required in cases where we need to discard old entries from the cache.
     */
    void setForceCacheUnmap();

    // implementation of Segment interface
    virtual BlockId translatePageId(PageId pageId);
    virtual PageId getPageSuccessor(PageId pageId);
    virtual void setPageSuccessor(PageId pageId, PageId successorId);
    virtual PageId allocatePageId(PageOwnerId ownerId);
    virtual void deallocatePageRange(PageId startPageId, PageId endPageId);
    virtual PageId updatePage(PageId pageId, bool needsTranslation = false);
    virtual MappedPageListener *getMappedPageListener(BlockId blockId);
    virtual void delegatedCheckpoint(
        Segment &delegatingSegment,
        CheckpointType checkpointType);
    virtual MappedPageListener *notifyAfterPageCheckpointFlush(CachePage &page);
    virtual bool canFlushPage(CachePage &page);
    virtual void notifyPageDirty(CachePage &page, bool bDataValid);
    virtual void discardCachePage(BlockId blockId);
};

FENNEL_END_NAMESPACE

#endif

// End SnapshotRandomAllocationSegment.h
