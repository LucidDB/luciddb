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
#include "fennel/segment/DelegatingSegment.h"
#include "fennel/segment/VersionedRandomAllocationSegment.h"

#include <map>
#include <boost/shared_ptr.hpp>

FENNEL_BEGIN_NAMESPACE

/**
 * Symbolic value for the owner of an uncommitted page.
 */
static const PageOwnerId UNCOMMITTED_PAGE_OWNER_ID = PageOwnerId(1);

/**
 * ModifiedPageEntry is a structure that keeps track of the number of updates
 * made to the page entry and extent entry corresponding to a page.  Also,
 * tracks whether the page can be updated in-place.
 */
struct ModifiedPageEntry
{
    enum ModType {
        ALLOCATED,
        DEALLOCATED,
        MODIFIED
    };

    /**
     * Number of updates made to the page entry, including allocations and
     * deallocations
     */
    uint updateCount;

    /**
     * The total number of allocations and deallocations of this page, which
     * corresponds to the number of updates made to the extent entry
     * corresponding to the page
     */
    uint allocationCount;

    /**
     * Keeps track of the net allocations/deallocations on a page.  If 1,
     * the page is still allocated.  If -1, the page was last deallocated.
     * If 0, an allocate and deallocate cancelled each other out.  This would
     * only occur if there is a multi-statement xact where for example, a
     * new page is allocated and then the table is dropped.
     */
    int netAllocations;

    /**
     * The ownerId that will replace the uncommitted owner id, once the page
     * allocation is committed.
     */
    PageOwnerId ownerId;
};

typedef boost::shared_ptr<ModifiedPageEntry> SharedModifiedPageEntry;

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
    typedef std::map<PageId, SharedModifiedPageEntry> SnapshotPageMap;

    typedef SnapshotPageMap::const_iterator SnapshotMapConstIter;

    /**
     * Keeps track of the number of modifications made to the page entry and
     * extent entry corresponding to a page
     */
    SnapshotPageMap snapshotPageMap;

    /**
     * Mutex that ensures only a single thread is modifying snapshotPageMap.
     * It also ensures that the map is in sync with the allocations and
     * deallocations in the underlying VersionedRandomAllocationSegment.
     */
    SXMutex mutex;

    /**
     * Underlying segment that provides versioning of pages
     */
    VersionedRandomAllocationSegment *pVersionedRandomSegment;

    /**
     * The commit sequence number used to determine which pages to read
     */
    TxnId snapshotCsn;

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

    // implementation of Segment interface
    virtual BlockId translatePageId(PageId pageId);
    virtual PageId getPageSuccessor(PageId pageId);
    virtual void setPageSuccessor(PageId pageId, PageId successorId);
    virtual PageId translateBlockId(BlockId blockId);
    virtual PageId allocatePageId(PageOwnerId ownerId);
    virtual void deallocatePageRange(PageId startPageId, PageId endPageId);
    virtual PageId updatePage(PageId pageId);
    virtual MappedPageListener *getMappedPageListener(PageId pageId);
    virtual void delegatedCheckpoint(
        Segment &delegatingSegment,
        CheckpointType checkpointType);
};

FENNEL_END_NAMESPACE

#endif

// End SnapshotRandomAllocationSegment.h
