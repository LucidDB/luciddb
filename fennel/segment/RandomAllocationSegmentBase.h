/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005-2007 The Eigenbase Project
// Copyright (C) 2005-2007 SQLstream, Inc.
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

#ifndef Fennel_RandomAllocationSegmentBase_Included
#define Fennel_RandomAllocationSegmentBase_Included

#include "fennel/synch/SynchObj.h"
#include "fennel/segment/DelegatingSegment.h"

FENNEL_BEGIN_NAMESPACE

struct SegmentAllocationNode;

/**
 * Allocation status for a single data page
 */
struct PageEntry
{
    /**
     * Identity of object to which this page is allocated, or
     * ANON_PAGE_OWNER_ID for an allocated page with no associated object,
     * or UNALLOCATED_PAGE_OWNER_ID for an unallocated page.
     */
    PageOwnerId ownerId;

    /**
     * Successor to page described by this entry, or NULL_PAGE_ID if page
     * is unallocated or has no successor.
     */
    PageId successorId;
};

/**
 * RandomAllocationSegmentBase is an abstract base class that implements
 * RANDOM_ALLOCATION in terms of an underlying segment supporting
 * LINEAR_ALLOCATION.  See <a
 * href="structSegmentDesign.html#RandomAllocationSegment">the design docs</a>
 * for more detail.
 *
 * <p>The segment tracks space allocation using segment allocation nodes and
 * extent allocation nodes.  This base class defines the structure of the
 * segment allocation node and a base page entry.  It is left to each
 * deriving class to define its own extent allocation node, and further refine
 * the page entry, if necessary.
 *
 * <p>The allocation nodes may be stored in a separate segment.
 */
class RandomAllocationSegmentBase
    : public DelegatingSegment
{
    /**
     * The maximum number of pages occupied by this segment instance
     */
    BlockNum nPagesOccupiedHighWater;

    /**
     * The number of data pages allocated for this segment
     */
    BlockNum nPagesAllocated;

    /**
     * The net number of deallocations on this segment.  New page allocations
     * that occur after a deallocation offset this count.
     */
    BlockNum netDeallocations;

    /**
     * Mutex used to ensure that only one thread is incrementing the page
     * counters
     */
    StrictMutex pageCounterMutex;

    /**
     * Increments the page counters for this segment instance, corresponding
     * to some page allocation.  The pages occupied counter takes into account
     * deallocated pages.
     */
    void incrementPageCounters();

    /**
     * Increments the pages occupied counter
     */
    void incrementPagesOccupiedCounter();

    /**
     * Decrements the page counters for this segment instance, corresponding
     * to some page deallocation.
     */
    void decrementPageCounters();

    /**
     * Counts the total number of allocated data pages as well as the total
     * number of allocated pages, which includes used allocation node pages.
     */
    void countAllocatedPages();

    /**
     * Counts the number of allocated pages recorded in a SegmentAllocationNode.
     *
     * @param segAllocPageId the pageId of the SegmentAllocationNode
     *
     * @param allocNodeSegment the segment corresponding to the node
     *
     * @param [out] nextSegAllocPageId the pageId of the next
     * SegmentAllocationNode following this one
     */
    void tallySegAllocNodePages(
        PageId segAllocPageId,
        SharedSegment allocNodeSegment,
        PageId &nextSegAllocPageId);

    /**
     * Deallocates a single page.
     *
     * @param pageId PageId of page to deallocate
     */
    void deallocatePageId(PageId pageId);

protected:
    friend class SegmentFactory;

    /**
     * Number of pages in one extent, including the extent allocation node
     * itself (so actual data capacity per extent is one less).  This is
     * immutable.
     */
    BlockNum nPagesPerExtent;

    /**
     * Number of pages mapped by one SegmentAllocationNode, including the
     * SegmentAllocationNode itself.  This is immutable.
     */
    BlockNum nPagesPerSegAlloc;

    /**
     * Number of extents mapped by a full SegmentAllocationNode.  This
     * is immutable.
     */
    ExtentNum nExtentsPerSegAlloc;

    explicit RandomAllocationSegmentBase(
        SharedSegment delegateSegment);

    /**
     * Calculates the PageId of a particular SegmentAllocationNode.
     *
     * @param iSegPage 0-based index of desired SegmentAllocationNode
     *
     * @return corresponding PageId
     */
    inline PageId getSegAllocPageId(uint iSegPage) const;

    /**
     * Retrieves the pageId of the SegmentAllocationNode that should be
     * updated when updates are made to that node.
     *
     * @param origSegAllocPageId original SegmentAllocationNode pageId
     *
     * @return pageId of the SegmentAllocationNode to be updated
     */
    virtual PageId getSegAllocPageIdForWrite(PageId origSegAllocPageId) = 0;

    /**
     * Indicates that no new pages were allocated from extents within a
     * SegmentAllocationNode.
     *
     * @param segAllocPageId SegmentAllocationNode pageId
     */
    virtual void undoSegAllocPageWrite(PageId segAllocPageId) = 0;

    /**
     * Calculates the PageId of a particular extent allocation node.
     *
     * @param extentNum absolute 0-based extent number
     *
     * @return corresponding PageId
     */
    inline PageId getExtentAllocPageId(ExtentNum extentNum) const;

    /**
     * Retrieves the pageId of the extent allocation node that should be
     * updated when updates are made to that node.
     *
     * @param extentNum absolute 0-based extent number
     *
     * @return pageId of the extent allocation to be updated
     */
    virtual PageId getExtAllocPageIdForWrite(ExtentNum extentNum) = 0;

    /**
     * Calculates a linear page number.
     *
     * @param extentNum absolute 0-based extent number of extent containing
     * desired page
     *
     * @param iPageInExtent 0-based index of page in extent
     *
     * @return BlockNum corresponding to a linear PageId in this segment
     */
    inline BlockNum makePageNum(
        ExtentNum extentNum,BlockNum iPageInExtent) const;

    /**
     * Maps a linear PageId from this segment into the corresponding
     * SegmentAllocationNode, extent allocation node, and extent-relative page
     * index.
     *
     * @param pageId input PageId
     *
     * @param [out] iSegAlloc 0-based index of containing SegmentAllocationNode
     *
     * @param [out] extentNum absolute 0-based extent number of containing
     * extent allocation node
     *
     * @param [out] iPageInExtent 0-based index of page in extent
     */
    void splitPageId(
        PageId pageId,uint &iSegAlloc,
        ExtentNum &extentNum,BlockNum &iPageInExtent) const;

    /**
     * Tests whether the given PageId has valid contents
     * (either an allocated data page or an allocation map page).
     *
     * @param pageId the PageId to test
     *
     * @return true iff pageId is valid
     */
    virtual bool isPageIdValid(PageId pageId);

    /**
     * Common implementation for isPageIdValid and isPageIdAllocated.
     */
    bool testPageId(PageId pageId,bool testAllocation,bool thisSegment);

    /**
     * Retrieves the ownerId corresponding to a page entry.
     *
     * @see getPageOwnerIdTemplate()
     *
     * @param pageId PageId of the page whose owner we are retrieving
     *
     * @param thisSegment if true, retrieve page entry from this segment;
     * otherwise, retrieve it from an alternative segment
     *
     * @return ownerId
     */
    virtual PageOwnerId getPageOwnerId(PageId pageId, bool thisSegment) = 0;

    /**
     * Retrieves the ownerId corresponding to a page entry.
     *
     * <p>This template method allows the caller to specify different page
     * entry types.
     *
     * @param pageId PageId of the page whose owner we are retrieving
     *
     * @param thisSegment if true, retrieve page entry from this segment;
     * otherwise, retrieve it from an alternative segment
     *
     * @return ownerId
     */
    template <class PageEntryT>
    PageOwnerId getPageOwnerIdTemplate(PageId pageId, bool thisSegment);

    /**
     * Marks the page entry corresponding to a deallocated page as unallocated.
     *
     * @see freePageEntryTemplate()
     *
     * @param extentNum absolute 0-based extent number
     *
     * @param iPageInExtent 0-based index of page in extent
     */
    virtual void freePageEntry(
        ExtentNum extentNum,
        BlockNum iPageInExtent) = 0;

    /**
     * Marks the page entry corresponding to a deallocated page as unallocated.
     * The extent is specified by the the extentNum, the index of the page in
     * the extent, and the segment where the extent allocation node originates
     * from.
     *
     * <p>This template method allows the caller to specify different extent
     * allocation node types.
     *
     * @param extentNum absolute 0-based extent number
     *
     * @param iPageInExtent 0-based index of deallocated page in extent
     */
    template <
        class ExtentAllocationNodeT,
        class ExtentAllocLockT,
        class PageEntryT>
    void freePageEntryTemplate(ExtentNum extentNum, BlockNum iPageInExtent);

    /**
     * Marks a page entry as unused.
     *
     * @param [in, out] pageEntry entry to be marked
     */
    virtual void markPageEntryUnused(PageEntry &pageEntry);

    /**
     * @return the PageId of the first SegmentAllocationNode
     */
    inline PageId getFirstSegAllocPageId() const;

    /**
     * Infers the number of SegmentAllocationNodes from the
     * size of the underlying segment.
     */
    uint inferSegAllocCount();

    /**
     * Formats allocation pages based on current size of underlying segment,
     * marking all pages as deallocated.
     */
    void format();

    /**
     * Formats each of the extents within a segment allocation node.
     *
     * @see formatPageExtentsTemplate()
     *
     * @param [in] segAllocNode locked segment allocation node
     *
     * @param [in, out] extentNum on input, the initial absolute 0-based
     * extent number that needs to be formatted; on output, the last
     * extent number formatted + 1
     */
    virtual void formatPageExtents(
        SegmentAllocationNode &segAllocNode,
        ExtentNum &extentNum) = 0;

    /**
     * Formats each of the extents within a segment allocation node.
     *
     * <p>This template method allows the caller to specify different extent
     * allocation node types.
     *
     * @param [in] segAllocNode locked segment allocation node
     *
     * @param [in, out] extentNum on input, the initial absolute 0-based
     * extent number that needs to be formatted; on output, the last
     * extent number formatted + 1
     */
    template <
        class ExtentAllocationNodeT,
        class ExtentAllocLockT,
        class PageEntryT>
    void formatPageExtentsTemplate(
        SegmentAllocationNode &segAllocNode,
        ExtentNum &extentNum);

    /**
     * Formats one extent allocation.
     *
     * <p>This template method allows the caller to specify different extent
     * allocation node types.
     *
     * @param [in] extentNode locked extent allocation node
     */
    template <
        class ExtentAllocationNodeT,
        class ExtentAllocLockT,
        class PageEntryT>
    void formatExtentTemplate(ExtentAllocationNodeT &extentNode);

    /**
     * Allocates a page without locking it into memory.  The allocation nodes
     * originate from a specified segment.
     *
     * @param ownerId the PageOwnerId of the object which will own this page,
     * or ANON_PAGE_OWNER_ID for pages unassociated with an owner
     *
     * @param allocNodeSegment segment from which the allocation nodes
     * originate
     *
     * @return the PageId of the allocated page, or NULL_PAGE_ID if none
     * could be allocated
     */
    PageId allocatePageIdFromSegment(
        PageOwnerId ownerId,
        SharedSegment allocNodeSegment);

    /**
     * Allocates a new page from an extent known to have space.
     *
     * @see allocateFromExtentTemplate()
     *
     * @param extentNum absolute 0-based extent number from which to allocate
     *
     * @param ownerId PageOwnerId of owning object
     *
     * @return allocated PageId
     */
    virtual PageId allocateFromExtent(
        ExtentNum extentNum,
        PageOwnerId ownerId) = 0;

    /**
     * Allocates a new page from an extent known to have space.  The extent
     * is specified by the absolute extent number and the segment where the
     * extent allocation node originates from.
     *
     * <p>This template method allows the caller to specify different extent
     * allocation node types.
     *
     * @param extentNum absolute 0-based extent number from which to allocate
     *
     * @param ownerId PageOwnerId of owning object
     *
     * @param allocNodeSegment segment that the allocation node page
     * originates from
     *
     * @return allocated PageId
     */
    template <
        class ExtentAllocationNodeT,
        class ExtentAllocLockT,
        class PageEntryT>
    PageId allocateFromExtentTemplate(
        ExtentNum extentNum,
        PageOwnerId ownerId,
        SharedSegment allocNodeSegment);

    /**
     * Allocates a page from a new extent allocation node.
     *
     * @see allocateFromNewExtentTemplate()
     *
     * @param extentNum absolute extent number from which to allocate
     *
     * @param ownerId PageOwnerId of owning object
     *
     * @return allocated PageId
     */
    virtual PageId allocateFromNewExtent(
        ExtentNum extentNum,
        PageOwnerId ownerId) = 0;

    /**
     * Allocates a page from a new extent allocation node.  The extent
     * is specified by the absolute extent number and the segment where the
     * extent allocation node originates from.
     *
     * <p>This template method allows the caller to specify different extent
     * allocation node types.
     *
     * @param extentNum absolute 0-based extent number from which to allocate
     *
     * @param ownerId PageOwnerId of owning object
     *
     * @param allocNodeSegment segment that the allocation node page
     * originates from
     *
     * @return allocated PageId
     */
    template <
        class ExtentAllocationNodeT,
        class ExtentAllocLockT,
        class PageEntryT>
    PageId allocateFromNewExtentTemplate(
        ExtentNum extentNum,
        PageOwnerId ownerId,
        SharedSegment allocNodeSegment);

    /**
     * Allocates a new page from an extent known to have space, with the extent
     * allocation node already locked.
     *
     * <p>This template method allows the caller to specify different extent
     * allocation node types.
     *
     * @param [in] extentNode locked extent allocation node corresponding to
     * extentNum
     *
     * @param extentNum absolute extent number from which to allocate
     *
     * @param ownerId PageOwnerId of owning object
     *
     * @return allocated PageId
     */
    template <class ExtentAllocationNodeT, class PageEntryT>
    PageId allocateFromLockedExtentTemplate(
         ExtentAllocationNodeT &extentNode,
         ExtentNum extentNum,
         PageOwnerId ownerId);

    /**
     * Sets the successor pageId for a page.
     *
     * <p>This template method allows the caller to specify different extent
     * allocation node types.
     *
     * @param pageId pageId of the page whose successor will be set
     *
     * @param successorId successor pageId
     *
     * @param allocNodeSegment segment that the allocation node page
     * originates from
     */
    template <class ExtentAllocationNodeT, class ExtentAllocLockT>
    void setPageSuccessorTemplate(
        PageId pageId,
        PageId successorId,
        SharedSegment allocNodeSegment);

    /**
     * Retrieves the actual pageId corresponding to the SegmentAllocationNode
     * that should be accessed when reading from the node.
     *
     * @param origSegAllocPageId original SegmentAllocationNode pageId
     *
     * @param [out] allocNodeSegment segment from which the allocation node to
     * be read originates
     *
     * @return pageId to be read
     */
    virtual PageId getSegAllocPageIdForRead(
        PageId origSegAllocPageId,
        SharedSegment &allocNodeSegment) = 0;

    /**
     * Retrieves the actual pageId corresponding to the extent allocation node
     * that should be accessed when reading from the node.
     *
     * @param extentNum absolute 0-based extent number
     *
     * @param [out] allocNodeSegment segment from which the allocation node to
     * be read originates
     *
     * @return pageId to be read
     */
    virtual PageId getExtAllocPageIdForRead(
        ExtentNum extentNum,
        SharedSegment &allocNodeSegment) = 0;

    /**
     * Retrieves a copy of the page entry for a specified page.
     *
     * @see getPageEntryCopyTemplate()
     *
     * @param pageId pageId of the page whose page entry data we are retrieving
     *
     * @param [out] pageEntryCopy copy of page entry retrieved
     *
     * @param isAllocated if true, assert that the page is allocated
     *
     * @param thisSegment if true, retrieve page entry from this segment;
     * otherwise, retrieve it from an alternative segment
     */
    virtual void getPageEntryCopy(
        PageId pageId,
        PageEntry &pageEntryCopy,
        bool isAllocated,
        bool thisSegment) = 0;

    /**
     * Retrieves a copy of the page entry for a specified page.
     *
     * <p>This template method allows the caller to specify different extent
     * allocation node and page entry types.
     *
     * @param pageId pageId of the page whose page entry data we are retrieving
     *
     * @param [out] pageEntryCopy copy of page entry retrieved
     *
     * @param isAllocated if true, assert that the page is allocated
     *
     * @param thisSegment if true, retrieve page entry from this segment;
     * otherwise, retrieve it from an alternative segment
     */
    template <
        class ExtentAllocationNodeT,
        class ExtentAllocLockT,
        class PageEntryT>
    void getPageEntryCopyTemplate(
         PageId pageId,
         PageEntryT &pageEntryCopy,
         bool isAllocated,
         bool thisSegment);

public:
    virtual ~RandomAllocationSegmentBase();

    // implementation of Segment interface
    virtual BlockId translatePageId(PageId);
    virtual bool isPageIdAllocated(PageId pageId);
    virtual AllocationOrder getAllocationOrder() const;
    virtual BlockNum getAllocatedSizeInPages();
    virtual BlockNum getNumPagesOccupiedHighWater();
    virtual void deallocatePageRange(PageId startPageId, PageId endPageId);
    virtual void initForUse();
};

FENNEL_END_NAMESPACE

#endif

// End RandomAllocationSegmentBase.h
