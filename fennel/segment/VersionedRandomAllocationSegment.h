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

#ifndef Fennel_VersionedRandomAllocationSegment_Included
#define Fennel_VersionedRandomAllocationSegment_Included

#include "fennel/synch/SXMutex.h"
#include "fennel/segment/RandomAllocationSegmentBase.h"

#include <map>
#include <boost/shared_ptr.hpp>

FENNEL_BEGIN_NAMESPACE

struct VersionedExtentAllocationNode;
struct ModifiedAllocationNode;

typedef boost::shared_ptr<ModifiedAllocationNode> SharedModifiedAllocationNode;

/**
 * Allocation status for a single data page in this extent.
 */
struct VersionedPageEntry : PageEntry
{
    /**
     * Commit sequence number corresponding to the id of the transaction
     * that allocated this page
     */
    TxnId allocationCsn;

    /**
     * PageId link in a page version chain
     */
    PageId versionChainPageId;
};

/**
 * VersionedRandomAllocationSegment refines RandomAllocationSegmentBase,
 * defining a VersionedExtentAllocationNode where each page entry within the
 * segment is versioned.
 *
 * <p>When modifications are made to either VersionedExtentAllocationNodes
 * or SegmentAllocationNodes, the changes are reflected in pages originating 
 * from a temporary segment.  The changes are only copied back to the permanent 
 * segment when a transaction commits and thrown away on a rollback.
 */
class VersionedRandomAllocationSegment : public RandomAllocationSegmentBase
{
    typedef std::map<PageId, SharedModifiedAllocationNode>
        ModifiedAllocationNodeMap;

    typedef ModifiedAllocationNodeMap::const_iterator NodeMapConstIter;

    /**
     * Maps allocation node pages to the modified copy of the node that
     * contains uncommitted page modifications
     */
    ModifiedAllocationNodeMap allocationNodeMap;

    /**
     * Mutex used to protect allocationNodeMap.  Multiple readers are allowed
     * but only a single writer.
     */
    SXMutex mutex;

    /**
     * Segment used to allocate pages to store uncommitted modifications to
     * segment and extent allocation node pages
     */
    SharedSegment pTempSegment;

    /**
     * If true, don't map allocation node pages to their corresponding pages in
     * the temporary segment
     */
    bool mapPages;

    /**
     * Retrieves the pageId in the temporary segment of the page corresponding
     * to an allocation node page.  If the page doesn't yet exist in the
     * temporary segment, creates it.
     *
     * <p>Assumes that the pageEntry will be updated.  Therefore, it also
     * increments the update count corresponding to the pageEntry.
     *
     * @param origNodePageId pageId of the node in the permanent segment
     *
     * @return pageId of node in the temporary segment
     */
    template <class AllocationLockT>
    PageId getTempAllocNodePage(PageId origNodePageId);

    /**
     * Determines whether an allocation node page has been modified.  If it
     * has been, passes back the temporary pageId corresponding to the page.
     * Otherwise, the original pageId is passed back.
     *
     * @param origAllocNodePageId pageId of the node in the permanent segment
     *
     * @param [out] allocNodeSegment the temp segment if the node has been 
     * modified; otherwise, the segment itself
     *
     * @return the pageId of the node in the temp segment if the page has been
     * modified; otherwise, the original pageId
     */
    PageId findAllocPageIdForRead(
        PageId origAllocNodePageId,
        SharedSegment &allocNodeSegment);

    /**
     * Updates an extent entry corresponding to either a commit or rollback
     * action of page allocations and/or deallocations.  In the case of a
     * commit, the permanent extent entry is updated.  In the case of a
     * rollback, the changes made in the temporary extent entry are thrown
     * away.
     *
     * <p>This method assumes that the caller has already acquired an exclusive
     * mutex on the page map.
     *
     * @param iSegAlloc 0-based index of containing SegmentAllocationNode
     *
     * @param extentNum absolute 0-based extent number of containing
     * extent allocation node
     *
     * @param allocationCount the total number of allocations and deallocations
     * made to a page, which corresponds to the number of updates performed on
     * the extent entry
     *
     * @param netAllocations if 1, corresponds to a page allocation; if -1,
     * corresponds to a page deallocation; if 0, corresponds to an update to
     * one of the fileds in the page entry; usd to update the net number of
     * unallocated pages in the extent entry
     *
     * @param commit if true, updates correspond to a commit; else updates
     * correspond to a rollback
     */
    void updateExtentEntry(
        uint iSegAlloc,
        ExtentNum extentNum,
        uint allocationCount,
        int netAllocations,
        bool commit);

    /**
     * Allocates a new SegmentAllocationNode and VersionedExtentAllocationNodes
     * if they haven't been allocated yet.  Also recursively allocates
     * predecessor SegmentAllocationNodes, as needed.
     *
     * @param iSegAlloc 0-based index corresponding to the SegmentAllocationNode
     * that needs to be allocated
     *
     * @param nextPageId the pageId to be set as the nextSegAllocPageId for
     * the SegmentAllocationNode being allocated
     *
     * @param extentNum absolute 0-based extent number of the extent
     * allocation node that needs to be allocated
     */
    void allocateAllocNodes(
        uint iSegAlloc,
        PageId nextPageId,
        ExtentNum extentNum);

    /**
     * Allocates a new VersionedExtentAllocationNode if it hasn't been 
     * allocated yet.  If extent nodes preceeding the one that needs to be
     * allocated haven't been allocated either, those are allocated as well.
     *
     * @param [in] segAllocNode SegmentAllocationNode containing the extent
     * that needs to be allocated
     *
     * @param iSegAlloc 0-based index corresponding to the SegmentAllocationNode
     * containing the extent that needs to be allocated
     *
     * @param extentNum absolute 0-based extent number of the extent
     * allocation node that needs to be allocated
     */
    void allocateExtAllocNodes(
        SegmentAllocationNode &segAllocNode,
        uint iSegAlloc,
        ExtentNum extentNum);

    /**
     * Copies the page entry from the temporary segment to the permanent one
     *
     * @param origPageId pageId of the extent page in the permanent segment
     *
     * @param tempPageId pageId of the extent page in the temporary segment
     *
     * @param iPageInExtent 0-based index of page in extent
     *
     * @param commitCsn sequence number to write into the pageEntry for new
     * page allocations
     *
     * @param ownerId the ownerId to set in the pageEntry for new page
     * allocations
     */
    void copyPageEntryFromTemp(
        PageId origPageId,
        PageId tempPageId,
        BlockNum iPageInExtent,
        TxnId commitCsn,
        PageOwnerId ownerId);

    /**
     * Copies the page entry from the permanent segment to the temporary one
     *
     * @param origPageId pageId of the extent page in the permanent segment
     *
     * @param tempPageId pageId of the extent page in the temporary segment
     *
     * @param iPageInExtent 0-based index of page in extent
     */
    void copyPageEntryToTemp(
        PageId origPageId,
        PageId tempPageId,
        BlockNum iPageInExtent);

    /**
     * Frees a temporary page corresponding to an allocation node page, since
     * all temporary updates to the page have either committed or rolled back.
     *
     * <p>This method assumes the caller has already acquired an exclusive
     * mutex on the page map.
     *
     * @param origAllocNodePageId pageId of the original allocation node
     * corresponding to the one being freed
     *
     * @param tempAllocNodePageId pageId of the temporary allocation node page
     * to be freed
     */
    void freeTempPage(PageId origAllocNodePageId, PageId tempAllocNodePageId);

    // implement RandomAllocationSegmentBase
    virtual PageId getSegAllocPageIdForWrite(PageId origSegAllocPageId);
    virtual PageId getExtAllocPageIdForWrite(ExtentNum extentNum);
    virtual PageId allocateFromExtent(ExtentNum extentNum, PageOwnerId ownerId);
    virtual void format();
    virtual void formatPageExtents(
        SegmentAllocationNode &segAllocNode,
        ExtentNum &extentNum);
    virtual PageId allocateFromNewExtent(
        ExtentNum extentNum,
        PageOwnerId ownerId);
    virtual void freePageEntry(ExtentNum extentNum, BlockNum iPageInExtent);
    virtual void markPageEntryUnused(PageEntry &pageEntry);
    virtual PageOwnerId getPageOwnerId(
        ExtentNum extentNum,
        BlockNum iPageInExtent);
    virtual PageId getSegAllocPageIdForRead(
        PageId origSegAllocPageId,
        SharedSegment &allocNodeSegment);
    virtual PageId getExtAllocPageIdForRead(
        ExtentNum extentNum,
        SharedSegment &allocNodeSegment);

public:
    explicit VersionedRandomAllocationSegment(
        SharedSegment delegateSegment,
        SharedSegment pTempSegmentInit);

    /**
     * Retrieves a copy of a page entry for a specified page
     *
     * @param pageId pageId of the page whose page entry data we are retrieving
     *
     * @param [out] pageEntryCopy copy of page entry retrieved
     */
    void getPageEntryCopy(
        PageId pageId,
        VersionedPageEntry &pageEntryCopy);

    /**
     * Initializes the versioning fields in a pageEntry for a specified page
     *
     * @param pageId pageId of the page whose PageEntry we are setting
     *
     * @param versionChainId version chain pageId to be set
     *
     * @param allocationCsn commit sequence number to be set
     */
    void initPageEntry(
        PageId pageId,
        PageId versionChainId,
        TxnId allocationCsn);

    /**
     * Chains one page to another.  Also may set the successorId of the first
     * page.
     *
     * @param pageId the pageId of the page that will be chained to the page
     * corresponding to the second parameter
     *
     * @param versionChainId the pageId of the page to be chained from the
     * first parameter
     *
     * @param successorId if not set to NULL_PAGE_ID, the successorId of the
     * pageEntry corresponding to the first parameter is set
     */
    void chainPageEntries(
        PageId pageId,
        PageId versionChainId,
        PageId successorId);

    /**
     * Copies updates made to a page entry from the temporary segment into the
     * permanent segment, or vice versa if the updates correspond to a rollback
     *
     * @param pageId pageId of the page entry
     *
     * @param updateCount number of times the entry has been updated
     *
     * @param allocationCount the total number of allocations and deallocations
     * of this page
     *
     * @param netAllocations if 1, corresponds to a page allocation; if -1,
     * corresponds to a page deallocation; if 0, corresponds to an update to
     * one of the fileds in the page entry
     *
     * @param commitCsn sequence number to write into the pageEntry on a commit
     * if the pageEntry corresponds to a page allocation; otherwise, set to
     * NULL_TXN_ID
     *
     * @param ownerId the ownerId to set in the pageEntry on a commit for new
     * page allocations
     *
     * @param commit true if the updates correspond to a commit
     */
    void updatePageEntry(
        PageId pageId,
        uint updateCount,
        uint allocationCount,
        int netAllocations,
        TxnId commitCsn,
        PageOwnerId ownerId,
        bool commit);

    // implementation of Segment interface
    virtual PageId allocatePageId(PageOwnerId ownerId);
    virtual PageId getPageSuccessor(PageId pageId);
    virtual void setPageSuccessor(PageId pageId, PageId successorId);
    virtual void deallocatePageRange(PageId startPageId, PageId endPageId);
};

FENNEL_END_NAMESPACE

#endif

// End VersionedRandomAllocationSegment.h
