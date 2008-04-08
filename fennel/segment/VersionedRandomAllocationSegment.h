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

#ifndef Fennel_VersionedRandomAllocationSegment_Included
#define Fennel_VersionedRandomAllocationSegment_Included

#include "fennel/synch/SXMutex.h"
#include "fennel/segment/RandomAllocationSegmentBase.h"

#include <hash_set>
#include <hash_map>
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
     * The total number of allocations of this page, which corresponds to the
     * number of updates made to the extent entry corresponding to the page
     */
    uint allocationCount;

    /**
     * Keeps track of whether the page was last allocated or deallocated.
     * If the page is modified after it was allocated, the field is set to
     * indicate allocation.
     */
    ModType lastModType;

    /**
     * The ownerId that will replace the uncommitted owner id, once the page
     * allocation is committed.
     */
    PageOwnerId ownerId;
};

typedef boost::shared_ptr<ModifiedPageEntry> SharedModifiedPageEntry;

// Use a shared pointer because the structure is dynamically allocated.
typedef std::hash_map<PageId, SharedModifiedPageEntry> ModifiedPageEntryMap;
typedef ModifiedPageEntryMap::const_iterator ModifiedPageEntryMapIter;

/**
 * Symbolic value for the owner of an uncommitted page.
 */
static const PageOwnerId UNCOMMITTED_PAGE_OWNER_ID = PageOwnerId(1);

/**
 * Deallocation-deferred pages are indicated as such by setting the high 
 * order bit in the pageOwnerId in the page entry, and storing the id
 * of the txn that deallocated the page in the remaining bits.  This mask
 * is used to set and turn off the high order bit.
 */
static const uint64_t DEALLOCATED_PAGE_OWNER_ID_MASK = 0x8000000000000000LL;

/**
 * VersionedRandomAllocationSegment refines RandomAllocationSegmentBase,
 * defining a VersionedExtentAllocationNode where each page entry within the
 * segment is versioned.
 *
 * <p>When modifications are made to either VersionedExtentAllocationNodes
 * or SegmentAllocationNodes, the changes are reflected in pages originating
 * from a temporary segment.  The changes are only copied back to the permanent
 * segment when a transaction commits; changes are thrown away on a rollback.
 *
 * <p>Page deallocations are handled differently.  Deallocated pages are
 * not actually deallocated, but instead are marked as deallocation-deferred.
 * This makes the page unusable but not yet available for re-allocation.  A
 * deallocation-deferred page is deallocated by calling the methods
 * getOldPageIds() and deallocateOldPages().  The former finds pages that need
 * to be deallocated, while the latter does the actual deallocation.  When the
 * deallocations are done, the modifications are made directly in the
 * permanent segment.  The two methods should be called in a loop, one after
 * the other, until getOldPageIds() can no longer find any old pages.
 */
class VersionedRandomAllocationSegment : public RandomAllocationSegmentBase
{
    typedef std::hash_map<PageId, SharedModifiedAllocationNode>
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
     *
     * <p>In the case where both mapMutex and deallocationMutex need to be
     * acquired, acquire deallocationMutex first.
     */
    SXMutex mapMutex;

    /**
     * Mutex that prevents multiple deallocations from occuring concurrently.
     *
     * <p>In the case where both mapMutex and deallocationMutex need to be
     * acquired, acquire deallocationMutex first.
     */
    SXMutex deallocationMutex;

    /**
     * Segment used to allocate pages to store uncommitted modifications to
     * segment and extent allocation node pages
     */
    SharedSegment pTempSegment;

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
     * @param isSegAllocNode true if the page being retrieved is a segment
     * allocation node
     *
     * @return pageId of node in the temporary segment
     */
    template <class AllocationLockT>
    PageId getTempAllocNodePage(PageId origNodePageId, bool isSegAllocNode);

    /**
     * Determines whether an allocation node page has been modified.  If it
     * has been, passes back the temporary pageId corresponding to the page.
     * Otherwise, the original pageId is passed back.
     *
     * <p>This method assumes that the caller has already acquired a shared
     * mutex on allocationNodeMap.
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
     * Marks a page entry as deallocation deferred in the temporary segment.
     * Once the txn commits, the permanent entry will also be marked so it
     * can then be deallocated by an ALTER SYSTEM DEALLOCATE OLD statement.
     *
     * @param pageId the pageId of the page entry to be marked
     */
    void deferDeallocation(PageId pageId);

    /**
     * Updates an extent entry corresponding to either a commit or rollback
     * action of page allocations and/or deallocations.  In the case of a
     * commit, the permanent extent entry is updated.  In the case of a
     * rollback, the changes made in the temporary extent entry are thrown
     * away.
     *
     * <p>This method assumes that the caller has already acquired an exclusive
     * mutex on allocationNodeMap.
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
     * @param commit if true, updates correspond to a commit; else updates
     * correspond to a rollback
     */
    void updateExtentEntry(
        uint iSegAlloc,
        ExtentNum extentNum,
        uint allocationCount,
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
     * allocated yet.  If extent nodes preceding the one that needs to be
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
     * Updates a page entry by either copying a page entry from the temporary
     * segment to the permanent one if we're committing the page entry, or
     * the reverse if we're rolling it back.
     *
     * <p>This method assumes that the caller has already acquired an exclusive
     * mutex on allocationNodeMap.
     *
     * @param pageId pageId of the page entry
     *
     * @param extentNum absolute 0-based extent number of the extent
     * allocation node corresponding to the page entry
     *
     * @param iPageInExtent 0-based index of page in extent
     *
     * @param pModEntry information about the modified page entry
     *
     * @param commitCsn sequence number to write into the pageEntry on a commit
     * if the pageEntry corresponds to a page allocation; otherwise, set to
     * NULL_TXN_ID
     *
     * @param commit true if the updates correspond to a commit
     *
     * @param pOrigSegment the originating segment that modified the page
     * entry that needs to be updated
     */
    void updatePageEntry(
        PageId pageId,
        ExtentNum extentNum,
        uint iPageInExtent,
        SharedModifiedPageEntry pModEntry,
        TxnId commitCsn,
        bool commit,
        SharedSegment pOrigSegment);

    /**
     * Copies a page entry from the temporary segment to the permanent one.
     *
     * @param pageId pageId of the page entry
     *
     * @param origPageId pageId of the extent page in the permanent segment
     *
     * @param tempPageId pageId of the extent page in the temporary segment
     *
     * @param iPageInExtent 0-based index of page in extent
     *
     * @param lastModType whether the page was last allocated or deallocated
     *
     * @param commitCsn sequence number to write into the pageEntry for
     * page allocations
     *
     * @param ownerId the ownerId to set in the pageEntry for page allocations
     */
    void copyPageEntryFromTemp(
        PageId pageId,
        PageId origPageId,
        PageId tempPageId,
        BlockNum iPageInExtent,
        ModifiedPageEntry::ModType lastModType,
        TxnId commitCsn,
        PageOwnerId ownerId);

    /**
     * Copies a page entry from the permanent segment to the temporary one.
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
     * Sanity checks the unallocated page count in a SegmentAllocationNode
     * against the pages marked as unallocated in the corresponding 
     * VersionedExtentAllocationNode.
     *
     * @param pageId pageId corresponding to the allocation nodes to be
     * verified
     *
     * @return true if the counts match
     */
    bool validateFreePageCount(PageId pageId);

    /**
     * Frees a temporary page corresponding to an allocation node page, since
     * all temporary updates to the page have either committed or rolled back.
     *
     * <p>This method assumes the caller has already acquired an exclusive
     * mutex on allocationNodeMap.
     *
     * @param origAllocNodePageId pageId of the original allocation node
     * corresponding to the one being freed
     *
     * @param tempAllocNodePageId pageId of the temporary allocation node page
     * to be freed
     */
    void freeTempPage(PageId origAllocNodePageId, PageId tempAllocNodePageId);

    /**
     * Determines the txnId corresponding to the oldest pageId in a page chain
     * that can be deallocated, provided there are pages that can be
     * deallocated.  If pages in the chain are marked deallocation-deferred
     * and are no longer being referenced, then set the flag indicating that
     * the entire page chain should be deallocated.
     *
     * @param pageId a pageId in a page chain corresponding to an old page
     *
     * @param oldestActiveTxnId the txnId of the current, oldest active txnId;
     * used as the threshhold for determining which pages are old
     *
     * @param [out] anchorPageId the pageId of the anchor in the page chain
     *
     * @param [in, out] deallocatedPageSet set of pages that have already been
     * deallocated or need to be skipped because they can't be deallocated
     *
     * @param [out] deallocateChain set to true if the entire page chain
     * needs to be deallocated because the pages are marked as deallocation-
     * deferred and are no longer referenced
     *
     * @return the txnId one larger than the commit sequence number on the
     * oldest page that can be deallocated or NULL_TXN_ID if the page chain
     * should be left as is (either because there's nothing to deallocate,
     * or the page chain contains deallocation-deferred pages)
     */
    TxnId getOldestTxnId(
        PageId pageId,
        TxnId oldestActiveTxnId,
        PageId &anchorPageId,
        std::hash_set<PageId> &deallocatedPageSet,
        bool &deallocateChain);

    /**
     * Deallocates an entire page chain.  All pages in the chain should be
     * marked as deallocation-deferred, and there must not be any active
     * txns older than the txn that marked the pages as deallocation-deferred.
     *
     * @param pageId a pageId in the page chain; not necessarily the anchor
     *
     * @param oldestActiveTxnId the txnId of the current, oldest active txnId
     *
     * @param [in, out] deallocatedPageSet set of pageIds deallocated
     */
    void deallocateEntirePageChain(
        PageId pageId,
        TxnId oldestActiveTxnId,
        std::hash_set<PageId> &deallocatedPageSet);

    /**
     * Deallocates a single page and also discards it from the cache.
     *
     * <p>This method assumes that the caller has already acquired an exclusive
     * mutex on allocationNodeMap.
     *
     * @param pageId pageId of the page to be deallocated
     *
     * @param [in, out] deallocatedPageSet set of pageIds deallocated
     */
    void deallocateSinglePage(
        PageId pageId, 
        std::hash_set<PageId> &deallocatedPageSet);

    /**
     * Deallocates all old pages in a page chain.
     *
     * @param anchorPageId pageId of the anchor page in the page chain
     *
     * @param deallocationCsn the open, upper bound on the allocationCsn
     * corresponding to pages that should be deallocated
     *
     * @param [in, out] deallocatedPageSet set of pageIds corresponding to
     * pages that have been deallocated
     */
    void deallocatePageChain(
        PageId anchorPageId,
        TxnId deallocationCsn,
        std::hash_set<PageId> &deallocatedPageSet);

    /**
     * Walks a page chain and ensures that all pages in the chain are allocated
     * pages.
     *
     * @param anchorPageId the pageId of the anchor page in the chain
     *
     * @return always returns true
     */
    bool validatePageChain(PageId anchorPageId);

    /**
     * Determines if a page chain is in the process of being marked
     * deallocation-deferred and has not yet been committed.
     *
     * <p>This method assumes that the caller has already acquired an exclusive
     * mutex on allocationNodeMap.
     *
     * @param anchorPageId pageId of the anchor page in a chain
     *
     * @param [in, out] deallocatedPageSet set of pages that have been
     * deallocated or need to be skipped
     *
     * @return true if the anchor page entry is marked as
     * deallocation-deferred in its temporary page entry
     */
    bool uncommittedDeallocation(
        PageId anchorPageId,
        std::hash_set<PageId> &deallocatedPageSet);

    /**
     * Adds all pages in a page chain corresponding to deallocation-deferred
     * pages to the set of pages that have been deallocated so we'll skip
     * over them.
     *
     * @param pageId pageId of a page in the chain
     *
     * @param [in, out] deallocatedPageSet set of pages that have been
     * deallocated or need to be skipped
     */
    void skipDeferredDeallocations(
        PageId pageId,
        std::hash_set<PageId> &deallocatedPageSet);

    /**
     * Updates a page entry in the temporary segment, if it exists.
     *
     * <p>This method assumes that the caller has already acquired an exclusive
     * mutex on allocationNodeMap.
     *
     * @param pageId of the page entry that needs to be updated
     *
     * @param [in] pageEntry source page entry
     */
    void updateTempPageEntry(
        PageId pageId,
        VersionedPageEntry const &pageEntry);

    /**
     * Retrieves the committed copy of a page entry for a specified page.
     *
     * @param pageId pageId of the page whose page entry data we are retrieving
     *
     * @param [out] pageEntryCopy copy of page entry retrieved
     */
    void getCommittedPageEntryCopy(
        PageId pageId,
        VersionedPageEntry &pageEntryCopy);

    /**
     * Determines if a page corresponds to a committed allocation.
     *
     * @param pageId pageId of the page being checked
     */
    bool isPageIdAllocateCommitted(PageId pageId);

    /**
     * Chains one page to another, updating either the corresponding permanent
     * page entry or the temporary one.  Also may set the successorId of the
     * first page.
     *
     * @param pageId the pageId of the page that will be chained to the page
     * corresponding to the second parameter
     *
     * @param versionChainId the pageId of the page to be chained from the
     * first parameter
     *
     * @param successorId if not set to NULL_PAGE_ID, the successorId of the
     * pageEntry corresponding to the first parameter is set
     *
     * @param thisSegment if true, make updates in the permanent page entry
     * rather than the temporary one
     */
    void chainPageEntries(
        PageId pageId,
        PageId versionChainId,
        PageId successorId,
        bool thisSegment);

    /**
     * Constructs a pageOwnerId corresponding to a deallocation-deferred page.
     *
     * @param txnId the id of the txn that deallocated the page
     *
     * @return constructed pageOwnerId
     */
    inline PageOwnerId makeDeallocatedPageOwnerId(TxnId txnId);

    /**
     * Determines whether a pageOwnerId corresponds to a deallocation-deferred
     * page.
     *
     * @param pageOwnerId the pageOwnerId under question
     *
     * @return true if the pageOwnerId corresponds to a deallocation-deferred
     * page
     */
    inline bool isDeallocatedPageOwnerId(PageOwnerId pageOwnerId);

    /**
     * Extracts the id corresponding to the txn that deallocated a page from
     * the page's pageOwnerId.
     *
     * @param pageOwnerId the pageOwnerId of the deallocated page
     *
     * @return the txnId
     */
    inline TxnId getDeallocatedTxnId(PageOwnerId pageOwnerId);

    // implement RandomAllocationSegmentBase
    virtual bool isPageIdValid(PageId pageId);
    virtual PageId getSegAllocPageIdForWrite(PageId origSegAllocPageId);
    virtual void undoSegAllocPageWrite(PageId segAllocPageId);
    virtual PageId getExtAllocPageIdForWrite(ExtentNum extentNum);
    virtual PageId allocateFromExtent(ExtentNum extentNum, PageOwnerId ownerId);
    virtual void formatPageExtents(
        SegmentAllocationNode &segAllocNode,
        ExtentNum &extentNum);
    virtual PageId allocateFromNewExtent(
        ExtentNum extentNum,
        PageOwnerId ownerId);
    virtual void freePageEntry(ExtentNum extentNum, BlockNum iPageInExtent);
    virtual void markPageEntryUnused(PageEntry &pageEntry);
    virtual PageOwnerId getPageOwnerId(PageId pageId, bool thisSegment);
    virtual PageId getSegAllocPageIdForRead(
        PageId origSegAllocPageId,
        SharedSegment &allocNodeSegment);
    virtual PageId getExtAllocPageIdForRead(
        ExtentNum extentNum,
        SharedSegment &allocNodeSegment);
    virtual void getPageEntryCopy(
        PageId pageId,
        PageEntry &pageEntryCopy,
        bool isAllocated,
        bool thisSegment);

public:
    explicit VersionedRandomAllocationSegment(
        SharedSegment delegateSegment,
        SharedSegment pTempSegmentInit);

    /**
     * Retrieves the latest copy of a page entry for a specified page, which
     * may correspond to an uncommitted, modified page entry.
     *
     * @param pageId pageId of the page whose page entry data we are retrieving
     *
     * @param [out] pageEntryCopy copy of page entry retrieved
     */
    void getLatestPageEntryCopy(
        PageId pageId,
        VersionedPageEntry &pageEntryCopy);

    /**
     * Initializes the versioning fields in a pageEntry for a specified page.
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
     * Chains one page to another, updating the corresponding page entry in
     * the temp segment page.  Also may set the successorId of the first
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
     * Updates the permanent allocation nodes to reflect changes currently
     * in the temporary segment, or vice versa if the updates correspond
     * to a rollback.
     *
     * @param modifiedPageEntryMap map containing information on page entries
     * that have been modified
     *
     * @param commitCsn sequence number to write into the pageEntry on a commit
     * if the pageEntry corresponds to a page allocation; otherwise, set to
     * NULL_TXN_ID
     *
     * @param commit true if the updates correspond to a commit
     *
     * @param pOrigSegment the originating segment that modified the allocation
     * nodes that need to be updated
     */
    void updateAllocNodes(
        ModifiedPageEntryMap const &modifiedPageEntryMap,
        TxnId commitCsn,
        bool commit,
        SharedSegment pOrigSegment);

    /**
     * @return the deallocation mutex that prevents reading of the page chain
     * while deallocations are in progress
     */
    SXMutex &getDeallocationMutex();

    /**
     * Retrieves a batch of pageIds corresponding either to old snapshot
     * pages that are no longer being referenced or pages marked for
     * deallocation.  The number of pageIds returned must be at least some
     * value as specified by an input parameter, unless there are no more
     * pages left to deallocate.  The old pages are located starting at a
     * location specified by the index of a SegmentAllocationNode and the
     * offset of an extent within that node.  Upon exit, those parameters are
     * replaced with the location where the search should be resumed the next
     * time the method is called.
     *
     * <p>Once a batch of pages have been identified, deallocateOldPages()
     * should then be called.  After deallocating that set of pages,
     * getOldPageIds() should be called again to identify the next set of
     * old pages.
     *
     * @param [in, out] iSegAlloc on input, 0-based index of starting
     * SegmentAllocationNode; on exit, the index where the search should be
     * resumed the next time this method is called
     *
     * @param [in, out] extentNum on input, the 0-based extent number of the
     * extent from which to start the search for old pages; upon exit, the
     * extent number where the search should be resumed the next time this
     * method is called
     *
     * @param oldestActiveTxnId the txnId of the current, oldest active txnId;
     * used as the threshhold for determining which pages are old
     *
     * @param numPages lower bound on the number of pages to pass back, unless
     * there are no more pages to deallocate
     *
     * @param [out] oldPageSet set of old pageIds found
     *
     * @return true if additional pages need to be searched; false if reached
     * end of search
     */
    bool getOldPageIds(
        uint &iSegAlloc,
        ExtentNum &extentNum,
        TxnId oldestActiveTxnId,
        uint numPages,
        PageSet &oldPageSet);

    /**
     * Deallocates an old set of pages and updates the version chain that the
     * pages may be a part of.  Old pages are identified by calling
     * getOldPageIds().
     *
     * <p>The actual deallocation of pages is in a separate method to minimize
     * the duration the deallocation mutex is held.
     *
     * @param [in] oldPageSet set of old pageIds that need to be deallocated
     *
     * @param oldestActiveTxnId the txnId of the current, oldest active txnId;
     * used as the threshhold for determining which pages should be deallocated
     */
    void deallocateOldPages(PageSet const &oldPageSet, TxnId oldestActiveTxnId);

    /**
     * Frees any of the remaining temp pages that were used to keep track of
     * allocation node updates.  All of the remaining pages should correspond
     * to segment allocation nodes, since extent allocation node pages are
     * freed once they no longer contain any pending updates.
     */
    void freeTempPages();

    // implementation of Segment interface
    virtual bool isPageIdAllocated(PageId pageId);
    virtual PageId allocatePageId(PageOwnerId ownerId);
    virtual PageId getPageSuccessor(PageId pageId);
    virtual void setPageSuccessor(PageId pageId, PageId successorId);
    virtual void deallocatePageRange(PageId startPageId, PageId endPageId);
    virtual void initForUse();
};

FENNEL_END_NAMESPACE

#endif

// End VersionedRandomAllocationSegment.h
