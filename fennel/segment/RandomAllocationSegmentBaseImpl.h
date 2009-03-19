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

#ifndef Fennel_RandomAllocationSegmentBaseImpl_Included
#define Fennel_RandomAllocationSegmentBaseImpl_Included

#include "fennel/segment/RandomAllocationSegmentBase.h"
#include "fennel/segment/SegPageLock.h"

FENNEL_BEGIN_NAMESPACE

// NOTE:  read comments on struct StoredNode before modifying
// the structs below

/**
 * Symbolic value for the owner of an unallocated page.
 */
static const PageOwnerId UNALLOCATED_PAGE_OWNER_ID = PageOwnerId(0);

/**
 * SegmentAllocationNode is the allocation map for a run of extents in a
 * RandomAllocationSegmentBase.
 */
struct SegmentAllocationNode : public StoredNode
{
    static const MagicNumber MAGIC_NUMBER = 0xa3db80b98208bfd4LL;

    /**
     * Allocation status for a single extent mapped by this node.
     */
    struct ExtentEntry
    {
        // NOTE:  This should be a BlockNum, but even with a very large block
        // size it will never need more than 32 bits to cover the number of
        // entries in an extent allocation node.  So, keep it to four bytes to
        // minimize the number of SegmentAllocationNodes needed.
        uint32_t nUnallocatedPages;
    };

    /**
     * Number of pages in one extent, including the extent allocation node
     * itself (so actual data capacity per extent is one less).  This is
     * redundant across all SegmentAllocationNodes in the same
     * RandomAllocationSegment.
     */
    BlockNum nPagesPerExtent;

    /**
     * Forward link to the next SegmentAllocationNode, or NULL_PAGE_ID
     * for the last one.  This is mostly redundant, since the PageId's
     * of SegmentAllocationNodes can always be computed.  However,
     * it is not completely redundant since it marks the last node
     * (rather than inferring it from the underlying segment size,
     * which may be unreliable after recovery).
     */
    PageId nextSegAllocPageId;

    /**
     * Number of extents mapped by this node.
     */
    uint nExtents;

    ExtentEntry &getExtentEntry(uint i)
    {
        assert(i < nExtents);
        return reinterpret_cast<ExtentEntry *>(this+1)[i];
    }

    ExtentEntry const &getExtentEntry(uint i) const
    {
        assert(i < nExtents);
        return reinterpret_cast<ExtentEntry const *>(this+1)[i];
    }
};

typedef SegNodeLock<SegmentAllocationNode> SegAllocLock;

inline PageId RandomAllocationSegmentBase::getFirstSegAllocPageId() const
{
    return FIRST_LINEAR_PAGE_ID;
}

inline PageId RandomAllocationSegmentBase::getSegAllocPageId(
    uint iSegPage) const
{
    return getLinearPageId(nPagesPerSegAlloc*iSegPage);
}

inline BlockNum RandomAllocationSegmentBase::makePageNum(
    ExtentNum extentNum,BlockNum iPageInExtent) const
{
    // weird calculation to take into account interspersal of SegAllocNodes
    uint nSegPages = extentNum / nExtentsPerSegAlloc + 1;
    return iPageInExtent + extentNum*nPagesPerExtent + nSegPages;
}

inline PageId RandomAllocationSegmentBase::getExtentAllocPageId(
    ExtentNum extentNum) const
{
    return getLinearPageId(makePageNum(extentNum,0));
}

template <class ExtentAllocationNodeT,
    class ExtentAllocLockT,
    class PageEntryT>
PageId RandomAllocationSegmentBase::allocateFromExtentTemplate(
    ExtentNum extentNum,
    PageOwnerId ownerId,
    SharedSegment allocNodeSegment)
{
    permAssert(ownerId != UNALLOCATED_PAGE_OWNER_ID);

    SegmentAccessor segAccessor(allocNodeSegment, pCache);
    ExtentAllocLockT extentAllocLock(segAccessor);
    extentAllocLock.lockExclusive(getExtAllocPageIdForWrite(extentNum));
    ExtentAllocationNodeT &node = extentAllocLock.getNodeForWrite();
    return
        allocateFromLockedExtentTemplate<ExtentAllocationNodeT, PageEntryT>(
            node,
            extentNum,
            ownerId);
}

template <class ExtentAllocationNodeT, class ExtentAllocLockT, class PageEntryT>
void RandomAllocationSegmentBase::formatPageExtentsTemplate(
    SegmentAllocationNode &segAllocNode,
    ExtentNum &extentNum)
{
    SegmentAccessor selfAccessor(getTracingSegment(), pCache);
    ExtentAllocLockT extentAllocLock(selfAccessor);
    uint startOffset = extentNum % nExtentsPerSegAlloc;
    for (uint i = startOffset; i < segAllocNode.nExtents; ++i, ++extentNum) {
        // -1 for the extent allocation node itself
        segAllocNode.getExtentEntry(i).nUnallocatedPages =
            nPagesPerExtent - 1;
        extentAllocLock.lockExclusive(getExtentAllocPageId(extentNum));
        extentAllocLock.setMagicNumber();
        formatExtentTemplate<
                ExtentAllocationNodeT,
                ExtentAllocLockT,
                PageEntryT>(
            extentAllocLock.getNodeForWrite());
    }
}

template <class ExtentAllocationNodeT, class ExtentAllocLockT, class PageEntryT>
void RandomAllocationSegmentBase::formatExtentTemplate(
    ExtentAllocationNodeT &extentNode)
{
    // mark all pages as free
    for (uint i = 0; i < nPagesPerExtent; i++) {
        PageEntryT &pageEntry = extentNode.getPageEntry(i);
        markPageEntryUnused(pageEntry);
    }

    // mark the first entry representing the extent allocation node itself
    // as permanently allocated
    extentNode.getPageEntry(0).ownerId = ANON_PAGE_OWNER_ID;
}

template <class ExtentAllocationNodeT, class ExtentAllocLockT, class PageEntryT>
PageId RandomAllocationSegmentBase::allocateFromNewExtentTemplate(
    ExtentNum extentNum,
    PageOwnerId ownerId,
    SharedSegment allocNodeSegment)
{
    SegmentAccessor segAccessor(allocNodeSegment, pCache);
    ExtentAllocLockT extentAllocLock(segAccessor);
    extentAllocLock.lockExclusive(getExtAllocPageIdForWrite(extentNum));
    extentAllocLock.setMagicNumber();
    ExtentAllocationNodeT &extentNode = extentAllocLock.getNodeForWrite();
    formatExtentTemplate<ExtentAllocationNodeT, ExtentAllocLockT, PageEntryT>(
        extentNode);
    return
        allocateFromLockedExtentTemplate<ExtentAllocationNodeT, PageEntryT>(
            extentNode,
            extentNum,
            ownerId);
}

template <class ExtentAllocationNodeT, class PageEntryT>
PageId RandomAllocationSegmentBase::allocateFromLockedExtentTemplate(
    ExtentAllocationNodeT &node, ExtentNum extentNum, PageOwnerId ownerId)
{
    // find a free page
    for (uint i = 0; i < nPagesPerExtent; i++) {
        PageEntryT &pageEntry = node.getPageEntry(i);
        if (pageEntry.ownerId == UNALLOCATED_PAGE_OWNER_ID) {
            if (i == 0) {
                // entry 0 is the extent allocation node itself so it
                // should never be marked as unallocated
                permAssert(false);
            }
            pageEntry.ownerId = ownerId;
            PageId pageId = getLinearPageId(makePageNum(extentNum,i));
            return pageId;
        }
    }

    permAssert(false);
}

template <class ExtentAllocationNodeT, class ExtentAllocLockT, class PageEntryT>
void RandomAllocationSegmentBase::freePageEntryTemplate(
    ExtentNum extentNum,
    BlockNum iPageInExtent)
{
    SegmentAccessor segAccessor(getTracingSegment(), pCache);
    ExtentAllocLockT extentAllocLock(segAccessor);

    extentAllocLock.lockExclusive(getExtentAllocPageId(extentNum));
    ExtentAllocationNodeT &extentNode = extentAllocLock.getNodeForWrite();
    PageEntryT &pageEntry = extentNode.getPageEntry(iPageInExtent);
    permAssert(pageEntry.ownerId != UNALLOCATED_PAGE_OWNER_ID);
    markPageEntryUnused(pageEntry);
}

template <class ExtentAllocationNodeT, class ExtentAllocLockT>
void RandomAllocationSegmentBase::setPageSuccessorTemplate(
    PageId pageId,
    PageId successorId,
    SharedSegment allocNodeSegment)
{
    assert(isPageIdAllocated(pageId));
    assert((successorId == NULL_PAGE_ID) || isPageIdAllocated(successorId));

    uint iSegAlloc;
    ExtentNum extentNum;
    BlockNum iPageInExtent;
    splitPageId(pageId, iSegAlloc, extentNum, iPageInExtent);
    permAssert(iPageInExtent);

    SegmentAccessor segAccessor(allocNodeSegment, pCache);
    ExtentAllocLockT extentAllocLock(segAccessor);
    extentAllocLock.lockExclusive(getExtAllocPageIdForWrite(extentNum));
    ExtentAllocationNodeT &node = extentAllocLock.getNodeForWrite();
    node.getPageEntry(iPageInExtent).successorId = successorId;
}

template <class PageEntryT>
PageOwnerId RandomAllocationSegmentBase::getPageOwnerIdTemplate(
    PageId pageId,
    bool thisSegment)
{
    PageEntryT pageEntry;

    getPageEntryCopy(pageId, pageEntry, false, thisSegment);
    return pageEntry.ownerId;
}

template <class ExtentAllocationNodeT, class ExtentAllocLockT, class PageEntryT>
void RandomAllocationSegmentBase::getPageEntryCopyTemplate(
    PageId pageId,
    PageEntryT &pageEntryCopy,
    bool isAllocated,
    bool thisSegment)
{
    if (isAllocated) {
        assert(testPageId(pageId, true, thisSegment));
    }

    ExtentNum extentNum;
    BlockNum iPageInExtent;
    uint iSegAlloc;
    splitPageId(pageId, iSegAlloc, extentNum, iPageInExtent);
    assert(iPageInExtent);

    SharedSegment allocNodeSegment;
    PageId extentPageId;
    if (thisSegment) {
        allocNodeSegment = getTracingSegment();
        extentPageId = getExtentAllocPageId(extentNum);
    } else {
        extentPageId = getExtAllocPageIdForRead(extentNum, allocNodeSegment);
    }

    SegmentAccessor segAccessor(allocNodeSegment, pCache);
    ExtentAllocLockT extentAllocLock(segAccessor);
    extentAllocLock.lockShared(extentPageId);
    ExtentAllocationNodeT const &extentNode =
        extentAllocLock.getNodeForRead();

    PageEntryT const &pageEntry =
        extentNode.getPageEntry(iPageInExtent);

    pageEntryCopy = pageEntry;
}

FENNEL_END_NAMESPACE

#endif

// End RandomAllocationSegmentBaseImpl.h
