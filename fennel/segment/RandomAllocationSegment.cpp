/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2005-2005 Disruptive Tech
// Copyright (C) 2005-2005 Red Square, Inc.
// Portions Copyright (C) 1999-2005 John V. Sichi
//
// This program is free software; you can redistribute it and/or
// modify it under the terms of the GNU General Public License
// as published by the Free Software Foundation; either version 2
// of the License, or (at your option) any later Eigenbase-approved version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program; if not, write to the Free Software
// Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307  USA
*/

#include "fennel/common/CommonPreamble.h"
#include "fennel/segment/RandomAllocationSegmentImpl.h"

FENNEL_BEGIN_CPPFILE("$Id$");

// REVIEW:  There are a lot of optimizations possible for reducing the number
// of arithmetic operations required by various methods.  

// TODO:  unit test for segment growth

// TODO:  integrity verifier

// TODO: maintain the PageId of the first SegAllocNode known to have free
// entries (requires synchronization); this would make allocation constant time
// in the common case; also consider using trylock to prevent segment
// allocation nodes from becoming bottlenecks

RandomAllocationSegment::RandomAllocationSegment(
    SharedSegment delegateSegment)
    : DelegatingSegment(delegateSegment)
{
    assert(DelegatingSegment::getAllocationOrder() == LINEAR_ALLOCATION);

    // calculate immutable segment parameters based on page size

    nPagesPerExtent = (getUsablePageSize()-sizeof(ExtentAllocationNode))
        / sizeof(ExtentAllocationNode::PageEntry);

    nExtentsPerSegAlloc =
        (getUsablePageSize()-sizeof(SegmentAllocationNode))
        / sizeof(SegmentAllocationNode::ExtentEntry);

    // + 1 is for SegAllocNode itself
    nPagesPerSegAlloc = nPagesPerExtent*nExtentsPerSegAlloc + 1;
}

RandomAllocationSegment::~RandomAllocationSegment()
{
}

void RandomAllocationSegment::format()
{
    // calculate number of SegAllocNodes based on current segment size
    uint nSegAllocPages = inferSegAllocCount();

    // calculate number of extents in all but last SegAllocNode
    ExtentNum nExtents = (nSegAllocPages-1)*nExtentsPerSegAlloc;

    // calculate number of pages in last SegAllocNode
    BlockNum nRemainderPages = DelegatingSegment::getAllocatedSizeInPages();
    nRemainderPages -= (nExtents*nPagesPerExtent+nSegAllocPages);
    
    if (nRemainderPages) {
        // last SegAllocNode is not full; add number of remainder extents,
        // rounding down
        nExtents += nRemainderPages/nPagesPerExtent;
    } else {
        // last SegAllocNode is full
        nExtents += nExtentsPerSegAlloc;
    }

    // always format at least one extent; this is somewhat arbitrary, but helps
    // to avoid spurious problems with tiny segments in test cases
    if (!nExtents) {
        nSegAllocPages = 1;
        nExtents = 1;
    }

    // make sure underlying segment is big enough
    bool bigEnough = DelegatingSegment::ensureAllocatedSize(
        nExtents*nPagesPerExtent + nSegAllocPages);
    assert(bigEnough);
    
    // format each SegAllocNode
    
    ExtentNum extentNum = 0;
    SegmentAccessor selfAccessor(shared_from_this(),pCache);
    SegAllocLock segAllocLock(selfAccessor);
    for (uint iSegAlloc = 0; iSegAlloc < nSegAllocPages; iSegAlloc++) {
        segAllocLock.lockExclusive(getSegAllocPageId(iSegAlloc));
        // REVIEW: have to do setMagicNumber() explicitly since we skipped
        // allocation.  Should figure out a way to indicate allocation of a
        // specific PageId.
        segAllocLock.setMagicNumber();
        SegmentAllocationNode &segAllocNode = segAllocLock.getNodeForWrite();

        uint iNextSegAlloc = iSegAlloc + 1;
        if (iNextSegAlloc < nSegAllocPages) {
            // set up SegAllocNode chain
            segAllocNode.nextSegAllocPageId = getSegAllocPageId(iNextSegAlloc);
        } else {
            // terminate SegAllocNode chain at last node
            segAllocNode.nextSegAllocPageId = NULL_PAGE_ID;
        }
        
        // format extent array
        segAllocNode.nPagesPerExtent = nPagesPerExtent;
        segAllocNode.nExtents = std::min(nExtents,nExtentsPerSegAlloc);
        nExtents -= segAllocNode.nExtents;

        // format each extent node
        ExtentAllocLock extentAllocLock(selfAccessor);
        for (uint i = 0; i < segAllocNode.nExtents; ++i, ++extentNum) {
            // -1 for the ExtentAllocationNode itself
            segAllocNode.getExtentEntry(i).nUnallocatedPages =
                nPagesPerExtent - 1;
            extentAllocLock.lockExclusive(getExtentAllocPageId(extentNum));
            extentAllocLock.setMagicNumber();
            formatExtent(extentAllocLock.getNodeForWrite());
        }
    }

    assert(!nExtents);
}

uint RandomAllocationSegment::inferSegAllocCount()
{
    BlockNum nPages = DelegatingSegment::getAllocatedSizeInPages();
    // round up
    return nPages/nPagesPerSegAlloc + 
        (nPages%nPagesPerSegAlloc?1:0);
}

void RandomAllocationSegment::formatExtent(ExtentAllocationNode &extentNode)
{
    // mark all pages as free
    for (uint i = 0; i < nPagesPerExtent; i++) {
        ExtentAllocationNode::PageEntry &pageEntry =
            extentNode.getPageEntry(i);
        pageEntry.ownerId = UNALLOCATED_PAGE_OWNER_ID;
        pageEntry.successorId = NULL_PAGE_ID;
    }
    
    // mark the first entry representing the ExtentAllocationNode itself
    // as permanently allocated
    extentNode.getPageEntry(0).ownerId = ANON_PAGE_OWNER_ID;
}

PageId RandomAllocationSegment::allocatePageId(PageOwnerId ownerId)
{
    ExtentNum extentNum = 0;
    SegmentAccessor selfAccessor(shared_from_this(),pCache);
    SegAllocLock segAllocLock(selfAccessor);

    // find a SegAllocNode with free space
    PageId segAllocPageId = getFirstSegAllocPageId();
    for (uint iSegAlloc = 0; ; ++iSegAlloc) {
        segAllocLock.lockExclusive(segAllocPageId);
        SegmentAllocationNode &segAllocNode = segAllocLock.getNodeForWrite();

        // check each extent
        for (uint i = 0; i < segAllocNode.nExtents; ++i, ++extentNum) {
            SegmentAllocationNode::ExtentEntry &extentEntry =
                segAllocNode.getExtentEntry(i);
            if (extentEntry.nUnallocatedPages) {
                // found one
                extentEntry.nUnallocatedPages--;
                // explicit unlock to minimize contention window and avoid
                // deadlock with deallocatePageId; this is like a Southwest
                // airlines reservation: we've got a flight reserved, not a
                // particular seat on that flight, but we're guaranteed to find
                // a seat when we get on
                segAllocLock.unlock();
                return allocateFromExtent(extentNum,ownerId);
            }
        }

        if (segAllocNode.nextSegAllocPageId != NULL_PAGE_ID) {
            // try next SegAllocNode
            segAllocPageId = segAllocNode.nextSegAllocPageId;
            continue;
        }
        
        // We have to extend the underlying segment, so we need to prevent
        // anyone else from trying to do the same thing at the same time.  So
        // hold onto segAllocLock during this process.

        if (segAllocNode.nExtents < nExtentsPerSegAlloc) {
            // Try to allocate a new extent.  The parameters to makePageNum
            // request just enough space to fit one more extent within the
            // current SegAllocNode.
            if (!DelegatingSegment::ensureAllocatedSize(
                    makePageNum(extentNum,nPagesPerExtent)))
            {
                // couldn't grow
                return NULL_PAGE_ID;
            }

            segAllocNode.nExtents++;
            SegmentAllocationNode::ExtentEntry &extentEntry =
                segAllocNode.getExtentEntry(segAllocNode.nExtents - 1);
            
            // -2 = -1 for ExtentAllocationNode, -1 for page we're
            // about to allocate
            extentEntry.nUnallocatedPages = nPagesPerExtent - 2;
            
            ExtentAllocLock extentAllocLock(selfAccessor);
            extentAllocLock.lockExclusive(getExtentAllocPageId(extentNum));
            extentAllocLock.setMagicNumber();
            ExtentAllocationNode &extentNode =
                extentAllocLock.getNodeForWrite();
            formatExtent(extentNode);
            return allocateFromLockedExtent(extentNode,extentNum,ownerId);
        }

        // Have to allocate a whole new SegAllocNode.  The parameters to
        // makePageNum request enough space to fit the first extent of a new
        // SegAllocNode.
        if (!DelegatingSegment::ensureAllocatedSize(
                makePageNum(extentNum+1,0)))
        {
            // couldn't grow
            return NULL_PAGE_ID;
        }

        SegAllocLock newSegAllocLock(selfAccessor);
        segAllocPageId = getSegAllocPageId(iSegAlloc+1);
        newSegAllocLock.lockExclusive(segAllocPageId);
        newSegAllocLock.setMagicNumber();
        SegmentAllocationNode &newNode = newSegAllocLock.getNodeForWrite();
        newNode.nPagesPerExtent = nPagesPerExtent;
        newNode.nExtents = 0;
        newNode.nextSegAllocPageId = NULL_PAGE_ID;
        segAllocNode.nextSegAllocPageId = segAllocPageId;

        // Carry on with the loop.  We'll unlock and relock the node just
        // allocated, but that's not a big deal since allocating a new
        // SegAllocNode is very rare.
    }
}

void RandomAllocationSegment::splitPageId(
    PageId pageId,uint &iSegAlloc,
    ExtentNum &extentNum,BlockNum &iPageInExtent) const
{
    // calculate block number relative to containing SegAllocNode
    BlockNum iPageInSegAlloc = getLinearBlockNum(pageId)%nPagesPerSegAlloc;
    iSegAlloc = getLinearBlockNum(pageId)/nPagesPerSegAlloc;
    if (!iPageInSegAlloc) {
        // this is the SegAllocNode itself!
        extentNum = MAXU;
        iPageInExtent = 0;
    } else {
        // account for the SegAllocNode
        --iPageInSegAlloc;
        extentNum = iPageInSegAlloc/nPagesPerExtent;
        iPageInExtent = iPageInSegAlloc%nPagesPerExtent;
    }
}

PageId RandomAllocationSegment::allocateFromExtent(
    ExtentNum extentNum, PageOwnerId ownerId)
{
    assert(ownerId != UNALLOCATED_PAGE_OWNER_ID);
    
    SegmentAccessor selfAccessor(shared_from_this(),pCache);
    ExtentAllocLock extentAllocLock(selfAccessor);
    extentAllocLock.lockExclusive(getExtentAllocPageId(extentNum));
    ExtentAllocationNode &node = extentAllocLock.getNodeForWrite();
    return allocateFromLockedExtent(node,extentNum,ownerId);
}

PageId RandomAllocationSegment::allocateFromLockedExtent(
    ExtentAllocationNode &node,ExtentNum extentNum, PageOwnerId ownerId)
{
    // find a free page
    for (uint i = 0; i < nPagesPerExtent; i++) {
        ExtentAllocationNode::PageEntry &pageEntry = node.getPageEntry(i);
        if (pageEntry.ownerId == UNALLOCATED_PAGE_OWNER_ID) {
            pageEntry.ownerId = ownerId;
            PageId pageId = getLinearPageId(makePageNum(extentNum,i));
            return pageId;
        }
    }

    permAssert(false);
}

void RandomAllocationSegment::deallocatePageRange(
    PageId startPageId,PageId endPageId)
{
    assert(startPageId == endPageId);
    if (startPageId != NULL_PAGE_ID) {
        deallocatePageId(startPageId);
    } else {
        format();
    }
}

void RandomAllocationSegment::deallocatePageId(PageId pageId)
{
    assert(pageId != NULL_PAGE_ID);
    assert(isPageIdAllocated(pageId));
    
    ExtentNum extentNum;
    BlockNum iPageInExtent;
    uint iSegAlloc;
    splitPageId(pageId,iSegAlloc,extentNum,iPageInExtent);
    assert(iPageInExtent);

    // note that we mark the free PageId on the extent page BEFORE
    // increasing the corresponding free page count on the segment page;
    // otherwise someone calling allocatePageId at the same time could fail
    SegmentAccessor selfAccessor(shared_from_this(),pCache);
    ExtentAllocLock extentAllocLock(selfAccessor);
    extentAllocLock.lockExclusive(
        getExtentAllocPageId(extentNum+nExtentsPerSegAlloc*iSegAlloc));
    ExtentAllocationNode &extentNode = extentAllocLock.getNodeForWrite();
    ExtentAllocationNode::PageEntry &pageEntry =
        extentNode.getPageEntry(iPageInExtent);
    assert(pageEntry.ownerId != UNALLOCATED_PAGE_OWNER_ID);
    pageEntry.ownerId = UNALLOCATED_PAGE_OWNER_ID;
    pageEntry.successorId = NULL_PAGE_ID;
    
    // explicit unlock to minimize contention window and avoid deadlock with
    // allocatePageId
    extentAllocLock.unlock();
    
    SegAllocLock segAllocLock(selfAccessor);
    segAllocLock.lockExclusive(getSegAllocPageId(iSegAlloc));
    SegmentAllocationNode &segAllocNode = segAllocLock.getNodeForWrite();
    segAllocNode.getExtentEntry(extentNum).nUnallocatedPages++;
    assert(
        segAllocNode.getExtentEntry(extentNum).nUnallocatedPages
        <= nPagesPerExtent);
}

PageId RandomAllocationSegment::getPageSuccessor(PageId pageId)
{
    assert(isPageIdAllocated(pageId));
    
    uint iSegAlloc;
    ExtentNum extentNum;
    BlockNum iPageInExtent;
    splitPageId(pageId,iSegAlloc,extentNum,iPageInExtent);
    assert(iPageInExtent);
    SegmentAccessor selfAccessor(shared_from_this(),pCache);
    ExtentAllocLock extentAllocLock(selfAccessor);
    extentAllocLock.lockShared(getExtentAllocPageId(extentNum));
    ExtentAllocationNode const &node = extentAllocLock.getNodeForRead();
    PageId successorId = node.getPageEntry(iPageInExtent).successorId;
    return successorId;
}

void RandomAllocationSegment::setPageSuccessor(
    PageId pageId, PageId successorId)
{
    assert(isPageIdAllocated(pageId));
    assert((successorId == NULL_PAGE_ID) || isPageIdAllocated(successorId));
    
    uint iSegAlloc;
    ExtentNum extentNum;
    BlockNum iPageInExtent;
    splitPageId(pageId,iSegAlloc,extentNum,iPageInExtent);
    assert(iPageInExtent);
    SegmentAccessor selfAccessor(shared_from_this(),pCache);
    ExtentAllocLock extentAllocLock(selfAccessor);
    extentAllocLock.lockExclusive(getExtentAllocPageId(extentNum));
    ExtentAllocationNode &node = extentAllocLock.getNodeForWrite();
    node.getPageEntry(iPageInExtent).successorId = successorId;
}

Segment::AllocationOrder RandomAllocationSegment::getAllocationOrder() const
{
    return RANDOM_ALLOCATION;
}

BlockId RandomAllocationSegment::translatePageId(PageId pageId)
{
    assert(
        const_cast<RandomAllocationSegment *>(this)->isPageIdValid(pageId));

    return DelegatingSegment::translatePageId(pageId);
}

bool RandomAllocationSegment::testPageId(PageId pageId,bool testAllocation)
{
    if (!DelegatingSegment::isPageIdAllocated(pageId)) {
        return false;
    }
    
    uint iSegAlloc;
    ExtentNum extentNum;
    BlockNum iPageInExtent;
    splitPageId(pageId,iSegAlloc,extentNum,iPageInExtent);
    if (!iPageInExtent) {
        // header pages are valid but not allocated (from the
        // perspective of the RandomAllocationSegment, not the
        // underlying linear segment)
        if (testAllocation) {
            return false;
        } else {
            return true;
        }
    }
    SegmentAccessor selfAccessor(shared_from_this(),pCache);
    ExtentAllocLock extentAllocLock(selfAccessor);
    extentAllocLock.lockShared(getExtentAllocPageId(extentNum));
    ExtentAllocationNode const &node = extentAllocLock.getNodeForRead();
    PageOwnerId ownerId = node.getPageEntry(iPageInExtent).ownerId;
    return (ownerId != UNALLOCATED_PAGE_OWNER_ID);
}

bool RandomAllocationSegment::isPageIdValid(PageId pageId)
{
    return testPageId(pageId,false);
}

bool RandomAllocationSegment::isPageIdAllocated(PageId pageId)
{
    return testPageId(pageId,true);
}

BlockNum RandomAllocationSegment::getAllocatedSizeInPages()
{
    BlockNum nPagesAllocated = 0;
    SegmentAccessor selfAccessor(shared_from_this(),pCache);
    SegAllocLock segAllocLock(selfAccessor);
    
    PageId segAllocPageId = getFirstSegAllocPageId();
    do {
        segAllocLock.lockExclusive(segAllocPageId);
        SegmentAllocationNode const &node = segAllocLock.getNodeForRead();
        uint i;
        for (i = 0; i < node.nExtents; i++) {
            // assume no partial extents; note that -1 is for extent header
            nPagesAllocated +=
                nPagesPerExtent
                - node.getExtentEntry(i).nUnallocatedPages
                - 1;
        }
        segAllocPageId = node.nextSegAllocPageId;
    } while (segAllocPageId != NULL_PAGE_ID);
    return nPagesAllocated;
}

FENNEL_END_CPPFILE("$Id$");

// End RandomAllocationSegment.cpp
