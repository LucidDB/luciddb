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

#ifndef Fennel_RandomAllocationSegmentImpl_Included
#define Fennel_RandomAllocationSegmentImpl_Included

#include "fennel/segment/RandomAllocationSegment.h"
#include "fennel/segment/SegPageLock.h"

FENNEL_BEGIN_NAMESPACE

// NOTE:  read comments on struct StoredNode before modifying
// the structs below

/**
 * Symbolic value for the owner of an unallocated page.
 */
static const PageOwnerId UNALLOCATED_PAGE_OWNER_ID = PageOwnerId(0);

/**
 * SegmentAllocationNode is is the allocation map for a run of extents in a
 * RandomAllocationSegment.
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
        // entries in an ExtentAllocationNode.   So, keep it to four bytes to
        // minimize the number of SegmentAllocationNodes needed.
        uint32_t nUnallocatedPages;
    };

    /**
     * Number of pages in one extent, including the ExtentAllocationNode itself
     * (so actual data capacity per extent is one less).  This is
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

/**
 * ExtentAllocationNode is the allocation map for one extent
 * in a RandomAllocationSegment.
 */
struct ExtentAllocationNode : public StoredNode
{
    static const MagicNumber MAGIC_NUMBER = 0xb9ca99dced182239LL;

    /**
     * Allocation status for a single data page in this extent.
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
    
    PageEntry &getPageEntry(uint i)
    {
        return reinterpret_cast<PageEntry *>(this+1)[i];
    }
    
    PageEntry const &getPageEntry(uint i) const
    {
        return reinterpret_cast<PageEntry const *>(this+1)[i];
    }
};

typedef SegNodeLock<SegmentAllocationNode> SegAllocLock;
typedef SegNodeLock<ExtentAllocationNode> ExtentAllocLock;

inline PageId RandomAllocationSegment::getFirstSegAllocPageId() const
{
    return FIRST_LINEAR_PAGE_ID;
}

inline PageId RandomAllocationSegment::getSegAllocPageId(uint iSegPage) const
{
    return getLinearPageId(nPagesPerSegAlloc*iSegPage);
}

inline BlockNum RandomAllocationSegment::makePageNum(
    ExtentNum extentNum,BlockNum iPageInExtent) const
{
    // weird calculation to take into account interspersal of SegAllocNodes
    uint nSegPages = extentNum/nExtentsPerSegAlloc + 1;
    return iPageInExtent + extentNum*nPagesPerExtent + nSegPages;
}

inline PageId RandomAllocationSegment::getExtentAllocPageId(
    ExtentNum extentNum) const
{
    return getLinearPageId(makePageNum(extentNum,0));
}

FENNEL_END_NAMESPACE

#endif

// End RandomAllocationSegmentImpl.h
