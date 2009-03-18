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

#ifndef Fennel_VersionedRandomAllocationSegmentImpl_Included
#define Fennel_VersionedRandomAllocationSegmentImpl_Included

#include "fennel/segment/VersionedRandomAllocationSegment.h"
#include "fennel/segment/SegPageLock.h"

#include <boost/shared_ptr.hpp>

FENNEL_BEGIN_NAMESPACE

// NOTE:  read comments on struct StoredNode before modifying
// the structs below

/**
 * VersionedExtentAllocationNode is the allocation map for one extent
 * in a VersionedRandomAllocationSegment.
 */
struct VersionedExtentAllocationNode : public StoredNode
{
    static const MagicNumber MAGIC_NUMBER = 0xbfc76ee9882a1be6LL;

    VersionedPageEntry &getPageEntry(uint i)
    {
        return reinterpret_cast<VersionedPageEntry *>(this+1)[i];
    }

    VersionedPageEntry const &getPageEntry(uint i) const
    {
        return reinterpret_cast<VersionedPageEntry const *>(this+1)[i];
    }
};

typedef SegNodeLock<VersionedExtentAllocationNode> VersionedExtentAllocLock;

/**
 * ModifiedAllocationNode is a structure that keeps track of the temporary page
 * corresponding to a modified allocation node
 */
struct ModifiedAllocationNode
{
    /**
     * True if this corresponds to a segment allocation node
     */
    bool isSegAllocNode;

    /**
     * Number of uncommitted updates on the node
     */
    uint updateCount;

    /**
     * PageId of the temporary page corresponding to the node
     */
    PageId tempPageId;
};

template <class AllocationLockT>
PageId VersionedRandomAllocationSegment::getTempAllocNodePage(
    PageId origNodePageId,
    bool isSegAllocNode)
{
    SXMutexExclusiveGuard mapGuard(mapMutex);

    PageId tempNodePageId;
    SharedModifiedAllocationNode pModAllocNode;

    // If we've already previously modified the allocation node, it
    // will be in our map.  Otherwise, this is the first time we're modifying
    // the node.  In that case, allocate a new page in our temp segment
    // corresponding to the node.
    NodeMapConstIter iter = allocationNodeMap.find(origNodePageId);
    if (iter == allocationNodeMap.end()) {
        // Allocate a new page and copy the contents of the original page on to
        // the new one.
        SegmentAccessor tempAccessor(pTempSegment, pCache);
        AllocationLockT tempAllocLock(tempAccessor);
        tempNodePageId = tempAllocLock.allocatePage();

        SegmentAccessor selfAccessor(getTracingSegment(), pCache);
        AllocationLockT allocLock(selfAccessor);
        allocLock.lockShared(origNodePageId);

        memcpy(
            tempAllocLock.getPage().getWritableData(),
            allocLock.getPage().getReadableData(),
            pTempSegment->getUsablePageSize());

        // Allocate a new map entry
        pModAllocNode =
            SharedModifiedAllocationNode(new ModifiedAllocationNode());
        pModAllocNode->tempPageId = tempNodePageId;
        pModAllocNode->updateCount = 0;
        pModAllocNode->isSegAllocNode = isSegAllocNode;
    } else {
        pModAllocNode = iter->second;
        tempNodePageId = pModAllocNode->tempPageId;
    }

    // Update the map
    pModAllocNode->updateCount++;

    if (iter == allocationNodeMap.end()) {
        allocationNodeMap.insert(
            ModifiedAllocationNodeMap::value_type(
                origNodePageId,
                pModAllocNode));
    }

    return tempNodePageId;
}

inline PageOwnerId VersionedRandomAllocationSegment::makeDeallocatedPageOwnerId(
    TxnId txnId)
{
    assert(VALID_PAGE_OWNER_ID(txnId));
    return PageOwnerId(DEALLOCATED_PAGE_OWNER_ID_MASK | opaqueToInt(txnId));
}

inline bool VersionedRandomAllocationSegment::isDeallocatedPageOwnerId(
    PageOwnerId pageOwnerId)
{
    return
        (pageOwnerId != ANON_PAGE_OWNER_ID &&
        (opaqueToInt(pageOwnerId) & DEALLOCATED_PAGE_OWNER_ID_MASK));
}

inline TxnId VersionedRandomAllocationSegment::getDeallocatedTxnId(
    PageOwnerId pageOwnerId)
{
    assert(isDeallocatedPageOwnerId(pageOwnerId));
    return TxnId(opaqueToInt(pageOwnerId) & ~DEALLOCATED_PAGE_OWNER_ID_MASK);
}

FENNEL_END_NAMESPACE

#endif

// End VersionedRandomAllocationSegmentImpl.h
