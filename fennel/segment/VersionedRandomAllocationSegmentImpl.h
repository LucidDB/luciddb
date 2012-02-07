/*
// Licensed to DynamoBI Corporation (DynamoBI) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  DynamoBI licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at

//   http://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
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
struct FENNEL_SEGMENT_EXPORT VersionedExtentAllocationNode
    : public StoredNode
{
    static const MagicNumber MAGIC_NUMBER = 0xbfc76ee9882a1be6LL;

    VersionedPageEntry &getPageEntry(uint i)
    {
        return reinterpret_cast<VersionedPageEntry *>(this + 1)[i];
    }

    VersionedPageEntry const &getPageEntry(uint i) const
    {
        return reinterpret_cast<VersionedPageEntry const *>(this + 1)[i];
    }
};

typedef SegNodeLock<VersionedExtentAllocationNode> VersionedExtentAllocLock;

/**
 * ModifiedAllocationNode is a structure that keeps track of the temporary page
 * corresponding to a modified allocation node
 */
struct FENNEL_SEGMENT_EXPORT ModifiedAllocationNode
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
        (pageOwnerId != ANON_PAGE_OWNER_ID
        && (opaqueToInt(pageOwnerId) & DEALLOCATED_PAGE_OWNER_ID_MASK));
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
