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

#include "fennel/common/CommonPreamble.h"
#include "fennel/btree/BTreeHeapNodeAccessor.h"

FENNEL_BEGIN_CPPFILE("$Id$");

BTreeHeapNodeAccessor::BTreeHeapNodeAccessor()
{
}

void BTreeHeapNodeAccessor::clearNode(BTreeNode &node,uint cbPage)
{
    BTreeNodeAccessor::clearNode(node, cbPage);
    node.cbCompactFree = node.cbTotalFree;
}

PBuffer BTreeHeapNodeAccessor::allocateEntry(
    BTreeNode &node,uint iEntry,uint cbEntry)
{
    uint cbEntryWithOverhead = getEntrySizeWithOverhead(cbEntry);
    assert(iEntry < node.nEntries + 1);
    assert(node.cbCompactFree >= cbEntryWithOverhead);

    EntryOffset *pFirstEntryOffset = getEntryOffsetPointer(node,0);

    // calculate the end of the compact free space, and subtract off
    // the entry size to determine where the entry should be stored
    PBuffer pAllocation =
        reinterpret_cast<PBuffer>(pFirstEntryOffset + node.nEntries)
        + node.cbCompactFree - cbEntry;

    // make room in the offset array
    EntryOffset *pEntryOffset = pFirstEntryOffset + iEntry;
    memmove(
        pEntryOffset + 1,
        pEntryOffset,
        getEntryOffsetArrayByteSize(node.nEntries - iEntry));
    // and write the new offset into the slot just vacated
    *pEntryOffset = pAllocation - reinterpret_cast<PBuffer>(&node);

    // update node control info
    node.nEntries++;
    node.cbTotalFree -= cbEntryWithOverhead;
    node.cbCompactFree -= cbEntryWithOverhead;

    return pAllocation;
}

void BTreeHeapNodeAccessor::deallocateEntry(
    BTreeNode &node,uint iEntry)
{
    tupleAccessor.setCurrentTupleBuf(getEntryForReadInline(node, iEntry));
    uint cbEntry = tupleAccessor.getCurrentByteCount();

    // see comments in BTreeCompactNodeAccessor::deallocateEntry
    if (iEntry != node.nEntries - 1) {
        // delete the entry from the offset array
        EntryOffset *pEntryOffset = getEntryOffsetPointer(node,iEntry);
        memmove(
            pEntryOffset,
            pEntryOffset + 1,
            getEntryOffsetArrayByteSize(node.nEntries - (iEntry + 1)));
    }

    // update node control info
    node.nEntries--;
    node.cbTotalFree += getEntrySizeWithOverhead(cbEntry);
}

bool BTreeHeapNodeAccessor::hasFixedWidthEntries() const
{
    return false;

    // REVIEW:  we might choose to store fixed-width entries in a heap just to
    // save on memmmove cost
#if 0
    return tupleAccessor.isFixedWidth();
#endif
}

BTreeNodeAccessor::Capacity
BTreeHeapNodeAccessor::calculateCapacity(BTreeNode const &node,uint cbEntry)
{
    uint cbEntryWithOverhead = getEntrySizeWithOverhead(cbEntry);
    if (cbEntryWithOverhead <= node.cbCompactFree) {
        return CAN_FIT;
    }
    if (cbEntryWithOverhead <= node.cbTotalFree) {
        return CAN_FIT_WITH_COMPACTION;
    }
    return CAN_NOT_FIT;
}

uint BTreeHeapNodeAccessor::getEntryByteCount(uint cb)
{
    return getEntrySizeWithOverhead(cb);
}

FENNEL_END_CPPFILE("$Id$");

// End BTreeHeapNodeAccessor.cpp
