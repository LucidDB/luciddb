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
#include "fennel/btree/BTreeHeapNodeAccessor.h"

FENNEL_BEGIN_CPPFILE("$Id$");

BTreeHeapNodeAccessor::BTreeHeapNodeAccessor()
{
}

void BTreeHeapNodeAccessor::clearNode(BTreeNode &node,uint cbPage)
{
    BTreeNodeAccessor::clearNode(node,cbPage);
    node.cbCompactFree = node.cbTotalFree;
}

PBuffer BTreeHeapNodeAccessor::allocateEntry(
    BTreeNode &node,uint iEntry,uint cbEntry)
{
    uint cbEntryWithOverhead = getEntrySizeWithOverhead(cbEntry);
    assert(iEntry < node.nEntries+1);
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
    tupleAccessor.setCurrentTupleBuf(getEntryForReadInline(node,iEntry));
    uint cbEntry = tupleAccessor.getCurrentByteCount();
    
    // see comments in BTreeCompactNodeAccessor::deallocateEntry
    if (iEntry != node.nEntries-1) {
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
