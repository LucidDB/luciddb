/*
// $Id$
// Fennel is a relational database kernel.
// Copyright (C) 1999-2004 John V. Sichi.
//
// This program is free software; you can redistribute it and/or
// modify it under the terms of the GNU Lesser General Public License
// as published by the Free Software Foundation; either version 2.1
// of the License, or (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with this program; if not, write to the Free Software
// Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
*/

#include "fennel/common/CommonPreamble.h"
#include "fennel/btree/BTreeCompactNodeAccessor.h"

FENNEL_BEGIN_CPPFILE("$Id$");

BTreeCompactNodeAccessor::BTreeCompactNodeAccessor()
{
    cbEntry = MAXU;
}
    
void BTreeCompactNodeAccessor::onInit()
{
    BTreeNodeAccessor::onInit();
    cbEntry = tupleAccessor.getMaxByteCount();
}
    
PBuffer BTreeCompactNodeAccessor::allocateEntry(
    BTreeNode &node,uint iEntry,uint)
{
    assert(iEntry < node.nEntries+1);
    assert(node.cbTotalFree >= cbEntry);

    // shift everything over to make room for the new entry
    PBuffer pBuffer = node.getDataForWrite() + iEntry*cbEntry;
    memmove(
        pBuffer + cbEntry,
        pBuffer,
        (node.nEntries - iEntry)*cbEntry);

    // update node control info
    node.nEntries++;
    node.cbTotalFree -= cbEntry;
    return pBuffer;
}

void BTreeCompactNodeAccessor::deallocateEntry(
    BTreeNode &node,uint iEntry)
{
    assert(iEntry < node.nEntries);

    // NOTE: this test is to avoid passing an address beyond the end of the
    // page to memmove.  It should be unnecessary, since in that case the
    // number of bytes to be moved is 0, but paranoid memmove
    // implementations might complain.
    if (iEntry != node.nEntries-1) {
        // shift over everything after the entry to delete it
        PBuffer pBuffer = node.getDataForWrite() + iEntry*cbEntry;
        memmove(
            pBuffer,
            pBuffer + cbEntry,
            (node.nEntries - (iEntry + 1))*cbEntry);
    }

    // update node control info
    node.nEntries--;
    node.cbTotalFree += cbEntry;
}

bool BTreeCompactNodeAccessor::hasFixedWidthEntries() const
{
    return true;
}

BTreeNodeAccessor::Capacity
BTreeCompactNodeAccessor::calculateCapacity(BTreeNode const &node,uint cbEntry)
{
    if (node.cbTotalFree >= cbEntry) {
        return CAN_FIT;
    } else {
        return CAN_NOT_FIT;
    }
}

uint BTreeCompactNodeAccessor::getEntryByteCount(uint cb)
{
    return cb;
}

void BTreeCompactNodeAccessor::compactNode(BTreeNode &,BTreeNode &)
{
    // Since we never return CAN_FIT_WITH_COMPACTION, no one should ever ask us
    // to do this.
    assert(false);
}

FENNEL_END_CPPFILE("$Id$");

// End BTreeCompactNodeAccessor.cpp
