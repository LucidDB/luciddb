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

#ifndef Fennel_BTreeHeapNodeAccessor_Included
#define Fennel_BTreeHeapNodeAccessor_Included

#include "fennel/btree/BTreeNodeAccessor.h"

FENNEL_BEGIN_NAMESPACE

/**
 * BTreeHeapNodeAccessor maintains the data on a BTreeNode using a standard
 * indirection scheme.  The start of the data area contains a contiguous array
 * of 2-byte offsets to the actual data, which is stored non-contiguously
 * (intermixed with free space) throughout the rest of the data area.
 */
class BTreeHeapNodeAccessor : public BTreeNodeAccessor
{
    // REVIEW:  this limits us to 64K page size max; could be templatized
    typedef uint16_t EntryOffset;

    inline EntryOffset const *getEntryOffsetPointer(
        BTreeNode const &node,uint iEntry);
    
    inline EntryOffset *getEntryOffsetPointer(
        BTreeNode &node,uint iEntry);
    
    inline uint getEntryOffset(BTreeNode const &node,uint iEntry);

    inline uint getEntrySizeWithOverhead(uint cbEntry);

    inline uint getEntryOffsetArrayByteSize(uint nEntries);

public:
    explicit BTreeHeapNodeAccessor();

    inline PConstBuffer getEntryForReadInline(
        BTreeNode const &node,uint iEntry);

    // implement the BTreeNodeAccessor interface
    virtual void clearNode(BTreeNode &node,uint cbPage);
    virtual PBuffer allocateEntry(BTreeNode &node,uint iEntry,uint cbEntry);
    virtual void deallocateEntry(BTreeNode &node,uint iEntry);
    virtual bool hasFixedWidthEntries() const;
    virtual Capacity calculateCapacity(BTreeNode const &node,uint cbEntry);
    virtual uint getEntryByteCount(uint cbTuple);
};

inline BTreeHeapNodeAccessor::EntryOffset const *
BTreeHeapNodeAccessor::getEntryOffsetPointer(
    BTreeNode const &node,uint iEntry)
{
    return reinterpret_cast<EntryOffset const *>(node.getDataForRead())
        + iEntry;
}
    
inline BTreeHeapNodeAccessor::EntryOffset *
BTreeHeapNodeAccessor::getEntryOffsetPointer(
    BTreeNode &node,uint iEntry)
{
    return reinterpret_cast<EntryOffset *>(node.getDataForWrite())
        + iEntry;
}
    
inline uint BTreeHeapNodeAccessor::getEntryOffset(
    BTreeNode const &node,uint iEntry)
{
    assert(iEntry < node.nEntries);
    return *getEntryOffsetPointer(node,iEntry);
}

inline uint BTreeHeapNodeAccessor::getEntrySizeWithOverhead(uint cbEntry)
{
    return cbEntry + sizeof(EntryOffset);
}

inline uint BTreeHeapNodeAccessor::getEntryOffsetArrayByteSize(uint nEntries)
{
    assert(sizeof(EntryOffset) == 2);
    return nEntries << 1;
}

inline PConstBuffer BTreeHeapNodeAccessor::getEntryForReadInline(
    BTreeNode const &node,uint iEntry)
{
    uint offset = getEntryOffset(node,iEntry);
    return reinterpret_cast<PConstBuffer>(&node) + offset;
}

FENNEL_END_NAMESPACE

#endif

// End BTreeHeapNodeAccessor.h
