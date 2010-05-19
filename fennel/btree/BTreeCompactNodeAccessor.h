/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005 The Eigenbase Project
// Copyright (C) 2005 SQLstream, Inc.
// Copyright (C) 2005 Dynamo BI Corporation
// Portions Copyright (C) 1999 John V. Sichi
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

#ifndef Fennel_BTreeCompactNodeAccessor_Included
#define Fennel_BTreeCompactNodeAccessor_Included

#include "fennel/btree/BTreeNodeAccessor.h"

FENNEL_BEGIN_NAMESPACE

/**
 * BTreeCompactNodeAccessor maintains the data on a BTreeNode as a
 * compact array of fixed-length entries, with all free space contiguous at the
 * end of the page.
 *
 *<p>
 *
 * TODO:  a high-performance template for builtin datatypes
 */
class FENNEL_BTREE_EXPORT BTreeCompactNodeAccessor
    : public BTreeNodeAccessor
{
    /**
     * Number of bytes per entry.
     */
    uint cbEntry;

public:
    explicit BTreeCompactNodeAccessor();

    // implement the interface expected by BTreeKeyedNodeAccessor
    inline PConstBuffer getEntryForReadInline(
        BTreeNode const &node,uint iEntry);

    // implement the BTreeNodeAccessor interface
    virtual void onInit();
    virtual PBuffer allocateEntry(BTreeNode &node,uint iEntry,uint cbEntry);
    virtual void deallocateEntry(BTreeNode &node,uint iEntry);
    virtual bool hasFixedWidthEntries() const;
    virtual Capacity calculateCapacity(BTreeNode const &node,uint cbEntry);
    virtual uint getEntryByteCount(uint cbTuple);
    virtual void compactNode(BTreeNode &node,BTreeNode &scratchNode);
};

inline PConstBuffer BTreeCompactNodeAccessor::getEntryForReadInline(
    BTreeNode const &node,uint iEntry)
{
    return node.getDataForRead() + iEntry*cbEntry;
}

// TODO: a slotmap representation to be used for indirection of long fixed-width
// entries?

FENNEL_END_NAMESPACE

#endif

// End BTreeCompactNodeAccessor.h
