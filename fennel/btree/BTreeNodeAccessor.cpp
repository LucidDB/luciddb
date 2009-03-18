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

#include "fennel/common/CommonPreamble.h"
#include "fennel/btree/BTreeNodeAccessor.h"
#include "fennel/tuple/TuplePrinter.h"

FENNEL_BEGIN_CPPFILE("$Id$");

BTreeNodeAccessor::~BTreeNodeAccessor()
{
}

void BTreeNodeAccessor::clearNode(BTreeNode &node,uint cbPage)
{
    node.rightSibling = NULL_PAGE_ID;
    node.nEntries = 0;
    node.height = 0;
    node.cbTotalFree = cbPage - sizeof(BTreeNode);
    node.cbCompactFree = MAXU;
}

void BTreeNodeAccessor::onInit()
{
    tupleAccessor.compute(tupleDescriptor);
    tupleData.compute(tupleDescriptor);
}

void BTreeNodeAccessor::dumpNode(
    std::ostream &os,BTreeNode const &node,PageId pageId)
{
    os << "PageId:  " << pageId << std::endl;
    if (node.rightSibling == NULL_PAGE_ID) {
        os << "rightSibling:  <null>" << std::endl;
    } else {
        os << "rightSibling:  " << node.rightSibling << std::endl;
    }
    os << "nEntries:  " << node.nEntries << std::endl;
    os << "height:  " << node.height << std::endl;
    os << "cbTotalFree:  " << node.cbTotalFree << std::endl;
    if (!isMAXU(node.cbCompactFree)) {
        os << "cbCompactFree:  " << node.cbCompactFree << std::endl;
    }
    os << std::endl;
    TuplePrinter tuplePrinter;
    // TODO:  print "+infinity" for last key on right fringe
    for (uint i = 0; i < node.nEntries; ++i) {
        PConstBuffer pEntry = getEntryForRead(node,i);
        os << "offset = "
           << pEntry - reinterpret_cast<PConstBuffer>(&node) << std::endl;
        tupleAccessor.setCurrentTupleBuf(pEntry);
        tupleAccessor.unmarshal(tupleData);
        tuplePrinter.print(os,tupleDescriptor,tupleData);
        os << std::endl;
    }
    os << std::endl;
}

// TODO:  warning about usage of tupleAccessor
// TODO:  override with a more efficient BTreeHeapNodeAccessor::compactNode
// (eliminate virtual calls and reduce pointer arithmetic)
void BTreeNodeAccessor::compactNode(BTreeNode &node,BTreeNode &scratchNode)
{
    assert(!scratchNode.nEntries);
    for (uint i = 0; i < node.nEntries; ++i) {
        accessTuple(node,i);
        uint cbTuple = tupleAccessor.getCurrentByteCount();
        PBuffer pBuffer = allocateEntry(scratchNode,i,cbTuple);
        memcpy(pBuffer,tupleAccessor.getCurrentTupleBuf(),cbTuple);
    }

    if (!isMAXU(node.cbCompactFree)) {
        node.cbCompactFree = node.cbTotalFree;
    }
    memcpy(&scratchNode,&node,sizeof(BTreeNode));
}

// REVIEW:  to maximize the allowed tuple length, I think the split algorithm
// below needs to use the index of the new tuple

// TODO:  override with a more efficient BTreeCompactNodeAccessor::splitNode
// (memcpy everything at once) and BTreeHeapNodeAccessor::splitNode (eliminate
// virtual calls and reduce pointer arithmetic)
void BTreeNodeAccessor::splitNode(
    BTreeNode &node,BTreeNode &newNode,
    uint cbNewTuple,
    bool monotonic)
{
    assert(!newNode.nEntries);
    assert(node.nEntries > 1);
    newNode.height = node.height; // split should be of the same height

    // if monotonic, for leaf page,
    // don't actually split the page; leave the left
    // page as is and force all inserts to go into the new page
    // on the right
    // for internal node,
    // put the last entry to the right page.
    if (monotonic && node.height == 0) {
        return;
    }

    // Calculate the balance point in bytes
    uint cbNeeded = getEntryByteCount(cbNewTuple);
    uint cbBalance = cbNeeded;
    if (!monotonic) {
        cbBalance = (node.cbTotalFree + newNode.cbTotalFree - cbNeeded) / 2;
        cbBalance = std::max(cbNeeded,cbBalance);
    }

    // Calculate the corresponding split point in terms of tuples
    uint cbFreeAfterSplit = node.cbTotalFree;
    uint iSplitTuple = node.nEntries;
    while (cbFreeAfterSplit < cbBalance) {
        --iSplitTuple;
        accessTuple(node,iSplitTuple);
        uint cbTuple = tupleAccessor.getCurrentByteCount();
        cbFreeAfterSplit += getEntryByteCount(cbTuple);
    }

    // Copy tuples accordingly.
    for (uint i = iSplitTuple; i < node.nEntries; ++i) {
        accessTuple(node,i);
        uint cbTuple = tupleAccessor.getCurrentByteCount();
        PBuffer pNewEntry = allocateEntry(newNode,newNode.nEntries,cbTuple);
        memcpy(pNewEntry,tupleAccessor.getCurrentTupleBuf(),cbTuple);
    }

    // Truncate old node.  NOTE: This isn't kosher, since it assumes too much
    // about data layout, but this whole method is going to go away soon
    // anyway.
    node.nEntries = iSplitTuple;
    node.cbTotalFree = cbFreeAfterSplit;
}

FENNEL_END_CPPFILE("$Id$");

// End BTreeNodeAccessor.cpp
