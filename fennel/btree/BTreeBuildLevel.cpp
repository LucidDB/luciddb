/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2005-2005 Disruptive Tech
// Copyright (C) 2005-2005 Red Square, Inc.
// Portions Copyright (C) 1999-2005 John V. Sichi
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
#include "fennel/btree/BTreeBuildLevel.h"
#include "fennel/btree/BTreeNodeAccessor.h"
#include "fennel/btree/BTreeAccessBaseImpl.h"
#include "fennel/segment/SegInputStream.h"
#include "fennel/segment/SegOutputStream.h"

FENNEL_BEGIN_CPPFILE("$Id$");

BTreeBuildLevel::BTreeBuildLevel(
    BTreeBuilder &builderInit,
    BTreeNodeAccessor &nodeAccessorInit)
    : builder(builderInit), nodeAccessor(nodeAccessorInit)
{
    iLevel = 0;
    nEntriesTotal = 0;
    cbReserved = 0;
    iNode = 0;
    nEntriesProcessed = 0;
    nEntriesPerNode = 0;
    pageLock.accessSegment(builder.treeDescriptor.segmentAccessor);
}

BTreeBuildLevel::~BTreeBuildLevel()
{
}

bool BTreeBuildLevel::isFinished() const
{
    return (pageLock.getNodeForRead().nEntries == nEntriesPerNode)
        && (nEntriesProcessed == nEntriesTotal);
}

void BTreeBuildLevel::processInput(ByteInputStream &sortedInputStream)
{
    BTreeNode *pNode = &(pageLock.getNodeForWrite());

    for (;;) {
        // Read one tuple.
        uint cbActual;
        PConstBuffer pCurrentTupleBuf =
            sortedInputStream.getReadPointer(1,&cbActual);
        if (!pCurrentTupleBuf) {
            break;
        }
        if (nEntriesTotal) {
            assert(nEntriesProcessed < nEntriesTotal);
        }
        nodeAccessor.tupleAccessor.setCurrentTupleBuf(pCurrentTupleBuf);
        uint cbTuple = nodeAccessor.tupleAccessor.getCurrentByteCount();
        assert(cbActual >= cbTuple);

        builder.validateTupleSize(nodeAccessor.tupleAccessor);

        // Make sure we have enough room.
        if (isNodeFull(*pNode,cbTuple)) {
            indexLastKey(false);
            pNode = allocateAndLinkNewNode();
            assert(!isNodeFull(*pNode,cbTuple));
            // indexLastKey used tupleAccessor, so have to rebind it
            nodeAccessor.tupleAccessor.setCurrentTupleBuf(pCurrentTupleBuf);
        }

        // Append the current tuple.
        PBuffer pEntry = nodeAccessor.allocateEntry(
            *pNode,
            pNode->nEntries,
            cbTuple);
        memcpy(
            pEntry,
            pCurrentTupleBuf,
            cbTuple);
        ++nEntriesProcessed;

        // Advance to the next input tuple.
        sortedInputStream.consumeReadPointer(cbTuple);
    }
}

void BTreeBuildLevel::indexLastChild()
{
    assert(iLevel > 0);
    if (nEntriesTotal) {
        assert(nEntriesProcessed < nEntriesTotal);
    }

    uint cbTuple = nodeAccessor.tupleAccessor.getByteCount(
        nodeAccessor.tupleData);
    
    BTreeNode *pNode = &(pageLock.getNodeForWrite());
    if (isNodeFull(*pNode,cbTuple)) {
        indexLastKey(false);
        pNode = allocateAndLinkNewNode();

        // FIXME: should override fillfactor here, or provide a proper error
        // msg; same thing somewhere else
        assert(!isNodeFull(*pNode,cbTuple));
        
        // indexLastKey used tupleData, so have to rebind it
        builder.getLevel(iLevel - 1).unmarshalLastKey();
    }

    // Now append the child entry.
    nodeAccessor.tupleData.back().pData =
        reinterpret_cast<PConstBuffer>(&(builder.getLevel(iLevel-1).pageId));
    PBuffer pEntry = nodeAccessor.allocateEntry(
        *pNode,pNode->nEntries,cbTuple);
    nodeAccessor.tupleAccessor.marshal(nodeAccessor.tupleData,pEntry);
    ++nEntriesProcessed;
}

void BTreeBuildLevel::unmarshalLastKey()
{
    BTreeNode const &node = pageLock.getNodeForRead();
    nodeAccessor.accessTuple(node,node.nEntries - 1);
    nodeAccessor.unmarshalKey(builder.pNonLeafNodeAccessor->tupleData);
}

BTreeNode *BTreeBuildLevel::allocateAndLinkNewNode()
{
    PageId prevPageId = pageId;

    BTreeNode &node = allocatePage();

    // Ugly:  we have to go back and fix up the rightSibling link on the
    // previous page.  There are better ways, but they require messing with
    // SegPageLock.
    BTreePageLock prevPageLock;
    prevPageLock.accessSegment(builder.treeDescriptor.segmentAccessor);
    prevPageLock.lockExclusive(prevPageId);
    BTreeNode &prevNode = prevPageLock.getNodeForWrite();
    builder.setRightSibling(
        prevNode,
        prevPageId,
        pageId);
    
    // Let the cache know we're not planning to revisit the page we just
    // finished.
    builder.getCacheAccessor()->nicePage(prevPageLock.getPage());
    
    ++iNode;
    if (nEntriesPerNode) {
        // Recalculate balancing.
        nEntriesPerNode = builder.calculateChildEntriesPerNode(
            builder.getLevel(iLevel+1).nEntriesTotal, 
            nEntriesTotal, 
            iNode);
    }
    return &node;
}

bool BTreeBuildLevel::isNodeFull(BTreeNode const &node,uint cbTuple)
{
    return nodeAccessor.calculateCapacity(node,cbReserved + cbTuple)
        != BTreeNodeAccessor::CAN_FIT;
}

BTreeNode &BTreeBuildLevel::allocatePage()
{
    pageId = pageLock.allocatePage(builder.getPageOwnerId());
    BTreeNode &node = pageLock.getNodeForWrite();
    nodeAccessor.clearNode(
        node,builder.getSegment()->getUsablePageSize());
    node.height = iLevel;
    return node;
}

FixedBuildLevel::FixedBuildLevel(
    BTreeBuilder &builderInit,
    BTreeNodeAccessor &nodeAccessorInit)
    : BTreeBuildLevel(builderInit,nodeAccessorInit)
{
}

void FixedBuildLevel::indexLastKey(bool finalize)
{
    assert(pageLock.getNodeForRead().nEntries == nEntriesPerNode);
    if (iLevel == builder.getRootHeight()) {
        assert(finalize);
        return;
    }
    unmarshalLastKey();
    builder.getLevel(iLevel + 1).indexLastChild();
}

bool FixedBuildLevel::isNodeFull(BTreeNode const &node,uint)
{
    return (node.nEntries >= nEntriesPerNode);
}

VariableBuildLevel::VariableBuildLevel(
    BTreeBuilder &builderInit,
    BTreeNodeAccessor &nodeAccessorInit)
    : BTreeBuildLevel(builderInit,nodeAccessorInit)
{
    assert(builder.pTempSegment);
    SegmentAccessor tempSegmentAccessor(
        builder.pTempSegment,builder.getCacheAccessor());
    pParentKeyStream = SegOutputStream::newSegOutputStream(tempSegmentAccessor);
}

VariableBuildLevel::~VariableBuildLevel()
{
    if (!pParentKeyStream->isClosed()) {
        // no one ever used the buffered keys, so we need to deallocate them
        // now
        getParentKeyStream();
    }
}

SharedSegInputStream VariableBuildLevel::getParentKeyStream()
{
    PageId tempPageId = pParentKeyStream->getFirstPageId();
    pParentKeyStream->close();
    SegmentAccessor tempSegmentAccessor(
        builder.pTempSegment,builder.getCacheAccessor());
    SharedSegInputStream pParentInputStream =
        SegInputStream::newSegInputStream(
            tempSegmentAccessor,
            tempPageId);
    // deallocate temp pages automatically
    pParentInputStream->setDeallocate(true);
    return pParentInputStream;
}

void VariableBuildLevel::indexLastKey(bool)
{
    unmarshalLastKey();
    BTreeNodeAccessor &nonLeafNodeAccessor = *builder.pNonLeafNodeAccessor;
    // REVIEW:  use parent level info instead?
    nonLeafNodeAccessor.tupleData.back().pData =
        reinterpret_cast<PConstBuffer>(&pageId);
    uint cbTuple = nonLeafNodeAccessor.tupleAccessor.getByteCount(
        nonLeafNodeAccessor.tupleData);
    PBuffer pStreamBuf = pParentKeyStream->getWritePointer(cbTuple);
    nonLeafNodeAccessor.tupleAccessor.marshal(
        nonLeafNodeAccessor.tupleData,
        pStreamBuf);
    pParentKeyStream->consumeWritePointer(cbTuple);
}

DynamicBuildLevel::DynamicBuildLevel(
    BTreeBuilder &builderInit,
    BTreeNodeAccessor &nodeAccessorInit)
    : BTreeBuildLevel(builderInit,nodeAccessorInit)
{
}

void DynamicBuildLevel::indexLastKey(bool finalize)
{
    if (iLevel == builder.getRootHeight()) {
        if (finalize) {
            return;
        } else {
            builder.growTree();
        }
    }
    unmarshalLastKey();
    builder.getLevel(iLevel + 1).indexLastChild();
}

FENNEL_END_CPPFILE("$Id$");

// End BTreeBuildLevel.cpp
