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
#include "fennel/btree/BTreeWriter.h"
#include "fennel/btree/BTreeRecoveryFactory.h"
#include "fennel/btree/BTreeAccessBaseImpl.h"
#include "fennel/btree/BTreeReaderImpl.h"
#include "fennel/btree/BTreeDuplicateKeyExcn.h"
#include "fennel/txn/LogicalTxn.h"
#include "fennel/common/ByteOutputStream.h"
#include "fennel/common/ByteInputStream.h"

FENNEL_BEGIN_CPPFILE("$Id$");

BTreeWriter::BTreeWriter(
    BTreeDescriptor const &descriptor,
    SegmentAccessor const &scratchAccessorInit)
    : BTreeReader(descriptor),
      scratchAccessor(scratchAccessorInit)
{
    // we need exclusive locks on leaves for update operations
    leafLockMode = LOCKMODE_X;
    scratchPageLock.accessSegment(scratchAccessor);
    splitTupleBuffer.reset(
        new FixedBuffer[pNonLeafNodeAccessor->tupleAccessor.getMaxByteCount()]);
    parentTupleBuffer.reset(
        new FixedBuffer[pNonLeafNodeAccessor->tupleAccessor.getMaxByteCount()]);
    leafTupleBuffer.reset(
        new FixedBuffer[pLeafNodeAccessor->tupleAccessor.getMaxByteCount()]);
}

BTreeWriter::~BTreeWriter()
{
}

void BTreeWriter::insertTupleData(
    TupleData const &tupleData,Distinctness distinctness)
{
    // TODO:  a small optimization is to use unmarshalled key data directly
    pLeafNodeAccessor->tupleAccessor.marshal(tupleData,leafTupleBuffer.get());
    insertTupleFromBuffer(leafTupleBuffer.get(),distinctness);
}

uint BTreeWriter::insertTupleFromBuffer(
    PConstBuffer pTupleBuffer,Distinctness distinctness)
{
    BTreeNodeAccessor &nodeAccessor = *pLeafNodeAccessor;
    nodeAccessor.tupleAccessor.setCurrentTupleBuf(pTupleBuffer);

    validateTupleSize(nodeAccessor.tupleAccessor);
    
    uint cbTuple = nodeAccessor.tupleAccessor.getCurrentByteCount();

    nodeAccessor.unmarshalKey(searchKeyData);
    
    pageStack.clear();
    bool duplicate = searchForKeyTemplate< true, std::vector<PageId> >(
        searchKeyData,DUP_SEEK_ANY,pageStack);

    // REVIEW:  This implements the SQL semantics whereby keys with null values
    // are considered duplicates for DISTINCT but not for UNIQUE.  Should
    // probably introduce new DUP_ options to make it explicit.
    if (duplicate) {
        switch(distinctness) {
        case DUP_ALLOW:
            break;
        case DUP_DISCARD:
            endSearch();
            return cbTuple;
        default:
            if (!searchKeyData.containsNull()) {
                endSearch();
                throw BTreeDuplicateKeyExcn(keyDescriptor,searchKeyData);
            }
        }
    }

    if (isLoggingEnabled()) {
        ByteOutputStream &logStream = getLogicalTxn()->beginLogicalAction(
            *this,
            ACTION_INSERT);
        memcpy(
            logStream.getWritePointer(cbTuple),
            pTupleBuffer,
            cbTuple);
        logStream.consumeWritePointer(cbTuple);
        getLogicalTxn()->endLogicalAction();
    }
    
    if (!attemptInsertWithoutSplit(
            pageLock,pTupleBuffer,cbTuple,iTupleOnLeaf))
    {
        BTreeNode &node = pageLock.getNodeForWrite();
        splitNode(node,pTupleBuffer,cbTuple,iTupleOnLeaf);
    }
    endSearch();

    return cbTuple;
}

bool BTreeWriter::attemptInsertWithoutSplit(
    BTreePageLock &targetPageLock,
    PConstBuffer pTupleBuffer,uint cbTuple,uint iPosition)
{
    BTreeNode *pNode = &(targetPageLock.getNodeForWrite());
    BTreeNodeAccessor &nodeAccessor = getNodeAccessor(*pNode);
    switch(nodeAccessor.calculateCapacity(*pNode,cbTuple)) {
    case BTreeNodeAccessor::CAN_FIT:
        break;
    case BTreeNodeAccessor::CAN_FIT_WITH_COMPACTION:
        compactNode(pageLock);
        // compactNode uses swapBuffers, which is why we have to use a pointer
        // rather than a reference for pNode
        pNode = &(targetPageLock.getNodeForWrite());
        assert(nodeAccessor.calculateCapacity(*pNode,cbTuple)
               == BTreeNodeAccessor::CAN_FIT);

#if 0
        std::cerr << "AFTER COMPACTION:" << std::endl;
        nodeAccessor.dumpNode(std::cerr,*pNode,pageId);
#endif
        
        break;
    case BTreeNodeAccessor::CAN_NOT_FIT:
        return false;
    }
    PBuffer pBuffer = nodeAccessor.allocateEntry(*pNode,iPosition,cbTuple);
    memcpy(pBuffer,pTupleBuffer,cbTuple);
    return true;
}

void BTreeWriter::compactNode(BTreePageLock &targetPageLock)
{
    BTreeNode &node = targetPageLock.getNodeForWrite();
    if (!scratchPageLock.isLocked()) {
        scratchPageLock.allocatePage();
    }
    BTreeNode &scratchNode = scratchPageLock.getNodeForWrite();
    BTreeNodeAccessor &nodeAccessor = getNodeAccessor(node);
    nodeAccessor.clearNode(scratchNode,getSegment()->getUsablePageSize());
    nodeAccessor.compactNode(node,scratchNode);
    pageLock.swapBuffers(scratchPageLock);
}

void BTreeWriter::splitNode(
    BTreeNode &node,PConstBuffer pTupleBuffer,uint cbTuple,uint iNewTuple)
{
    BTreeNodeAccessor &nodeAccessor = getNodeAccessor(node);
    
    BTreePageLock newPageLock(treeDescriptor.segmentAccessor);
    PageId newPageId = newPageLock.allocatePage(getPageOwnerId());
    BTreeNode &newNode = newPageLock.getNodeForWrite();
    nodeAccessor.clearNode(newNode,getSegment()->getUsablePageSize());

#if 0
    std::cerr << "BEFORE SPLIT:" << std::endl;
    nodeAccessor.dumpNode(std::cerr,node,pageId);
#endif
    
    setRightSibling(newNode,newPageId,node.rightSibling);
    setRightSibling(node,pageId,newPageId);
    
    nodeAccessor.splitNode(node,newNode,cbTuple);
    
#if 0
    std::cerr << "LEFT AFTER SPLIT:" << std::endl;
    nodeAccessor.dumpNode(std::cerr,node,pageId);
    std::cerr << "RIGHT AFTER SPLIT:" << std::endl;
    nodeAccessor.dumpNode(std::cerr,newNode,newPageId);
#endif

    // Figure out where new tuple goes
    BTreePageLock *pLockForNewTuple = NULL;
    if (iNewTuple < node.nEntries) {
        // definitely on left
        pLockForNewTuple = &pageLock;
    } else if (iNewTuple > node.nEntries) {
        // definitely on right
        pLockForNewTuple = &newPageLock;
    } else {
        // could go either way depending on how the balancing worked out
        if (nodeAccessor.calculateCapacity(newNode,cbTuple) !=
            BTreeNodeAccessor::CAN_NOT_FIT)
        {
            pLockForNewTuple = &newPageLock;
        } else {
            pLockForNewTuple = &pageLock;
        }
    }
    if (pLockForNewTuple == &newPageLock) {
        iNewTuple -= node.nEntries;
    }
    // NOTE:  After this,  either the node or newNode reference may be
    // invalid, so don't use them.
    if (!attemptInsertWithoutSplit(
            *pLockForNewTuple,pTupleBuffer,cbTuple,iNewTuple)) {
        permAssert(false);
    }

    // TODO:  parent update
#if 0
    BTreeNode &leftNode = pageLock.getNodeForWrite();
    assert(leftNode.nEntries);
    nodeAccessor.accessTuple(leftNode,leftNode.nEntries - 1);
    memcpy(splitTupleBuffer.get(),
           nodeAccessor.tupleAccessor.getCurrentTupleBuf(),
           nodeAccessor.tupleAccessor.getCurrentByteCount());

    BTreeNode &rightNode = newPageLock.getNodeForWrite();
    assert(rightNode.nEntries);
    nodeAccessor.accessTuple(rightNode,rightNode.nEntries - 1);
    memcpy(parentTupleBuffer.get(),
           nodeAccessor.tupleAccessor.getCurrentTupleBuf(),
           nodeAccessor.tupleAccessor.getCurrentByteCount());
    nodeAccessor.tupleAccessor.setCurrentTupleBuf(parentTupleBuffer.get());
    nodeAccessor.unmarshalKey(searchKeyData);

    newPageLock.unlock();

    insertIntoParent();
#endif
    
}

inline void BTreeWriter::optimizeRootLockMode()
{
    if (pageId != getRootPageId()) {
        // In case the tree has grown, counteract the effect of
        // adjustRootLockMode for subsequent searches.
        rootLockMode = LOCKMODE_S;
    }
}

void BTreeWriter::deleteCurrent()
{
    // TODO:  Balancing, all that jazz?  Maybe.
    
    assert(pageLock.isLocked());

    optimizeRootLockMode();
    
    BTreeNode &node = pageLock.getNodeForWrite();
    BTreeNodeAccessor &nodeAccessor = getLeafNodeAccessor(node);

    if (isLoggingEnabled()) {
        uint cbTuple = nodeAccessor.tupleAccessor.getCurrentByteCount();
        ByteOutputStream &logStream = getLogicalTxn()->beginLogicalAction(
            *this,
            ACTION_DELETE);
        memcpy(
            logStream.getWritePointer(cbTuple),
            nodeAccessor.tupleAccessor.getCurrentTupleBuf(),
            cbTuple);
        logStream.consumeWritePointer(cbTuple);
        getLogicalTxn()->endLogicalAction();
    }
    
    nodeAccessor.deallocateEntry(node,iTupleOnLeaf);

    // precompensate for subsequent calls to searchNext()
    --iTupleOnLeaf;
}

bool BTreeWriter::updateCurrent(TupleData const &tupleData)
{
    // TODO:  assert that key has not changed
    
    PBuffer pTupleBuf;
    
    assert(pageLock.isLocked());

    optimizeRootLockMode();
    
    BTreeNode *pNode = &(pageLock.getNodeForWrite());
    BTreeNodeAccessor &nodeAccessor = getLeafNodeAccessor(*pNode);
    
    assert(!isLoggingEnabled());

    if (nodeAccessor.hasFixedWidthEntries()) {
        // we can use a direct overwrite
        pTupleBuf = const_cast<PBuffer>(
            nodeAccessor.getEntryForRead(*pNode,iTupleOnLeaf));
        nodeAccessor.tupleAccessor.marshal(tupleData,pTupleBuf);
        return true;
    }
    
    // calculate whether updated tuple can fit
    uint cbTuple =
        nodeAccessor.tupleAccessor.getByteCount(tupleData);
    int cbDelta =
        cbTuple - nodeAccessor.tupleAccessor.getCurrentByteCount();
    if (cbDelta < 0) {
        cbDelta = 0;
    }

    // NOTE:  there's a tiny discrepancy here due to entry overhead, but it
    // errs on the safe side, so ignore it
    switch (nodeAccessor.calculateCapacity(*pNode,cbDelta)) {
    case BTreeNodeAccessor::CAN_FIT:
        nodeAccessor.deallocateEntry(*pNode,iTupleOnLeaf);
        break;
    case BTreeNodeAccessor::CAN_FIT_WITH_COMPACTION:
        nodeAccessor.deallocateEntry(*pNode,iTupleOnLeaf);
        compactNode(pageLock);
        // compactNode uses swapBuffers, which is why we have to use a pointer
        // rather than a reference for pNode
        pNode = &(pageLock.getNodeForWrite());
        assert(nodeAccessor.calculateCapacity(*pNode,cbTuple)
               == BTreeNodeAccessor::CAN_FIT);
        break;
    case BTreeNodeAccessor::CAN_NOT_FIT:
        return false;
    default:
        permAssert(false);
    }

    pTupleBuf = nodeAccessor.allocateEntry(*pNode,iTupleOnLeaf,cbTuple);
    nodeAccessor.tupleAccessor.marshal(tupleData,pTupleBuf);
    return true;
}

void BTreeWriter::insertIntoParent()
{
    // TODO:  deal with case where tree has grown since we walked down
    assert(!pageStack.empty());
    pageId = pageStack.back();
    pageStack.pop_back();
    BTreeNodeAccessor &nodeAccessor = *pNonLeafNodeAccessor;

    uint iPosition;
    for (;;) {
        pageLock.lockPageWithCoupling(pageId,LOCKMODE_X);
        
        bool found;
        BTreeNode const &node = pageLock.getNodeForRead();
        // TODO:  deal with duplicates
        iPosition = nodeAccessor.binarySearch(
            node,
            nodeAccessor.tupleDescriptor,
            searchKeyData,
            DUP_SEEK_ANY,
            nodeAccessor.tupleData,
            found);

        if (iPosition == node.nEntries) {
            assert(!found);
            // What we're searching for is bigger than everything on
            // this node.  Have to search right.
            assert(node.rightSibling != NULL_PAGE_ID);
            pageId = node.rightSibling;
            continue;
        }

        assert(found);
        break;
    }
    nodeAccessor.tupleAccessor.setCurrentTupleBuf(splitTupleBuffer.get());
    uint cbTuple = nodeAccessor.tupleAccessor.getCurrentByteCount();
    if (attemptInsertWithoutSplit(
            pageLock,splitTupleBuffer.get(),cbTuple,iPosition))
    {
        return;
    }
    BTreeNode &node = pageLock.getNodeForWrite();
    splitNode(node,splitTupleBuffer.get(),cbTuple,iPosition);
}

LogicalTxnClassId BTreeWriter::getParticipantClassId() const
{
    return BTreeRecoveryFactory::getParticipantClassId();
}

void BTreeWriter::describeParticipant(
    ByteOutputStream &logStream)
{
    PageId rootPageId = getRootPageId();
    logStream.writeValue(rootPageId);
    getTupleDescriptor().writePersistent(logStream);
    getKeyProjection().writePersistent(logStream);
}

void BTreeWriter::undoLogicalAction(
    LogicalActionType actionType,
    ByteInputStream &logStream)
{
    switch(actionType) {
    case ACTION_INSERT:
        deleteLogged(logStream);
        break;
    case ACTION_DELETE:
        insertLogged(logStream);
        break;
    default:
        permAssert(false);
    }
}

void BTreeWriter::redoLogicalAction(
    LogicalActionType actionType,
    ByteInputStream &logStream)
{
    switch(actionType) {
    case ACTION_INSERT:
        insertLogged(logStream);
        break;
    case ACTION_DELETE:
        deleteLogged(logStream);
        break;
    default:
        permAssert(false);
    }
}

void BTreeWriter::insertLogged(ByteInputStream &logStream)
{
    // REVIEW:  do we ever need to use a different Distinctness?
    uint cbTuple = insertTupleFromBuffer(
        logStream.getReadPointer(1),DUP_ALLOW);
    logStream.consumeReadPointer(cbTuple);
}

void BTreeWriter::deleteLogged(ByteInputStream &logStream)
{
    pLeafNodeAccessor->tupleAccessor.setCurrentTupleBuf(
        logStream.getReadPointer(1));
    uint cbTuple = pLeafNodeAccessor->tupleAccessor.getCurrentByteCount();
    pLeafNodeAccessor->unmarshalKey(searchKeyData);
    if (searchForKey(searchKeyData,DUP_SEEK_ANY)) {
        deleteCurrent();
    } else {
        // TODO:  assert?
    }
    endSearch();
    logStream.consumeReadPointer(cbTuple);
}

void BTreeWriter::releaseScratchBuffers()
{
    scratchPageLock.unlock();
}

FENNEL_END_CPPFILE("$Id$");

// End BTreeWriter.cpp
