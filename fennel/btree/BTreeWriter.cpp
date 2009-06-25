/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005-2009 The Eigenbase Project
// Copyright (C) 2005-2009 SQLstream, Inc.
// Copyright (C) 2005-2009 LucidEra, Inc.
// Portions Copyright (C) 1999-2009 John V. Sichi
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
#include "fennel/btree/BTreeWriter.h"
#include "fennel/btree/BTreeRecoveryFactory.h"
#include "fennel/btree/BTreeAccessBaseImpl.h"
#include "fennel/btree/BTreeReaderImpl.h"
#include "fennel/btree/BTreeDuplicateKeyExcn.h"
#include "fennel/txn/LogicalTxn.h"
#include "fennel/common/ByteOutputStream.h"
#include "fennel/common/ByteInputStream.h"
#include "fennel/tuple/TuplePrinter.h"

FENNEL_BEGIN_CPPFILE("$Id$");


BTreeWriter::BTreeWriter(
    BTreeDescriptor const &descriptor,
    SegmentAccessor const &scratchAccessorInit,
    bool monotonicInit)
    : BTreeReader(descriptor),
      scratchAccessor(scratchAccessorInit)
{
    // we need exclusive locks on leaves for update operations
    leafLockMode = LOCKMODE_X;
    scratchPageLock.accessSegment(scratchAccessor);
    splitTupleBuffer.reset(
        new FixedBuffer[pNonLeafNodeAccessor->tupleAccessor.getMaxByteCount()]);
    leafTupleBuffer.reset(
        new FixedBuffer[pLeafNodeAccessor->tupleAccessor.getMaxByteCount()]);
    monotonic = monotonicInit;
}

BTreeWriter::~BTreeWriter()
{
}

void BTreeWriter::insertTupleData(
    TupleData const &tupleData,Distinctness distinctness)
{
    // TODO:  a small optimization is to use unmarshalled key data directly
    pLeafNodeAccessor->tupleAccessor.marshal(tupleData, leafTupleBuffer.get());
    insertTupleFromBuffer(leafTupleBuffer.get(), distinctness);
}

uint BTreeWriter::insertTupleFromBuffer(
    PConstBuffer pTupleBuffer, Distinctness distinctness)
{
    BTreeNodeAccessor &nodeAccessor = *pLeafNodeAccessor;
    nodeAccessor.tupleAccessor.setCurrentTupleBuf(pTupleBuffer);

    validateTupleSize(nodeAccessor.tupleAccessor);

    uint cbTuple = nodeAccessor.tupleAccessor.getCurrentByteCount();

#if 0
    TuplePrinter tuplePrinter;
    tuplePrinter.print(std::cout, keyDescriptor, searchKeyData);
    std::cout << std::endl;
#endif

    // monotonic inserts do not need to search for key; we will always
    // be inserting after where we are positioned except the first time
    // through or after a split, where we will need to do an initial search
    // to setup the appropriate position
    if (monotonic) {
        if (isPositioned()) {
            ++iTupleOnLowestLevel;
            assert(checkMonotonicity(nodeAccessor, pTupleBuffer));
        } else {
            positionSearchKey(nodeAccessor);
        }
    } else {
        bool duplicate = positionSearchKey(nodeAccessor);

        // REVIEW:  This implements the SQL semantics whereby keys with null
        // values are considered duplicates for DISTINCT but not for UNIQUE.
        // Should probably introduce new DUP_ options to make it explicit.
        if (duplicate) {
            switch (distinctness) {
            case DUP_ALLOW:
                break;
            case DUP_DISCARD:
                endSearch();
                return cbTuple;
            default:
                if (!searchKeyData.containsNull()) {
                    endSearch();
                    throw BTreeDuplicateKeyExcn(keyDescriptor, searchKeyData);
                }
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

    bool split = !attemptInsertWithoutSplit(
        pageLock, pTupleBuffer, cbTuple, iTupleOnLowestLevel);
    if (split) {
        splitCurrentNode(pTupleBuffer, cbTuple, iTupleOnLowestLevel);
    }

    // restart the search after a split even if monotonic insert to
    // ensure correct path to newnode
    if (!monotonic || split) {
        endSearch();
    }

    return cbTuple;
}

bool BTreeWriter::attemptInsertWithoutSplit(
    BTreePageLock &targetPageLock,
    PConstBuffer pTupleBuffer, uint cbTuple, uint iPosition)
{
    BTreeNode *pNode = &(targetPageLock.getNodeForWrite());
    BTreeNodeAccessor &nodeAccessor = getNodeAccessor(*pNode);
    switch (nodeAccessor.calculateCapacity(*pNode,cbTuple)) {
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
        nodeAccessor.dumpNode(std::cerr, *pNode,pageId);
#endif

        break;
    case BTreeNodeAccessor::CAN_NOT_FIT:
        return false;
    }
    PBuffer pBuffer = nodeAccessor.allocateEntry(*pNode,iPosition,cbTuple);
    memcpy(pBuffer, pTupleBuffer, cbTuple);
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
    nodeAccessor.clearNode(scratchNode, getSegment()->getUsablePageSize());
    nodeAccessor.compactNode(node, scratchNode);
    pageLock.swapBuffers(scratchPageLock);
}

void BTreeWriter::splitCurrentNode(
    PConstBuffer pTupleBuffer, uint cbTuple, uint iNewTuple)
{
    // Current node will be on left after split.
    PageId leftPageId = pageId;
    BTreeNode &node = pageLock.getNodeForWrite();
    BTreeNodeAccessor &nodeAccessor = getNodeAccessor(node);

    // New node will be on right after split.
    BTreePageLock newPageLock(treeDescriptor.segmentAccessor);
    PageId newPageId = newPageLock.allocatePage(getPageOwnerId());
    BTreeNode &newNode = newPageLock.getNodeForWrite();
    nodeAccessor.clearNode(newNode, getSegment()->getUsablePageSize());

#if 0
    std::cerr << "BEFORE SPLIT:" << std::endl;
    nodeAccessor.dumpNode(std::cerr, node, pageId);
#endif

    setRightSibling(newNode, newPageId, node.rightSibling);
    setRightSibling(node, pageId, newPageId);

    nodeAccessor.splitNode(node, newNode, cbTuple, monotonic);

#if 0
    std::cerr << "LEFT AFTER SPLIT:" << std::endl;
    nodeAccessor.dumpNode(std::cerr, node, pageId);
    std::cerr << "RIGHT AFTER SPLIT:" << std::endl;
    nodeAccessor.dumpNode(std::cerr, newNode, newPageId);
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
        if (nodeAccessor.calculateCapacity(newNode, cbTuple)
            != BTreeNodeAccessor::CAN_NOT_FIT)
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
    bool inserted = attemptInsertWithoutSplit(
        *pLockForNewTuple,pTupleBuffer,cbTuple,iNewTuple);
    permAssert(inserted);

    // We cannot safely access node or newNode any more, so from here
    // on use these instead:
    // leftNode == node
    // rightNode == newNode

    BTreeNode const &leftNode = pageLock.getNodeForRead();
    BTreeNode const &rightNode = newPageLock.getNodeForRead();

    if (pageStack.empty()) {
        // We're splitting the root:  time to grow the tree.
        grow(rightNode, newPageId);

        // NOTE:  newPageLock will unlock automatically on return,
        // and pageLock will be unlocked by the endSearch() call
        // in insertTupleFromBuffer, so there's no need to
        // explicitly unlock either one here.
        return;
    }

    // We're done splitting current node; now update parent node with new keys
    // from split.  In the case of monotonic inserts, flush the left node.
    assert(leftNode.nEntries);
    if (monotonic) {
        pageLock.flushPage(true);
    }

    // This will be the same as nodeAccessor if we're splitting a non-leaf.
    BTreeNodeAccessor &upperNodeAccessor = *pNonLeafNodeAccessor;

    // Prepare the parent entry to associate with the left node,
    // and marshal it into splitTupleBuffer.
    nodeAccessor.accessTuple(leftNode, leftNode.nEntries - 1);
    nodeAccessor.unmarshalKey(upperNodeAccessor.tupleData);
    upperNodeAccessor.tupleData.back().pData =
        reinterpret_cast<PConstBuffer>(&leftPageId);
    upperNodeAccessor.tupleAccessor.marshal(
        upperNodeAccessor.tupleData,
        splitTupleBuffer.get());

    // Save a copy of the right page's high key in searchKeyData; we'll need it
    // later inside of lockParentPage.  Note that this means rightNode needs to
    // remain locked for the duration, because searchKeyData will point into
    // it.
    uint nRightKeys = nodeAccessor.getKeyCount(rightNode);
    nodeAccessor.accessTuple(rightNode, nRightKeys - 1);
    nodeAccessor.unmarshalKey(searchKeyData);

    // Now, lock parent page and find the position of the entry pointing
    // to the original node (pre-split).  If that node is the rightmost node,
    // then the position in the parent node corresponds to the infinity
    // key, i.e., the last entry in the node.
    bool rightMostNode = (rightNode.rightSibling == NULL_PAGE_ID);
    assert(!monotonic || (monotonic && rightMostNode));
    uint iPosition = lockParentPage(leftNode.height, rightMostNode);

    // TODO jvs 12-Feb-2006: The code below uses getEntryForRead, even though
    // we're actually going to write; should either rename that method or add a
    // new getEntryForWrite which returns a non-const pointer.

    // Update the original entry in the parent, changing its child pointer
    // to reference newPageId instead of leftPageId.
    BTreeNode &upperNode = pageLock.getNodeForWrite();
    upperNodeAccessor.tupleAccessor.setCurrentTupleBuf(
        const_cast<PBuffer>(
            upperNodeAccessor.getEntryForRead(upperNode, iPosition)));
    TupleDatum &childDatum = upperNodeAccessor.tupleData.back();
    pChildAccessor->unmarshalValue(
        upperNodeAccessor.tupleAccessor,
        childDatum);
    PageId &childPageId =
        *(reinterpret_cast<PageId *>(const_cast<PBuffer>(childDatum.pData)));
    assert(childPageId == leftPageId);
    childPageId = newPageId;

    // Finally, insert the splitter entry; this may entail recursively
    // splitting again.
    upperNodeAccessor.tupleAccessor.setCurrentTupleBuf(splitTupleBuffer.get());
    cbTuple = upperNodeAccessor.tupleAccessor.getCurrentByteCount();
    inserted = attemptInsertWithoutSplit(
        pageLock, splitTupleBuffer.get(), cbTuple, iPosition);
    if (inserted) {
        // The entry fit; no need for recursion.
        return;
    }

    // Oops, couldn't fit:  go recursive.

    // REVIEW jvs 12-Feb-2006:  split locking in general.
    newPageLock.unlock();

    splitCurrentNode(splitTupleBuffer.get(), cbTuple, iPosition);
}

uint BTreeWriter::lockParentPage(uint height, bool rightMostNode)
{
    assert(!pageStack.empty());
    pageId = pageStack.back();
    pageStack.pop_back();

    uint iPosition;
    for (;;) {
        pageLock.lockPageWithCoupling(pageId, LOCKMODE_X);

        bool found;
        BTreeNode const &parentNode = pageLock.getNodeForRead();
        if (pageId == getRootPageId() && parentNode.height != height + 1) {
            // REVIEW jvs 12-Feb-2006:  I promised Xiaoyang I would
            // review this code, but I still haven't gotten to it.
            // Should only get here in page-level concurrency scenarios.
            // Also need to devise some tests which force this situation.

            // the tree has grown during the time.
            // the new root is not the parent any more.
            //
            pageLock.unlock();
            BTreePageLock stackPageLock(treeDescriptor.segmentAccessor);
            uint iPosition;
            for (;;) {
                stackPageLock.lockPageWithCoupling(pageId, LOCKMODE_S);
                bool found;
                BTreeNode const &node = stackPageLock.getNodeForRead();
                if (node.height == height) {
                    // it is the same level now. get out of the loop.
                    stackPageLock.unlock();
                    break;
                }
                BTreeNodeAccessor &nodeAccessor = getNodeAccessor(node);

                iPosition = nodeAccessor.binarySearch(
                    node,
                    keyDescriptor,
                    searchKeyData,
                    DUP_SEEK_ANY,
                    true,
                    nodeAccessor.tupleData,
                    found);
                nodeAccessor.accessTuple(node, iPosition);
                nodeAccessor.unmarshalKey(nodeAccessor.tupleData);
                if (iPosition == nodeAccessor.getKeyCount(node)) {
                    assert(!found);
                    // What we're searching for is bigger than everything on
                    // this node.  Have to search right.
                    if (node.rightSibling != NULL_PAGE_ID) {
                        pageId = node.rightSibling;
                        continue;
                    }
                }
                pageStack.push_back(pageId);
                pageId = *(PageId *)(nodeAccessor.tupleData.back().pData);
                stackPageLock.unlock();
            }
            pageId = pageStack.back();
            pageStack.pop_back();
            // now lock the real parent.
            pageLock.lockPageWithCoupling(pageId, LOCKMODE_X);
        }

        // we can not use parentNode because it might be changed.
        BTreeNode const &node = pageLock.getNodeForRead();

        BTreeNodeAccessor &nodeAccessor = getNodeAccessor(node);
        if (rightMostNode) {
            // If the split node is the rightmost node, return the position of
            // the rightmost entry in the parent node.
            iPosition = nodeAccessor.getKeyCount(node);
            break;
        }

        // TODO:  deal with duplicates here and above

        iPosition = nodeAccessor.binarySearch(
            node,
            keyDescriptor,
            searchKeyData,
            DUP_SEEK_ANY,
            true,
            nodeAccessor.tupleData,
            found);

        if (iPosition == nodeAccessor.getKeyCount(node)) {
            assert(!found);
            // What we're searching for is bigger than everything on
            // this node.  Have to search right.
            if (node.rightSibling != NULL_PAGE_ID) {
                pageId = node.rightSibling;
                continue;
            } else {
                break;
            }
        }
        // it could be before the first entry, so not found is OK!
        break;
    }
    return iPosition;
}

void BTreeWriter::grow(
    BTreeNode const &rightNode, PageId rightPageId)
{
    BTreeNode &node = pageLock.getNodeForWrite();
    BTreeNodeAccessor &nodeAccessor = getNodeAccessor(node);
    // GROW BTREE
    // the btree needs to grow. The root should have
    // two entries: one to the left page and one the right page.
    // and the btree needs to keep the old root page id.
#if  0
    std::cerr << "before grow root:" << std::endl;
    nodeAccessor.dumpNode(std::cerr, node, pageId);
    std::cerr << "before grow right:" << std::endl;
    nodeAccessor.dumpNode(std::cerr, rightNode, rightPageId);
#endif

    BTreePageLock newLeftPageLock(treeDescriptor.segmentAccessor);
    PageId newLeftPageId = newLeftPageLock.allocatePage(getPageOwnerId());
    BTreeNode &newLeftNode = newLeftPageLock.getNodeForWrite();
    nodeAccessor.clearNode(newLeftNode, getSegment()->getUsablePageSize());
    // 1. copy the content of old root page to the new page2. (left).
    newLeftNode = node;
    memcpy(
        newLeftNode.getDataForWrite(),
        node.getDataForRead(),
        getSegment()->getUsablePageSize() - sizeof(BTreeNode));

    // 1a. fix the prefetch links to match the games we're playing
    // with the root
    getSegment()->setPageSuccessor(newLeftPageId, rightPageId);
    getSegment()->setPageSuccessor(pageId, NULL_PAGE_ID);

    // 2. clear the root page and set the right height.
    // we use the old nodeAccessor to clear the node.

    nodeAccessor.clearNode(node, getSegment()->getUsablePageSize());
    node.height = newLeftNode.height + 1;
    BTreeNodeAccessor &rootNodeAccessor = getNonLeafNodeAccessor(node);

    // 3. add two entries to the root page.

    nodeAccessor.accessTuple(newLeftNode, newLeftNode.nEntries - 1);
    nodeAccessor.unmarshalKey(rootNodeAccessor.tupleData);
    rootNodeAccessor.tupleData.back().pData =
        reinterpret_cast<PConstBuffer>(&newLeftPageId);
    uint cb = rootNodeAccessor.tupleAccessor.getByteCount(
                        rootNodeAccessor.tupleData);
    PBuffer pBuffer1 = rootNodeAccessor.allocateEntry(node, 0, cb);
    rootNodeAccessor.tupleAccessor.marshal(
        rootNodeAccessor.tupleData, pBuffer1);

    nodeAccessor.accessTuple(rightNode, rightNode.nEntries - 1);
    nodeAccessor.unmarshalKey(rootNodeAccessor.tupleData);
    rootNodeAccessor.tupleData.back().pData =
            reinterpret_cast<PConstBuffer>(&rightPageId);
    cb = rootNodeAccessor.tupleAccessor.getByteCount(
                rootNodeAccessor.tupleData);

    PBuffer pBuffer2 = rootNodeAccessor.allocateEntry(node, 1, cb);
    rootNodeAccessor.tupleAccessor.marshal(
        rootNodeAccessor.tupleData, pBuffer2);
#if 0
    std::cerr << "after grow root:" << std::endl;
    rootNodeAccessor.dumpNode(std::cerr, node, pageId);
    std::cerr << "after grow left:" << std::endl;
    nodeAccessor.dumpNode(std::cerr, newLeftNode, newLeftPageId);
    std::cerr << "after grow right:" << std::endl;
    nodeAccessor.dumpNode(std::cerr, rightNode, rightPageId);
#endif
    if (monotonic) {
        newLeftPageLock.flushPage(true);
    }
    newLeftPageLock.unlock();
    return;
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

    nodeAccessor.deallocateEntry(node, iTupleOnLowestLevel);

    // precompensate for subsequent calls to searchNext()
    --iTupleOnLowestLevel;
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
            nodeAccessor.getEntryForRead(*pNode, iTupleOnLowestLevel));
        nodeAccessor.tupleAccessor.marshal(tupleData, pTupleBuf);
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
        nodeAccessor.deallocateEntry(*pNode,iTupleOnLowestLevel);
        break;
    case BTreeNodeAccessor::CAN_FIT_WITH_COMPACTION:
        nodeAccessor.deallocateEntry(*pNode,iTupleOnLowestLevel);
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

    pTupleBuf = nodeAccessor.allocateEntry(*pNode,iTupleOnLowestLevel,cbTuple);
    nodeAccessor.tupleAccessor.marshal(tupleData, pTupleBuf);
    return true;
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
    switch (actionType) {
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
    switch (actionType) {
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
        logStream.getReadPointer(1), DUP_ALLOW);
    logStream.consumeReadPointer(cbTuple);
}

void BTreeWriter::deleteLogged(ByteInputStream &logStream)
{
    pLeafNodeAccessor->tupleAccessor.setCurrentTupleBuf(
        logStream.getReadPointer(1));
    uint cbTuple = pLeafNodeAccessor->tupleAccessor.getCurrentByteCount();
    pLeafNodeAccessor->unmarshalKey(searchKeyData);
    if (searchForKey(searchKeyData, DUP_SEEK_ANY)) {
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

bool BTreeWriter::positionSearchKey(BTreeNodeAccessor &nodeAccessor)
{
    nodeAccessor.unmarshalKey(searchKeyData);
    pageStack.clear();
    DuplicateSeek seekMode = (monotonic) ? DUP_SEEK_END : DUP_SEEK_ANY;
    bool duplicate = searchForKeyTemplate< true, std::vector<PageId> >(
        searchKeyData, seekMode, true, pageStack, getRootPageId(), rootLockMode,
        READ_ALL);
    return duplicate;
}

bool BTreeWriter::checkMonotonicity(
    BTreeNodeAccessor &nodeAccessor, PConstBuffer pTupleBuffer)
{
    // unmarshal previous key, if accessible
    if (iTupleOnLowestLevel == 0) {
        return true;
    }
    BTreeNode const &node = pageLock.getNodeForRead();
    accessTupleInline(node, iTupleOnLowestLevel - 1);
    nodeAccessor.unmarshalKey(comparisonKeyData);

    nodeAccessor.tupleAccessor.setCurrentTupleBuf(pTupleBuffer);
    nodeAccessor.unmarshalKey(searchKeyData);
    int keyComp = keyDescriptor.compareTuples(comparisonKeyData, searchKeyData);

    return (keyComp <= 0 && node.nEntries == iTupleOnLowestLevel);
}
FENNEL_END_CPPFILE("$Id$");

// End BTreeWriter.cpp
