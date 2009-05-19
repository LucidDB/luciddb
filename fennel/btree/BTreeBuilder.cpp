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
#include "fennel/btree/BTreeBuilder.h"
#include "fennel/btree/BTreeBuildLevel.h"
#include "fennel/btree/BTreeAccessBaseImpl.h"
#include "fennel/btree/BTreeReader.h"
#include "fennel/segment/SegInputStream.h"
#include "fennel/segment/SegOutputStream.h"

FENNEL_BEGIN_CPPFILE("$Id$");

BTreeBuilder::BTreeBuilder(
    BTreeDescriptor const &descriptor,
    SharedSegment pTempSegmentInit)
    : BTreeAccessBase(descriptor)
{
    pTempSegment = pTempSegmentInit;
}

BTreeBuilder::~BTreeBuilder()
{
}

uint BTreeBuilder::calculateNodesOnLevel(
    uint nEntries,uint nEntriesPerNode)
{
    uint nNodes = nEntries / nEntriesPerNode;
    if (nEntries % nEntriesPerNode) {
        // need an extra node for the remainder
        ++nNodes;
    }
    return nNodes;
}

void BTreeBuilder::build(
    ByteInputStream &sortedInputStream,
    RecordNum nEntriesTotal,
    double fillFactor)
{
    assert(fillFactor <= 1.0);
    assert(fillFactor > 0);
    try {
        if (pLeafNodeAccessor->hasFixedWidthEntries()
            && pNonLeafNodeAccessor->hasFixedWidthEntries())
        {
            buildBalanced(sortedInputStream,0,nEntriesTotal,fillFactor);
        } else if (pNonLeafNodeAccessor->hasFixedWidthEntries()) {
            buildTwoPass(
                sortedInputStream,nEntriesTotal,fillFactor);
        } else {
            assert(!pLeafNodeAccessor->hasFixedWidthEntries());
            buildUnbalanced(sortedInputStream,nEntriesTotal,fillFactor);
        }
    } catch (...) {
        try {
            levels.clear();
        } catch (...) {
            // TODO:  trace suppressed excn
        }
        throw;
    }
    levels.clear();
}

void BTreeBuilder::buildTwoPass(
    ByteInputStream &sortedInputStream,
    RecordNum nEntriesTotal,
    double fillFactor)
{
    // calculate amount of space to reserve on each leaf from fillfactor
    uint cbReserved = getSegment()->getUsablePageSize() - sizeof(BTreeNode);
    cbReserved = uint(cbReserved*(1-fillFactor));

    levels.resize(1);
    BTreeNodeAccessor &nodeAccessor = *pLeafNodeAccessor;
    assert(!nodeAccessor.hasFixedWidthEntries());
    VariableBuildLevel *pLeafLevel = new VariableBuildLevel(*this,nodeAccessor);
    levels[0].reset(pLeafLevel);

    BTreeBuildLevel &level = getLevel(0);
    level.nEntriesTotal = nEntriesTotal;
    level.cbReserved = cbReserved;

    level.allocatePage();

    // feed data into the leaf level; this will collect the entries for
    // level 1 (the parent of the leaf level) in a temp stream
    level.processInput(sortedInputStream);

    level.indexLastKey(true);

    // now we know how many entries to expect for level 1:  the number of
    // leaf nodes just filled
    RecordNum nEntriesParent = level.iNode + 1;

    if (nEntriesParent == 1) {
        // The leaf we just filled turns out to be the root.
        swapRoot();
        return;
    }

    // REVIEW:  distinguish leaf fillFactor from non-leaf fillFactor?

    SharedSegInputStream pParentInputStream =
        pLeafLevel->getParentKeyStream();
    assert(pParentInputStream);
    // feed buffered entries into level 1, and build up balanced from there
    buildBalanced(*pParentInputStream,1,nEntriesParent,fillFactor);
}

void BTreeBuilder::swapRoot()
{
    BTreeBuildLevel &rootLevel = getLevel(getRootHeight());
    if (getRootPageId() == NULL_PAGE_ID) {
        // No rootPageId has been assigned yet, so no need to copy.
        treeDescriptor.rootPageId = rootLevel.pageId;
        return;
    }

    // We're supposed to preserve the PageId of the existing root, so
    // swap buffers with the original root.
    BTreePageLock reusedRootPageLock(treeDescriptor.segmentAccessor);
    reusedRootPageLock.lockExclusive(getRootPageId());
    BTreeNode &reusedRoot = reusedRootPageLock.getNodeForWrite();
    assert(!reusedRoot.nEntries);
    reusedRootPageLock.swapBuffers(rootLevel.pageLock);

    // deallocate the abandonded tempRoot
    rootLevel.pageLock.deallocateLockedPage();
}

void BTreeBuilder::buildUnbalanced(
    ByteInputStream &sortedInputStream,
    RecordNum nEntriesTotal,
    double fillFactor)
{
    // TODO:  common fcn
    // calculate amount of space to reserve on each leaf from fillfactor
    uint cbReserved = getSegment()->getUsablePageSize() - sizeof(BTreeNode);
    cbReserved = uint(cbReserved*(1-fillFactor));

    // start with just a leaf level
    growTree();
    BTreeBuildLevel &level = getLevel(0);
    level.cbReserved = cbReserved;
    level.nEntriesTotal = nEntriesTotal;
    level.processInput(sortedInputStream);

    // NOTE:  It's important to realize that growTree() could be called inside
    // this loop, so it's necessary to recompute levels.size() after each
    // iteration.
    for (uint i = 0; i < levels.size(); ++i) {
        BTreeBuildLevel &level = getLevel(i);
        level.indexLastKey(true);
    }

    swapRoot();
}

void BTreeBuilder::buildBalanced(
    ByteInputStream &sortedInputStream,
    uint iInputLevel,
    RecordNum nEntriesTotal,
    double fillFactor)
{
    // First, determine node capacities based on fixed-width entry sizes.  This
    // gives us the maximum fanout.

    uint nEntriesPerNonLeaf =
        getSegment()->getUsablePageSize() - sizeof(BTreeNode);
    nEntriesPerNonLeaf /=
        pNonLeafNodeAccessor->getEntryByteCount(
            pNonLeafNodeAccessor->tupleAccessor.getMaxByteCount());

    uint nEntriesPerLeaf =
        getSegment()->getUsablePageSize() - sizeof(BTreeNode);
    nEntriesPerLeaf /=
        pLeafNodeAccessor->getEntryByteCount(
            pLeafNodeAccessor->tupleAccessor.getMaxByteCount());

    nEntriesPerNonLeaf = uint(nEntriesPerNonLeaf*fillFactor);
    nEntriesPerLeaf = uint(nEntriesPerLeaf*fillFactor);

    if (!nEntriesPerNonLeaf) {
        nEntriesPerNonLeaf = 1;
    }
    if (!nEntriesPerLeaf) {
        nEntriesPerLeaf = 1;
    }

    // Next, calculate how high a "full" tree with this fanout would have to be
    // in order to accommodate the expected number of entries.  In most cases
    // the tree won't actually be full, but the height can't be any lower than
    // this.

    RecordNum nEntriesFull = iInputLevel ? nEntriesPerNonLeaf : nEntriesPerLeaf;
    uint nLevels = iInputLevel + 1;
    while (nEntriesFull < nEntriesTotal) {
        ++nLevels;
        nEntriesFull *= nEntriesPerNonLeaf;
    }
    levels.resize(nLevels);

    // Now, calculate how many entries to expect on each level.  We could do
    // the per-level balancing here as well, but then we'd have to keep around
    // an in-memory structure proportional to the number of nodes in the tree.
    // Instead, we calculate the balancing on the fly later.
    RecordNum nEntriesInLevel = nEntriesTotal;
    for (uint i = iInputLevel; i < levels.size(); i++) {
        BTreeNodeAccessor &nodeAccessor =
            i ? *pNonLeafNodeAccessor : *pLeafNodeAccessor;

        assert(nodeAccessor.hasFixedWidthEntries());
        levels[i].reset(new FixedBuildLevel(*this,nodeAccessor));

        BTreeBuildLevel &level = getLevel(i);
        level.iLevel = i;
        level.nEntriesTotal = nEntriesInLevel;

        // number of parent entries is same as number of child nodes
        nEntriesInLevel = calculateNodesOnLevel(
            nEntriesInLevel,
            i ? nEntriesPerNonLeaf : nEntriesPerLeaf);

        if (i == getRootHeight()) {
            // Set up the root info, which can be fully determined ahead of
            // time.
            level.nEntriesPerNode = level.nEntriesTotal;
            if (getRootPageId() == NULL_PAGE_ID) {
                level.allocatePage();
                treeDescriptor.rootPageId = level.pageId;
            } else {
                // We're from Berkeley, so we reuse the existing root rather
                // than allocating a new one.
                level.pageId = getRootPageId();
                level.pageLock.lockExclusive(level.pageId);
                BTreeNode &node = level.pageLock.getNodeForWrite();
                assert(!node.nEntries);
                level.nodeAccessor.clearNode(
                    node,getSegment()->getUsablePageSize());
                node.height = i;
            }
        } else {
            // Allocate the first empty page of a non-root level.
            level.allocatePage();
        }
        if (i) {
            // Prepare the first page of a non-leaf level.
            // Calculate balancing for first child node.
            BTreeBuildLevel &childLevel = getLevel(i - 1);
            childLevel.nEntriesPerNode = calculateChildEntriesPerNode(
                level.nEntriesTotal,
                childLevel.nEntriesTotal,
                0);
        }
    }

    // should end up with exactly one node at the root level, which corresponds
    // to one entry in an imaginary parent of the root
    assert(nEntriesInLevel == 1);

    // feed data into the correct level
    getLevel(iInputLevel).processInput(sortedInputStream);

    // finalize rightist fringe
    for (uint i = iInputLevel; i < levels.size(); ++i) {
        BTreeBuildLevel &level = getLevel(i);
        level.indexLastKey(true);
        assert(level.isFinished());
    }
}

uint BTreeBuilder::calculateChildEntriesPerNode(
    RecordNum parentLevelTotalEntries,
    RecordNum childLevelTotalEntries,
    RecordNum parentLevelProcessedEntries)
{
    // Determine the desired fanout.  This is non-integral, so our job is to
    // distribute it as evenly as possible.
    // TODO:  we could remember this instead of recalculating it.
    double fanout =
        double(childLevelTotalEntries) / double(parentLevelTotalEntries);

    uint childLevelNewTotal;
    if (parentLevelProcessedEntries == (parentLevelTotalEntries - 1)) {
        // Last child node needs to make everything come out exact.
        childLevelNewTotal = childLevelTotalEntries;
    } else {
        childLevelNewTotal =
            uint(0.5 + fanout * (parentLevelProcessedEntries + 1));
    }

    // Use what we returned from the previous call as a baseline.
    // TODO:  we could remember this instead of recalculating it.
    uint childLevelPrevTotal = uint(0.5 + fanout * parentLevelProcessedEntries);

    uint n = childLevelNewTotal - childLevelPrevTotal;
    assert(n > 0);
    return n;
}

void BTreeBuilder::createEmptyRoot()
{
    assert(getRootPageId() == NULL_PAGE_ID);
    BTreePageLock pageLock;
    pageLock.accessSegment(treeDescriptor.segmentAccessor);
    treeDescriptor.rootPageId = pageLock.allocatePage(getPageOwnerId());
    BTreeNode &node = pageLock.getNodeForWrite();
    pLeafNodeAccessor->clearNode(
        node,getSegment()->getUsablePageSize());
}

void BTreeBuilder::growTree()
{
    BTreeNodeAccessor &nodeAccessor =
        levels.size() ? *pNonLeafNodeAccessor : *pLeafNodeAccessor;
    levels.resize(levels.size() + 1);
    levels[getRootHeight()].reset(new DynamicBuildLevel(*this,nodeAccessor));
    BTreeBuildLevel &level = *levels.back();
    level.iLevel = getRootHeight();
    if (level.iLevel) {
        // inherit cbReserved
        level.cbReserved = getLevel(level.iLevel - 1).cbReserved;
    }
    level.allocatePage();
}


void BTreeBuilder::truncate(
    bool rootless, TupleProjection const *pLeafPageIdProj)
{
    if (pLeafPageIdProj) {
        truncateExternal(*pLeafPageIdProj);
    }
    BTreePageLock pageLock;
    pageLock.accessSegment(treeDescriptor.segmentAccessor);

    pageLock.lockExclusive(getRootPageId());

    // Try a read-only access to see if we can skip dirtying a page.
    BTreeNode const &rootReadOnly = pageLock.getNodeForRead();
    if (!rootReadOnly.height) {
        if (rootless) {
            pageLock.deallocateLockedPage();
            treeDescriptor.rootPageId = NULL_PAGE_ID;
            return;
        }
        if (!rootReadOnly.nEntries) {
            // root is already empty
            return;
        }
    }

    BTreeNode &root = pageLock.getNodeForWrite();
    if (root.height) {
        truncateChildren(root);
    }
    if (rootless) {
        pageLock.deallocateLockedPage();
        treeDescriptor.rootPageId = NULL_PAGE_ID;
    } else {
        pLeafNodeAccessor->clearNode(
            root,getSegment()->getUsablePageSize());
    }
}

void BTreeBuilder::truncateChildren(BTreeNode const &node)
{
    assert(node.height);
    assert(node.nEntries);
    PageId pageId = getChild(node,0);
    BTreePageLock pageLock;
    pageLock.accessSegment(treeDescriptor.segmentAccessor);
    if (node.height > 1) {
        pageLock.lockExclusive(pageId);
        truncateChildren(pageLock.getNodeForRead());
        pageLock.unlock();
    }
    while (pageId != NULL_PAGE_ID) {
        PageId nextPageId = getRightSibling(pageId);
        pageLock.deallocateUnlockedPage(pageId);
        pageId = nextPageId;
    }
}

void BTreeBuilder::truncateExternal(TupleProjection const &leafPageIdProj)
{
    // REVIEW jvs 24-Dec-2005:  Here we pre-scan the tree, dropping
    // the external pages.  This scan could be combined with
    // the main truncate traversal for better efficiency.
    BTreeReader reader(treeDescriptor);
    TupleProjectionAccessor projAccessor;
    projAccessor.bind(
        reader.getTupleAccessorForRead(),
        leafPageIdProj);
    TupleDescriptor projDesc;
    projDesc.projectFrom(treeDescriptor.tupleDescriptor, leafPageIdProj);
    TupleData projData(projDesc);
    BTreePageLock pageLock;
    pageLock.accessSegment(treeDescriptor.segmentAccessor);
    if (reader.searchFirst()) {
        do {
            projAccessor.unmarshal(projData);
            for (uint i = 0; i < projData.size(); ++i) {
                if (!projData[i].pData) {
                    continue;
                }
                PageId pageId = *reinterpret_cast<PageId const *>(
                    projData[i].pData);
                pageLock.deallocateUnlockedPage(pageId);
            }
        } while (reader.searchNext());
    }
}

FENNEL_END_CPPFILE("$Id$");

// End BTreeBuilder.cpp
