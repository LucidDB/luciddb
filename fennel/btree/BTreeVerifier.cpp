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
#include "fennel/btree/BTreeVerifier.h"
#include "fennel/btree/BTreeAccessBaseImpl.h"

FENNEL_BEGIN_CPPFILE("$Id$");

// TODO:  change asserts to proper excns

BTreeVerifier::BTreeVerifier(BTreeDescriptor const &descriptor)
    : BTreeAccessBase(descriptor)
{
    permAssert(getRootPageId() != NULL_PAGE_ID);
    keyData.compute(keyDescriptor);
    keyData2 = keyData;
}

BTreeVerifier::~BTreeVerifier()
{
}

void BTreeVerifier::verify(bool strictInit, bool keysInit, bool leafInit)
{
    strict = strictInit;
    keys = keysInit;
    leaf = leafInit;
    lowerBoundKey.clear();
    upperBoundKey.clear();
    stats.nLevels = MAXU;
    stats.nNonLeafNodes = 0;
    stats.nLeafNodes = 0;
    stats.nTuples = 0;
    stats.nUniqueKeys = 0;
    PageId pageId = getRootPageId();
    do {
        expectedRightSibling = NULL_PAGE_ID;
        if (isMAXU(stats.nLevels)) {
            expectedHeight = MAXU;
        } else {
            expectedHeight = stats.nLevels - 1;
        }
        pageId = verifyNode(pageId);
    } while (pageId != NULL_PAGE_ID);
}

// TODO:  guard against cyclic sibling links

PageId BTreeVerifier::verifyNode(
    PageId pageId)
{
    BTreePageLock pageLock;
    pageLock.accessSegment(treeDescriptor.segmentAccessor);
    pageLock.lockShared(pageId);
    BTreeNode const &node = pageLock.getNodeForRead();
    PageId returnPageId = NULL_PAGE_ID;

    // for optimized build, we don't check node magic numbers implicitly,
    // so do it explicitly here
    permAssert(node.magicNumber == BTreeNode::MAGIC_NUMBER);

    if (isMAXU(expectedHeight)) {
        stats.nLevels = node.height + 1;
    } else {
        permAssert(node.height == expectedHeight);
    }

    if (strict) {
        permAssert(node.rightSibling == getRightSibling(pageId));
        permAssert(node.rightSibling == expectedRightSibling);
    } else {
        if (node.rightSibling != expectedRightSibling) {
            permAssert(node.rightSibling != NULL_PAGE_ID);
            returnPageId = node.rightSibling;
        }
    }

    BTreeNodeAccessor &nodeAccessor = getNodeAccessor(node);

    // TODO:  delegate to nodeAccessor for checking node integrity

    // verify key ordering, including lower bound
    if (keys) {
        bool countUniqueKeys = (node.height == 0);
        keyData = lowerBoundKey;
        uint nKeys = nodeAccessor.getKeyCount(node);
        for (uint i = 0; i < nKeys; ++i) {
            nodeAccessor.accessTuple(node,i);
            nodeAccessor.unmarshalKey(keyData2);
            if (keyData.size()) {
                int c = keyDescriptor.compareTuples(keyData,keyData2);

                // TODO:  move this somewhere else
                if (c > 0) {
                    nodeAccessor.dumpNode(std::cerr,node,pageId);
                }
                permAssert(c <= 0);
                // TODO:  for unique, assert(c == 0)

                if (countUniqueKeys && c == -1) {
                    // Only count differences in the first column of the key.
                    stats.nUniqueKeys++;
                }
            } else if (countUniqueKeys) {
                stats.nUniqueKeys++;
            }
            keyData = keyData2;
        }
    }

    // verify upper bound (using last key left over from previous loop)
    if (keyData.size() && upperBoundKey.size()) {
        keyData2 = upperBoundKey;
        int c = keyDescriptor.compareTuples(keyData,keyData2);
        permAssert(c <= 0);
    }
    if (node.height) {
        stats.nNonLeafNodes++;
        verifyChildren(node);
    } else {
        stats.nLeafNodes++;
        stats.nTuples += node.nEntries;
    }
    return returnPageId;
}

void BTreeVerifier::verifyChildren(BTreeNode const &node)
{
    // skip over leaf nodes if we are not traversing them
    if (node.height == 1 && !leaf) {
        stats.nLeafNodes += node.nEntries;
        return;
    }

    BTreeNodeAccessor &nodeAccessor = getNodeAccessor(node);
    for (uint i = 0; i < node.nEntries; ++i) {
        PageId nextChildPageId;
        if (i + 1 < node.nEntries) {
            nextChildPageId = getChild(node,i+1);
        } else {
            nextChildPageId = getFirstChild(node.rightSibling);
        }
        PageId childPageId = getChild(node,i);
        do {
            expectedRightSibling = nextChildPageId;
            expectedHeight = node.height - 1;
            nodeAccessor.accessTuple(node,i);
            nodeAccessor.unmarshalKey(keyData);
            if (nextChildPageId == NULL_PAGE_ID) {
                // pretend last key is +infinity
                upperBoundKey.clear();
            } else {
                upperBoundKey = keyData;
            }
            childPageId = verifyNode(childPageId);
        } while (childPageId != NULL_PAGE_ID);
        nodeAccessor.accessTuple(node,i);
        nodeAccessor.unmarshalKey(keyData);
        lowerBoundKey = keyData;
    }
}

BTreeStatistics const &BTreeVerifier::getStatistics()
{
    return stats;
}

FENNEL_END_CPPFILE("$Id$");

// End BTreeVerifier.cpp
