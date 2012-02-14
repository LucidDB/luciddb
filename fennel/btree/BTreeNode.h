/*
// Licensed to DynamoBI Corporation (DynamoBI) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  DynamoBI licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at

//   http://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
*/

#ifndef Fennel_BTreeNode_Included
#define Fennel_BTreeNode_Included

#include "fennel/segment/SegPageLock.h"

FENNEL_BEGIN_NAMESPACE

/**
 * Header stored on each page of a BTree.
 */
struct FENNEL_BTREE_EXPORT BTreeNode
    : public StoredNode
{
    static const MagicNumber MAGIC_NUMBER = 0x9d4ec481f86aa93eLL;

    /**
     * Link to right sibling.  This is redundant with
     * Segment::getPageSuccessor.  We store it in both places since when we've
     * already locked a node, it's faster to use rightSibling, but when we're
     * doing prefetch, we need Segment::getPageSuccessor (since if we already
     * knew what was on a page, we wouldn't need to prefetch it!).
     */
    PageId rightSibling;

    /**
     * Number of entries stored on this node.
     */
    uint nEntries;

    /**
     * Height of this node in the tree (0 for leaf).
     */
    uint height;

    /**
     * Amount of (possibly discontiguous) free space available on this page.
     */
    uint cbTotalFree;

    /**
     * Amount of contiguous free space available on this page.  If MAXU, ignore
     * (that means only cbTotalFree is maintained).
     */
    uint cbCompactFree;

    // NOTE:  interpretation of the data is dependent on the node's height in
    // the tree and the way in which the tree is defined.
    // See BTreeNodeAccessor.

    /**
     * @return writable start of data after header
     */
    PBuffer getDataForWrite()
    {
        return reinterpret_cast<PBuffer>(this + 1);
    }

    /**
     * @return read-only start of data after header
     */
    PConstBuffer getDataForRead() const
    {
        return reinterpret_cast<PConstBuffer>(this + 1);
    }
};

typedef SegNodeLock<BTreeNode> BTreePageLock;

FENNEL_END_NAMESPACE

#endif

// End BTreeNode.h
