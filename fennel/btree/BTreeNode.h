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

#ifndef Fennel_BTreeNode_Included
#define Fennel_BTreeNode_Included

#include "fennel/segment/SegPageLock.h"

FENNEL_BEGIN_NAMESPACE

/**
 * Header stored on each page of a BTree.
 */
struct BTreeNode : public StoredNode
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
