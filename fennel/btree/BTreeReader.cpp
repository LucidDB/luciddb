/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2005-2005 Disruptive Tech
// Copyright (C) 2005-2005 LucidEra, Inc.
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
#include "fennel/btree/BTreeReader.h"
#include "fennel/btree/BTreeAccessBaseImpl.h"
#include "fennel/btree/BTreeReaderImpl.h"

FENNEL_BEGIN_CPPFILE("$Id$");

BTreeReader::BTreeReader(BTreeDescriptor const &descriptor)
    : BTreeAccessBase(descriptor)
{
    pageLock.accessSegment(treeDescriptor.segmentAccessor);
    pSearchKey = NULL;
    rootLockMode = LOCKMODE_S;
    nonLeafLockMode = LOCKMODE_S;
    leafLockMode = LOCKMODE_S;
    comparisonKeyData.compute(keyDescriptor);
    searchKeyData = comparisonKeyData;
    singular = true;
}

BTreeReader::~BTreeReader()
{
    endSearch();
}

inline void BTreeReader::accessLeafTuple()
{
    BTreeNode const &node = pageLock.getNodeForRead();
    getLeafNodeAccessor(node).accessTuple(node,iTupleOnLeaf);
}

bool BTreeReader::searchExtreme(bool first)
{
    singular = false;
    pageId = getRootPageId();
    LockMode lockMode = rootLockMode;
    for (;;) {
        pageLock.lockPage(pageId,lockMode);
        BTreeNode const &node = pageLock.getNodeForRead();
        switch(node.height) {
        case 0:
            // at leaf level
            if (!adjustRootLockMode(lockMode)) {
                // Retry with correct lock mode
                continue;
            }
            if (!node.nEntries) {
                pageId = node.rightSibling;
                if (pageId == NULL_PAGE_ID) {
                    // FIXME jvs 11-Nov-2005:  see note in method documentation
                    // for searchLast.
                    singular = true;
                    return false;
                }
                continue;
            }
            if (first) {
                iTupleOnLeaf = 0;
            } else {
                iTupleOnLeaf = node.nEntries - 1;
            }
            accessLeafTuple();
            return true;
        case 1:
            // next level down is leaf, so take the correct lock
            lockMode = leafLockMode;
            break;
        default:
            lockMode = nonLeafLockMode;
            break;
        }

        assert(node.nEntries);
        if (first) {
            // continue searching on first child
            pageId = getChild(node,0);
        } else {
            // continue searching on last child
            pageId = getChild(node,node.nEntries - 1);
        }
    }
}

// TODO:  prefetch
bool BTreeReader::searchNext()
{
    assert(!singular);
    assert(pageLock.isLocked());
    assert(!pageLock.getNodeForRead().height);
    ++iTupleOnLeaf;
    for (;;) {
        BTreeNode const &node = pageLock.getNodeForRead();
        if (iTupleOnLeaf < node.nEntries) {
            break;
        }
        pageId = node.rightSibling;
        if (pageId == NULL_PAGE_ID) {
            // might as well preserve position
            --iTupleOnLeaf;
            singular = true;
            return false;
        }
        pageLock.lockPage(pageId,leafLockMode);
        iTupleOnLeaf = 0;
    }
    accessLeafTuple();
    return true;
}

bool BTreeReader::searchForKey(TupleData const &key,DuplicateSeek dupSeek,
                               bool leastUpper)
{
    singular = false;
    NullPageStack nullPageStack;
    bool found = searchForKeyTemplate<false,NullPageStack>(
        key,dupSeek,leastUpper,nullPageStack);
    pSearchKey = NULL;
    return found;
}

void BTreeReader::endSearch()
{
    pLeafNodeAccessor->tupleAccessor.resetCurrentTupleBuf();
    pageLock.unlock();
    singular = true;
}

TupleAccessor const &BTreeReader::getTupleAccessorForRead() const
{
    return pLeafNodeAccessor->tupleAccessor;
}

FENNEL_END_CPPFILE("$Id$");

// End BTreeReader.cpp
