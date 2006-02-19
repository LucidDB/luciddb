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

#ifndef Fennel_BTreeReaderImpl_Included
#define Fennel_BTreeReaderImpl_Included

#include "fennel/btree/BTreeReader.h"

FENNEL_BEGIN_NAMESPACE

inline TupleData const &BTreeReader::getSearchKey()
{
    assert(pSearchKey);
    return *pSearchKey;
}

inline uint BTreeReader::binarySearch(
    BTreeNode const &node,
    DuplicateSeek dupSeek,
    bool leastUpper,
    bool &found)
{
    return getNodeAccessor(node).binarySearch(
        node,
        keyDescriptor,
        getSearchKey(),
        dupSeek,
        leastUpper,
        comparisonKeyData,
        found);
}

inline bool BTreeReader::adjustRootLockMode(LockMode &lockMode)
{
    if (lockMode == leafLockMode) {
        // We already got the correct lock mode.
        return true;
    }
    
    // Oops, we got the wrong lock mode.  Next time we'll get it right.
    rootLockMode = leafLockMode;
    lockMode = leafLockMode;
    
    // We can try an upgrade.  If it succeeds, great.  Otherwise, it will fail
    // immediately so we can do it the hard way.
    if (pageLock.tryUpgrade()) {
        return true;
    }
    
    // Shucks, have to unlock and retry original operation.
    pageLock.unlock();
    return false;
}

inline int BTreeReader::compareFirstKey(BTreeNode const &node)
{
    return getNodeAccessor(node).compareFirstKey(
        node,
        keyDescriptor,
        getSearchKey(),
        comparisonKeyData);
}

inline void BTreeReader::accessTupleInline(BTreeNode const &node, uint iEntry)
{
    getNodeAccessor(node).accessTupleInline(node, iEntry);
}

template <bool leafLockCoupling,class PageStack>
inline bool BTreeReader::searchForKeyTemplate(
    TupleData const &key, DuplicateSeek dupSeek, bool leastUpper,
    PageStack &pageStack)
{
    pSearchKey = &key;
    
    // At each level, we may have to search right due to splits.  To bound
    // this search, we record the parent's notion of the PageId for the
    // right sibling of the child page.  Lehman-Yao uses keys instead, but
    // PageId should work as well?
    PageId rightSearchTerminator = NULL_PAGE_ID;
    pageId = getRootPageId();
    LockMode lockMode = rootLockMode;
    bool lockCoupling = false;
    for (;;) {
        if (leafLockCoupling && lockCoupling) {
            pageLock.lockPageWithCoupling(pageId,lockMode);
        } else {
            pageLock.lockPage(pageId,lockMode);
        }
        
        BTreeNode const &node = pageLock.getNodeForRead();

        // TODO:  pull this out of loop
        if (!node.height && !adjustRootLockMode(lockMode)) {
            // Retry with correct lock mode
            continue;
        }
        
        bool found;
        uint iKeyBound = binarySearch(node,dupSeek,leastUpper,found);

        // if we're searching for the greatest lower bound, we didn't
        // find an exact match, and we're positioned at the rightmost
        // key entry, need to search the first key in the right sibling
        // to be sure we have the correct glb
        if (!leastUpper && !found && iKeyBound == node.nEntries - 1 &&
                node.rightSibling != NULL_PAGE_ID) {

            // not currently handling leaf lock coupling for reads,
            // which is the only time we're searching for glb
            assert(leafLockCoupling == false);

            pageLock.unlock();
            pageLock.lockPage(node.rightSibling, lockMode);
            BTreeNode const &rightNode = pageLock.getNodeForRead();
            int res = compareFirstKey(rightNode);

            pageLock.unlock();
            if (res < 0) {
                // stick with the current node, so go back and relock it
                // and reset the tuple accessor back to the prior key
                //
                // FIXME zfong 7-Dec-2005: Need to handle case where a
                // node split has occurred since the current node was
                // unlocked.  To do so, need to search starting at the
                // current node and continue searching right until you
                // find a node whose right sibling is equal to the
                // original right sibling.  The key we want should then
                // be the last entry on that node.
                pageLock.lockPage(pageId, lockMode);
                accessTupleInline(node, iKeyBound);
            } else {
                // switch over to the right sibling
                pageId = node.rightSibling;
                continue;
            }
        }
                
        if (iKeyBound == node.nEntries) {
            assert(!found || (dupSeek == DUP_SEEK_END));
            // What we're searching for is bigger than everything on
            // this node.
            if (node.rightSibling == rightSearchTerminator) {
                // No need to search rightward.  This should only
                // happen at the leaf level (since we never delete keys from
                // nodes, the upper bound should match at parent and child
                // levels.)
                assert(!node.height);
                if (rightSearchTerminator == NULL_PAGE_ID) {
                    singular = true;
                }
            } else {
                // have to search right
                pageId = node.rightSibling;
                if (leafLockCoupling && !node.height) {
                    lockCoupling = true;
                }
                continue;
            }
        }

        switch(node.height) {
        case 0:
            // at leaf level
            iTupleOnLeaf = iKeyBound;
            return found;
            
        case 1:
            // prepare to hit rock bottom
            lockMode = leafLockMode;
            break;
        }

        // leave a trail of breadcrumbs
        pageStack.push_back(pageId);
        
        // we'll continue search on child
        pageId = getChildForCurrent();

        // record the successor child as a terminator for rightward
        // searches once we descend to the child level
        if (iKeyBound < (node.nEntries - 1)) {
            rightSearchTerminator = getChild(node,iKeyBound + 1);
        } else {
            // have to consult our own sibling to find the successor
            // child
            // need to get the pageId first, then unlock.
            PageId rightSiblingPageId = node.rightSibling;
            pageLock.unlock();
            rightSearchTerminator = getFirstChild(rightSiblingPageId);
        }
    }
}

FENNEL_END_NAMESPACE

#endif

// End BTreeReaderImpl.h
