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

#ifndef Fennel_BTreeReader_Included
#define Fennel_BTreeReader_Included

#include "fennel/btree/BTreeAccessBase.h"
#include "fennel/btree/BTreeNode.h"
#include "fennel/tuple/TupleData.h"

FENNEL_BEGIN_NAMESPACE

/**
 * BTreeReader provides read-only access to the contents of a BTree.
 */
class BTreeReader : public BTreeAccessBase
{
    /**
     * Dummy stack implementation used when we don't care about keeping track
     * of PageId's on the way down.
     */
    class NullPageStack 
    {
    public:
        void push_back(PageId)
        {
        }
    };
    
    inline void accessLeafTuple();

protected:
    /**
     * Lock on node being searched.
     */
    BTreePageLock pageLock;

    /**
     * PageId of node being searched.
     */
    PageId pageId;

    /**
     * 0-based position on leaf.
     */
    uint iTupleOnLeaf;

    /**
     * LockMode to use when acquiring lock on root node.
     */
    LockMode rootLockMode;

    /**
     * LockMode to use when acquiring lock on non-leaf nodes other than the
     * root.
     */
    LockMode nonLeafLockMode;

    /**
     * LockMode to use when acquiring lock on leaf nodes.
     */
    LockMode leafLockMode;

    /**
     * TupleData used as a temp variable for comparisons while searching.
     */
    TupleData comparisonKeyData;
    
    /**
     * Key being sought.  NULL except while search in progress.
     */
    TupleData const *pSearchKey;

    /**
     * TupleData for key used in searches.
     */
    TupleData searchKeyData;
    
    /**
     * Searches a node for the current search key.
     *
     * @param node the node to search
     *
     * @param dupSeek what to do if duplicates are found
     *
     * @param found receives whether the search key was found; if false,
     * the position of the least upper bound is returned instead
     *
     * @return 0-based position of found key on node
     */
    inline uint binarySearch(
        BTreeNode const &node,DuplicateSeek dupSeek,bool &found);

    /**
     * @return the key being sought
     */
    inline TupleData const &getSearchKey();

    /**
     * Deals with the fact that when we lock the root, we don't know whether it
     * happens to be a leaf as well.  When that happens, and rootLockMode !=
     * leafLockMode, we have to compensate.
     *
     * @param lockMode the lock mode used to lock a root+leaf; receives
     * the adjusted lock mode on return
     *
     * @return false if the adjustment requires the original operation to
     * be restarted
     */
    inline bool adjustRootLockMode(LockMode &lockMode);

    /**
     * Implements the workhorse algorithm for performing the actual search
     * through the tree; templated to efficiently allow for certain
     * variations needed when the search is used in preparation for an
     * insertion by BTreeWriter.  leafLockCoupling controls whether lock
     * coupling is enforced while moving rightward at the leaf level.
     *
     * @param key the key being searched for
     *
     * @param dupSeek how to handle duplicates
     *
     * @param pageStack receives a path of rightmost PageId's encountered
     * from root to the level above the leaf (PageStack must support
     * the push_back method)
     */
    template <bool leafLockCoupling,class PageStack>
    inline bool BTreeReader::searchForKeyTemplate(
        TupleData const &key,DuplicateSeek dupSeek,PageStack &pageStack);
    
public:
    explicit BTreeReader(BTreeDescriptor const &descriptor);
    virtual ~BTreeReader();

    /**
     * Gets a read-only accessor for leaf tuples.  Tuples found by a search are
     * returned implicitly by binding this accessor.
     *
     * @return the leaf tuple accessor
     */
    TupleAccessor const &getTupleAccessorForRead() const;

    /**
     * Gets writable TupleData which can be used to prepare a key to be used in
     * searchForKey.  This is strictly an optional convenience; any key can be
     * passed to searchForKey.
     *
     * @return writable TupleData for key
     */
    inline TupleData &getSearchKeyForWrite();

    /**
     * Searches for the first tuple in the tree.
     *
     * @return true if tuple found; false if tree is empty
     */
    bool searchFirst();

    /**
     * Searches for a tuple in the tree with the given key.
     * TODO:  duplicate handling
     *
     * @param key the key to search for
     *
     * @param dupSeek what to do if duplicates are found
     *
     * @return true if tuple found; false if not found, in which case
     * reader is positioned on tuple with least upper bound of key
     */
    bool searchForKey(TupleData const &key,DuplicateSeek dupSeek);
    
    /**
     * Searches for the next tuple.  Can be used after either searchFirst
     * or searchForKey.
     *
     * @return true if next tuple found; false if end of tree reached
     */
    bool searchNext();

    /**
     * Forgets the current search, releasing any page lock.
     */
    void endSearch();

    /**
     * @return true if a search method has been called without a subsequent
     * endSearch yet
     */
    inline bool isPositioned() const;
};

inline TupleData &BTreeReader::getSearchKeyForWrite()
{
    return searchKeyData;
}

inline bool BTreeReader::isPositioned() const
{
    return pageLock.isLocked();
}

FENNEL_END_NAMESPACE

#endif

// End BTreeReader.h
