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

#ifndef Fennel_BTreeReader_Included
#define Fennel_BTreeReader_Included

#include "fennel/btree/BTreeAccessBase.h"
#include "fennel/btree/BTreeNode.h"
#include "fennel/tuple/TupleData.h"

FENNEL_BEGIN_NAMESPACE

/**
 * BTreeReader provides read-only access to the contents of a BTree.
 */
class FENNEL_BTREE_EXPORT BTreeReader
    : public BTreeAccessBase
{
protected:
    /**
     * Enumeration of which node types the reader should be reading
     */
    enum ReadMode {
        /**
         * Read both non-leaf and leaf nodes
         */
        READ_ALL,
        /**
         * Read only non-leaf nodes
         */
        READ_NONLEAF_ONLY,
        /**
         * Read only leaf nodes
         */
        READ_LEAF_ONLY
    };

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

    /**
     * Lock on node being searched.
     */
    BTreePageLock pageLock;

    /**
     * PageId of node being searched.
     */
    PageId pageId;

    /**
     * 0-based position on lowest level searched by the reader.
     */
    uint iTupleOnLowestLevel;

    /**
     * @see isSingular()
     */
    bool singular;

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
     * @param leastUpper whether to position on least upper bound or greatest
     * lower bound
     *
     * @param found receives whether the search key was found; if false,
     * the position of the least upper bound is returned instead
     *
     * @return 0-based position of found key on node
     */
    inline uint binarySearch(
        BTreeNode const &node, DuplicateSeek dupSeek, bool leastUpper,
        bool &found);

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
     * Compares the first key on a node to the current search key.
     *
     * @param node the node to search
     *
     * @return result of comparing searchKey with the first key (0, -1, or 1)
     */
    inline int compareFirstKey(BTreeNode const &node);

    /**
     * Sets tuple accessor to provided node entry
     *
     * @param node the current node positioned on
     *
     * @param iEntry the entry within the node to set the tuple accessor to
     */
    inline void accessTupleInline(BTreeNode const &node, uint iEntry);

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
     * @param leastUpper whether to position on least upper bound or greatest
     * lower bound
     *
     * @param pageStack receives a path of rightmost PageId's encountered
     * from root to the level above the leaf (PageStack must support
     * the push_back method)
     *
     * @param startPageId the pageId at which the search should start
     *
     * @param initialLockMode the initial lockmode to use when searching the
     * tree
     *
     * @param readMode which node types should be searched
     */
    template <bool leafLockCoupling, class PageStack>
    inline bool searchForKeyTemplate(
        TupleData const &key, DuplicateSeek dupSeek, bool leastUpper,
        PageStack &pageStack, PageId startPageId, LockMode initialLockMode,
        ReadMode readMode);

    /**
     * @see searchForKeyTemplate()
     */
    bool searchForKeyInternal(
        TupleData const &key, DuplicateSeek dupSeek, bool leastUpper,
        PageId startPageId, LockMode initialLockMode,
        ReadMode readMode);

    /**
     * Searches for the first or last tuple in the tree.
     *
     * @param first true for first; false for last
     *
     * @return true if tuple found; false if tree is empty
     */
    virtual bool searchExtreme(bool first);

    /**
     * Searches for the first or last tuple in the tree.
     *
     * @param first true for first; false for last
     *
     * @param readMode which types of nodes should be searched
     *
     * @return true if tuple found; false if tree is empty
     */
    bool searchExtremeInternal(bool first, ReadMode readMode);

    /**
     * Searches for next tuple.
     *
     * @return true if next tuple found; false if end of tree reached
     */
    bool searchNextInternal();

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
    inline bool searchFirst();

    /**
     * Searches for the last tuple in the tree.
     *
     *<p>
     *
     * FIXME jvs 11-Nov-2005:  This method isn't currently guaranteed
     * to work after deletions on a tree.  The problem is that deletions
     * may leave the last page of the tree empty, so when we hit leaf
     * level, we may not find anything, even though a predecessor
     * page is non-empty.  We only have right-sibling links, not
     * left-siblings, so the fix requires keeping a breadcrumb trail
     * on the way down and backtracking when we hit an empty leaf.
     *
     * @return true if tuple found; false if tree is empty
     */
    inline bool searchLast();

    /**
     * Searches for a tuple in the tree based on the given key.
     *
     *<p>
     *
     * NOTE jvs 27-May-2007:  This method's interface has some problems;
     * resolving http://issues.eigenbase.org/browse/FNL-65 will
     * involve coming up with a more rational interface.  In particular,
     * note that the return value is unreliable in the case of
     * DUP_SEEK_END, and should be ignored by callers.
     *
     * @param key the key to search for
     *
     * @param dupSeek what to do if duplicates are found
     *
     * @param leastUpper (default true) - if true, reader will position on the
     * least upper bound of key; otherwise, it will position on tuple with
     * greatest lower bound
     *
     * @return true if tuple found; false if not found, in which case reader is
     * positioned on tuple depending on leastUpper parameter
     */
    virtual bool searchForKey(
        TupleData const &key,
        DuplicateSeek dupSeek,
        bool leastUpper = true);

    /**
     * Searches for the next tuple.  Can be used after either searchFirst
     * or searchForKey, but illegal when isSingular().
     *
     * @return true if next tuple found; false if end of tree reached
     */
    virtual bool searchNext();

    /**
     * Forgets the current search, releasing any page lock.
     */
    virtual void endSearch();

    /**
     * @return true if a search method has been called without a subsequent
     * endSearch yet
     */
    inline bool isPositioned() const;

    /**
     * @return true if reader is either unpositioned or past last
     * entry in tree
     */
    inline bool isSingular() const;
};

inline TupleData &BTreeReader::getSearchKeyForWrite()
{
    return searchKeyData;
}

inline bool BTreeReader::isSingular() const
{
    return singular;
}

inline bool BTreeReader::isPositioned() const
{
    return pageLock.isLocked();
}

inline bool BTreeReader::searchFirst()
{
    return searchExtreme(true);
}

inline bool BTreeReader::searchLast()
{
    return searchExtreme(false);
}

FENNEL_END_NAMESPACE

#endif

// End BTreeReader.h
