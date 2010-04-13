/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005 The Eigenbase Project
// Copyright (C) 2005 SQLstream, Inc.
// Copyright (C) 2005 Dynamo BI Corporation
// Portions Copyright (C) 1999 John V. Sichi
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

#ifndef Fennel_BTreeNonLeafReader_Included
#define Fennel_BTreeNonLeafReader_Included

#include "fennel/btree/BTreeReader.h"

FENNEL_BEGIN_NAMESPACE

/**
 * BTreeNonLeafReader extends BTreeReader by only doing reads of non-leaf
 * pages in a btree.
 */
class FENNEL_BTREE_EXPORT BTreeNonLeafReader
    : public BTreeReader
{
    /**
     * Deals with the fact that when we lock the root, we don't know whether it
     * happens to be a leaf as well.  This method should never be called
     * since this class does not read leaf pages.
     *
     * @param lockMode the lock mode used to lock the root
     *
     * @return always returns false
     */
    bool adjustRootLockMode(LockMode &lockMode);

    /**
     * Searches for the first or last tuple in the non-leaf node that's one
     * level above the leaf level in the tree.  This method should never
     * be called on a tree consisting of a single root node.  So, therefore,
     * it should never be called on an empty tree.
     *
     * @param first true for first; false for last
     *
     * @return always returns true since it is never called on an empty tree
     */
    virtual bool searchExtreme(bool first);

public:
    explicit BTreeNonLeafReader(BTreeDescriptor const &descriptor);

    /**
     * Gets a read-only accessor for non-leaf tuples.  Tuples found by a
     * search are returned implicitly by binding this accessor.
     *
     * @return the non-leaf tuple accessor
     */
    TupleAccessor const &getTupleAccessorForRead() const;

    /**
     * Searches for a tuple in the tree based on the given key.  The search
     * is stopped at the level just above the leaf.  This method should not
     * but called on a btree that consists of a single root node.
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
     * Searches for the next tuple at the level just above the leaf.  Can be
     * used after either searchFirst or searchForKey, but illegal when
     * isSingular().
     *
     * @return true if next tuple found; false if end of tree reached
     */
    virtual bool searchNext();

    /**
     * Indicates whether the btree, at the time the method is called, consists
     * of only a single root node page.
     *
     * @return true if the btree consists of only a single root node
     */
    bool isRootOnly();

    /**
     * @return true if the reader is currently positioned on the rightmost
     * key, i.e., the special infinity key
     */
    bool isPositionedOnInfinityKey();

    /**
     * @return node accessor corresponding to this reader
     */
    BTreeNodeAccessor &getNonLeafNodeAccessor();

    /**
     * @return child pageId in the current non-leaf record
     */
    PageId getChildForCurrent();

};

FENNEL_END_NAMESPACE

#endif

// End BTreeNonLeafReader.h
