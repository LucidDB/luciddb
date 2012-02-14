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
