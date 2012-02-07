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

#ifndef Fennel_BTreeLeafReader_Included
#define Fennel_BTreeLeafReader_Included

#include "fennel/btree/BTreeReader.h"

FENNEL_BEGIN_NAMESPACE

/**
 * BTreeLeafReader extends BTreeReader by only doing reads of leaf pages in
 * a btree.
 */
class FENNEL_BTREE_EXPORT BTreeLeafReader
    : public BTreeReader
{
    /**
     * The current leaf page to be searched/read
     */
    PageId currLeafPageId;

    /**
     * Deals with the fact that when we lock the root, we don't know whether it
     * happens to be a leaf as well.  This method is a no-op for this class.
     * The lockMode passed in should always be the same as the current lock
     * mode since this class only reads leaf pages.
     *
     * @param lockMode the lock mode used to lock a leaf
     *
     * @return always returns true
     */
    bool adjustRootLockMode(LockMode &lockMode);

    /**
     * Searches for the first or last tuple in the current leaf node.
     *
     * @param first true for first; false for last
     *
     * @return false if the leaf page is empty
     */
    virtual bool searchExtreme(bool first);

public:
    explicit BTreeLeafReader(BTreeDescriptor const &descriptor);

    /**
     * Searches for a tuple in the current leaf page based on the given key.
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
     * Searches for the next tuple in the current leaf page.  Can be used
     * after either searchFirst or searchForKey has positioned within the leaf,
     * but illegal when isSingular().
     *
     * @return true if next tuple found; false if end of leaf page reached
     */
    virtual bool searchNext();

    /**
     * Resets the search such that there no longer is a current leaf page.
     */
    virtual void endSearch();

    /**
     * Sets the current leaf page that will later be searched/read.
     *
     * @param pageId page id of the leaf page
     */
    void setCurrentPageId(PageId pageId);
};

FENNEL_END_NAMESPACE

#endif

// End BTreeLeafReader.h
