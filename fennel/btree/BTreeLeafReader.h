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
