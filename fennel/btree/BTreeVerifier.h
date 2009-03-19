/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005-2007 The Eigenbase Project
// Copyright (C) 2005-2007 SQLstream, Inc.
// Copyright (C) 2005-2007 LucidEra, Inc.
// Portions Copyright (C) 1999-2007 John V. Sichi
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

#ifndef Fennel_BTreeVerifier_Included
#define Fennel_BTreeVerifier_Included

#include "fennel/btree/BTreeAccessBase.h"
#include "fennel/tuple/TupleData.h"

FENNEL_BEGIN_NAMESPACE

/**
 * BTreeStatistics is used to return information about the tree computed
 * as a side-effect of verification.
 */
struct BTreeStatistics
{
    /**
     * Number of levels in tree.
     */
    uint nLevels;

    /**
     * Number of non-leaf nodes in tree.
     */
    RecordNum nNonLeafNodes;

    /**
     * Number of leaf nodes in tree.
     */
    RecordNum nLeafNodes;

    /**
     * Number of leaf tuples in tree.
     */
    RecordNum nTuples;

    /**
     * Number of unique keys in tree.  Counts only the unique first keys.
     */
    RecordNum nUniqueKeys;
};

/**
 * BTreeVerifier checks BTree integrity.
 */
class BTreeVerifier : public BTreeAccessBase
{
    /**
     * Key data used for verifying that all keys in a node are greater than or
     * equal to an expected lower bound.  If size is 0, represents negative
     * infinity.
     */
    TupleData lowerBoundKey;

    /**
     * Key data used for verifying that all keys in a node are
     * less than or equal to an expected upper bound.  If size is 0, represents
     * positive infinity.
     */
    TupleData upperBoundKey;

    /**
     * Expected height for node about to be verified.  When MAXU, indicates
     * that we don't know the height of the tree because we haven't yet
     * verified the root.
     */
    uint expectedHeight;

    /**
     * Expected right sibling for node about to be verified.  For non-strict
     * verification, this may not match (although it must be reachable
     * by traversing right siblings).
     */
    PageId expectedRightSibling;

    /**
     * Statistics being gather during verification.
     */
    BTreeStatistics stats;

    /**
     * Whether to be strict during verification.
     */
    bool strict;

    /**
     * Whether to perform key verification
     */
    bool keys;

    /**
     * Whether to traverse the leaf level
     */
    bool leaf;

    /**
     * Key data used for comparing successive keys.
     */
    TupleData keyData;

    /**
     * Key data used for comparing successive keys.
     */
    TupleData keyData2;

    /**
     * Verifies one node.  Various expected values should already have been set
     * up when this is called.
     *
     * @param pageId PageId of node to verify
     *
     * @return PageId of additional nodes to check with same parameters
     * (only for non-strict checks; for strict checks, will always
     * be NULL_PAGE_ID)
     */
    PageId verifyNode(
        PageId pageId);

    /**
     * Verifies all of the children of a non-leaf node.
     *
     * @param node the non-leaf node to be verified
     */
    void verifyChildren(
        BTreeNode const &node);

public:
    explicit BTreeVerifier(BTreeDescriptor const &);
    virtual ~BTreeVerifier();

    /**
     * Performs verification over the entire tree.
     *
     * @param strict if true, the tree is assumed to be in a quiescent state
     * with all update operations completed; if false, violations which are
     * possible with incomplete update operations are ignored
     *
     * @param keys whether to verify key ordering (and count unique keys)
     *
     * @param leaf whether to traverse the leaf level
     */
    void verify(bool strict = true, bool keys = true, bool leaf = true);

    /**
     * Gets statistics collected during the previous verification.
     * This is only valid after verify is called, and is overwritten by
     * each verify call, so copy as needed.
     *
     * @return collected statistics
     */
    BTreeStatistics const &getStatistics();
};

FENNEL_END_NAMESPACE

#endif

// End BTreeVerifier.h
