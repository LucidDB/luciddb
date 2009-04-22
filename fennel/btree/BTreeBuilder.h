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

#ifndef Fennel_BTreeBuilder_Included
#define Fennel_BTreeBuilder_Included

#include "fennel/btree/BTreeAccessBase.h"

#include <vector>

FENNEL_BEGIN_NAMESPACE

class BTreeBuildLevel;
typedef boost::shared_ptr<BTreeBuildLevel> SharedBTreeBuildLevel;

// TODO:  doc internals

/**
 * BTreeBuilder implements bulk load for BTrees.  When non-leaf nodes store
 * fixed-width entries, it builds a nicely balanced tree.  In other cases, it
 * builds an unbalanced tree.  An optional fill factor is supported for all
 * cases.
 *
 *<p>
 *
 * BTreeBuilder is also used for creating empty trees and truncating or
 * dropping existing ones.
 */
class FENNEL_BTREE_EXPORT BTreeBuilder
    : public BTreeAccessBase
{
    // TODO:  something better
    friend class BTreeBuildLevel;
    friend class FixedBuildLevel;
    friend class VariableBuildLevel;
    friend class DynamicBuildLevel;

    std::vector<SharedBTreeBuildLevel> levels;

    SharedSegment pTempSegment;

// ----------------------------------------------------------------------
// internal helper methods
// ----------------------------------------------------------------------

    static uint calculateChildEntriesPerNode(
        RecordNum parentLevelTotalEntries,
        RecordNum childLevelTotalEntries,
        RecordNum parentLevelProcessedEntries);

    static uint calculateNodesOnLevel(
        uint nChildEntries,uint nEntriesPerChildNode);

    uint getRootHeight()
    {
        return levels.size() - 1;
    }

    void buildBalanced(
        ByteInputStream &sortedStream,
        uint iInputLevel,
        RecordNum nEntriesTotal,
        double fillFactor);

    void buildTwoPass(
        ByteInputStream &sortedStream,
        RecordNum nEntries,
        double fillFactor);

    void buildUnbalanced(
        ByteInputStream &sortedStream,
        RecordNum nEntries,
        double fillFactor);

    void processInput(
        ByteInputStream &sortedStream);

    inline BTreeBuildLevel &getLevel(uint i);

    void growTree();

    void swapRoot();

    void truncateChildren(BTreeNode const &node);
    void truncateExternal(TupleProjection const &leafPageIdProj);

public:
    /**
     * Creates a new BTreeBuilder.  In order to build a non-empty
     * tree with variable-width tuples in the leaf nodes and
     * fixed-width entries in the non-leaf nodes, a temporary disk
     * buffer is required.
     *
     * @param descriptor descriptor for tree to be built
     *
     * @param pTempSegment segment to use for temporary buffer, or
     * NULL if it is known that none is needed
     */
    explicit BTreeBuilder(
        BTreeDescriptor const &descriptor,
        SharedSegment pTempSegment = SharedSegment());
    virtual ~BTreeBuilder();

    /**
     * Builds the tree, which must be currently empty or non-existent.  Call
     * getRootPageId() afterwards to find the root of a newly created tree.
     *
     * @param sortedStream stream containing tuples presorted by the tree's
     * key
     *
     * @param nEntries number of tuples to be read from pSortedStream
     *
     * @param fillFactor fraction of each node to fill, where 1.0 (the
     * default) represents 100%
     */
    void build(
        ByteInputStream &sortedStream,
        RecordNum nEntries,
        double fillFactor = 1.0);

    /**
     * Creates an empty tree (just a root node with no tuples).
     * On entry, the builder should have NULL_PAGE_ID for its root.
     * Call getRootPageId() afterwards to get the new root.
     */
    void createEmptyRoot();

    /**
     * Truncates or drops an existing tree.
     *
     * @param rootless if true, all nodes of the existing tree are deallocated;
     * if false, the root is cleared but remains allocated while all
     * other nodes are deallocated
     *
     * @param pLeafPageIdProj if non-NULL, leaf tuples will be read
     * and non-null projected attributes will be interpreted as additional
     * PageId's to be dropped
     */
    void truncate(
        bool rootless,
        TupleProjection const *pLeafPageIdProj = NULL);
};

inline BTreeBuildLevel &BTreeBuilder::getLevel(uint i)
{
    return *(levels[i]);
}

FENNEL_END_NAMESPACE

#endif

// End BTreeBuilder.h
