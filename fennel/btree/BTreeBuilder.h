/*
// $Id$
// Fennel is a relational database kernel.
// Copyright (C) 1999-2004 John V. Sichi.
//
// This program is free software; you can redistribute it and/or
// modify it under the terms of the GNU Lesser General Public License
// as published by the Free Software Foundation; either version 2.1
// of the License, or (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with this program; if not, write to the Free Software
// Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
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
class BTreeBuilder : public BTreeAccessBase
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

public:
    /**
     * Create a new BTreeBuilder.  To build a non-empty
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
     * Build the tree, which must be currently empty or non-existent.  Use
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
     * Create an empty tree (just a root node with no tuples).
     * On entry, the builder should have NULL_PAGE_ID for its root.
     * Use getRootPageId() afterwards to get the new root.
     */
    void createEmptyRoot();

    /**
     * Truncate or drop an existing tree.
     *
     * @param rootless if true, all nodes of the existing tree are deallocated;
     * if false, the root is cleared but remains allocated while all
     * other nodes are deallocated
     */
    void truncate(bool rootless);
};

inline BTreeBuildLevel &BTreeBuilder::getLevel(uint i)
{
    return *(levels[i]);
}

FENNEL_END_NAMESPACE

#endif

// End BTreeBuilder.h
