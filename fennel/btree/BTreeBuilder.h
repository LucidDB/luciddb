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
        uint nChildEntries, uint nEntriesPerChildNode);

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
