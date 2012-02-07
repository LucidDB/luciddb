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

#ifndef Fennel_BTreeBuildLevel_Included
#define Fennel_BTreeBuildLevel_Included

#include "fennel/btree/BTreeBuilder.h"
#include "fennel/btree/BTreeNode.h"

#include <boost/scoped_array.hpp>

FENNEL_BEGIN_NAMESPACE

// TODO:  doc internals

/**
 * BTreeBuildLevel is subordinate to BTreeBuilder.  It manages the build state
 * for one level of a BTree being built.
 */
class FENNEL_BTREE_EXPORT BTreeBuildLevel
{
    friend class BTreeBuilder;

protected:
    /**
     * Owning BTreeBuilder.
     */
    BTreeBuilder &builder;

    /**
     * BTreeNodeAccessor to use for accessing nodes in this level.
     */
    BTreeNodeAccessor &nodeAccessor;

    /**
     * 0-based height of this level in tree.
     */
    uint iLevel;

    /**
     * 0-based sequence number of node being built on this level.
     */
    RecordNum iNode;

    /**
     * Total number of entries expected at this level.  If 0, total is unknown.
     */
    RecordNum nEntriesTotal;

    /**
     * Lock on current node.
     */
    BTreePageLock pageLock;

    /**
     * PageId of current node.
     */
    PageId pageId;

    /**
     * Number of entries to store on each node.  This varies over the
     * level to achieve the desired balancing.  If 0, no balancing is being
     * performed.
     */
    uint nEntriesPerNode;

    /**
     * Number of entries appended on this level so far (across all nodes).
     * For non-leaf levels, this should stay in sync with the iNode of the
     * child level.
     */
    RecordNum nEntriesProcessed;

    /**
     * Number of bytes of free space to reserve on each page (determined by
     * fill factor).  0 for fixed-width entries.
     */
    uint cbReserved;

    bool isFinished() const;

    void processInput(ByteInputStream &sortedInputStream);

    void unmarshalLastKey();

    BTreeNode *allocateAndLinkNewNode();

    BTreeNode &allocatePage();

    explicit BTreeBuildLevel(
        BTreeBuilder &builderInit,
        BTreeNodeAccessor &nodeAccessorInit);

    virtual bool isNodeFull(BTreeNode const &node,uint cbTuple);

    virtual void indexLastKey(bool finalize) = 0;

public:
    virtual ~BTreeBuildLevel();

    void indexLastChild();
};

class FENNEL_BTREE_EXPORT FixedBuildLevel
    : public BTreeBuildLevel
{
    friend class BTreeBuilder;

    explicit FixedBuildLevel(
        BTreeBuilder &builderInit,
        BTreeNodeAccessor &nodeAccessorInit);

    // implement the BTreeBuildLevel interface
    virtual void indexLastKey(bool finalize);
    virtual bool isNodeFull(BTreeNode const &node,uint cbTuple);
};

class FENNEL_BTREE_EXPORT VariableBuildLevel
    : public BTreeBuildLevel
{
    friend class BTreeBuilder;

    SharedSegOutputStream pParentKeyStream;

    explicit VariableBuildLevel(
        BTreeBuilder &builderInit,
        BTreeNodeAccessor &nodeAccessorInit);

    SharedSegInputStream getParentKeyStream();

    // implement the BTreeBuildLevel interface
    virtual void indexLastKey(bool finalize);

public:
    virtual ~VariableBuildLevel();
};

class FENNEL_BTREE_EXPORT DynamicBuildLevel
    : public BTreeBuildLevel
{
    friend class BTreeBuilder;

    explicit DynamicBuildLevel(
        BTreeBuilder &builderInit,
        BTreeNodeAccessor &nodeAccessorInit);

    // implement the BTreeBuildLevel interface
    virtual void indexLastKey(bool finalize);
};

FENNEL_END_NAMESPACE

#endif

// End BTreeBuildLevel.h
