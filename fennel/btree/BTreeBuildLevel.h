/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005-2007 The Eigenbase Project
// Copyright (C) 2005-2007 Disruptive Tech
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

#ifndef Fennel_BTreeBuildLevel_Included
#define Fennel_BTreeBuildLevel_Included

#include "fennel/btree/BTreeBuilder.h"
#include "fennel/btree/BTreeNode.h"

#include <boost/scoped_array.hpp>

FENNEL_BEGIN_NAMESPACE

// TODO:  doc inernals

/**
 * BTreeBuildLevel is subordinate to BTreeBuilder.  It manages the build state
 * for one level of a BTree being built.
 */
class BTreeBuildLevel
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

class FixedBuildLevel : public BTreeBuildLevel
{
    friend class BTreeBuilder;

    explicit FixedBuildLevel(
        BTreeBuilder &builderInit,
        BTreeNodeAccessor &nodeAccessorInit);

    // implement the BTreeBuildLevel interface
    virtual void indexLastKey(bool finalize);
    virtual bool isNodeFull(BTreeNode const &node,uint cbTuple);
};

class VariableBuildLevel : public BTreeBuildLevel
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

class DynamicBuildLevel : public BTreeBuildLevel
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
