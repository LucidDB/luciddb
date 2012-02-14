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

#ifndef Fennel_BTreeCompactNodeAccessor_Included
#define Fennel_BTreeCompactNodeAccessor_Included

#include "fennel/btree/BTreeNodeAccessor.h"

FENNEL_BEGIN_NAMESPACE

/**
 * BTreeCompactNodeAccessor maintains the data on a BTreeNode as a
 * compact array of fixed-length entries, with all free space contiguous at the
 * end of the page.
 *
 *<p>
 *
 * TODO:  a high-performance template for builtin datatypes
 */
class FENNEL_BTREE_EXPORT BTreeCompactNodeAccessor
    : public BTreeNodeAccessor
{
    /**
     * Number of bytes per entry.
     */
    uint cbEntry;

public:
    explicit BTreeCompactNodeAccessor();

    // implement the interface expected by BTreeKeyedNodeAccessor
    inline PConstBuffer getEntryForReadInline(
        BTreeNode const &node,uint iEntry);

    // implement the BTreeNodeAccessor interface
    virtual void onInit();
    virtual PBuffer allocateEntry(BTreeNode &node,uint iEntry,uint cbEntry);
    virtual void deallocateEntry(BTreeNode &node,uint iEntry);
    virtual bool hasFixedWidthEntries() const;
    virtual Capacity calculateCapacity(BTreeNode const &node,uint cbEntry);
    virtual uint getEntryByteCount(uint cbTuple);
    virtual void compactNode(BTreeNode &node,BTreeNode &scratchNode);
};

inline PConstBuffer BTreeCompactNodeAccessor::getEntryForReadInline(
    BTreeNode const &node,uint iEntry)
{
    return node.getDataForRead() + iEntry*cbEntry;
}

// TODO: a slotmap representation to be used for indirection of long fixed-width
// entries?

FENNEL_END_NAMESPACE

#endif

// End BTreeCompactNodeAccessor.h
