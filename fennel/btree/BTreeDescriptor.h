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

#ifndef Fennel_BTreeDescriptor_Included
#define Fennel_BTreeDescriptor_Included

#include "fennel/tuple/TupleDescriptor.h"
#include "fennel/segment/SegmentAccessor.h"

FENNEL_BEGIN_NAMESPACE

/**
 * BTreeDescriptor defines the information required for accessing a BTree.
 */
struct FENNEL_BTREE_EXPORT BTreeDescriptor
{
    /**
     * Accessor for segment storing BTree.
     */
    SegmentAccessor segmentAccessor;

    /**
     * Descriptor for leaf tuples.
     */
    TupleDescriptor tupleDescriptor;

    /**
     * Projection from tupleDescriptor to key.
     */
    TupleProjection keyProjection;

    /**
     * PageOwnerId used to mark pages.  Defaults to ANON_PAGE_OWNER_ID.
     */
    PageOwnerId pageOwnerId;

    /**
     * PageId of the root node, which never changes.  Set to NULL_PAGE_ID for a
     * new tree.
     */
    PageId rootPageId;

    /**
     * Optional Id of segment containing BTree data.
     */
    SegmentId segmentId;

    explicit BTreeDescriptor()
    {
        pageOwnerId = ANON_PAGE_OWNER_ID;
        rootPageId = NULL_PAGE_ID;
        segmentId = SegmentId(0);
    }
};

FENNEL_END_NAMESPACE

#endif

// End BTreeDescriptor.h
