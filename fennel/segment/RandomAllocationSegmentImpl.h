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

#ifndef Fennel_RandomAllocationSegmentImpl_Included
#define Fennel_RandomAllocationSegmentImpl_Included

#include "fennel/segment/RandomAllocationSegment.h"
#include "fennel/segment/SegPageLock.h"

FENNEL_BEGIN_NAMESPACE

// NOTE:  read comments on struct StoredNode before modifying
// the structs below

/**
 * ExtentAllocationNode is the allocation map for one extent
 * in a RandomAllocationSegment.
 */
struct FENNEL_SEGMENT_EXPORT ExtentAllocationNode
    : public StoredNode
{
    static const MagicNumber MAGIC_NUMBER = 0xb9ca99dced182239LL;

    PageEntry &getPageEntry(uint i)
    {
        return reinterpret_cast<PageEntry *>(this + 1)[i];
    }

    PageEntry const &getPageEntry(uint i) const
    {
        return reinterpret_cast<PageEntry const *>(this + 1)[i];
    }
};

typedef SegNodeLock<ExtentAllocationNode> ExtentAllocLock;


FENNEL_END_NAMESPACE

#endif

// End RandomAllocationSegmentImpl.h
