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

#ifndef Fennel_RandomAllocationSegment_Included
#define Fennel_RandomAllocationSegment_Included

#include "fennel/segment/RandomAllocationSegmentBase.h"

FENNEL_BEGIN_NAMESPACE

struct ExtentAllocationNode;

/**
 * RandomAllocationSegment refines RandomAllocationSegmentBase, defining an
 * ExtentAllocationNode where each page entry within the segment is unversioned.
 * The ExtentAllocationNodes are stored in the segment itself.
 */
class FENNEL_SEGMENT_EXPORT RandomAllocationSegment
    : public RandomAllocationSegmentBase
{
    // implement RandomAllocationSegmentBase
    virtual PageId getSegAllocPageIdForWrite(PageId origSegAllocPageId);
    virtual void undoSegAllocPageWrite(PageId segAllocPageId);
    virtual PageId getExtAllocPageIdForWrite(ExtentNum extentNum);
    virtual PageId allocateFromExtent(ExtentNum extentNum, PageOwnerId ownerId);
    virtual void formatPageExtents(
        SegmentAllocationNode &segAllocNode,
        ExtentNum &extentNum);
    virtual PageId allocateFromNewExtent(
        ExtentNum extentNum,
        PageOwnerId ownerId);
    virtual void freePageEntry(ExtentNum extentNum, BlockNum iPageInExtent);
    virtual PageOwnerId getPageOwnerId(PageId pageId, bool thisSegment);
    virtual PageId getSegAllocPageIdForRead(
        PageId origSegAllocPageId,
        SharedSegment &allocNodeSegment);
    virtual PageId getExtAllocPageIdForRead(
        ExtentNum extentNum,
        SharedSegment &allocNodeSegment);
    virtual void getPageEntryCopy(
        PageId pageId,
        PageEntry &pageEntryCopy,
        bool isAllocated,
        bool thisSegment);

public:
    explicit RandomAllocationSegment(SharedSegment delegateSegment);

    // implementation of Segment interface
    virtual PageId allocatePageId(PageOwnerId ownerId);
    virtual PageId getPageSuccessor(PageId pageId);
    virtual void setPageSuccessor(PageId pageId, PageId successorId);
};

FENNEL_END_NAMESPACE

#endif

// End RandomAllocationSegment.h
