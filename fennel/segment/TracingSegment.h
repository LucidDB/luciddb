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

#ifndef Fennel_TracingSegment_Included
#define Fennel_TracingSegment_Included

#include "fennel/segment/DelegatingSegment.h"
#include "fennel/common/TraceSource.h"

FENNEL_BEGIN_NAMESPACE

/**
 * TracingSegment implements tracing for the Segment interface.
 */
class FENNEL_SEGMENT_EXPORT TracingSegment
    : public DelegatingSegment, public TraceSource
{
public:
    /**
     * Constructs a new TracingSegment.
     *
     * @param delegateSegment the underlying segment
     *
     * @param pTraceTarget the target for trace messages
     *
     * @param sourceName the source name for trace messages
     */
    explicit TracingSegment(
        SharedSegment delegateSegment,
        SharedTraceTarget pTraceTarget,
        std::string sourceName);

    virtual ~TracingSegment();

    // implement the Segment interface
    virtual void setPageSuccessor(PageId pageId, PageId successorId);
    virtual BlockId translatePageId(PageId);
    virtual PageId translateBlockId(BlockId);
    virtual PageId allocatePageId(PageOwnerId ownerId = ANON_PAGE_OWNER_ID);
    virtual bool ensureAllocatedSize(BlockNum nPages);
    virtual void deallocatePageRange(PageId startPageId, PageId endPageId);
    virtual void delegatedCheckpoint(
        Segment &delegatingSegment,CheckpointType checkpointType);
    virtual MappedPageListener *getMappedPageListener(BlockId blockId);
    virtual bool isWriteVersioned();

    // delegate the MappedPageListener interface
    virtual void notifyPageMap(CachePage &page);
    virtual void notifyPageUnmap(CachePage &page);
    virtual void notifyAfterPageRead(CachePage &page);
    virtual void notifyPageDirty(CachePage &page,bool bDataValid);
    virtual void notifyBeforePageFlush(CachePage &page);
    virtual void notifyAfterPageFlush(CachePage &page);
    virtual MappedPageListener *notifyAfterPageCheckpointFlush(CachePage &page);
};

FENNEL_END_NAMESPACE

#endif

// End TracingSegment.h
