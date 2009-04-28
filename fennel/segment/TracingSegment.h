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
    virtual void deallocatePageRange(PageId startPageId,PageId endPageId);
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
