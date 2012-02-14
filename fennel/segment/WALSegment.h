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

#ifndef Fennel_WALSegment_Included
#define Fennel_WALSegment_Included

#include "fennel/segment/DelegatingSegment.h"
#include "fennel/synch/SynchObj.h"

FENNEL_BEGIN_NAMESPACE

/**
 * WALSegment is an implementation of Segment which keeps track of pages as
 * they are dirtied and flushed.  This information can be used to implement the
 * write-ahead log (WAL) protocol.  See <a
 * href="structSegmentDesign.html#WALSegment">the design docs</a> for more
 * detail.
 */
class FENNEL_SEGMENT_EXPORT WALSegment
    : public DelegatingSegment
{
    friend class SegmentFactory;

    mutable StrictMutex mutex;
    PageSet dirtyPageSet;

    explicit WALSegment(SharedSegment logSegment);

public:

    virtual ~WALSegment();

    /**
     * Determines the ID of the lowest dirty log page.
     *
     * @return the PageId, or NULL_PAGE_ID if no dirty log pages
     */
    PageId getMinDirtyPageId() const;

    // implement the MappedPageListener interface
    virtual void notifyPageDirty(CachePage &page,bool bDataValid);
    virtual void notifyAfterPageFlush(CachePage &page);
    virtual void notifyPageUnmap(CachePage &page);
};

FENNEL_END_NAMESPACE

#endif

// End WALSegment.h
