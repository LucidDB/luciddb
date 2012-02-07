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

#include "fennel/common/CommonPreamble.h"
#include "fennel/segment/WALSegment.h"
#include "fennel/cache/CachePage.h"

FENNEL_BEGIN_CPPFILE("$Id$");

WALSegment::WALSegment(SharedSegment logSegment)
    : DelegatingSegment(logSegment)
{
    assert(
        DelegatingSegment::getAllocationOrder()
        >= Segment::ASCENDING_ALLOCATION);
}

WALSegment::~WALSegment()
{
    assert(dirtyPageSet.empty());
}

void WALSegment::notifyPageDirty(CachePage &page,bool bDataValid)
{
    DelegatingSegment::notifyPageDirty(page, bDataValid);
    PageId logPageId = translateBlockId(
        page.getBlockId());
    StrictMutexGuard mutexGuard(mutex);
    dirtyPageSet.insert(dirtyPageSet.end(), logPageId);
}

void WALSegment::notifyAfterPageFlush(CachePage &page)
{
    DelegatingSegment::notifyAfterPageFlush(page);
    PageId logPageId = translateBlockId(page.getBlockId());
    StrictMutexGuard mutexGuard(mutex);
    dirtyPageSet.erase(logPageId);
}

void WALSegment::notifyPageUnmap(CachePage &page)
{
    if (!page.isDirty()) {
        return;
    }
    notifyAfterPageFlush(page);
}

PageId WALSegment::getMinDirtyPageId() const
{
    StrictMutexGuard mutexGuard(mutex);
    if (dirtyPageSet.empty()) {
        return NULL_PAGE_ID;
    }
    PageId minDirtyPageId = *(dirtyPageSet.begin());
    return minDirtyPageId;
}

FENNEL_END_CPPFILE("$Id$");

// End WALSegment.cpp
