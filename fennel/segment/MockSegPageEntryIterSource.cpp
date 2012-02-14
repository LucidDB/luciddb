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
#include "fennel/segment/MockSegPageEntryIterSource.h"
#include "fennel/segment/Segment.h"

FENNEL_BEGIN_CPPFILE("$Id$");

MockSegPageEntryIterSource::MockSegPageEntryIterSource(
    SegmentAccessor const &segmentAccessorInit,
    PageId beginPageId)
    : SegPageEntryIterSource<int>()
{
    counter = 0;
    segmentAccessor = segmentAccessorInit;
    nextPageId = beginPageId;
}

MockSegPageEntryIterSource::~MockSegPageEntryIterSource()
{
}

PageId MockSegPageEntryIterSource::getNextPageForPrefetch(
    int &entry,
    bool &found)
{
    found = true;
    entry = counter++;
    // Only need to figure out the next page on every other iteration
    if (counter % 2) {
        return nextPageId;
    }
    PageId currPageId = nextPageId;
    nextPageId = segmentAccessor.pSegment->getPageSuccessor(nextPageId);
    if (nextPageId != NULL_PAGE_ID) {
        nextPageId = segmentAccessor.pSegment->getPageSuccessor(nextPageId);
    }
    return currPageId;
}

FENNEL_END_CPPFILE("$Id:");

// End MockSegPageEntryIterSource.cpp
