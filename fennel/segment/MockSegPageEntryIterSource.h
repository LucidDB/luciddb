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

#ifndef Fennel_MockSegPageEntryIterSource_Included
#define Fennel_MockSegPageEntryIterSource_Included

#include "fennel/segment/SegPageEntryIterSource.h"
#include "fennel/segment/SegmentAccessor.h"

FENNEL_BEGIN_NAMESPACE

class Segment;

/**
 * A mock SegPageEntryIterSource that pre-fetches every other page, returning
 * each page twice.  The context associated with each page returned is a
 * sequencing counter, starting at 0.
 */
class FENNEL_SEGMENT_EXPORT MockSegPageEntryIterSource
    : public SegPageEntryIterSource<int>
{
    int counter;
    PageId nextPageId;
    SegmentAccessor segmentAccessor;

public:
    explicit MockSegPageEntryIterSource(
        SegmentAccessor const &segmentAccessorInit,
        PageId beginPageId);
    virtual ~MockSegPageEntryIterSource();
    virtual PageId getNextPageForPrefetch(int &entry, bool &found);
};

FENNEL_END_NAMESPACE

#endif

// End MockSegPageEntryIterSource.h
