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

#ifndef Fennel_SegmentTestBase_Included
#define Fennel_SegmentTestBase_Included

#include "fennel/test/SegStorageTestBase.h"
#include "fennel/test/PagingTestBase.h"

FENNEL_BEGIN_NAMESPACE

/**
 * SegmentTestBase is a common base for any test of an implementation of the
 * Segment interface.
 */
class FENNEL_TEST_EXPORT SegmentTestBase
    : virtual public SegStorageTestBase,
        virtual public PagingTestBase
{
protected:
    /**
     * Queue of allocated pages waiting to be freed.
     */
    std::vector<PageId> freeablePages;

    /**
     * Mutex protecting freeablePages.
     */
    StrictMutex freeablePagesMutex;

    /**
     * PageOwnerId to use when allocating pages.
     */
    PageOwnerId objId;

public:
    virtual void openStorage(DeviceMode openMode);

    virtual CachePage *lockPage(OpType opType,uint iPage);

    virtual void unlockPage(CachePage &page,LockMode lockMode);

    virtual void prefetchPage(uint iPage);

    virtual void prefetchBatch(uint, uint);

    virtual void testAllocate();

    virtual void testDeallocate();

    virtual void testCheckpoint();

    explicit SegmentTestBase();

    void testSingleThread();
};

FENNEL_END_NAMESPACE

#endif

// End SegmentTestBase.h

