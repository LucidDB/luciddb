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

#ifndef Fennel_SnapshotSegmentTestBase_Included
#define Fennel_SnapshotSegmentTestBase_Included

#include "fennel/test/SegmentTestBase.h"

FENNEL_BEGIN_NAMESPACE

/**
 * SnapshotSegmentTestBase is a common base for any test that uses a
 * SnapshotRandomAllocationSegment for storage.
 */
class FENNEL_TEST_EXPORT SnapshotSegmentTestBase
    : virtual public SegmentTestBase
{
protected:
    uint nDiskPagesTotal;
    PageId firstPageId;
    DeviceId tempDeviceId;
    SharedRandomAccessDevice pTempDevice;
    SharedSegment pTempSegment;
    TxnId currCsn;
    std::vector<TxnId> updatedCsns;
    bool commit;
    SharedSegment pSnapshotRandomSegment2;

    /**
     * Forces the underlying snapshot segment to always execute its checkpoints
     * during a cache flush and unmap.
     */
    void setForceCacheUnmap(SharedSegment pSegment);

    /**
     * Commits transactions associated with a specified csn.
     *
     * @param commitCsn the specified csn
     */
    void commitChanges(TxnId commitCsn);

public:
    explicit SnapshotSegmentTestBase();

    virtual void testCaseSetUp();
    virtual void openSegmentStorage(DeviceMode openMode);
    virtual void closeStorage();
    virtual void testAllocateAll();
    virtual void verifyPage(CachePage &page, uint x);
    virtual void fillPage(CachePage &page, uint x);
};

FENNEL_END_NAMESPACE

#endif

// End SnapshotSegmentTestBase.h
