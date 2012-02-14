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

#ifndef Fennel_SegStorageTestBase_Included
#define Fennel_SegStorageTestBase_Included

#include "fennel/test/CacheTestBase.h"
#include "fennel/segment/SegmentFactory.h"

FENNEL_BEGIN_NAMESPACE

/**
 * SegStorageTestBase is a common base for any test which depends on the
 * Segment interface.
 */
class FENNEL_TEST_EXPORT SegStorageTestBase
    : virtual public CacheTestBase
{
protected:
    SharedSegmentFactory pSegmentFactory;

    /**
     * Segment supporting linear page allocation.
     */
    SharedSegment pLinearSegment;

    /**
     * (Optional) segment supporting random page allocation.
     */
    SharedSegment pRandomSegment;

    /**
     * (Optional) segment supporting versioned random page allocation.
     */
    SharedSegment pVersionedRandomSegment;

    /**
     * (Optional) segment supporting snapshot random page allocation.
     */
    SharedSegment pSnapshotRandomSegment;

public:
    virtual void openStorage(DeviceMode openMode);

    virtual void openSegmentStorage(DeviceMode openMode);

    virtual void openRandomSegment();

    SharedSegment createLinearDeviceSegment(DeviceId deviceId, uint nPages);

    void closeLinearSegment();

    void closeRandomSegment();

    void closeVersionedRandomSegment();

    void closeSnapshotRandomSegment();

    virtual void closeStorage();

    explicit SegStorageTestBase();
};

FENNEL_END_NAMESPACE

#endif

// End SegStorageTestBase.h

