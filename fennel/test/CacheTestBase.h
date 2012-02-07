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

#ifndef Fennel_CacheTestBase_Included
#define Fennel_CacheTestBase_Included

#include "fennel/test/TestBase.h"
#include "fennel/cache/CacheParams.h"
#include "fennel/device/DeviceMode.h"

FENNEL_BEGIN_NAMESPACE

/**
 * CacheTestBase is a common base for any test which depends on the Cache
 * interface.
 */
class FENNEL_TEST_EXPORT CacheTestBase
    : virtual public TestBase
{
protected:
    /**
     * The available victim policy implementations.
     */
    enum VictimPolicy {
        victimTwoQ,
        victimLRU,
        victimRandom
    };

    /**
     * VictimPolicy to instantiate.
     */
    VictimPolicy victimPolicy;

    /**
     * Parameters for cache initialization.
     */
    CacheParams cacheParams;

    /**
     * Cache instance being tested.
     */
    SharedCache pCache;

    /**
     * The default cached device.
     */
    SharedRandomAccessDevice pRandomAccessDevice;

    /**
     * Size of cache in memory pages.
     */
    uint nMemPages;

    /**
     * Size of device in disk pages.
     */
    uint nDiskPages;

    /**
     * Disk page size.
     */
    uint cbPageFull;

    /**
     * Fixed ID to assign to data device.
     */
    DeviceId dataDeviceId;

public:
    explicit CacheTestBase();

    Cache &getCache();

    virtual SharedCache newCache();

    SharedRandomAccessDevice openDevice(
        std::string devName, DeviceMode openMode, uint nDevicePages,
        DeviceId deviceId);

    virtual void openStorage(DeviceMode openMode);

    virtual void closeStorage();

    virtual void testCaseTearDown();

    void closeDevice(DeviceId deviceId, SharedRandomAccessDevice &pDevice);
};

FENNEL_END_NAMESPACE

#endif

// End CacheTestBase.h
