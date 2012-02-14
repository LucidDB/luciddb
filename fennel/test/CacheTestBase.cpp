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
#include "fennel/test/CacheTestBase.h"
#include "fennel/common/FileSystem.h"
#include "fennel/device/RandomAccessFileDevice.h"
#include "fennel/synch/Thread.h"
#include "fennel/cache/CachePage.h"
#include "fennel/cache/CacheImpl.h"
#include "fennel/cache/RandomVictimPolicy.h"
#include "fennel/cache/LRUVictimPolicy.h"
#include <boost/test/test_tools.hpp>
#include <strstream>

#ifdef __MSVC__
#include <process.h>
#endif

using namespace fennel;

Cache &CacheTestBase::getCache()
{
    return *pCache;
}

typedef CacheImpl<
    CachePage,
    RandomVictimPolicy<CachePage>
> RandomCache;

class LRUPage : public CachePage, public LRUVictim
{
public:
    LRUPage(Cache &cache,PBuffer buffer)
        : CachePage(cache, buffer)
    {
    }
};

typedef CacheImpl<
    LRUPage,
    LRUVictimPolicy<LRUPage>
> LRUCache;

SharedCache CacheTestBase::newCache()
{
    switch (victimPolicy) {
    case victimRandom:
        return SharedCache(
            new RandomCache(cacheParams),
            ClosableObjectDestructor());
    case victimLRU:
        return SharedCache(
            new LRUCache(cacheParams),
            ClosableObjectDestructor());
    case victimTwoQ:
        return Cache::newCache(cacheParams);
    default:
        permAssert(false);
    }
}

SharedRandomAccessDevice CacheTestBase::openDevice(
    std::string devName, DeviceMode openMode, uint nDevicePages,
    DeviceId deviceId)
{
    if (openMode.create) {
        FileSystem::remove(devName.c_str());
    }
    SharedRandomAccessDevice pDevice(
        new RandomAccessFileDevice(devName, openMode));
    if (openMode.create) {
        pDevice->setSizeInBytes(nDevicePages*cbPageFull);
    }
    pCache->registerDevice(deviceId, pDevice);
    return pDevice;
}

void CacheTestBase::openStorage(DeviceMode openMode)
{
    // make a test.dat filename unique to each process
    std::ostrstream testDataFile;
    testDataFile << "test-" << getpid() << ".dat" << ends;

    pCache = newCache();

    statsTimer.addSource(pCache);
    statsTimer.start();

    pRandomAccessDevice = openDevice(
        testDataFile.str(), openMode, nDiskPages, dataDeviceId);
}

void CacheTestBase::closeStorage()
{
    closeDevice(dataDeviceId, pRandomAccessDevice);
    statsTimer.stop();
    if (pCache) {
        assert(pCache.unique());
        pCache.reset();
    }
}

void CacheTestBase::testCaseTearDown()
{
    closeStorage();
}

void CacheTestBase::closeDevice(
    DeviceId deviceId, SharedRandomAccessDevice &pDevice)
{
    if (!pDevice) {
        return;
    }
    DeviceIdPagePredicate pagePredicate(deviceId);
    pCache->checkpointPages(pagePredicate, CHECKPOINT_FLUSH_AND_UNMAP);
    pCache->unregisterDevice(deviceId);
    assert(pDevice.unique());
    pDevice.reset();
}

CacheTestBase::CacheTestBase()
{
    cacheParams.readConfig(configMap);

    nDiskPages = configMap.getIntParam("diskPages", 1000);
    dataDeviceId = DeviceId(23);
    nMemPages = cacheParams.nMemPagesMax;
    cbPageFull = cacheParams.cbPage;
    std::string victimPolicyString = configMap.getStringParam(
        "victimPolicy", "twoq");
    if (victimPolicyString == "random") {
        victimPolicy = victimRandom;
    } else if (victimPolicyString == "lru") {
        victimPolicy = victimLRU;
    } else if (victimPolicyString == "twoq") {
        victimPolicy = victimTwoQ;
    } else {
        std::cerr << "Unknown victim policy " << victimPolicyString
                  << std::endl;
        exit(-1);
    }

}

// End CacheTestBase.cpp
