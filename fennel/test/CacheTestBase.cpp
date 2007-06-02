/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005-2007 The Eigenbase Project
// Copyright (C) 2005-2007 Disruptive Tech
// Copyright (C) 2005-2007 LucidEra, Inc.
// Portions Copyright (C) 1999-2007 John V. Sichi
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

#include "fennel/common/CommonPreamble.h"
#include "fennel/test/CacheTestBase.h"
#include "fennel/common/FileSystem.h"
#include "fennel/device/RandomAccessFileDevice.h"
#include "fennel/synch/Thread.h"
#include "fennel/cache/CachePage.h"
#include "fennel/cache/CacheImpl.h"
#include "fennel/cache/RandomVictimPolicy.h"
#include <boost/test/test_tools.hpp>
#include <strstream>

using namespace fennel;

Cache &CacheTestBase::getCache()
{
    return *pCache;
}
    
typedef CacheImpl<
    CachePage,
    RandomVictimPolicy<CachePage>
> RandomCache;

SharedCache CacheTestBase::newCache()
{
    switch (victimPolicy) {
    case victimRandom:
        return SharedCache(
            new RandomCache(cacheParams),
            ClosableObjectDestructor());
    case victimLRU:
        return Cache::newCache(cacheParams);
    default:
        permAssert(false);
    }
}

SharedRandomAccessDevice CacheTestBase::openDevice(
    std::string devName,DeviceMode openMode,uint nDevicePages,
    DeviceId deviceId)
{
    if (openMode.create) {
        FileSystem::remove(devName.c_str());
    }
    SharedRandomAccessDevice pDevice(
        new RandomAccessFileDevice(devName,openMode));
    if (openMode.create) {
        pDevice->setSizeInBytes(nDevicePages*cbPageFull);
    }
    pCache->registerDevice(deviceId,pDevice);
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
        testDataFile.str(),openMode,nDiskPages,dataDeviceId);
}

void CacheTestBase::closeStorage()
{
    closeDevice(dataDeviceId,pRandomAccessDevice);
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
    DeviceId deviceId,SharedRandomAccessDevice &pDevice)
{
    if (!pDevice) {
        return;
    }
    DeviceIdPagePredicate pagePredicate(deviceId);
    pCache->checkpointPages(pagePredicate,CHECKPOINT_FLUSH_AND_UNMAP);
    pCache->unregisterDevice(deviceId);
    assert(pDevice.unique());
    pDevice.reset();
}

CacheTestBase::CacheTestBase()
{
    cacheParams.readConfig(configMap);
    
    nDiskPages = configMap.getIntParam("diskPages",1000);
    dataDeviceId = DeviceId(23);
    nMemPages = cacheParams.nMemPagesMax;
    cbPageFull = cacheParams.cbPage;
    std::string victimPolicyString = configMap.getStringParam(
        "victimPolicy","lru");
    if (victimPolicyString == "random") {
        victimPolicy = victimRandom;
    } else if (victimPolicyString == "lru") {
        victimPolicy = victimLRU;
    } else {
        std::cerr << "Unknown victim policy " << victimPolicyString
                  << std::endl;
        exit(-1);
    }
    
}

// End CacheTestBase.cpp
