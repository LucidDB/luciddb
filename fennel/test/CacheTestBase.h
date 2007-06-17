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
class CacheTestBase : virtual public TestBase
{
protected:
    /**
     * The available victim policy implementations.
     */
    enum VictimPolicy {
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
        std::string devName,DeviceMode openMode,uint nDevicePages,
        DeviceId deviceId);
    
    virtual void openStorage(DeviceMode openMode);

    virtual void closeStorage();

    virtual void testCaseTearDown();

    void closeDevice(DeviceId deviceId,SharedRandomAccessDevice &pDevice);
};

FENNEL_END_NAMESPACE

#endif

// End CacheTestBase.h
