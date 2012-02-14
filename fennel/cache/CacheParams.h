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

#ifndef Fennel_CacheParams_Included
#define Fennel_CacheParams_Included

#include "fennel/device/DeviceAccessSchedulerParams.h"

FENNEL_BEGIN_NAMESPACE

class ConfigMap;

/**
 * CacheParams defines parameters used to instantiate a Cache.
 */
class FENNEL_CACHE_EXPORT CacheParams
{
public:
    static ParamName paramMaxPages;
    static ParamName paramPagesInit;
    static ParamName paramPageSize;
    static ParamName paramIdleFlushInterval;
    static ParamName paramFreshmenQueuePercentage;
    static ParamName paramPageHistoryQueuePercentage;
    static ParamName paramPrefetchPagesMax;
    static ParamName paramPrefetchThrottleRate;
    static ParamName paramProcessorCacheBytes;

    static uint defaultMemPagesMax;
    static uint defaultMemPagesInit;
    static uint defaultPageSize;
    static uint defaultIdleFlushInterval;
    static uint defaultFreshmenQueuePercentage;
    static uint defaultPageHistoryQueuePercentage;
    static uint defaultPrefetchPagesMax;
    static uint defaultPrefetchThrottleRate;
    static uint defaultProcessorCacheBytes;

    /**
     * Parameters for instantiating DeviceAccessScheduler.
     */
    DeviceAccessSchedulerParams schedParams;

    /**
     * Maximum number of buffer pages the cache can
     * manage.
     */
    uint nMemPagesMax;

    /**
     * Number of bytes per page.
     */
    uint cbPage;

    /**
     * Initial number of page buffers to allocate (up to nMemPagesMax).
     */
    uint nMemPagesInit;

    /**
     * Number of milliseconds between idle flushes, or 0 to disable.
     */
    uint idleFlushInterval;

    /**
     * Percentage of the total cache set aside for the freshmen queue
     * when using the 2Q page victimization policy
     */
    uint freshmenQueuePercentage;

    /**
     * The percentage of the total number of cache pages that dictates the
     * number of pages in the history queue.  This is used as part of the 2Q
     * page victimization policy.
     */
    uint pageHistoryQueuePercentage;

    /**
     * Maximum number of outstanding pre-fetch page requests
     */
    uint prefetchPagesMax;

    /**
     * Number of successful pre-fetches that must occur before the pre-fetch
     * rate is throttled back up, in the event that it has been throttled down
     * because of rejected requests.
     */
    uint prefetchThrottleRate;

    /**
     * Number of bytes assumed for the last-level CPU cache (typically L2, but
     * sometimes L3) for the hardware in use.  Depending on how this
     * parameter is set, this may only be a default or estimate based on
     * calibration.
     */
    uint processorCacheBytes;

    /**
     * Define a default set of cache parameters.
     */
    explicit CacheParams();

    /**
     * Read parameter settings from a ConfigMap.
     */
    void readConfig(ConfigMap const &configMap);
};

FENNEL_END_NAMESPACE

#endif

// End CacheParams.h
