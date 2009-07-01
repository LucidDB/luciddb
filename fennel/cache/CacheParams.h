/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005-2009 The Eigenbase Project
// Copyright (C) 2005-2009 SQLstream, Inc.
// Copyright (C) 2005-2009 LucidEra, Inc.
// Portions Copyright (C) 1999-2009 John V. Sichi
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
