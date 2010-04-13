/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005 The Eigenbase Project
// Copyright (C) 2005 SQLstream, Inc.
// Copyright (C) 2005 Dynamo BI Corporation
// Portions Copyright (C) 1999 John V. Sichi
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
#include "fennel/cache/CacheParams.h"
#include "fennel/common/ConfigMap.h"

FENNEL_BEGIN_CPPFILE("$Id$");

ParamName CacheParams::paramMaxPages = "cachePagesMax";
ParamName CacheParams::paramPagesInit = "cachePagesInit";
ParamName CacheParams::paramPageSize = "cachePageSize";
ParamName CacheParams::paramIdleFlushInterval = "cacheIdleFlushInterval";
ParamName CacheParams::paramFreshmenQueuePercentage =
    "freshmenPageQueuePercentage";
ParamName CacheParams::paramPageHistoryQueuePercentage =
    "pageHistoryQueuePercentage";
ParamName CacheParams::paramPrefetchPagesMax =
    "prefetchPagesMax";
ParamName CacheParams::paramPrefetchThrottleRate = "prefetchThrottleRate";
ParamName CacheParams::paramProcessorCacheBytes = "processorCacheBytes";

uint CacheParams::defaultMemPagesMax = 1024;
uint CacheParams::defaultMemPagesInit = MAXU;
uint CacheParams::defaultPageSize = 4096;
uint CacheParams::defaultIdleFlushInterval = 100;
uint CacheParams::defaultFreshmenQueuePercentage = 25;
uint CacheParams::defaultPageHistoryQueuePercentage = 100;
uint CacheParams::defaultPrefetchPagesMax = 12;
uint CacheParams::defaultPrefetchThrottleRate = 10;
// assume 2MB is typical
uint CacheParams::defaultProcessorCacheBytes = 2097152;

CacheParams::CacheParams()
{
    nMemPagesMax = defaultMemPagesMax;
    cbPage = defaultPageSize;
    nMemPagesInit = defaultMemPagesInit;
    idleFlushInterval = defaultIdleFlushInterval;
    freshmenQueuePercentage = defaultFreshmenQueuePercentage;
    pageHistoryQueuePercentage = defaultPageHistoryQueuePercentage;
    prefetchPagesMax = defaultPrefetchPagesMax;
    prefetchThrottleRate = defaultPrefetchThrottleRate;
    processorCacheBytes = defaultProcessorCacheBytes;
}

void CacheParams::readConfig(ConfigMap const &configMap)
{
    schedParams.readConfig(configMap);
    nMemPagesMax = configMap.getIntParam(
        paramMaxPages, nMemPagesMax);
    cbPage = configMap.getIntParam(
        paramPageSize, cbPage);
    nMemPagesInit = configMap.getIntParam(
        paramPagesInit, nMemPagesInit);
    if (!isMAXU(nMemPagesInit)) {
        if (nMemPagesMax < nMemPagesInit) {
            nMemPagesMax = nMemPagesInit;
        }
    }
    idleFlushInterval = configMap.getIntParam(
        paramIdleFlushInterval, idleFlushInterval);
    freshmenQueuePercentage = configMap.getIntParam(
        paramFreshmenQueuePercentage, freshmenQueuePercentage);
    pageHistoryQueuePercentage = configMap.getIntParam(
        paramPageHistoryQueuePercentage, pageHistoryQueuePercentage);
    prefetchPagesMax = configMap.getIntParam(
        paramPrefetchPagesMax, prefetchPagesMax);
    prefetchThrottleRate = configMap.getIntParam(
        paramPrefetchThrottleRate, prefetchThrottleRate);
    int iProcessorCacheBytes = configMap.getIntParam(
        paramProcessorCacheBytes, processorCacheBytes);
    if (iProcessorCacheBytes != -1) {
        processorCacheBytes = iProcessorCacheBytes;
    }
}

FENNEL_END_CPPFILE("$Id: //open/dev/fennel/cache/CacheParams.cpp#13 $");

// End CacheParams.cpp
