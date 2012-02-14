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

FENNEL_END_CPPFILE("$Id$");

// End CacheParams.cpp
