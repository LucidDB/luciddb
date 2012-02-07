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
#include "fennel/exec/ExecStreamGovernor.h"
#include "fennel/common/StatsTarget.h"

FENNEL_BEGIN_CPPFILE("$Id$");

using std::string;
using std::ostream;
using std::endl;

ExecStreamGovernor::ExecStreamGovernor(
    ExecStreamResourceKnobs const &knobSettingsInit,
    ExecStreamResourceQuantity const &resourcesAvailableInit,
    SharedTraceTarget pTraceTargetInit,
    string nameInit)
    : TraceSource(pTraceTargetInit, nameInit)
{
    knobSettings.cacheReservePercentage =
        knobSettingsInit.cacheReservePercentage;
    knobSettings.expectedConcurrentStatements =
        knobSettingsInit.expectedConcurrentStatements;

    resourcesAvailable.nCachePages =
        resourcesAvailableInit.nCachePages
        * (100 - knobSettings.cacheReservePercentage) / 100;
    resourcesAssigned.nCachePages = 0;
}

inline ostream& operator<< (ostream& os, const ExecStreamGovernor& gov)
{
    gov.print(os);
    return os;
}

ExecStreamGovernor::~ExecStreamGovernor()
{
    if (!resourceMap.empty()) {
        FENNEL_TRACE(
            TRACE_SEVERE,
            "ExecStreamGovernor deleted still holding resources; " << *this);
        assert(false);
    }
}

void ExecStreamGovernor::traceCachePageRequest(
    uint assigned,
    ExecStreamResourceRequirements const &reqt,
    string const &name)
{
    switch (reqt.optType) {
    case EXEC_RESOURCE_ACCURATE:
        FENNEL_TRACE(
            TRACE_FINER,
            "Stream " << name << " assigned " << assigned
            << " pages based on accurate (min,opt) request of " << "("
            << reqt.minReqt << "," << reqt.optReqt << ") pages");
        break;
    case EXEC_RESOURCE_ESTIMATE:
        FENNEL_TRACE(
            TRACE_FINER,
            "Stream " << name << " assigned " << assigned
            << " pages based on estimated (min,opt) request of " << "("
            << reqt.minReqt << "," << reqt.optReqt << ") pages");
        break;
    case EXEC_RESOURCE_UNBOUNDED:
        FENNEL_TRACE(
            TRACE_FINER,
            "Stream " << name << " assigned " << assigned
            << " pages based on an unbounded opt request with "
            << reqt.minReqt << " min pages");
    }
}

void ExecStreamGovernor::writeStats(StatsTarget &target)
{
    StrictMutexGuard mutexGuard(mutex);
    target.writeCounter(
        "ExpectedConcurrentStatements",
        knobSettings.expectedConcurrentStatements);
    target.writeCounter(
        "CacheReservePercentage",
        knobSettings.cacheReservePercentage);
    target.writeCounter(
        "CachePagesGoverned",
        resourcesAvailable.nCachePages);
    target.writeCounter(
        "CachePagesReserved",
        resourcesAssigned.nCachePages);
}

inline ostream& operator<< (ostream& os, const ExecStreamResourceKnobs& k)
{
    os << " expectedConcurrentStatements=" << k.expectedConcurrentStatements;
    os << " cacheReservePercentage=" << k.cacheReservePercentage;
    return os;
}

inline ostream& operator<< (ostream& os, const ExecStreamResourceQuantity& q)
{
    os << " nThreads=" << q.nThreads;
    os << " nCachePages=" << q.nCachePages;
    return os;
}

inline ostream& operator<< (ostream& os, const ExecStreamResourceType t)
{
    switch (t) {
    case EXEC_RESOURCE_THREADS:
        return os << "threads";
    case EXEC_RESOURCE_CACHE_PAGES:
        return os << "cache pages";
    default:
        return os << "??";
    }
}

inline ostream& operator<< (
    ostream& os, const ExecStreamResourceRequirements& req)
{
    os << req.optType << " min=" << req.minReqt << " opt=" << req.optReqt;
    return os;
}

void ExecStreamGovernor::print(ostream& os) const
{
    os << "knobs: " << knobSettings << endl;
    os << "resources available: " << resourcesAvailable << endl;
    os << "resources assigned:  " << resourcesAssigned << endl;
    os << "resource map: {" << endl;
    for (ExecStreamGraphResourceMap::const_iterator
             p = resourceMap.begin(); p != resourceMap.end(); ++p)
    {
        os << "  stream graph " << p->first
           << " => (" << *(p->second) << ")" << endl;
    }
    os << "}" << endl;
}

FENNEL_END_CPPFILE("$Id$");

// End ExecStreamGovernor.cpp
