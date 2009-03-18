/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005-2007 The Eigenbase Project
// Copyright (C) 2005-2007 Disruptive Tech
// Copyright (C) 2005-2007 LucidEra, Inc.
// Portions Copyright (C) 2004-2007 John V. Sichi
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
#include "fennel/exec/ExecStreamGovernor.h"
#include "fennel/common/StatsTarget.h"

FENNEL_BEGIN_CPPFILE("$Id$");

ExecStreamGovernor::ExecStreamGovernor(
    ExecStreamResourceKnobs const &knobSettingsInit,
    ExecStreamResourceQuantity const &resourcesAvailableInit,
    SharedTraceTarget pTraceTargetInit,
    std::string nameInit)
    : TraceSource(pTraceTargetInit, nameInit)
{
    knobSettings.cacheReservePercentage =
        knobSettingsInit.cacheReservePercentage;
    knobSettings.expectedConcurrentStatements =
        knobSettingsInit.expectedConcurrentStatements;

    resourcesAvailable.nCachePages =
        resourcesAvailableInit.nCachePages *
        (100 - knobSettings.cacheReservePercentage) / 100;
    resourcesAssigned.nCachePages = 0;
}

ExecStreamGovernor::~ExecStreamGovernor()
{
    assert(resourceMap.empty());
}

void ExecStreamGovernor::traceCachePageRequest(
    uint assigned,
    ExecStreamResourceRequirements const &reqt,
    std::string const &name)
{
    switch (reqt.optType) {
    case EXEC_RESOURCE_ACCURATE:
        FENNEL_TRACE(TRACE_FINER,
            "Stream " << name << " assigned " << assigned <<
            " pages based on accurate (min,opt) request of " << "(" <<
            reqt.minReqt << "," << reqt.optReqt << ") pages");
        break;
    case EXEC_RESOURCE_ESTIMATE:
        FENNEL_TRACE(TRACE_FINER,
            "Stream " << name << " assigned " << assigned <<
            " pages based on estimated (min,opt) request of " << "(" <<
            reqt.minReqt << "," << reqt.optReqt << ") pages");
        break;
    case EXEC_RESOURCE_UNBOUNDED:
        FENNEL_TRACE(TRACE_FINER,
            "Stream " << name << " assigned " << assigned <<
            " pages based on an unbounded opt request with " <<
            reqt.minReqt << " min pages");
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

FENNEL_END_CPPFILE("$Id$");

// End ExecStreamGovernor.cpp
