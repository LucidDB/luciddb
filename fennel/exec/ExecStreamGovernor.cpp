/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2005-2005 Disruptive Tech
// Copyright (C) 2005-2005 LucidEra, Inc.
// Portions Copyright (C) 2004-2005 John V. Sichi
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

    perGraphAllocation = computePerGraphAllocation();
}

ExecStreamGovernor::~ExecStreamGovernor()
{
}

bool ExecStreamGovernor::setResourceKnob(
    ExecStreamResourceKnobs const &knob, ExecStreamResourceKnobType knobType)
{
    StrictMutexGuard mutexGuard(mutex);

    switch (knobType) {
    case ExpectedConcurrentStatements:
        knobSettings.expectedConcurrentStatements =
            knob.expectedConcurrentStatements;
        perGraphAllocation = computePerGraphAllocation();
        FENNEL_TRACE(TRACE_FINE,
            "Expected concurrent statements set to " <<
            knobSettings.expectedConcurrentStatements <<
            ". Per graph allocation is now " << perGraphAllocation <<
            " cache pages.");
        break;

    case CacheReservePercentage:
        // make sure we have enough unassigned pages to set aside the new
        // reserve amount
        double percent = (100 - knobSettings.cacheReservePercentage) / 100.0;
        uint totalPagesAvailable = (uint)
            ((resourcesAvailable.nCachePages + resourcesAssigned.nCachePages) /
            percent);
        uint numReserve =
            totalPagesAvailable * knob.cacheReservePercentage / 100;
        if (totalPagesAvailable - numReserve < resourcesAssigned.nCachePages) {
            return false;
        }
        knobSettings.cacheReservePercentage = knob.cacheReservePercentage;
        resourcesAvailable.nCachePages =
            totalPagesAvailable - numReserve - resourcesAssigned.nCachePages;
        perGraphAllocation = computePerGraphAllocation();
        FENNEL_TRACE(TRACE_FINE,
            "Cache reserve percentage set to " <<
            knobSettings.cacheReservePercentage <<
            ". Per graph allocation is now " << perGraphAllocation <<
            " cache pages.");
        break;
    }

    return true;
}

bool ExecStreamGovernor::setResourceAvailability(
    ExecStreamResourceQuantity const &available,
    ExecStreamResourceType resourceType)
{
    StrictMutexGuard mutexGuard(mutex);

    switch (resourceType) {
    case CachePages:
        {
        uint pagesAvailable =
            available.nCachePages *
            (100 - knobSettings.cacheReservePercentage) / 100;
        if (pagesAvailable < resourcesAssigned.nCachePages) {
            return false;
        }
        resourcesAvailable.nCachePages =
            (pagesAvailable - resourcesAssigned.nCachePages);
        perGraphAllocation = computePerGraphAllocation();
        FENNEL_TRACE(TRACE_FINE,
            resourcesAvailable.nCachePages <<
            " cache pages now available for assignment.  " <<
            "Per graph allocation is now " << perGraphAllocation <<
            " cache pages.");
        break;
        }

    case Threads:
        resourcesAvailable.nThreads = available.nThreads;
        break;
    }

    return true;
}
    
FENNEL_END_CPPFILE("$Id$");

// End ExecStreamGovernor.cpp
