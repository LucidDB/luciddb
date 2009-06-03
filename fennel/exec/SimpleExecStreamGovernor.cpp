/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005-2009 The Eigenbase Project
// Copyright (C) 2005-2009 SQLstream, Inc.
// Copyright (C) 2005-2009 LucidEra, Inc.
// Portions Copyright (C) 2004-2009 John V. Sichi
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
#include "fennel/segment/ScratchMemExcn.h"
#include "fennel/exec/SimpleExecStreamGovernor.h"
#include "fennel/exec/ExecStream.h"
#include "fennel/exec/ExecStreamGraphImpl.h"

#include <math.h>

FENNEL_BEGIN_CPPFILE("$Id$");

SimpleExecStreamGovernor::SimpleExecStreamGovernor(
    ExecStreamResourceKnobs const &knobSettings,
    ExecStreamResourceQuantity const &resourcesAvailable,
    SharedTraceTarget pTraceTargetInit,
    std::string nameInit)
    : TraceSource(pTraceTargetInit, nameInit),
      ExecStreamGovernor(
            knobSettings, resourcesAvailable, pTraceTargetInit, nameInit)
{
    perGraphAllocation = computePerGraphAllocation();
}

SimpleExecStreamGovernor::~SimpleExecStreamGovernor()
{
}

bool SimpleExecStreamGovernor::setResourceKnob(
    ExecStreamResourceKnobs const &knob, ExecStreamResourceKnobType knobType)
{
    StrictMutexGuard mutexGuard(mutex);

    switch (knobType) {
    case EXEC_KNOB_EXPECTED_CONCURRENT_STATEMENTS:
        knobSettings.expectedConcurrentStatements =
            knob.expectedConcurrentStatements;
        perGraphAllocation = computePerGraphAllocation();
        FENNEL_TRACE(
            TRACE_FINE,
            "Expected concurrent statements set to " <<
            knobSettings.expectedConcurrentStatements <<
            ". Per graph allocation is now " << perGraphAllocation <<
            " cache pages.");
        break;

    case EXEC_KNOB_CACHE_RESERVE_PERCENTAGE:
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
        FENNEL_TRACE(
            TRACE_FINE,
            "Cache reserve percentage set to " <<
            knobSettings.cacheReservePercentage <<
            ". Per graph allocation is now " << perGraphAllocation <<
            " cache pages.");
        break;
    }

    return true;
}

bool SimpleExecStreamGovernor::setResourceAvailability(
    ExecStreamResourceQuantity const &available,
    ExecStreamResourceType resourceType)
{
    StrictMutexGuard mutexGuard(mutex);

    switch (resourceType) {
    case EXEC_RESOURCE_CACHE_PAGES:
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
        FENNEL_TRACE(
            TRACE_FINE,
            resourcesAvailable.nCachePages <<
            " cache pages now available for assignment.  " <<
            "Per graph allocation is now " << perGraphAllocation <<
            " cache pages.");
        break;
        }

    case EXEC_RESOURCE_THREADS:
        resourcesAvailable.nThreads = available.nThreads;
        break;
    }

    return true;
}
void SimpleExecStreamGovernor::requestResources(ExecStreamGraph &graph)
{
    FENNEL_TRACE(TRACE_FINE, "requestResources");

    StrictMutexGuard mutexGuard(mutex);

    std::vector<SharedExecStream> sortedStreams = graph.getSortedStreams();
    boost::scoped_array<ExecStreamResourceRequirements> resourceReqts;
    boost::scoped_array<double> sqrtDiffOptMin;
    uint nStreams = sortedStreams.size();

    resourceReqts.reset(new ExecStreamResourceRequirements[nStreams]);
    sqrtDiffOptMin.reset(new double[nStreams]);

    // scale down the number of pages that can be allocated based on how
    // much still remains
    uint allocationAmount =
        std::min(resourcesAvailable.nCachePages, perGraphAllocation);
    FENNEL_TRACE(
        TRACE_FINE,
        allocationAmount << " cache pages available for stream graph");

    // Total the minimum and optimum resource requirements and determine
    // if we have any estimate/unbounded optimum settings
    uint totalMin = 0;
    uint totalOpt = 0;
    double totalSqrtDiffs = 0;
    bool allAccurate = true;
    for (uint i = 0; i < nStreams; i++) {
        ExecStreamResourceQuantity minQuantity, optQuantity;
        ExecStreamResourceSettingType optType;
        sortedStreams[i]->getResourceRequirements(
            minQuantity, optQuantity, optType);
        assert(
            optType == EXEC_RESOURCE_UNBOUNDED ||
            minQuantity.nCachePages <= optQuantity.nCachePages);
        assert(minQuantity.nThreads <= optQuantity.nThreads);

        ExecStreamResourceRequirements &reqt = resourceReqts[i];
        reqt.minReqt = minQuantity.nCachePages;
        totalMin += reqt.minReqt;
        reqt.optType = optType;

        switch (optType) {
        case EXEC_RESOURCE_ACCURATE:
            reqt.optReqt = optQuantity.nCachePages;
            sqrtDiffOptMin[i] = sqrt(double(reqt.optReqt - reqt.minReqt));
            break;
        case EXEC_RESOURCE_ESTIMATE:
            reqt.optReqt = optQuantity.nCachePages;
            sqrtDiffOptMin[i] = sqrt(double(reqt.optReqt - reqt.minReqt));
            allAccurate = false;
            break;
        case EXEC_RESOURCE_UNBOUNDED:
            // in the unbounded case, since we're trying to use as much
            // memory as available, set the difference to how much is
            // available; this way, we set it to something large relative
            // to availability, but still set it to a finite value to
            // allow some allocation to go towards those streams that
            // have estimated optimums
            sqrtDiffOptMin[i] = sqrt(double(allocationAmount));
            allAccurate = false;
            // in the unbounded case, we don't have an optimum setting, so
            // set it to assume the full allocation amount plus the min
            reqt.optReqt = reqt.minReqt + allocationAmount;
            break;
        }
        totalOpt += reqt.optReqt;
        totalSqrtDiffs += sqrtDiffOptMin[i];
    }

    // not enough pages even to assign the minimum requirements
    if (totalMin > allocationAmount &&
        totalMin > resourcesAvailable.nCachePages)
    {
        FENNEL_TRACE(
            TRACE_FINE,
            "Minimum request of " << totalMin << " cache pages not met");
        throw ScratchMemExcn();
    }

    uint totalAssigned;

    // only enough to assign the minimum
    if (totalMin >= allocationAmount) {
        assignCachePages(sortedStreams, resourceReqts, true);
        totalAssigned = totalMin;
        FENNEL_TRACE(
            TRACE_FINE,
            "Mininum request of " << totalMin << " cache pages assigned");

    } else if (totalOpt <= allocationAmount) {
        // if all streams have accurate optimum settings, and we have enough
        // to assign the optimum amount, then do so
        if (allAccurate) {
            assignCachePages(sortedStreams, resourceReqts, false);
            totalAssigned = totalOpt;
            FENNEL_TRACE(
                TRACE_FINE,
                "Optimum request of " << totalOpt << " cache pages assigned");

        } else {
            // even though total optimum is less than the allocation amount,
            // since some streams have estimate settings, we want to try and
            // give a little extra to those streams; the streams that have
            // accurate settings will receive their full optimum amount;
            // note that in this case, there should not be any streams with
            // optimum settings
            uint assigned =
                distributeCachePages(
                    sortedStreams, resourceReqts, sqrtDiffOptMin,
                    totalSqrtDiffs, allocationAmount - totalOpt, true);
            totalAssigned = assigned;
            FENNEL_TRACE(
                TRACE_FINE,
                assigned <<
                " cache pages assigned, based on an optimum request for " <<
                totalOpt << " cache pages");
        }

    } else {
        // allocate the minimum to each stream and then distribute what
        // remains
        uint assigned =
            distributeCachePages(
                sortedStreams, resourceReqts, sqrtDiffOptMin, totalSqrtDiffs,
                allocationAmount - totalMin, false);
        totalAssigned = assigned;
        FENNEL_TRACE(
            TRACE_FINE,
            assigned <<
            " cache pages assigned based on an optimum request for " <<
            totalOpt << " cache pages");
    }

    // update structures to reflect what's been assigned
    resourcesAssigned.nCachePages += totalAssigned;
    resourcesAvailable.nCachePages -= totalAssigned;
    SharedExecStreamResourceQuantity
        pQuantity(new ExecStreamResourceQuantity());
    pQuantity->nCachePages = totalAssigned;
    resourceMap.insert(
        ExecStreamGraphResourceMap::value_type(&graph, pQuantity));

    FENNEL_TRACE(
        TRACE_FINE,
        resourcesAvailable.nCachePages <<
        " cache pages remaining for assignment");
}

void SimpleExecStreamGovernor::assignCachePages(
    std::vector<SharedExecStream> &streams,
    boost::scoped_array<ExecStreamResourceRequirements> const &reqts,
    bool assignMin)
{
    for (uint i = 0; i < streams.size(); i++) {
        ExecStreamResourceQuantity quantity;
        quantity.nCachePages =
            (assignMin) ? reqts[i].minReqt : reqts[i].optReqt;
        streams[i]->setResourceAllocation(quantity);
        if (isTracingLevel(TRACE_FINER)) {
            traceCachePageRequest(
                quantity.nCachePages, reqts[i], streams[i]->getName());
        }
    }
}

uint SimpleExecStreamGovernor::distributeCachePages(
    std::vector<SharedExecStream> &streams,
    boost::scoped_array<ExecStreamResourceRequirements> const &reqts,
    boost::scoped_array<double> const &sqrtDiffOptMin,
    double totalSqrtDiffs,
    uint excessAvailable, bool assignOpt)
{
    // if there's enough to assign the optimum amount to each stream, then
    // adjust totalSqrtDiffs so we don't allocate any extra to the
    // streams with accurate settings
    if (assignOpt) {
        totalSqrtDiffs = 0;
        for (uint i = 0; i < streams.size(); i++) {
            if (reqts[i].optType != EXEC_RESOURCE_ACCURATE) {
                totalSqrtDiffs += sqrtDiffOptMin[i];
            }
        }
    }

    uint excessAssigned = 0;
    uint totalAssigned = 0;
    for (uint i = 0; i < streams.size(); i++) {
        uint amount;
        ExecStreamResourceRequirements &reqt = reqts[i];
        if (assignOpt && reqt.optType == EXEC_RESOURCE_ACCURATE) {
            amount = 0;
        } else {
            amount = (uint) floor(excessAvailable * sqrtDiffOptMin[i] /
                totalSqrtDiffs);
        }
        assert(amount <= (excessAvailable - excessAssigned));
        excessAssigned += amount;

        ExecStreamResourceQuantity quantity;
        if (assignOpt) {
            assert(reqt.optType != EXEC_RESOURCE_UNBOUNDED);
            quantity.nCachePages = reqt.optReqt;
        } else {
            quantity.nCachePages = reqt.minReqt;
        }
        quantity.nCachePages += amount;
        totalAssigned += quantity.nCachePages;
        streams[i]->setResourceAllocation(quantity);
        if (isTracingLevel(TRACE_FINER)) {
            traceCachePageRequest(
                quantity.nCachePages, reqt, streams[i]->getName());
        }
    }

    return totalAssigned;
}

void SimpleExecStreamGovernor::returnResources(ExecStreamGraph &graph)
{
    StrictMutexGuard mutexGuard(mutex);

    ExecStreamGraphResourceMap::const_iterator iter = resourceMap.find(&graph);
    if (iter == resourceMap.end()) {
        // no allocation may have been done
        return;
    }
    SharedExecStreamResourceQuantity pQuantity = iter->second;
    resourcesAssigned.nCachePages -= pQuantity->nCachePages;
    resourcesAvailable.nCachePages += pQuantity->nCachePages;
    FENNEL_TRACE(
        TRACE_FINE,
        "Returned " << pQuantity->nCachePages << " cache pages. " <<
        resourcesAvailable.nCachePages <<
        " cache pages now available for assignment");

    resourceMap.erase(&graph);
}

FENNEL_END_CPPFILE("$Id$");

// End SimpleExecStreamGovernor.cpp
