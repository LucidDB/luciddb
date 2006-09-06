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
}

SimpleExecStreamGovernor::~SimpleExecStreamGovernor()
{
}

void SimpleExecStreamGovernor::requestResources(
    SharedExecStreamGraph pExecStreamGraph)
{
    FENNEL_TRACE(TRACE_FINE, "requestResources");

    StrictMutexGuard mutexGuard(mutex);

    std::vector<SharedExecStream> sortedStreams =
        pExecStreamGraph->getSortedStreams();
    boost::scoped_array<uint> minReqts;
    boost::scoped_array<uint> optReqts;
    boost::scoped_array<ExecStreamResourceSettingType> optTypes;
    boost::scoped_array<double> sqrtDiffOptMin;
    uint nStreams = sortedStreams.size();

    minReqts.reset(new uint[nStreams]);
    optReqts.reset(new uint[nStreams]);
    optTypes.reset(new ExecStreamResourceSettingType[nStreams]);
    sqrtDiffOptMin.reset(new double[nStreams]);

    // scale down the number of pages that can be allocated based on how
    // much still remains
    uint allocationAmount =
        std::min(resourcesAvailable.nCachePages, perGraphAllocation);
    FENNEL_TRACE(TRACE_FINE,
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

        minReqts[i] = minQuantity.nCachePages;
        totalMin += minReqts[i];
        optTypes[i] = optType;

        switch (optType) {
        case EXEC_RESOURCE_ACCURATE:
            optReqts[i] = optQuantity.nCachePages;
            sqrtDiffOptMin[i] = sqrt(optReqts[i] - minReqts[i]);
            break;
        case EXEC_RESOURCE_ESTIMATE:
            optReqts[i] = optQuantity.nCachePages;
            sqrtDiffOptMin[i] = sqrt(optReqts[i] - minReqts[i]);
            allAccurate = false;
            break;
        case EXEC_RESOURCE_UNBOUNDED:
            // in the unbounded case, since we're trying to use as much
            // memory as available, set the difference to how much is
            // available; this way, we set it to something large relative
            // to availability, but still set it to a finite value to
            // allow some allocation to go towards those streams that
            // have estimated optimums
            sqrtDiffOptMin[i] = sqrt(allocationAmount);
            allAccurate = false;
            // in the unbounded case, we don't have an optimum setting, so
            // set it to assume the full allocation amount plus the min
            optReqts[i] = minReqts[i] + allocationAmount;
            break;
        }
        totalOpt += optReqts[i];
        totalSqrtDiffs += sqrtDiffOptMin[i];
    }

    // not enough pages even to assign the minimum requirements
    if (totalMin > allocationAmount &&
        totalMin > resourcesAvailable.nCachePages)
    {
        FENNEL_TRACE(TRACE_FINE,
            "Minimum request of " << totalMin << " cache pages not met");
        throw ScratchMemExcn();
    }

    uint totalAssigned;

    // only enough to assign the minimum
    if (totalMin >= allocationAmount) {
        assignCachePages(sortedStreams, minReqts);
        totalAssigned = totalMin;
        FENNEL_TRACE(TRACE_FINE,
            "Mininum request of " << totalMin << " cache pages assigned");

    } else if (totalOpt <= allocationAmount) {
        // if all streams have accurate optimum settings, and we have enough
        // to assign the optimum amount, then do so
        if (allAccurate) {
            assignCachePages(sortedStreams, optReqts);
            totalAssigned = totalOpt;
            FENNEL_TRACE(TRACE_FINE,
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
                    sortedStreams, minReqts, optReqts, optTypes,
                    sqrtDiffOptMin, totalSqrtDiffs,
                    allocationAmount - totalOpt, true);
            totalAssigned = assigned;
            FENNEL_TRACE(TRACE_FINE,
                assigned <<
                " cache pages assigned, based on an optimum request for " <<
                totalOpt << " cache pages");
        }

    } else {
        // allocate the minimum to each stream and then distribute what
        // remains
        uint assigned =
            distributeCachePages(
                sortedStreams, minReqts, optReqts, optTypes,
                sqrtDiffOptMin, totalSqrtDiffs,
                allocationAmount - totalMin, false);
        totalAssigned = assigned; 
        FENNEL_TRACE(TRACE_FINE,
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
        ExecStreamGraphResourceMap::value_type(
            pExecStreamGraph.get(), pQuantity));

    FENNEL_TRACE(TRACE_FINE,
        resourcesAvailable.nCachePages <<
        " cache pages remaining for assignment");
}

void SimpleExecStreamGovernor::assignCachePages(
    std::vector<SharedExecStream> &streams,
    boost::scoped_array<uint> const &nCachePages)
{
    for (uint i = 0; i < streams.size(); i++) {
        ExecStreamResourceQuantity quantity;
        quantity.nCachePages = nCachePages[i];
        streams[i]->setResourceAllocation(quantity);
        FENNEL_TRACE(TRACE_FINER,
            quantity.nCachePages << " cache pages assigned to stream " <<
            streams[i]->getName());
    }
}

uint SimpleExecStreamGovernor::distributeCachePages(
    std::vector<SharedExecStream> &streams,
    boost::scoped_array<uint> const &minReqts,
    boost::scoped_array<uint> const &optReqts,
    boost::scoped_array<ExecStreamResourceSettingType> const &optTypes,
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
            if (optTypes[i] != EXEC_RESOURCE_ACCURATE) {
                totalSqrtDiffs += sqrtDiffOptMin[i];
            }
        }
    }

    uint excessAssigned = 0;
    uint totalAssigned = 0;
    for (uint i = 0; i < streams.size(); i++) {
        uint amount;
        if (assignOpt && optTypes[i] == EXEC_RESOURCE_ACCURATE) {
            amount = 0;
        } else {
            amount = (uint) floor(excessAvailable * sqrtDiffOptMin[i] /
                totalSqrtDiffs);
        }
        assert(amount <= (excessAvailable - excessAssigned));
        excessAssigned += amount;

        ExecStreamResourceQuantity quantity;
        if (assignOpt) {
            assert(optTypes[i] != EXEC_RESOURCE_UNBOUNDED);
            quantity.nCachePages = optReqts[i];
        } else {
            quantity.nCachePages = minReqts[i];
        }
        quantity.nCachePages += amount;
        totalAssigned += quantity.nCachePages;
        streams[i]->setResourceAllocation(quantity);
        FENNEL_TRACE(TRACE_FINER,
            quantity.nCachePages << " cache pages assigned to stream " <<
            streams[i]->getName());
    }

    return totalAssigned;
}

void SimpleExecStreamGovernor::returnResources(
    ExecStreamGraph *pExecStreamGraph)
{
    StrictMutexGuard mutexGuard(mutex);

    ExecStreamGraphResourceMap::const_iterator iter =
        resourceMap.find(pExecStreamGraph);
    if (iter == resourceMap.end()) {
        // no allocation may have been done
        return;
    }
    SharedExecStreamResourceQuantity pQuantity = iter->second;
    resourcesAssigned.nCachePages -= pQuantity->nCachePages;
    resourcesAvailable.nCachePages += pQuantity->nCachePages;
    FENNEL_TRACE(TRACE_FINE,
        "Returned " << pQuantity->nCachePages << " cache pages. " <<
        resourcesAvailable.nCachePages <<
        " cache pages now available for assignment");

    resourceMap.erase(pExecStreamGraph);

}
    
FENNEL_END_CPPFILE("$Id$");

// End SimpleExecStreamGovernor.cpp
