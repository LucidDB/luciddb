/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2005-2005 Disruptive Tech
// Copyright (C) 2005-2005 LucidEra, Inc.
// Portions Copyright (C) 1999-2005 John V. Sichi
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

#ifndef Fennel_ExecStreamGovernor_Included
#define Fennel_ExecStreamGovernor_Included

#include "fennel/common/SharedTypes.h"
#include "fennel/common/TraceSource.h"
#include "fennel/exec/ExecStreamDefs.h"
#include "fennel/synch/SynchObj.h"

#include <boost/utility.hpp>
#include <map>

FENNEL_BEGIN_NAMESPACE

/**
 * Resource knobs available
 */
enum ExecStreamResourceKnobType {
    ExpectedConcurrentStatements,
    CacheReservePercentage
};

/**
 * ExecStreamResourceKnobs is a structure that stores the settings of the
 * different knobs that are used to determine exec stream resource allocation
 */
struct ExecStreamResourceKnobs
{
    /**
     * Max expected number of concurrent, active statements
     */
    uint expectedConcurrentStatements;

    /**
     * Percentage of cache pages to keep in reserve
     */
    int cacheReservePercentage;
};

/**
 * Resource types
 */
enum ExecStreamResourceType { Threads, CachePages };

/**
 * ExecStreamGovernor defines an abstract base for determining
 * resource allocation for an execution stream graph as well as the individual
 * execution streams within the graph.
 *
 * @author Zelaine Fong
 * @version $Id$
 */
class ExecStreamGovernor
    : public boost::noncopyable, public virtual TraceSource
{
protected:
    typedef std::map<ExecStreamGraph *, SharedExecStreamResourceQuantity>
        ExecStreamGraphResourceMap;

    /**
     * Current resource knob settings
     */
    ExecStreamResourceKnobs knobSettings;

    /**
     * Portion of resources that can be allocated to an exec stream graph
     */
    uint perGraphAllocation;

    /**
     * Keeps track of the total resources that are currently available for
     * assignment
     */
    ExecStreamResourceQuantity resourcesAvailable;

    /**
     * Keeps track of resources that have been assigned
     */
    ExecStreamResourceQuantity resourcesAssigned;

    /**
     * Used to keep track of how much resources have been assigned to each of
     * the currently active exec stream graphs
     */
    ExecStreamGraphResourceMap resourceMap;

    /**
     * Used to synchronize access
     */
    StrictMutex mutex;

    /**
     * Initializes the resource governor object.  Initializes
     * current resource availability. Computes the perGraphAllocation
     * based on current resource availability and current resource
     * knob settings.
     *
     * @param knobSettings initial knob settings
     * @param resourcesAvailable initial available resources
     * @param pTraceTarget the TraceTarget to which messages will be sent,
     * or NULL to disable tracing entirely
     * @param name the name to use for tracing this resource governor
     */
    explicit ExecStreamGovernor(
        ExecStreamResourceKnobs const &knobSettings,
        ExecStreamResourceQuantity const &resourcesAvailable,
        SharedTraceTarget pTraceTarget,
        std::string name);

    inline uint computePerGraphAllocation();

public:
    virtual ~ExecStreamGovernor();

    /**
     * Informs the resource governor of a new knob setting.  Called by
     * ALTER SYSTEM SET commands that dynamically modify parameters
     * controlling resource allocation.  Recomputes perGraphAllocation.
     *
     * @param knob new resource knob setting
     * @param knobType indicates which knob setting to change
     *
     * @return True if possible to apply new setting; false if
     * the setting is below current in-use threshholds.  E.g., if modifying
     * cacheReservePercentage, the new number of reserved pages must allow
     * for pages already assigned.
     */
     bool setResourceKnob(
         ExecStreamResourceKnobs const &knob,
         ExecStreamResourceKnobType knobType);

    /**
     * Informs the resource governor of a new resource availablity.
     * Called by ALTER SYSTEM SET commands that dynamically
     * modify resources available.
     *
     * @param available amount of resources now available
     * @param resourceType type of resource to be set
     *
     * @return True if possible to apply new settings; false if
     * the setting is below current in-use threshholds.  E.g.,
     * if modifying cachePagesInit, the value needs to be >= current
     * number of cache pages that have been assigned.
     */
    bool setResourceAvailability(
        ExecStreamResourceQuantity const &available,
        ExecStreamResourceType resourceType);

    /**
     * Requests resources for an exec stream graph and assigns
     * resources to each exec stream in the graph.  If the minimum
     * resource requirements for any exec stream cannot be met,
     * an exception is thrown.
     *
     * @param pExecStreamGraph the exec stream graph for resources are being
     * requested
     */
    virtual void requestResources(SharedExecStreamGraph pExecStreamGraph) = 0;

    /**
     * Returns to the available resource pool resources that have been
     * assigned to an exec stream graph.
     *
     * @param pExecStreamGraph the exec stream graph that is returning its
     * resources
     */
    virtual void returnResources(ExecStreamGraph *pExecStreamGraph) = 0;
};

inline uint ExecStreamGovernor::computePerGraphAllocation()
{
    return (resourcesAvailable.nCachePages + resourcesAssigned.nCachePages) /
        knobSettings.expectedConcurrentStatements;
}

FENNEL_END_NAMESPACE

#endif

// End ExecStreamGovernor.h
