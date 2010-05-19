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

#ifndef Fennel_StatsTimer_Included
#define Fennel_StatsTimer_Included

#include "fennel/synch/TimerThread.h"

#include <vector>

FENNEL_BEGIN_NAMESPACE

class StatsTarget;

/**
 * StatsTimer publishes stats snapshots from StatsSources to a StatsTarget.
 */
class FENNEL_SYNCH_EXPORT StatsTimer : private TimerThreadClient
{
    StatsTarget *pTarget;
    std::vector<SharedStatsSource> sources;
    TimerThread timerThread;
    uint intervalInMillis;

    virtual uint getTimerIntervalMillis();
    virtual void onThreadStart();
    virtual void onTimerInterval();
    virtual void onThreadEnd();

public:
    /**
     * Creates a new StatsTimer without any initial target.
     *
     * @param intervalInMillis interval between publications
     */
    explicit StatsTimer(uint intervalInMillis);

    /**
     * Creates a new StatsTimer with an initial target.
     *
     * @param target target to receive events
     *
     * @param intervalInMillis interval between publications
     */
    explicit StatsTimer(StatsTarget &target, uint intervalInMillis);

    virtual ~StatsTimer();

    /**
     * Sets the target for events.
     *
     * @param target target to receive events
     */
    void setTarget(StatsTarget &target);

    /**
     * Adds a source to be published.  Should not be used after start().
     *
     * @param pSource source from which stats will be collected
     */
    void addSource(SharedStatsSource pSource);

    /**
     * Starts publication.  Target and sources must all remain valid until
     * stop().
     */
    void start();

    /**
     * Stops publication and forgets all sources.
     */
    void stop();
};

FENNEL_END_NAMESPACE

#endif

// End StatsTimer.h
