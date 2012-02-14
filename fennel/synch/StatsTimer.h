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
