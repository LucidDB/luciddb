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
#include "fennel/synch/StatsTimer.h"
#include "fennel/common/StatsSource.h"
#include "fennel/common/StatsTarget.h"

FENNEL_BEGIN_CPPFILE("$Id$");

StatsTimer::StatsTimer(
    StatsTarget &target,
    uint intervalInMillisInit)
    : timerThread(*this)
{
    pTarget = &target;
    intervalInMillis = intervalInMillisInit;
}

StatsTimer::StatsTimer(
    uint intervalInMillisInit)
    : timerThread(*this)
{
    pTarget = NULL;
    intervalInMillis = intervalInMillisInit;
}

StatsTimer::~StatsTimer()
{
}

void StatsTimer::setTarget(StatsTarget &target)
{
    pTarget = &target;
}

void StatsTimer::addSource(SharedStatsSource pSource)
{
    sources.push_back(pSource);
}

void StatsTimer::start()
{
    timerThread.start();
}

void StatsTimer::stop()
{
    timerThread.stop();

    // clear target counters
    if (pTarget) {
        pTarget->beginSnapshot();
        pTarget->endSnapshot();
    }

    sources.clear();
}

uint StatsTimer::getTimerIntervalMillis()
{
    return intervalInMillis;
}

void StatsTimer::onThreadStart()
{
    if (pTarget) {
        pTarget->onThreadStart();
    }
}

void StatsTimer::onThreadEnd()
{
    if (pTarget) {
        pTarget->onThreadEnd();
    }
}

void StatsTimer::onTimerInterval()
{
    if (!pTarget) {
        return;
    }
    pTarget->beginSnapshot();
    for (uint i = 0; i < sources.size(); ++i) {
        sources[i]->writeStats(*pTarget);
    }
    pTarget->endSnapshot();
}

void StatsTarget::onThreadStart()
{
    // by default do nothing
}

void StatsTarget::onThreadEnd()
{
    // by default do nothing
}

FENNEL_END_CPPFILE("$Id$");

// End StatsTimer.cpp
