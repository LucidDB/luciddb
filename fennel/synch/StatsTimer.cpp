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

FENNEL_END_CPPFILE("$Id$");

// End StatsTimer.cpp
