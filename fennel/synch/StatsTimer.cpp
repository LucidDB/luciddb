/*
// $Id$
// Fennel is a relational database kernel.
// Copyright (C) 1999-2004 John V. Sichi.
//
// This program is free software; you can redistribute it and/or
// modify it under the terms of the GNU Lesser General Public License
// as published by the Free Software Foundation; either version 2.1
// of the License, or (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with this program; if not, write to the Free Software
// Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
*/

#include "fennel/common/CommonPreamble.h"
#include "fennel/synch/StatsTimer.h"
#include "fennel/common/StatsSource.h"
#include "fennel/common/StatsTarget.h"

FENNEL_BEGIN_CPPFILE("$Id$");

StatsTimer::StatsTimer(
    StatsTarget &targetInit,
    uint intervalInMillisInit)
    : target(targetInit), timerThread(*this)
{
    intervalInMillis = intervalInMillisInit;
}

StatsTimer::~StatsTimer()
{
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
    target.beginSnapshot();
    target.endSnapshot();

    sources.clear();
}

uint StatsTimer::getTimerIntervalMillis()
{
    return intervalInMillis;
}

void StatsTimer::onTimerInterval()
{
    target.beginSnapshot();
    for (uint i = 0; i < sources.size(); ++i) {
        sources[i]->writeStats(target);
    }
    target.endSnapshot();
}

FENNEL_END_CPPFILE("$Id$");

// End StatsTimer.cpp
