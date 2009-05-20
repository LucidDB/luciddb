/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005-2009 The Eigenbase Project
// Copyright (C) 2005-2009 SQLstream, Inc.
// Copyright (C) 2005-2009 LucidEra, Inc.
// Portions Copyright (C) 1999-2009 John V. Sichi
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

#ifndef Fennel_TimerThread_Included
#define Fennel_TimerThread_Included

#include "fennel/synch/Thread.h"
#include "fennel/synch/SynchMonitoredObject.h"
#include "fennel/synch/ThreadTracker.h"

FENNEL_BEGIN_NAMESPACE

/**
 * TimerThreadClient receives callbacks from a TimerThread.
 */
class FENNEL_SYNCH_EXPORT TimerThreadClient : public ThreadTracker
{
public:
    virtual ~TimerThreadClient();

    /**
     * Calculates the interval which should elapse before the next call to
     * onTimerInterval.  This can be different each time.  A return value of 0
     * will cause the TimerThread to cease calling back.
     */
    virtual uint getTimerIntervalMillis() = 0;

    /**
     * Receives notification from TimerThread that interval has elapsed.
     */
    virtual void onTimerInterval() = 0;
};

/**
 * TimerThread implements a timer callback via a dedicated thread.  Once
 * started, the thread runs until stop() is called.
 */
class FENNEL_SYNCH_EXPORT TimerThread
    : public Thread, private SynchMonitoredObject
{
    TimerThreadClient &client;
    bool bStop;

    virtual void run();

public:
    explicit TimerThread(
        TimerThreadClient &clientInit);

    /**
     * Stops (and joins) the timer thread.
     */
    void stop();

    /**
     * Requests an immediate execution of onTimerInterval() in the timer thread
     * context.  Afterwards, timed execution resumes as usual.
     */
    void signalImmediate();
};

FENNEL_END_NAMESPACE

#endif

// End TimerThread.h
