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

#ifndef Fennel_TimerThread_Included
#define Fennel_TimerThread_Included

#include "fennel/synch/Thread.h"
#include "fennel/synch/SynchMonitoredObject.h"

FENNEL_BEGIN_NAMESPACE

/**
 * TimerThreadClient receives callbacks from a TimerThread.
 */
class TimerThreadClient 
{
public:
    /**
     * Called from TimerThread to obtain the interval which should elapse
     * before the next call to onTimerInterval.  This can be different each
     * time.  A return value of 0 will cause the TimerThread to cease calling
     * back.
     */
    virtual uint getTimerIntervalMillis() = 0;

    /**
     * Called from TimerThread after interval has elapsed.
     */
    virtual void onTimerInterval() = 0;
};

/**
 * TimerThread implements a timer callback via a dedicated thread.  Once
 * started, the thread runs until stop() is called.
 */
class TimerThread : public Thread, private SynchMonitoredObject
{
    TimerThreadClient &client;
    bool bStop;
    
    virtual void run();

public:
    explicit TimerThread(
        TimerThreadClient &clientInit);
    
    /**
     * Stop (and join) the timer thread.
     */
    void stop();

    /**
     * Request an immediate execution of onTimerInterval() in the timer thread
     * context.  Afterwards, timed execution resumes as usual.
     */
    void signalImmediate();
};

FENNEL_END_NAMESPACE

#endif

// End TimerThread.h
