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
#include "fennel/synch/TimerThread.h"
 
FENNEL_BEGIN_CPPFILE("$Id$");

TimerThread::TimerThread(
    TimerThreadClient &clientInit)
    : Thread("TimerThread"),
      client(clientInit)
{
    bStop = false;
}
    
void TimerThread::run()
{
    for (;;) {
        uint millis = client.getTimerIntervalMillis();
        if (!millis) {
            break;
        }
        boost::xtime atv;
        convertTimeout(millis,atv);
        StrictMutexGuard mutexGuard(mutex);
        while (!bStop) {
            if (!condition.timed_wait(mutexGuard,atv)) {
                break;
            }
        }
        if (bStop) {
            break;
        }
        client.onTimerInterval();
    }
}

void TimerThread::stop()
{
    StrictMutexGuard mutexGuard(mutex);
    if (bStop || !isStarted()) {
        return;
    }
    bStop = true;
    condition.notify_all();
    mutexGuard.unlock();
    join();
    mutexGuard.lock();
    bStop = false;
}

void TimerThread::signalImmediate()
{
    StrictMutexGuard mutexGuard(mutex);
    condition.notify_all();
}

FENNEL_END_CPPFILE("$Id$");

// End TimerThread.cpp
