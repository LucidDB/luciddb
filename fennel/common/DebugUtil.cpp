/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2011 The Eigenbase Project
// Copyright (C) 2011 SQLstream, Inc.
// Copyright (C) 2011 Dynamo BI Corporation
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

#include "fennel/common/CommonPreamble.h"
#include "fennel/common/DebugUtil.h"

#ifdef __MSVC__
#include <process.h>
#endif

#ifndef __MSVC__
#include <signal.h>
#endif

FENNEL_BEGIN_CPPFILE("$Id$");

#ifndef __MSVC__
static void debugger_signalHandler(int signum)
{
    // do nothing
}
#endif

void DebugUtil::waitForDebugger(bool hup)
{
    fprintf(stderr, "Waiting for debugger; pid=%d\n", getpid());
#ifdef __MSVC__
    // A "cont" in gdb will wake this sleep up immediately, which
    // is disturbing but useful.
    _sleep(600000);
#else
    // On older versions of Linux, a "cont" in gdb will wake this
    // sleep up immediately, which is disturbing but useful.
    // On newer versions, the continue command resumes
    // the sleep().  So, if HUP, wait for SIGHUP.
    // Use the "signal 1" command to wake the pause up.
    if (!hup) {
        sleep(60000);
    } else {
        struct sigaction act;
        struct sigaction oldact;

        act.sa_handler = debugger_signalHandler;
        sigemptyset(&act.sa_mask);
        act.sa_flags = 0;

        if (!sigaction(SIGHUP, &act, &oldact)) {
            // Signal handler installed properly.  Wait for signal.
            pause();

            // Restore the old signal handler.
            sigaction(SIGHUP, &oldact, NULL);
        } else {
            // Fall back on sleeping.
            sleep(60000);
        }
    }
#endif
}

FENNEL_END_CPPFILE("$Id$");
// End DebugUtil.cpp
