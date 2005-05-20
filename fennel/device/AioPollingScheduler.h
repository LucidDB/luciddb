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

#ifndef Fennel_AioPollingScheduler_Included
#define Fennel_AioPollingScheduler_Included

#ifdef HAVE_AIO_H

#include "fennel/synch/Thread.h"
#include "fennel/device/DeviceAccessScheduler.h"
#include "fennel/device/RandomAccessRequest.h"
#include <vector>

#include <aio.h>
struct aiocb;

FENNEL_BEGIN_NAMESPACE

/**
 * AioPollingScheduler implements DeviceAccessScheduler via Unix aio calls and
 * threads which poll for completion.
 */
class AioPollingScheduler : public DeviceAccessScheduler, public Thread
{
    StrictMutex mutex;
    LocalCondition newRequestPending;
    bool quit;
    
    std::vector<aiocb *> currentRequests;
    std::vector<aiocb *> newRequests;
    
public:
    /**
     * Constructor.
     */
    explicit AioPollingScheduler(
        DeviceAccessSchedulerParams const &);
    
    /**
     * Destructor:  stop must already have been called.
     */
    virtual ~AioPollingScheduler();

// ----------------------------------------------------------------------
// Implementation of DeviceAccessScheduler interface (q.v.)
// ----------------------------------------------------------------------
    virtual void schedule(RandomAccessRequest &request);
    virtual void stop();
    
// ----------------------------------------------------------------------
// Implementation of Thread interface (q.v.)
// ----------------------------------------------------------------------
    virtual void run();
};

FENNEL_END_NAMESPACE

#endif

#endif

// End AioPollingScheduler.h
