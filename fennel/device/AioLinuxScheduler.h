/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2005-2005 Disruptive Tech
// Copyright (C) 2005-2005 LucidEra, Inc.
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

#ifndef Fennel_AioLinuxScheduler_Included
#define Fennel_AioLinuxScheduler_Included

#ifdef USE_LIBAIO_H

#include "fennel/device/DeviceAccessScheduler.h"
#include "fennel/device/RandomAccessRequest.h"
#include "fennel/common/AtomicCounter.h"
#include "fennel/synch/Thread.h"

#include <vector>
#include <boost/scoped_array.hpp>

FENNEL_BEGIN_NAMESPACE
    
/**
 * AioLinuxScheduler implements DeviceAccessScheduler via Linux-specific
 * kernel-mode libaio calls.
 *
 * @author John V. Sichi
 * @version $Id$
 */
class AioLinuxScheduler : public DeviceAccessScheduler, public Thread
{
    io_context_t context;
    uint nRequestsMax;
    AtomicCounter nRequestsPending;
    bool quit;

    inline bool isStarted() const;

public:
    /**
     * Constructor.
     */
    explicit AioLinuxScheduler(DeviceAccessSchedulerParams const &);

    /**
     * Destructor:  stop must already have been called.
     */
    virtual ~AioLinuxScheduler();

// ----------------------------------------------------------------------
// Implementation of DeviceAccessScheduler interface (q.v.)
// ----------------------------------------------------------------------
    virtual void registerDevice(SharedRandomAccessDevice pDevice);
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

// End AioLinuxScheduler.h
