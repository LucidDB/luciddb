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

#ifndef Fennel_IoCompletionPortScheduler_Included
#define Fennel_IoCompletionPortScheduler_Included

#ifdef __MINGW32__

#include <vector>
#include "fennel/device/DeviceAccessScheduler.h"
#include "fennel/device/RandomAccessRequest.h"

#include <windows.h>

FENNEL_BEGIN_NAMESPACE

class IoCompletionPortThread;

/**
 * IoCompletionPortScheduler implements DeviceAccessScheduler via
 * the Win32 IoCompletionPort facility.
 */
class IoCompletionPortScheduler : public DeviceAccessScheduler
{
    friend class IoCompletionPortThread;
    
    HANDLE hCompletionPort;
    std::vector<IoCompletionPortThread *> threads;
    bool quit;
    
    bool isStarted() const
    {
        return !threads.empty();
    }

public:
    /**
     * Constructor.
     */
    explicit IoCompletionPortScheduler(DeviceAccessSchedulerParams const &);
    
    /**
     * Destructor:  stop must already have been called.
     */
    virtual ~IoCompletionPortScheduler();

// ----------------------------------------------------------------------
// Implementation of DeviceAccessScheduler interface (q.v.)
// ----------------------------------------------------------------------
    virtual void registerDevice(SharedRandomAccessDevice pDevice);
    virtual void schedule(RandomAccessRequest &request);
    virtual void stop();
};

FENNEL_END_NAMESPACE

#endif

#endif

// End IoCompletionPortScheduler.h
