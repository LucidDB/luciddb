/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2005-2005 Disruptive Tech
// Copyright (C) 2005-2005 Red Square, Inc.
// Portions Copyright (C) 1999-2005 John V. Sichi
//
// This program is free software; you can redistribute it and/or
// modify it under the terms of the GNU General Public License
// as published by the Free Software Foundation; either version 2
// of the License, or (at your option) any later Eigenbase-approved version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program; if not, write to the Free Software
// Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307  USA
*/

#include "fennel/common/CommonPreamble.h"
#include "fennel/device/DeviceAccessScheduler.h"
#include "fennel/device/DeviceAccessSchedulerParams.h"
#include "fennel/device/ThreadPoolScheduler.h"
#include "fennel/device/RandomAccessDevice.h"
#include "fennel/device/RandomAccessRequest.h"

#ifdef __MINGW32__
#include "fennel/device/IoCompletionPortScheduler.h"
#include "fennel/common/SysCallExcn.h"
#include <windows.h>
#endif

#ifdef HAVE_AIO_H
#include "fennel/device/AioPollingScheduler.h"
#include "fennel/device/AioSignalScheduler.h"
#endif

FENNEL_BEGIN_CPPFILE("$Id$");

DeviceAccessScheduler *
DeviceAccessScheduler::newScheduler(
    DeviceAccessSchedulerParams const &params)
{
    switch(params.schedulerType) {
        
    case DeviceAccessSchedulerParams::THREAD_POOL_SCHEDULER:
        return new ThreadPoolScheduler(params);
        
#ifdef __MINGW32__
    case DeviceAccessSchedulerParams::IO_COMPLETION_PORT_SCHEDULER:
        return new IoCompletionPortScheduler(params);
#endif
        
#ifdef HAVE_AIO_H
    case DeviceAccessSchedulerParams::AIO_POLLING_SCHEDULER:
        return new AioPollingScheduler(params);
    case DeviceAccessSchedulerParams::AIO_SIGNAL_SCHEDULER:
        return new AioSignalScheduler(params);
#endif
        
    default:
        permAssert(false);
    }
}

DeviceAccessScheduler::~DeviceAccessScheduler()
{
}

RandomAccessRequestBinding::RandomAccessRequestBinding()
{
#ifdef __MINGW32__
    // TODO:  only create this when ThreadPoolScheduler is being used?
    hEvent = CreateEvent(NULL,1,0,NULL);
    if (!hEvent) {
        throw new SysCallExcn("CreateEvent failed");
    }
#endif    
}

RandomAccessRequestBinding::~RandomAccessRequestBinding()
{
#ifdef __MINGW32__
    CloseHandle(hEvent);
#endif    
}

void RandomAccessRequest::execute()
{
    pDevice->transfer(*this);
}

void DeviceAccessScheduler::registerDevice(
    SharedRandomAccessDevice)
{
}

void DeviceAccessScheduler::unregisterDevice(
    SharedRandomAccessDevice)
{
}

FENNEL_END_CPPFILE("$Id$");

// End DeviceAccessScheduler.cpp
