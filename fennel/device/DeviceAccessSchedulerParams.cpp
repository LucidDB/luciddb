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
#include "fennel/device/DeviceAccessSchedulerParams.h"
#include "fennel/common/ConfigMap.h"

FENNEL_BEGIN_CPPFILE("$Id$");

ParamName DeviceAccessSchedulerParams::paramSchedulerType = "schedType";
ParamName DeviceAccessSchedulerParams::paramThreadCount = "schedThreadCount";
ParamName DeviceAccessSchedulerParams::paramMaxRequests = "schedMaxRequests";

ParamVal DeviceAccessSchedulerParams::valThreadPoolScheduler = "threadPool";
ParamVal DeviceAccessSchedulerParams::valIoCompletionPortScheduler =
"ioCompletionPort";
ParamVal DeviceAccessSchedulerParams::valAioPollingScheduler = "aioPolling";
ParamVal DeviceAccessSchedulerParams::valAioSignalScheduler = "aioSignal";

DeviceAccessSchedulerParams::DeviceAccessSchedulerParams()
{
#ifdef __MINGW32__
    schedulerType = IO_COMPLETION_PORT_SCHEDULER;
#else
    schedulerType = THREAD_POOL_SCHEDULER;
#endif
    nThreads = 3;
    maxRequests = 10;
}

void DeviceAccessSchedulerParams::readConfig(ConfigMap const &configMap)
{
    std::string s = configMap.getStringParam(paramSchedulerType);
    if (s == valThreadPoolScheduler) {
        schedulerType = THREAD_POOL_SCHEDULER;
    } else if (s == valAioPollingScheduler) {
        schedulerType = AIO_POLLING_SCHEDULER;
    } else if (s == valAioSignalScheduler) {
        schedulerType = AIO_SIGNAL_SCHEDULER;
    } else if (s == valIoCompletionPortScheduler) {
        schedulerType = IO_COMPLETION_PORT_SCHEDULER;
    } else {
        assert(s == "");
    }
    nThreads = configMap.getIntParam(
        paramThreadCount,nThreads);
    maxRequests = configMap.getIntParam(
        paramMaxRequests,maxRequests);
}

FENNEL_END_CPPFILE("$Id$");

// End DeviceAccessSchedulerParams.cpp
