/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005 The Eigenbase Project
// Copyright (C) 2005 SQLstream, Inc.
// Copyright (C) 2005 Dynamo BI Corporation
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
#include "fennel/device/ThreadPoolScheduler.h"
#include "fennel/synch/Thread.h"
#include "fennel/synch/ThreadPool.h"
#include "fennel/device/RandomAccessRequest.h"
#include "fennel/device/RandomAccessDevice.h"
#include "fennel/device/DeviceAccessSchedulerParams.h"

FENNEL_BEGIN_CPPFILE("$Id$");

ThreadPoolScheduler::ThreadPoolScheduler(
    DeviceAccessSchedulerParams const &params)
{
    // threads and requests are 1-to-1, but threads are expensive,
    // so arbitrarily cap at 10
    uint nThreads = std::min<uint>(10, params.maxRequests);
    pool.start(nThreads);
}

ThreadPoolScheduler::~ThreadPoolScheduler()
{
}

bool ThreadPoolScheduler::schedule(RandomAccessRequest &request)
{
    RandomAccessRequest::BindingListMutator bindingMutator(request.bindingList);
    FileSize cbOffset = request.cbOffset;
    // break up the request into one per binding
    // TODO:  don't do this if device supports scatter/gather; and skip
    // breakup if only one binding in the first place
    while (bindingMutator) {
        RandomAccessRequestBinding *pBinding = bindingMutator.detach();
        if (!pBinding) {
            break;
        }
        RandomAccessRequest subRequest;
        subRequest.pDevice = request.pDevice;
        subRequest.cbOffset = cbOffset;
        subRequest.cbTransfer = pBinding->getBufferSize();
        cbOffset += subRequest.cbTransfer;
        subRequest.type = request.type;
        subRequest.bindingList.push_back(*pBinding);
        pool.submitTask(subRequest);
    }
    assert(cbOffset == request.cbOffset + request.cbTransfer);
    return true;
}

void ThreadPoolScheduler::stop()
{
    pool.stop();
}

FENNEL_END_CPPFILE("$Id$");

// End ThreadPoolScheduler.cpp
