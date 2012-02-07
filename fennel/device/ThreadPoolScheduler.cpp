/*
// Licensed to DynamoBI Corporation (DynamoBI) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  DynamoBI licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at

//   http://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
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
