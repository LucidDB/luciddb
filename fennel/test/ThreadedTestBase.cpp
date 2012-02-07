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
#include "fennel/test/ThreadedTestBase.h"
#include "fennel/synch/ThreadPool.h"

#include <boost/test/test_tools.hpp>
#include <numeric>

using namespace fennel;

ThreadedTestBase::ThreadedTestBase()
{
    nSeconds = configMap.getIntParam("testDuration", 10);
    defaultThreadCount = configMap.getIntParam("defaultThreads", 1);
}

ThreadedTestBase::~ThreadedTestBase()
{
}

void ThreadedTestBase::runThreadedTestCase()
{
    bDone = false;

    // materialize default thread counts
    std::replace_if(
        threadCounts.begin(),
        threadCounts.end(),
        std::bind2nd(std::equal_to<int>(),-1),
        defaultThreadCount);

    // calculate how many threads are needed
    int nThreads = std::accumulate(
        threadCounts.begin(),
        threadCounts.end(),
        0);

    // initialize a barrier to make sure they all start at once
    pStartBarrier.reset(new boost::barrier(nThreads));

    // fire 'em up
    ThreadPool<ThreadedTestBaseTask> threadPool;
    threadPool.start(nThreads);

    // and distribute the tasks
    for (uint i = 0; i < threadCounts.size(); ++i) {
        for (int j = 0; j < threadCounts[i]; ++j) {
            ThreadedTestBaseTask task(*this,i);
            threadPool.submitTask(task);
        }
    }

    // run the tests for the requested duration
    snooze(nSeconds);

    // tell threads to quit and then wait for them to finish up
    bDone = true;
    threadPool.stop();
}

void ThreadedTestBase::threadInit()
{
}

void ThreadedTestBase::threadTerminate()
{
}

ThreadedTestBaseTask::ThreadedTestBaseTask(
    ThreadedTestBase &testInit,
    int iOpInit)
    : test(testInit), iOp(iOpInit)
{
}

void ThreadedTestBaseTask::execute()
{
    test.threadInit();
    test.pStartBarrier->wait();
    try {
        while (!test.bDone) {
            if (!test.testThreadedOp(iOp)) {
                break;
            }
        }
    } catch (...) {
        test.threadTerminate();
        throw;
    }
    test.threadTerminate();
}

// End ThreadedTestBase.cpp
