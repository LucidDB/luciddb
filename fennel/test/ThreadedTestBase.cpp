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
#include "fennel/test/ThreadedTestBase.h"
#include "fennel/synch/ThreadPool.h"

#include <boost/test/test_tools.hpp>
#include <numeric>

using namespace fennel;

ThreadedTestBase::ThreadedTestBase()
{
    nSeconds = configMap.getIntParam("testDuration",10);
    defaultThreadCount = configMap.getIntParam("defaultThreads",1);
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
