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
#include "fennel/synch/Thread.h"
#include <boost/thread/thread.hpp>
#include <boost/bind.hpp>

FENNEL_BEGIN_CPPFILE("$Id$");

// NOTE: it's important to realize that pBoostThread and bRunning are not
// redundant.  The reason is that pBoostThread is updated in the context of the
// thread calling start(), while bRunning is updated in the
// context of the spawned thread.

Thread::Thread(std::string const & desc)
{
    // TODO:  do something with description
    name = desc;
    pBoostThread = NULL;
    bRunning = false;
}

Thread::~Thread()
{
    assert(!bRunning);
    assert(!pBoostThread);
}

void Thread::start()
{
    pBoostThread = new boost::thread(
        boost::bind(&Thread::initAndRun,this));
}

void Thread::join()
{
    assert(pBoostThread);
    boost::thread t;
    assert(*pBoostThread != t);
    pBoostThread->join();
    delete pBoostThread;
    pBoostThread = NULL;
}

void Thread::initAndRun()
{
    beforeRun();
    run();
    afterRun();
}

void Thread::beforeRun()
{
    bRunning = true;
}

void Thread::afterRun()
{
    bRunning = false;
}

FENNEL_END_CPPFILE("$Id$");

// End Thread.cpp
