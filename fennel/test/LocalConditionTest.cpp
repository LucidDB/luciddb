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
#include "fennel/test/TestBase.h"

#include <boost/test/test_tools.hpp>

using namespace fennel;

class LocalConditionTest : virtual public TestBase
{
public:
    StrictMutex mutex;
    LocalCondition cond;
    bool bFlag;

    explicit LocalConditionTest()
    {
        bFlag = 0;
        FENNEL_UNIT_TEST_CASE(LocalConditionTest, testNotifyAll);
    }

    virtual ~LocalConditionTest()
    {
    }

    void testNotifyAll();
};

class TestThread : public Thread
{
    LocalConditionTest &test;

public:

    TestThread(LocalConditionTest &testInit)
        : test(testInit)
    {
    }

    virtual void run()
    {
        StrictMutexGuard mutexGuard(test.mutex);
        test.bFlag = 1;
        BOOST_MESSAGE("broadcast");
        test.cond.notify_all();
    }
};

void LocalConditionTest::testNotifyAll()
{
    StrictMutexGuard mutexGuard(mutex);
    TestThread thread(*this);
    BOOST_MESSAGE("starting");
    thread.start();
    while (!bFlag) {
        BOOST_MESSAGE("waiting");
        cond.wait(mutexGuard);
    }
    BOOST_MESSAGE("joining");
    thread.join();
    BOOST_MESSAGE("joined");
}

FENNEL_UNIT_TEST_SUITE(LocalConditionTest);

// End LocalConditionTest.cpp

