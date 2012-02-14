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
#include "fennel/test/TestBase.h"
#include "fennel/common/FennelResource.h"
#include "fennel/synch/Thread.h"

#include <boost/test/test_tools.hpp>
#include <boost/thread/barrier.hpp>

#include <vector>

using namespace fennel;

class ResourceTest : virtual public TestBase
{
public:

    explicit ResourceTest()
    {
        FENNEL_UNIT_TEST_CASE(ResourceTest, testEnUsLocale);
        FENNEL_UNIT_TEST_CASE(ResourceTest, testConcurrency);
    }

    void testEnUsLocale();
    void testConcurrency();
};

void ResourceTest::testEnUsLocale()
{
    Locale locale("en", "US");
    std::string actual =
        FennelResource::instance(locale).sysCallFailed("swizzle");
    std::string expected = "System call failed:  swizzle";
    BOOST_CHECK_EQUAL(expected, actual);
}

class ResourceThread : public Thread
{
private:
    boost::barrier &barrier;

    int count;
    int completed;

    std::vector<std::string> variants;

public:
    explicit ResourceThread(
        std::string desc, boost::barrier &barrier, int count)
        : Thread(desc), barrier(barrier), count(count), completed(0)
    {
        for (int i = 0; i < count; i++) {
            std::stringstream ss;
            ss << "var_" << (i + 1);
            variants.push_back(ss.str());
        }
    }

    virtual ~ResourceThread()
    {
    }

    int getCompleted()
    {
        return completed;
    }

    virtual void run()
    {
        try {
            barrier.wait();

            for (int i = 0; i < count; i++) {
                std::string &variant = variants[i];

                Locale locale("en", "US", variant);

                FennelResource::instance(locale).sysCallFailed(variant);

                completed++;
            }
        } catch (...) {
            completed = -1;
        }
    }
};

#define CONC_ITER (1000)

// Test thread sync bug fix (change 3930).  Note that the bug only seems
// to occur if two threads are attempting to create a ResourceBundle for
// the same Locale at the same time.
void ResourceTest::testConcurrency()
{
    boost::barrier barrier(2);

    ResourceThread thread1("resThread1", barrier, CONC_ITER);
    ResourceThread thread2("resThread2", barrier, CONC_ITER);

    thread1.start();
    thread2.start();

    thread1.join();
    thread2.join();

    BOOST_CHECK_EQUAL(thread1.getCompleted(), CONC_ITER);
    BOOST_CHECK_EQUAL(thread2.getCompleted(), CONC_ITER);
}

FENNEL_UNIT_TEST_SUITE(ResourceTest);

// End ResourceTest.cpp

