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

#ifndef Fennel_ThreadedTestBase_Included
#define Fennel_ThreadedTestBase_Included

#include "fennel/test/TestBase.h"

#include <vector>

#include <boost/thread/barrier.hpp>
#include <boost/scoped_ptr.hpp>

FENNEL_BEGIN_NAMESPACE

/**
 * ThreadedTestBase is a common base for tests which execute multiple threads
 * with various operations over a configurable duration.
 */
class FENNEL_TEST_EXPORT ThreadedTestBase
    : virtual public TestBase
{
    friend class ThreadedTestBaseTask;
private:
    /**
     * Barrier used to synchronize start of multi-threaded test.
     */
    boost::scoped_ptr<boost::barrier> pStartBarrier;

    /**
     * Flag indicating that threads should quit because time is up.
     */
    bool bDone;

    /**
     * Default number of threads to run for a particular operation when
     * unspecified.
     */
    bool defaultThreadCount;

protected:
    /**
     * Duration of multi-threaded test.
     */
    uint nSeconds;

    /**
     * Number of threads to run for each type of operation.
     */
    std::vector<int> threadCounts;

    explicit ThreadedTestBase();

    virtual ~ThreadedTestBase();

    virtual void threadInit();

    virtual void threadTerminate();

    /**
     * Test implementation must be supplied by derived test class.
     *
     * @param iOp operation type to test
     *
     * @return true if test should run again
     */
    virtual bool testThreadedOp(int iOp) = 0;

    /**
     * Executes specified test threads.
     */
    void runThreadedTestCase();
};

class FENNEL_TEST_EXPORT ThreadedTestBaseTask
{
    ThreadedTestBase &test;
    int iOp;

public:
    explicit ThreadedTestBaseTask(
        ThreadedTestBase &testCaseInit,
        int iOpInit);

    void execute();
};

FENNEL_END_NAMESPACE

#endif

// End ThreadedTestBase.h
