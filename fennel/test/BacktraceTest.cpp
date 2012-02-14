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
#include "fennel/common/Backtrace.h"
#include "fennel/test/TestBase.h"
#include "fennel/common/TraceSource.h"

#include <boost/test/test_tools.hpp>
#include <boost/scoped_array.hpp>

using namespace fennel;

class BacktraceTest : virtual public TestBase, public TraceSource
{
    void testDeliberateBacktrace();
    void testBacktraceOnAbort();
public:
    explicit BacktraceTest()
        : TraceSource(shared_from_this(), "BacktraceTest")
    {
        FENNEL_UNIT_TEST_CASE(BacktraceTest, testDeliberateBacktrace);
        // FENNEL_UNIT_TEST_CASE(BacktraceTest, testBacktraceOnAbort);
    }

    virtual ~BacktraceTest()
    {
    }
};

void BacktraceTest::testDeliberateBacktrace()
{
    Backtrace bt;
    FENNEL_TRACE(TRACE_INFO, "this is a backtrace" << std::endl << bt);
}

void BacktraceTest::testBacktraceOnAbort()
{
    FENNEL_TRACE(TRACE_INFO, "asserting to force a backtrace");
    assert(false);
}

FENNEL_UNIT_TEST_SUITE(BacktraceTest);

// End BacktraceTest.cpp
