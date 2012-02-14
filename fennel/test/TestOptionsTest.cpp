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
#include "fennel/common/ConfigMap.h"
#include "fennel/test/TestBase.h"
#include "fennel/common/TraceSource.h"
#include <boost/test/test_tools.hpp>
#include <iostream>

using namespace fennel;
using std::string;

/** tests the command-line options features of TestBase */

class TestOptionsTest : public TestBase, public TraceSource
{
    // 2 dummy default tests
    void test1();
    void test2();

    // 1 dummy extra test
    void extra();

public:
    explicit TestOptionsTest()
        : TraceSource(shared_from_this(), "TestOptionsTest")
    {
        FENNEL_UNIT_TEST_CASE(TestOptionsTest, test1);
        FENNEL_UNIT_TEST_CASE(TestOptionsTest, test2);
        FENNEL_EXTRA_UNIT_TEST_CASE(TestOptionsTest, extra);
    }
};

void TestOptionsTest::test1()
{
    int n = configMap.getIntParam("n", 100);
    string s = configMap.getStringParam("s", "fennel");
    FENNEL_TRACE(TRACE_INFO, "test1(): n = " << n << "; s = " << s);
}

void TestOptionsTest::test2()
{
    int m = configMap.getIntParam("m", 200);
    int n = configMap.getIntParam("n", 201);
    string s = configMap.getStringParam("s", "fennel");
    FENNEL_TRACE(
        TRACE_INFO,
        "test2(): m = " << m << "; n = " << n << "; s = " << s);
}

void TestOptionsTest::extra()
{
    int m = configMap.getIntParam("m", 300);
    int n = configMap.getIntParam("n", 301);
    string s = configMap.getStringParam("s", "fennel");
    FENNEL_TRACE(
        TRACE_INFO,
        "extra(): m = " << m << "; n = " << n << "; s = " << s);
}

FENNEL_UNIT_TEST_SUITE(TestOptionsTest)
// End TestOptionsTest.cpp

