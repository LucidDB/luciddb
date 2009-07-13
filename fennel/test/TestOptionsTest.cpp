/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005-2009 The Eigenbase Project
// Copyright (C) 2005-2009 SQLstream, Inc.
// Copyright (C) 2005-2009 LucidEra, Inc.
// Portions Copyright (C) 1999-2009 John V. Sichi
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

