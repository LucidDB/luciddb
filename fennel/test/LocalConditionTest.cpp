/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2005-2005 Disruptive Tech
// Copyright (C) 2005-2005 Red Square, Inc.
// Portions Copyright (C) 1999-2005 John V. Sichi
//
// This program is free software; you can redistribute it and/or
// modify it under the terms of the GNU General Public License
// as published by the Free Software Foundation; either version 2
// of the License, or (at your option) any later Eigenbase-approved version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program; if not, write to the Free Software
// Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307  USA
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
        FENNEL_UNIT_TEST_CASE(LocalConditionTest,testNotifyAll);
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

