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
#include "fennel/synch/Thread.h"
#include "fennel/test/TestBase.h"

#include <boost/test/test_tools.hpp>

using namespace fennel;

class TestLocalCondition : virtual public TestBase
{
public:
    StrictMutex mutex;
    LocalCondition cond;
    bool bFlag;
    
    explicit TestLocalCondition()
    {
        bFlag = 0;
        FENNEL_UNIT_TEST_CASE(TestLocalCondition,testNotifyAll);
    }
    
    virtual ~TestLocalCondition()
    {
    }
    
    void testNotifyAll();
};

class TestThread : public Thread
{
    TestLocalCondition &test;

public:
    
    TestThread(TestLocalCondition &testInit)
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

void TestLocalCondition::testNotifyAll()
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

FENNEL_UNIT_TEST_SUITE(TestLocalCondition);

// End TestLocalCondition.cpp

