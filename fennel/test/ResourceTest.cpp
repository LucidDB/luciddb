/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005-2007 The Eigenbase Project
// Copyright (C) 2005-2007 SQLstream, Inc.
// Copyright (C) 2005-2007 LucidEra, Inc.
// Portions Copyright (C) 1999-2007 John V. Sichi
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
    Locale locale("en","US");
    std::string actual =
        FennelResource::instance(locale).sysCallFailed("swizzle");
    std::string expected = "System call failed:  swizzle";
    BOOST_CHECK_EQUAL(expected,actual);
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

