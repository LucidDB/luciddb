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
