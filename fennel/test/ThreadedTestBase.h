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

#ifndef Fennel_ThreadedTestBase_Included
#define Fennel_ThreadedTestBase_Included

#include "fennel/test/TestBase.h"
#include "fennel/synch/Barrier.h"

#include <vector>

FENNEL_BEGIN_NAMESPACE

/**
 * ThreadedTestBase is a common base for tests which execute multiple threads
 * with various operations over a configurable duration.
 */
class ThreadedTestBase : virtual public TestBase
{
    friend class ThreadedTestBaseTask;
private:
    /**
     * Barrier used to synchronize start of multi-threaded test.
     */
    Barrier startBarrier;

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
     * Execute specified test threads.
     */
    void runThreadedTestCase();
};

class ThreadedTestBaseTask 
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
