/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2008-2008 The Eigenbase Project
// Copyright (C) 2008-2008 Disruptive Tech
// Copyright (C) 2008-2008 LucidEra, Inc.
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
#include "fennel/exec/ParallelExecStreamScheduler.h"
#include "fennel/test/ExecStreamTestSuite.h"
#include "fennel/synch/ThreadTracker.h"

/**
 * ParallelExecStreamSchedulerTest repeats the tests from ExecStreamTestSuite,
 * but using a parallel scheduler.
 */
class ParallelExecStreamSchedulerTest : public ExecStreamTestSuite
{
    ThreadTracker threadTracker;
    
    // override ExecStreamTestBase
    virtual ExecStreamScheduler *newScheduler()
    {
        // hard-code two threads (TODO jvs 22-Jul-2008:  test parameter)
        return new ParallelExecStreamScheduler(
            shared_from_this(),
            "ParallelExecStreamScheduler",
            threadTracker,
            2);
    }
};

using namespace fennel;

// instantiate the ExecStreamTestSuite as a stand-alone program
FENNEL_UNIT_TEST_SUITE(ParallelExecStreamSchedulerTest);

// End ParallelExecStreamSchedulerTest.cpp
