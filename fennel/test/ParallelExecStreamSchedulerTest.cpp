/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2008-2009 The Eigenbase Project
// Copyright (C) 2008-2009 SQLstream, Inc.
// Copyright (C) 2008-2009 LucidEra, Inc.
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

    uint degreeOfParallelism;

    // override ExecStreamTestBase
    virtual ExecStreamScheduler *newScheduler()
    {
        return new ParallelExecStreamScheduler(
            shared_from_this(),
            "ParallelExecStreamScheduler",
            threadTracker,
            degreeOfParallelism);
    }

    uint getDegreeOfParallelism()
    {
        return degreeOfParallelism;
    }

public:

    // we have to be selective about inheritance of test cases
    // since some of them are not parallel-safe yet
    explicit ParallelExecStreamSchedulerTest()
        : ExecStreamTestSuite(false)
    {
        degreeOfParallelism =
            configMap.getIntParam(paramDegreeOfParallelism, 4);

        FENNEL_UNIT_TEST_CASE(ExecStreamTestSuite,testScratchBufferExecStream);
        FENNEL_UNIT_TEST_CASE(ExecStreamTestSuite,testDoubleBufferExecStream);
        FENNEL_UNIT_TEST_CASE(ExecStreamTestSuite,testCopyExecStream);
        FENNEL_UNIT_TEST_CASE(ExecStreamTestSuite,testMergeExecStream);
        FENNEL_UNIT_TEST_CASE(ExecStreamTestSuite,testSegBufferExecStream);
        FENNEL_UNIT_TEST_CASE(ExecStreamTestSuite,testCartesianJoinExecStreamOuter);
        FENNEL_UNIT_TEST_CASE(ExecStreamTestSuite,testCartesianJoinExecStreamInner);
        FENNEL_UNIT_TEST_CASE(ExecStreamTestSuite,testCountAggExecStream);
        FENNEL_UNIT_TEST_CASE(ExecStreamTestSuite,testSumAggExecStream);
        FENNEL_UNIT_TEST_CASE(ExecStreamTestSuite,testGroupAggExecStream1);
        FENNEL_UNIT_TEST_CASE(ExecStreamTestSuite,testGroupAggExecStream2);
        FENNEL_UNIT_TEST_CASE(ExecStreamTestSuite,testGroupAggExecStream3);
        FENNEL_UNIT_TEST_CASE(ExecStreamTestSuite,testGroupAggExecStream4);
        FENNEL_UNIT_TEST_CASE(
            ExecStreamTestSuite,testReshapeExecStreamCastFilter);
        FENNEL_UNIT_TEST_CASE(
            ExecStreamTestSuite,testReshapeExecStreamNoCastFilter);
        FENNEL_UNIT_TEST_CASE(
            ExecStreamTestSuite,testReshapeExecStreamDynamicParams);
        FENNEL_UNIT_TEST_CASE(
            ExecStreamTestSuite,
            testSingleValueAggExecStream);
        FENNEL_UNIT_TEST_CASE(
            ExecStreamTestSuite,
            testMergeImplicitPullInputs);
        FENNEL_UNIT_TEST_CASE(
            ExecStreamTestSuite,
            testBTreeInsertExecStreamStaticBTree);
        FENNEL_UNIT_TEST_CASE(
            ExecStreamTestSuite,
            testBTreeInsertExecStreamDynamicBTree);

        // TODO jvs 4-Aug-2008:  enable these once
        // NLJ is parallel-safe
        FENNEL_EXTRA_UNIT_TEST_CASE(
            ExecStreamTestSuite,
            testNestedLoopJoinExecStream1);
        FENNEL_EXTRA_UNIT_TEST_CASE(
            ExecStreamTestSuite,
            testNestedLoopJoinExecStream2);

        FENNEL_UNIT_TEST_CASE(
            ExecStreamTestSuite,
            testSplitterPlusBarrier);

        FENNEL_UNIT_TEST_CASE(
            ExecStreamTestSuite,
            testSegBufferReaderWriterExecStream1);
        FENNEL_UNIT_TEST_CASE(
            ExecStreamTestSuite,
            testSegBufferReaderWriterExecStream2);
        FENNEL_UNIT_TEST_CASE(
            ExecStreamTestSuite,
            testSegBufferReaderWriterExecStream3);
        FENNEL_UNIT_TEST_CASE(
            ExecStreamTestSuite,
            testSegBufferReaderWriterExecStream4);
    }
};

using namespace fennel;

// instantiate the ExecStreamTestSuite as a stand-alone program
FENNEL_UNIT_TEST_SUITE(ParallelExecStreamSchedulerTest);

// End ParallelExecStreamSchedulerTest.cpp
