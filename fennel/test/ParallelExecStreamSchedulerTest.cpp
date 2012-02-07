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

        FENNEL_UNIT_TEST_CASE(ExecStreamTestSuite, testScratchBufferExecStream);
        FENNEL_UNIT_TEST_CASE(ExecStreamTestSuite, testDoubleBufferExecStream);
        FENNEL_UNIT_TEST_CASE(ExecStreamTestSuite, testCopyExecStream);
        FENNEL_UNIT_TEST_CASE(ExecStreamTestSuite, testMergeExecStream);
        FENNEL_UNIT_TEST_CASE(ExecStreamTestSuite, testSegBufferExecStream);
        FENNEL_UNIT_TEST_CASE(
            ExecStreamTestSuite, testCartesianJoinExecStreamOuter);
        FENNEL_UNIT_TEST_CASE(
            ExecStreamTestSuite, testCartesianJoinExecStreamInner);
        FENNEL_UNIT_TEST_CASE(ExecStreamTestSuite, testCountAggExecStream);
        FENNEL_UNIT_TEST_CASE(ExecStreamTestSuite, testSumAggExecStream);
        FENNEL_UNIT_TEST_CASE(ExecStreamTestSuite, testGroupAggExecStream1);
        FENNEL_UNIT_TEST_CASE(ExecStreamTestSuite, testGroupAggExecStream2);
        FENNEL_UNIT_TEST_CASE(ExecStreamTestSuite, testGroupAggExecStream3);
        FENNEL_UNIT_TEST_CASE(ExecStreamTestSuite, testGroupAggExecStream4);
        FENNEL_UNIT_TEST_CASE(
            ExecStreamTestSuite, testReshapeExecStreamCastFilter);
        FENNEL_UNIT_TEST_CASE(
            ExecStreamTestSuite, testReshapeExecStreamNoCastFilter);
        FENNEL_UNIT_TEST_CASE(
            ExecStreamTestSuite, testReshapeExecStreamDynamicParams);
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
