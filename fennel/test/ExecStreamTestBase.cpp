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
#include "fennel/test/ExecStreamTestBase.h"
#include "fennel/exec/ExecStreamGraph.h"
#include "fennel/exec/ExecStreamGraphEmbryo.h"
#include "fennel/exec/ExecStream.h"
#include "fennel/exec/DfsTreeExecStreamScheduler.h"
#include "fennel/exec/SimpleExecStreamGovernor.h"
#include "fennel/cache/Cache.h"

#include <boost/test/test_tools.hpp>

FENNEL_BEGIN_CPPFILE("$Id$");

SharedExecStreamGraph ExecStreamTestBase::newStreamGraph()
{
    SharedExecStreamGraph pGraph = ExecStreamGraph::newExecStreamGraph();
    pGraph->enableDummyTxnId(true);
    return pGraph;
}

SharedExecStreamGraphEmbryo
ExecStreamTestBase::newStreamGraphEmbryo(SharedExecStreamGraph g)
{
    return SharedExecStreamGraphEmbryo(
        new ExecStreamGraphEmbryo(
            g, pScheduler, pCache, pSegmentFactory));
}

ExecStreamScheduler *ExecStreamTestBase::newScheduler()
{
    return new DfsTreeExecStreamScheduler(
        shared_from_this(),
        "DfsTreeExecStreamScheduler");
}

ExecStreamGovernor *ExecStreamTestBase::newResourceGovernor(
    ExecStreamResourceKnobs const &knobSettings,
    ExecStreamResourceQuantity const &resourcesAvailable)
{
    return new SimpleExecStreamGovernor(
        knobSettings, resourcesAvailable, shared_from_this(),
        "SimpleExecStreamGovernor");
}

void ExecStreamTestBase::testCaseSetUp()
{
    SegStorageTestBase::testCaseSetUp();
    openStorage(DeviceMode::createNew);
    pScheduler.reset(newScheduler());
    ExecStreamResourceKnobs knobSettings;
    knobSettings.cacheReservePercentage = DefaultCacheReservePercent;
    knobSettings.expectedConcurrentStatements = DefaultConcurrentStatements;
    ExecStreamResourceQuantity resourcesAvailable;
    resourcesAvailable.nCachePages = nMemPages;
    pResourceGovernor.reset(
        newResourceGovernor(knobSettings, resourcesAvailable));
}

void ExecStreamTestBase::testCaseTearDown()
{
    // first stop the scheduler
    if (pScheduler) {
        pScheduler->stop();
    }
    pCacheAccessor.reset();
    // destroy the graph
    tearDownExecStreamTest();
    // free the scheduler last, since an ExecStreamGraph holds a raw Scheduler
    // ptr
    pScheduler.reset();
    assert(pResourceGovernor.unique());
    pResourceGovernor.reset();
    SegStorageTestBase::testCaseTearDown();
}

void ExecStreamTestBase::tearDownExecStreamTest()
{
}

FENNEL_END_CPPFILE("$Id$");

// End ExecStreamTestBase.cpp
