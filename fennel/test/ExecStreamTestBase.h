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

#ifndef Fennel_ExecStreamTestBase_Included
#define Fennel_ExecStreamTestBase_Included

#include "fennel/exec/ExecStreamGovernor.h"
#include "fennel/test/SegStorageTestBase.h"

FENNEL_BEGIN_NAMESPACE

class ExecStreamScheduler;

/**
 * ExecStreamTestBase is a common base for tests of ExecStream
 * implementations.
 *
 * @author John V. Sichi
 * @version $Id$
 */
class FENNEL_TEST_EXPORT ExecStreamTestBase
    : virtual public SegStorageTestBase
{
protected:
    static const uint DefaultCacheReservePercent = 5;

    static const uint DefaultConcurrentStatements = 4;

    SharedExecStreamScheduler pScheduler;

    SharedExecStreamGovernor pResourceGovernor;

    SharedCacheAccessor pCacheAccessor;

    /**
     * Creates a stream graph.
     */
    virtual SharedExecStreamGraph newStreamGraph();

    /**
     * Creates an embryo for a stream graph.
     */
    virtual SharedExecStreamGraphEmbryo newStreamGraphEmbryo(
        SharedExecStreamGraph);

    /**
     * Creates a scheduler.
     */
    virtual ExecStreamScheduler *newScheduler();

    /**
     * Creates the resource governor
     */
    virtual ExecStreamGovernor *newResourceGovernor(
        ExecStreamResourceKnobs const &knobSettings,
        ExecStreamResourceQuantity const &resourcesAvailable);

    /**
     * ExecStream-specific handler called from testCaseTearDown.
     */
    virtual void tearDownExecStreamTest();

public:
    virtual ~ExecStreamTestBase() {}

    // override TestBase
    virtual void testCaseSetUp();
    virtual void testCaseTearDown();
};

FENNEL_END_NAMESPACE
#endif
// End ExecStreamTestBase.h
