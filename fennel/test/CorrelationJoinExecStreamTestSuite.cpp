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
#include "fennel/test/CorrelationJoinExecStreamTestSuite.h"
#include "fennel/exec/CorrelationJoinExecStream.h"
#include "fennel/tuple/StandardTypeDescriptor.h"
#include "fennel/tuple/TupleOverflowExcn.h"
#include "fennel/exec/MockProducerExecStream.h"
#include "fennel/exec/ExecStreamEmbryo.h"
#include "fennel/exec/ExecStreamGraph.h"

using namespace fennel;

CorrelationJoinExecStreamTestSuite::CorrelationJoinExecStreamTestSuite(
    bool addAllTests)
{
    if (addAllTests) {
        FENNEL_UNIT_TEST_CASE(
            CorrelationJoinExecStreamTestSuite, testCorrelationJoin);
    }

    StandardTypeDescriptorFactory stdTypeFactory;

    descAttrInt64 = TupleAttributeDescriptor(
        stdTypeFactory.newDataType(STANDARD_TYPE_INT_64));
    descInt64.push_back(descAttrInt64);
}

void CorrelationJoinExecStreamTestSuite::testCorrelationJoin()
{
    MockProducerExecStreamParams paramsMockLeft;
    paramsMockLeft.outputTupleDesc.push_back(descAttrInt64);
    paramsMockLeft.pGenerator.reset(new RampExecStreamGenerator);
    paramsMockLeft.nRows = 5000;

    ExecStreamEmbryo leftStreamEmbryo;
    leftStreamEmbryo.init(new MockProducerExecStream(), paramsMockLeft);
    leftStreamEmbryo.getStream()->setName("LeftProducerExecStream");

    DynamicParamId dynamicParamId(1);
    MockProducerExecStreamParams paramsMockRight(paramsMockLeft);
    paramsMockRight.pGenerator.reset(
        new DynamicParamExecStreamGenerator(
            dynamicParamId,
            pGraph->getDynamicParamManager()));
    paramsMockRight.nRows = 10;

    ExecStreamEmbryo rightStreamEmbryo;
    rightStreamEmbryo.init(new MockProducerExecStream(), paramsMockRight);
    rightStreamEmbryo.getStream()->setName("RightProducerExecStream");

    CorrelationJoinExecStreamParams paramsJoin;

    Correlation correlation(dynamicParamId, 0);
    paramsJoin.correlations.push_back(correlation);

    ExecStreamEmbryo joinStreamEmbryo;
    joinStreamEmbryo.init(new CorrelationJoinExecStream(), paramsJoin);
    joinStreamEmbryo.getStream()->setName("CorrelationJoinExecStream");

    SharedExecStream pOutputStream = prepareConfluenceGraph(
        leftStreamEmbryo,
        rightStreamEmbryo,
        joinStreamEmbryo);

    StairCaseExecStreamGenerator rampExpectedGenerator(
        1, paramsMockRight.nRows);
    verifyOutput(
        *pOutputStream,
        paramsMockLeft.nRows * paramsMockRight.nRows,
        rampExpectedGenerator);
}

// End CorrelationJoinExecStreamTestSuite.cpp
