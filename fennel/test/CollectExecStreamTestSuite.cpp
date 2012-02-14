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
#include "fennel/test/CollectExecStreamTestSuite.h"
#include "fennel/exec/CollectExecStream.h"
#include "fennel/exec/UncollectExecStream.h"
#include "fennel/tuple/StandardTypeDescriptor.h"
#include "fennel/tuple/TupleOverflowExcn.h"
#include "fennel/exec/MockProducerExecStream.h"
#include "fennel/exec/ExecStreamEmbryo.h"

using namespace fennel;

CollectExecStreamTestSuite::CollectExecStreamTestSuite(bool addAllTests)
{
    if (addAllTests) {
        FENNEL_UNIT_TEST_CASE(CollectExecStreamTestSuite, testCollectInts);
        FENNEL_UNIT_TEST_CASE(CollectExecStreamTestSuite, testCollectUncollect);
        FENNEL_UNIT_TEST_CASE(
            CollectExecStreamTestSuite, testCollectCollectUncollectUncollect);
    }

    StandardTypeDescriptorFactory stdTypeFactory;

    descAttrInt64 =
        TupleAttributeDescriptor(
            stdTypeFactory.newDataType(STANDARD_TYPE_INT_64));
    descInt64.push_back(descAttrInt64);

    descAttrVarbinary32 =
        TupleAttributeDescriptor(
            stdTypeFactory.newDataType(STANDARD_TYPE_VARBINARY), true, 32);
    descVarbinary32.push_back(descAttrVarbinary32);
}

void CollectExecStreamTestSuite::testCollectInts()
{
    uint rows = 2;
    MockProducerExecStreamParams mockParams;
    mockParams.outputTupleDesc.push_back(descAttrInt64);
    mockParams.nRows = rows;
    mockParams.pGenerator.reset(new RampExecStreamGenerator(1));

    CollectExecStreamParams collectParams;
    collectParams.outputTupleDesc = descVarbinary32;

    ExecStreamEmbryo mockStreamEmbryo;
    mockStreamEmbryo.init(new MockProducerExecStream(), mockParams);
    mockStreamEmbryo.getStream()->setName("MockProducerExecStream");

    ExecStreamEmbryo collectStreamEmbryo;
    collectStreamEmbryo.init(new CollectExecStream(), collectParams);
    collectStreamEmbryo.getStream()->setName("CollectExecStream");


    // setup the expected result
    uint8_t intArrayBuff[32];
    uint64_t one = 1;
    TupleData oneData(descInt64);
    oneData[0].pData = (PConstBuffer) &one;
    TupleAccessor oneAccessor;
    oneAccessor.compute(descInt64);
    assert(oneAccessor.getMaxByteCount() <= sizeof(intArrayBuff));
    oneAccessor.marshal(oneData, (PBuffer) intArrayBuff);

    uint64_t two = 2;
    TupleData twoData(descInt64);
    twoData[0].pData = (PConstBuffer) &two;
    TupleAccessor twoAccessor;
    twoAccessor.compute(descInt64);
    assert(
        (oneAccessor.getMaxByteCount() + twoAccessor.getMaxByteCount())
        <= sizeof(intArrayBuff));
    twoAccessor.marshal(
        twoData,
        ((PBuffer)intArrayBuff) + oneAccessor.getMaxByteCount());

    uint8_t varbinaryBuff[1000];
    TupleData binData(descVarbinary32);
    binData[0].pData = (PConstBuffer) intArrayBuff;
    binData[0].cbData =
        oneAccessor.getMaxByteCount() + twoAccessor.getMaxByteCount();
    TupleAccessor binAccessor;
    binAccessor.compute(descVarbinary32);
    binAccessor.marshal(binData, (PBuffer) varbinaryBuff);


    SharedExecStream pOutputStream = prepareTransformGraph(
        mockStreamEmbryo, collectStreamEmbryo);

    verifyConstantOutput(*pOutputStream, binData, 1);
}

void CollectExecStreamTestSuite::testCollectUncollect()
{
    StandardTypeDescriptorFactory stdTypeFactory;
    uint rows = 127;

    TupleAttributeDescriptor tupleDescAttr(
        stdTypeFactory.newDataType(STANDARD_TYPE_VARBINARY),
        true,
        2 * rows * sizeof(uint64_t));
    TupleDescriptor tupleDesc;
    tupleDesc.push_back(tupleDescAttr);

    MockProducerExecStreamParams mockParams;
    mockParams.outputTupleDesc.push_back(descAttrInt64);
    mockParams.nRows = rows;
    mockParams.pGenerator.reset(new RampExecStreamGenerator());

    CollectExecStreamParams collectParams;
    collectParams.outputTupleDesc = tupleDesc;

    UncollectExecStreamParams uncollectParams;
    uncollectParams.outputTupleDesc = descInt64;

    ExecStreamEmbryo mockStreamEmbryo;
    mockStreamEmbryo.init(new MockProducerExecStream(), mockParams);
    mockStreamEmbryo.getStream()->setName("MockProducerExecStream");

    ExecStreamEmbryo collectStreamEmbryo;
    collectStreamEmbryo.init(new CollectExecStream(), collectParams);
    collectStreamEmbryo.getStream()->setName("CollectExecStream");

    ExecStreamEmbryo uncollectStreamEmbryo;
    uncollectStreamEmbryo.init(new UncollectExecStream(), uncollectParams);
    uncollectStreamEmbryo.getStream()->setName("UncollectExecStream");


    std::vector<ExecStreamEmbryo> transforms;
    transforms.push_back(collectStreamEmbryo);
    transforms.push_back(uncollectStreamEmbryo);
    SharedExecStream pOutputStream = prepareTransformGraph(
        mockStreamEmbryo, transforms);

    RampExecStreamGenerator rampExpectedGenerator;

    verifyOutput(*pOutputStream, rows, rampExpectedGenerator);
}

void CollectExecStreamTestSuite::testCollectCollectUncollectUncollect() {
    StandardTypeDescriptorFactory stdTypeFactory;
    uint rows = 3;

    TupleAttributeDescriptor tupleDescAttr1(
        stdTypeFactory.newDataType(STANDARD_TYPE_VARBINARY),
        true,
        2 * rows * sizeof(uint64_t));
    TupleDescriptor vbDesc1;
    vbDesc1.push_back(tupleDescAttr1);

    TupleAttributeDescriptor tupleDescAttr2(
        stdTypeFactory.newDataType(STANDARD_TYPE_VARBINARY),
        true,
        2 * rows * rows * sizeof(uint64_t));
    TupleDescriptor vbDesc2;
    vbDesc2.push_back(tupleDescAttr2);

    MockProducerExecStreamParams mockParams;
    mockParams.outputTupleDesc.push_back(descAttrInt64);
    mockParams.nRows = rows;
    mockParams.pGenerator.reset(new RampExecStreamGenerator());

    CollectExecStreamParams collectParams1;
    collectParams1.outputTupleDesc = vbDesc1;

    CollectExecStreamParams collectParams2;
    collectParams2.outputTupleDesc = vbDesc2;

    UncollectExecStreamParams uncollectParams1;
    uncollectParams1.outputTupleDesc = vbDesc1;

    UncollectExecStreamParams uncollectParams2;
    uncollectParams2.outputTupleDesc = descInt64;

    ExecStreamEmbryo mockStreamEmbryo;
    mockStreamEmbryo.init(new MockProducerExecStream(), mockParams);
    mockStreamEmbryo.getStream()->setName("MockProducerExecStream");

    ExecStreamEmbryo collectStreamEmbryo1;
    collectStreamEmbryo1.init(new CollectExecStream(), collectParams1);
    collectStreamEmbryo1.getStream()->setName("CollectExecStream1");

    ExecStreamEmbryo collectStreamEmbryo2;
    collectStreamEmbryo2.init(new CollectExecStream(), collectParams2);
    collectStreamEmbryo2.getStream()->setName("CollectExecStream2");

    ExecStreamEmbryo uncollectStreamEmbryo1;
    uncollectStreamEmbryo1.init(new UncollectExecStream(), uncollectParams1);
    uncollectStreamEmbryo1.getStream()->setName("UncollectExecStream1");

    ExecStreamEmbryo uncollectStreamEmbryo2;
    uncollectStreamEmbryo2.init(new UncollectExecStream(), uncollectParams2);
    uncollectStreamEmbryo2.getStream()->setName("UncollectExecStream2");

    std::vector<ExecStreamEmbryo> transforms;
    transforms.push_back(collectStreamEmbryo1);
    transforms.push_back(collectStreamEmbryo2);
    transforms.push_back(uncollectStreamEmbryo1);
    transforms.push_back(uncollectStreamEmbryo2);
    SharedExecStream pOutputStream = prepareTransformGraph(
        mockStreamEmbryo, transforms);

    RampExecStreamGenerator rampExpectedGenerator;

    verifyOutput(*pOutputStream, rows, rampExpectedGenerator);
}

// End CollectExecStreamTestSuite.cpp
