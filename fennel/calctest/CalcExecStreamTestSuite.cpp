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
#include "fennel/calctest/CalcExecStreamTestSuite.h"
#include "fennel/calculator/CalcExecStream.h"
#include "fennel/tuple/TupleOverflowExcn.h"
#include "fennel/exec/MockProducerExecStream.h"
#include "fennel/exec/ExecStreamEmbryo.h"

using namespace fennel;

CalcExecStreamTestSuite::CalcExecStreamTestSuite(bool addAllTests)
{
    StandardTypeDescriptorFactory stdTypeFactory;
    TupleAttributeDescriptor attrDesc(
        stdTypeFactory.newDataType(STANDARD_TYPE_UINT_64));
    uint64Desc = attrDesc;

    if (addAllTests) {
        FENNEL_UNIT_TEST_CASE(CalcExecStreamTestSuite, testConstantOneForOne);
        FENNEL_UNIT_TEST_CASE(CalcExecStreamTestSuite, testEmptyInput);
        FENNEL_UNIT_TEST_CASE(CalcExecStreamTestSuite, testConstantTwoForOne);
        FENNEL_UNIT_TEST_CASE(CalcExecStreamTestSuite, testConstantOneForTwo);
        FENNEL_UNIT_TEST_CASE(CalcExecStreamTestSuite, testTupleOverflow);
    }
}

void CalcExecStreamTestSuite::testConstantOneForOneImpl(uint nRowsInput)
{
    std::string program =
        "O u8; "
        "I u8; "
        "L u8; "
        "C u8; "
        "V 18446744073709551615; "
        "T; "
        "ADD L0, I0, C0; "
        "REF O0, L0; "
        "RETURN; ";

    TupleDescriptor tupleDesc;
    tupleDesc.push_back(uint64Desc);

    testConstant(program, tupleDesc, tupleDesc, sizeof(uint64_t), nRowsInput);
}

void CalcExecStreamTestSuite::testConstantOneForOne()
{
    testConstantOneForOneImpl();
}

void CalcExecStreamTestSuite::testEmptyInput()
{
    testConstantOneForOneImpl(0);
}

void CalcExecStreamTestSuite::testConstantTwoForOne()
{
    std::string program =
        "O u8, u8; "
        "I u8; "
        "L u8; "
        "C u8; "
        "V 18446744073709551615; "
        "T; "
        "ADD L0, I0, C0; "
        "REF O0, L0; "
        "REF O1, L0; "
        "RETURN; ";

    TupleDescriptor outputDesc;
    outputDesc.push_back(uint64Desc);
    outputDesc.push_back(uint64Desc);

    TupleDescriptor inputDesc;
    inputDesc.push_back(uint64Desc);

    testConstant(program, inputDesc, outputDesc, 2*sizeof(uint64_t));
}

void CalcExecStreamTestSuite::testConstantOneForTwo()
{
    std::string program =
        "O u8; "
        "I u8, u8; "
        "L u8, u8, u8; "
        "C u8; "
        "V 18446744073709551615; "
        "T; "
        "ADD L0, I0, C0; "
        "ADD L1, I0, I1; "
        "ADD L2, L0, L1; "
        "REF O0, L2; "
        "RETURN; ";

    TupleDescriptor outputDesc;
    outputDesc.push_back(uint64Desc);

    TupleDescriptor inputDesc;
    inputDesc.push_back(uint64Desc);
    inputDesc.push_back(uint64Desc);

    testConstant(program, inputDesc, outputDesc, sizeof(uint64_t));
}

void CalcExecStreamTestSuite::testTupleOverflow()
{
    std::string program =
        "O c,40000; "
        "I u8; "
        "L c,40000; "
        "C vc, 5; "
        "V 0x68656C6C6F; "
        "T; "
        "CALL 'castA(L0, C0); "
        "REF O0, L0; "
        "RETURN; ";

    TupleDescriptor inputDesc;
    inputDesc.push_back(uint64Desc);

    TupleDescriptor outputDesc;
    StandardTypeDescriptorFactory stdTypeFactory;
    TupleAttributeDescriptor charDesc(
        stdTypeFactory.newDataType(STANDARD_TYPE_CHAR),
        false,
        40000);
    outputDesc.push_back(charDesc);

    /*
    BOOST_CHECK_THROW(
        testConstant(program, inputDesc, outputDesc, 0),
        TupleOverflowExcn);
    */
    try {
        testConstant(program, inputDesc, outputDesc, 0);
        BOOST_ASSERT(false);
    } catch (TupleOverflowExcn &excn) {
    } catch (std::exception &stdExcn) {
        BOOST_ASSERT(false);
    }
}

void CalcExecStreamTestSuite::testConstant(
    std::string program,
    TupleDescriptor const &inputDesc,
    TupleDescriptor const &outputDesc,
    uint expectedFactor,
    uint nRowsInput)
{
    MockProducerExecStreamParams mockParams;
    mockParams.outputTupleDesc = inputDesc;
    mockParams.nRows = nRowsInput;

    CalcExecStreamParams calcParams;
    calcParams.outputTupleDesc = outputDesc;
    calcParams.program = program;
    calcParams.isFilter = false;

    ExecStreamEmbryo mockStreamEmbryo;
    mockStreamEmbryo.init(new MockProducerExecStream(), mockParams);
    mockStreamEmbryo.getStream()->setName("MockProducerExecStream");

    ExecStreamEmbryo calcStreamEmbryo;
    calcStreamEmbryo.init(new CalcExecStream(), calcParams);
    calcStreamEmbryo.getStream()->setName("CalcExecStream");

    SharedExecStream pOutputStream = prepareTransformGraph(
        mockStreamEmbryo, calcStreamEmbryo);


    uint64_t fff = 0xFFFFFFFFFFFFFFFFLL;
    TupleData expectedTuple;
    expectedTuple.compute(outputDesc);
    for (uint i = 0; i < expectedTuple.size(); ++i) {
        expectedTuple[i].pData = reinterpret_cast<PBuffer>(&fff);
    }
    verifyConstantOutput(
        *pOutputStream,
        expectedTuple,
        mockParams.nRows);
}

// End CalcExecStreamTestSuite.cpp
