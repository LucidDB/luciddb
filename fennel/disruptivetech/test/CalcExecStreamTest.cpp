/*
// $Id$
// Fennel is a relational database kernel.
// Copyright (C) 2004-2004 Disruptive Tech
// Copyright (C) 2004-2004 John V. Sichi.
//
// This program is free software; you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation; either version 2 of the License, or
// (at your option) any later version.
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
#include "fennel/test/ExecStreamTestBase.h"
#include "fennel/disruptivetech/xo/CalcExecStream.h"
#include "fennel/tuple/StandardTypeDescriptor.h"
#include "fennel/tuple/TupleOverflowExcn.h"
#include "fennel/exec/MockProducerExecStream.h"
#include "fennel/exec/ExecStreamEmbryo.h"

#include <boost/test/test_tools.hpp>

using namespace fennel;

class CalcExecStreamTest : public ExecStreamTestBase
{
    TupleAttributeDescriptor uint64Desc;

    /**
     * Tests that running a given program results in uniform output of
     * byte '0xFF'.
     *
     * @param program to execute
     *
     * @param inputDesc descriptor for tuples consumed by calc
     *
     * @param outputDesc descriptor for tuples produced by calc
     *
     * @param expectedFactor factor by which byte length of output should
     * exceed number of rows of input
     *
     * @param nRowsInput number of rows of input
     */
    void testConstant(
        std::string program,
        TupleDescriptor const &inputDesc,
        TupleDescriptor const &outputDesc,
        uint expectedFactor,
        uint nRowsInput = 1000);
    
    void testConstantOneForOneImpl(uint nRowsInput = 1000);
    
public:
    explicit CalcExecStreamTest();

    /**
     * Tests with program that produces same amount of output as input.
     *
     * @param nRowsInput number of rows of input
     */
    void testConstantOneForOne();
    
    /**
     * Tests with no input.
     */
    void testEmptyInput();
    
    /**
     * Tests with program that produces twice as much output as input.
     */
    void testConstantTwoForOne();
    
    /**
     * Tests with program that produces half as much output as input.
     */
    void testConstantOneForTwo();
    
    /**
     * Tests with program that produces a tuple which overflows output buffer.
     */
    void testTupleOverflow();
};

CalcExecStreamTest::CalcExecStreamTest()
{
    // NOTE:  work with 64-bit data so that alignment paddings doesn't
    // introduce zeros.  This will still break with 128-bit alignment.
    
    StandardTypeDescriptorFactory stdTypeFactory;
    TupleAttributeDescriptor attrDesc(
        stdTypeFactory.newDataType(STANDARD_TYPE_UINT_64));
    uint64Desc = attrDesc;
        
    FENNEL_UNIT_TEST_CASE(CalcExecStreamTest,testConstantOneForOne);
    FENNEL_UNIT_TEST_CASE(CalcExecStreamTest,testEmptyInput);
    FENNEL_UNIT_TEST_CASE(CalcExecStreamTest,testConstantTwoForOne);
    FENNEL_UNIT_TEST_CASE(CalcExecStreamTest,testConstantOneForTwo);
    FENNEL_UNIT_TEST_CASE(CalcExecStreamTest,testTupleOverflow);
}

void CalcExecStreamTest::testConstantOneForOneImpl(uint nRowsInput)
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

void CalcExecStreamTest::testConstantOneForOne()
{
    testConstantOneForOneImpl();
}

void CalcExecStreamTest::testEmptyInput()
{
    testConstantOneForOneImpl(0);
}

void CalcExecStreamTest::testConstantTwoForOne()
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

void CalcExecStreamTest::testConstantOneForTwo()
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

void CalcExecStreamTest::testTupleOverflow()
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

    BOOST_CHECK_THROW(
        testConstant(program, inputDesc, outputDesc, 0),
        TupleOverflowExcn);
}

void CalcExecStreamTest::testConstant(
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

    verifyConstantOutput(
        *pOutputStream,
        mockParams.nRows*expectedFactor,
        0xFF);
}

FENNEL_UNIT_TEST_SUITE(CalcExecStreamTest);

// End CalcExecStreamTest.cpp
