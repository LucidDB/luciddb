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

#ifndef Fennel_DT_Test_CalcExecStreamTestSuite_Included
#define Fennel_DT_Test_CalcExecStreamTestSuite_Included

#include "fennel/test/ExecStreamUnitTestBase.h"
#include "fennel/tuple/StandardTypeDescriptor.h"
#include <boost/test/test_tools.hpp>

using namespace fennel;

/**
 * CalcExecStreamTestSuite tests the CalcExecStream.
 * Derived classes can add tests and/or use a different scheduler
 * implementation.
 */
class CalcExecStreamTestSuite : public ExecStreamUnitTestBase
{
    TupleAttributeDescriptor uint64Desc;

protected:
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
    explicit CalcExecStreamTestSuite(bool addAllTests = true);

    /**
     * Tests with program that produces same amount of output as input.
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

#endif

// End CalcExecStreamTestSuite.h
