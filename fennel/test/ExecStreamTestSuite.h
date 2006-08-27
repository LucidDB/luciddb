/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2005-2005 Disruptive Tech
// Copyright (C) 2005-2005 LucidEra, Inc.
// Portions Copyright (C) 2003-2005 John V. Sichi
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

#ifndef Fennel_Test_ExecStreamTestSuite_Included
#define Fennel_Test_ExecStreamTestSuite_Included

#include "fennel/test/ExecStreamUnitTestBase.h"
#include <boost/test/test_tools.hpp>

using namespace fennel;

/**
 * ExecStreamTestSuite tests various implementations of ExecStream.
 *
 * Derived classes can add tests and/or use a different scheduler
 * implementation.
 */
class ExecStreamTestSuite : public ExecStreamUnitTestBase
{
protected:
    void testCartesianJoinExecStream(uint nRowsLeft,uint nRowsRight);
    void testGroupAggExecStreamNrows(uint nrows);
    void testReshapeExecStream(
        bool cast, bool filter, uint expectedNRows, int expectedStart);
    
public:
    /**
     * Create a ExecStreamTestSuite
     *
     * @param initTestCases If true (the default), add test cases to the test
     * suite. A derived class might supply false, if it wants to disinherit
     * some or all of the tests.
     */
    explicit ExecStreamTestSuite(bool initTestCases = true)
    {
        if (initTestCases) {
            FENNEL_UNIT_TEST_CASE(ExecStreamTestSuite,testScratchBufferExecStream);
            FENNEL_UNIT_TEST_CASE(ExecStreamTestSuite,testCopyExecStream);
            FENNEL_UNIT_TEST_CASE(ExecStreamTestSuite,testMergeExecStream);
            FENNEL_UNIT_TEST_CASE(ExecStreamTestSuite,testSegBufferExecStream);
            FENNEL_UNIT_TEST_CASE(ExecStreamTestSuite,testCartesianJoinExecStreamOuter);
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
            FENNEL_UNIT_TEST_CASE(ExecStreamTestSuite,testSingleValueAggExecStream);
        }
    }

    void testScratchBufferExecStream();
    void testCopyExecStream();
    void testMergeExecStream();
    void testSegBufferExecStream();
    void testCountAggExecStream();
    void testSumAggExecStream();
    void testReshapeExecStream();
    void testSingleValueAggExecStream();
    
    void testCartesianJoinExecStreamOuter()
    {
        // iterate multiple outer buffers
        testCartesianJoinExecStream(10000,5);
    }
    
    void testCartesianJoinExecStreamInner()
    {
        // iterate multiple inner buffers
        testCartesianJoinExecStream(5,10000);
    }

    void testGroupAggExecStream1()
    {
        testGroupAggExecStreamNrows(10000);
    }
    
    // 258*2 values seems to be the point at which buffer
    // overflow occurs, so test that case as well as +/- 1
    // from there
    void testGroupAggExecStream2()
    {
        testGroupAggExecStreamNrows(257*2);
    }
    
    void testGroupAggExecStream3()
    {
        testGroupAggExecStreamNrows(258*2);
    }
    
    void testGroupAggExecStream4()
    {
        testGroupAggExecStreamNrows(259*2);
    }

    void testReshapeExecStreamCastFilter()
    {
        testReshapeExecStream(true, true, 10, 500);
    }

    void testReshapeExecStreamNoCastFilter()
    {
        testReshapeExecStream(false, false, 1000, 0);
    }
};

#endif
// End ExecStreamTestSuite.h
