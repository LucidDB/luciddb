/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2005-2005 Disruptive Tech
// Copyright (C) 2005-2005 Red Square, Inc.
// Portions Copyright (C) 2003-2005 John V. Sichi
//
// This library is free software; you can redistribute it and/or
// modify it under the terms of the GNU Library General Public
// License as published by the Free Software Foundation; either
// version 2 of the License, or (at your option) any later Eigenbase-approved version.
//
// This library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
// Library General Public License for more details.
//
// You should have received a copy of the GNU Library General Public
// License along with this library; if not, write to the
// Free Software Foundation, Inc., 59 Temple Place - Suite 330,
// Boston, MA  02111-1307  USA
//
// See the LICENSE.html file located in the top-level-directory of
// the archive of this library for complete text of license.
*/

#ifndef Fennel_Test_ExecStreamTestSuite_Included
#define Fennel_Test_ExecStreamTestSuite_Included

#include "fennel/test/ExecStreamTestBase.h"
#include <boost/test/test_tools.hpp>

using namespace fennel;

/**
 * ExecStreamTestSuite tests various implementations of ExecStream.
 *
 * Derived classes can add tests and/or use a different scheduler
 * implementation.
 */
class ExecStreamTestSuite : public ExecStreamTestBase
{
protected:
    void verifyZeroedOutput(ExecStream &stream,uint nBytesExpected);
    void testCartesianJoinExecStream(uint nRowsLeft,uint nRowsRight);
    
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
            FENNEL_UNIT_TEST_CASE(ExecStreamTestSuite,testSegBufferExecStream);
            FENNEL_UNIT_TEST_CASE(ExecStreamTestSuite,testCartesianJoinExecStreamOuter);
            FENNEL_UNIT_TEST_CASE(ExecStreamTestSuite,testCartesianJoinExecStreamInner);
        }
    }

    void testScratchBufferExecStream();
    void testCopyExecStream();
    void testSegBufferExecStream();
    
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
};

#endif
// End ExecStreamTestSuite.h
