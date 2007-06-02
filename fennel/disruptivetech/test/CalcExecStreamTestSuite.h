/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2004-2007 Disruptive Tech
// Copyright (C) 2005-2007 The Eigenbase Project
// Portions Copyright (C) 2004-2007 John V. Sichi
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
