/*
// $Id$
// Fennel is a relational database kernel.
// Copyright (C) 2004-2004 Disruptive Technologies, Inc.
//
// This program is free software; you can redistribute it and/or
// modify it under the terms of the GNU Lesser General Public License
// as published by the Free Software Foundation; either version 2.1
// of the License, or (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with this program; if not, write to the Free Software
// Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
*/

#include "fennel/common/CommonPreamble.h"
#include "fennel/test/TestBase.h"
#include "fennel/common/TraceSource.h"

#include "fennel/tuple/TupleDataWithBuffer.h"
#include "fennel/tuple/TuplePrinter.h"
#include "fennel/calc/CalcCommon.h"
#include "fennel/common/FennelExcn.h"

#include <boost/test/test_tools.hpp>
#include <boost/scoped_array.hpp>
#include <string>
#include <limits>
#include <math.h>

using namespace fennel;
using namespace std;


class CalcExtMathTest : virtual public TestBase, public TraceSource
{
    void testCalcExtMathLogarithms();
    void testCalcExtMathPowMod();
    void testCalcExtMathAbs();

public:
    explicit CalcExtMathTest()
        : TraceSource(this,"CalcExtMathTest")
    {
        srand(time(NULL));
        CalcInit::instance();
        FENNEL_UNIT_TEST_CASE(CalcExtMathTest, testCalcExtMathLogarithms);
        FENNEL_UNIT_TEST_CASE(CalcExtMathTest, testCalcExtMathAbs);
        FENNEL_UNIT_TEST_CASE(CalcExtMathTest, testCalcExtMathPowMod);
    }
     
    virtual ~CalcExtMathTest()
    {
    }
};

void
CalcExtMathTest::testCalcExtMathLogarithms()
{
    ostringstream pg("");
    
    pg << "O d, d;" << endl;
    pg << "C d, d;" << endl;
    pg << "V 2.71828, 10.0;" << endl;
    pg << "T;" << endl;
    pg << "CALL 'LN(O0, C0);" << endl;
    pg << "CALL 'LOG10(O1, C1);" << endl;
    // BOOST_MESSAGE(pg.str());

    CalcInit::instance();
    Calculator calc;
    
    try {
        calc.assemble(pg.str().c_str());
    }
    catch (FennelExcn& ex) {
        BOOST_MESSAGE("Assemble exception " << ex.getMessage());
        BOOST_MESSAGE(pg.str());
        BOOST_REQUIRE(0);
    }

    TupleDataWithBuffer outTuple(calc.getOutputRegisterDescriptor());
    TupleDataWithBuffer inTuple(calc.getInputRegisterDescriptor());

    calc.bind(&inTuple, &outTuple);
    calc.exec();

    TuplePrinter tuplePrinter;
    tuplePrinter.print(cout, calc.getOutputRegisterDescriptor(), outTuple);
    cout << endl;

    for(int i=0;i<2;i++) {
        BOOST_CHECK(fabs(*(reinterpret_cast<double*>
    	       (const_cast<PBuffer>(outTuple[i].pData)))-1.0)<0.0001);
    }

}
    


void
CalcExtMathTest::testCalcExtMathAbs()
{
    ostringstream pg("");
    
    pg << "O d, s8;" << endl;
    pg << "C d, s8;" << endl;
    pg << "V -10.0, -10;" << endl;
    pg << "T;" << endl;
    pg << "CALL 'ABS(O0, C0);" << endl;
    pg << "CALL 'ABS(O1, C1);" << endl;
    // BOOST_MESSAGE(pg.str());

    CalcInit::instance();
    Calculator calc;
    
    try {
        calc.assemble(pg.str().c_str());
    }
    catch (FennelExcn& ex) {
        BOOST_MESSAGE("Assemble exception " << ex.getMessage());
        BOOST_MESSAGE(pg.str());
        BOOST_REQUIRE(0);
    }

    TupleDataWithBuffer outTuple(calc.getOutputRegisterDescriptor());
    TupleDataWithBuffer inTuple(calc.getInputRegisterDescriptor());

    calc.bind(&inTuple, &outTuple);
    calc.exec();

    TuplePrinter tuplePrinter;
    tuplePrinter.print(cout, calc.getOutputRegisterDescriptor(), outTuple);
    cout << endl;

    BOOST_CHECK(fabs(*(reinterpret_cast<double*>
		       (const_cast<PBuffer>(outTuple[0].pData)))-10)<0.0001);
    
    BOOST_CHECK_EQUAL(*(reinterpret_cast<int*>
			(const_cast<PBuffer>(outTuple[1].pData))),10);
}

void
CalcExtMathTest::testCalcExtMathPowMod()
{
    ostringstream pg("");
    
    pg << "O d, s8;" << endl;
    pg << "C d, d, s8, s8;" << endl;
    pg << "V 5.0, 3.0, 5, 3;" << endl;
    pg << "T;" << endl;
    pg << "CALL 'POW(O0, C0, C1);" << endl;
    pg << "CALL 'MOD(O1, C2, C3);" << endl;
    // BOOST_MESSAGE(pg.str());

    CalcInit::instance();
    Calculator calc;
    
    try {
        calc.assemble(pg.str().c_str());
    }
    catch (FennelExcn& ex) {
        BOOST_MESSAGE("Assemble exception " << ex.getMessage());
        BOOST_MESSAGE(pg.str());
        BOOST_REQUIRE(0);
    }

    TupleDataWithBuffer outTuple(calc.getOutputRegisterDescriptor());
    TupleDataWithBuffer inTuple(calc.getInputRegisterDescriptor());

    calc.bind(&inTuple, &outTuple);
    calc.exec();

    TuplePrinter tuplePrinter;
    tuplePrinter.print(cout, calc.getOutputRegisterDescriptor(), outTuple);
    cout << endl;


    BOOST_CHECK(fabs(*(reinterpret_cast<double*>
		       (const_cast<PBuffer>(outTuple[0].pData)))-125)<0.0001);
    
    BOOST_CHECK_EQUAL(*(reinterpret_cast<int*>
			(const_cast<PBuffer>(outTuple[1].pData))),2);
}

FENNEL_UNIT_TEST_SUITE(CalcExtMathTest);

