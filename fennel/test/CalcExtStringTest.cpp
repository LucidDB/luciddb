/*
// $Id$
// Fennel is a relational database kernel.
// Copyright (C) 2004-2004 Disruptive Tech
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
#include "fennel/calc/StringToHex.h"
#include "fennel/common/FennelExcn.h"

#include <boost/test/test_tools.hpp>
#include <boost/scoped_array.hpp>
#include <string>
#include <limits>


using namespace fennel;
using namespace std;


class CalcExtStringTest : virtual public TestBase, public TraceSource
{
    void testCalcExtStringToLower();
    
public:
    explicit CalcExtStringTest()
        : TraceSource(this,"CalcExtStringTest")
    {
        srand(time(NULL));
        CalcInit::instance();
        FENNEL_UNIT_TEST_CASE(CalcExtStringTest, testCalcExtStringToLower);
    }
     
    virtual ~CalcExtStringTest()
    {
    }
};

void
CalcExtStringTest::testCalcExtStringToLower()
{
    ostringstream pg("");

    pg << "O vc,5, c,5;" << endl;
    pg << "C vc,5, c,5;" << endl;
    pg << "V 0x" << stringToHex("ABC") << ", 0x" << stringToHex("GHIJ ")  << ";" << endl;
    pg << "T;" << endl;
    pg << "CALL 'strToLowerA(O0, C0);" << endl;

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

#if 0
    TuplePrinter tuplePrinter;
    tuplePrinter.print(cout, calc.getOutputRegisterDescriptor(), outTuple);
    cout << endl;
#endif

#if 0
    BOOST_CHECK_EQUAL(*(reinterpret_cast<uint16_t *>
                        (const_cast<PBuffer>((*statusTuple)[0].pData))),
                      4);
    BOOST_CHECK_EQUAL(*(reinterpret_cast<uint16_t *>
                        (const_cast<PBuffer>((*statusTuple)[1].pData))),
                      5);
    BOOST_CHECK_EQUAL(*(reinterpret_cast<uint16_t *>
                        (const_cast<PBuffer>((*statusTuple)[2].pData))),
                      6);
#endif
    
}


FENNEL_UNIT_TEST_SUITE(CalcExtStringTest);

