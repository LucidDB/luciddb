/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2004-2007 Disruptive Tech
// Copyright (C) 2005-2007 The Eigenbase Project
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

#include "fennel/common/CommonPreamble.h"
#include "fennel/test/TestBase.h"
#include "fennel/common/TraceSource.h"

#include "fennel/tuple/TupleDataWithBuffer.h"
#include "fennel/tuple/TuplePrinter.h"
#include "fennel/disruptivetech/calc/CalcCommon.h"
#include "fennel/common/FennelExcn.h"

#include <boost/test/test_tools.hpp>
#include <boost/scoped_array.hpp>
#include <string>
#include <limits>

using namespace fennel;
using namespace std;


class CalcExtDateTimeTest : virtual public TestBase, public TraceSource
{
    void testCalcExtConvertDateToString();
    void testCalcExtLocalTime();
    void testCalcExtLocalTimestamp();

    void checkWarnings(Calculator& calc, string expected);
    void printOutput(TupleData const & tup,
                     Calculator const & calc);
    
public:
    explicit CalcExtDateTimeTest()
        : TraceSource(shared_from_this(),"CalcExtDateTimeTest")
    {
        srand(time(NULL));
        CalcInit::instance();
        FENNEL_UNIT_TEST_CASE(CalcExtDateTimeTest, testCalcExtConvertDateToString);
        FENNEL_UNIT_TEST_CASE(CalcExtDateTimeTest, testCalcExtLocalTime);
        FENNEL_UNIT_TEST_CASE(CalcExtDateTimeTest, testCalcExtLocalTimestamp);
    }

    virtual ~CalcExtDateTimeTest()
    {
    }

    // helpers

    bool equals(TupleDatum buf, const char *expected) {
        char *s = reinterpret_cast<char *>(const_cast<PBuffer>(buf.pData));
        return !strncmp(expected, s, buf.cbData);
    }
};

// for nitty-gritty debugging. sadly, doesn't use BOOST_MESSAGE.
void
CalcExtDateTimeTest::printOutput(TupleData const & tup,
                                 Calculator const & calc)
{
#if 1
    TuplePrinter tuplePrinter;
    tuplePrinter.print(cout, calc.getOutputRegisterDescriptor(), tup);
    cout << endl;
#endif
}


void
CalcExtDateTimeTest::testCalcExtConvertDateToString()
{
    ostringstream pg("");

    pg << "O vc,10;" << endl;
    pg << "L vc,10;" << endl;
    pg << "C s8;" << endl;
    pg << "V 115200000;" << endl; // (in PDT).
    pg << "T;" << endl;
    pg << "CALL 'CastDateToStrA(L0, C0);" << endl;
    pg << "REF O0, L0;" << endl;

    Calculator calc(0);
    
    try {
        calc.assemble(pg.str().c_str());
    }
    catch (FennelExcn& ex) {
        BOOST_FAIL("Assemble exception " << ex.getMessage()<< pg.str());
    }

    TupleDataWithBuffer outTuple(calc.getOutputRegisterDescriptor());
    TupleDataWithBuffer inTuple(calc.getInputRegisterDescriptor());

    calc.bind(&inTuple, &outTuple);
    calc.exec();
    //    printOutput(outTuple, calc);

    BOOST_CHECK(equals(outTuple[0], "1970-01-02"));
}

void
CalcExtDateTimeTest::testCalcExtLocalTime()
{
    ostringstream pg("");
    
    pg << "O s8;" << endl;
    pg << "I s4;" << endl;
    pg << "L s8;" << endl;
    pg << "C bo, bo, c,23;" << endl;
    pg << "V 1, 0, 0x5053542D385044542C4D332E322E302C4D31312E312E30 /* PST-8PDT,M3.2.0,M11.1.0 */;" << endl;
    pg << "T;" << endl;
    pg << "CALL 'LocalTime2(L0, C2) /* 0: LOCALTIME($t1) */;" << endl;
    pg << "REF O0, L0 /* 1: */;" << endl;
    //    pg << "RETURN /* 2: */;|" << endl;
    
    Calculator calc(0);

    try {
        calc.assemble(pg.str().c_str());
    }
    catch (FennelExcn& ex) {
        BOOST_FAIL("Assemble exception " << ex.getMessage()<< pg.str());
    }

    TupleDataWithBuffer outTuple(calc.getOutputRegisterDescriptor());
    TupleDataWithBuffer inTuple(calc.getInputRegisterDescriptor());

    calc.bind(&inTuple, &outTuple);
    calc.exec();
    printOutput(outTuple, calc);    
    
}

void
CalcExtDateTimeTest::testCalcExtLocalTimestamp()
{
    ostringstream pg("");
    
    pg << "O s8;" << endl;
    pg << "I s4;" << endl;
    pg << "L s8;" << endl;
    pg << "C bo, bo, c,23;" << endl;
    pg << "V 1, 0, 0x5053542D385044542C4D332E322E302C4D31312E312E30 /* PST-8PDT,M3.2.0,M11.1.0 */;" << endl;
    pg << "T;" << endl;
    pg << "CALL 'LocalTimestamp2(L0, C2) /* 0: LOCALTIMESTAMP($t1) */;" << endl;
    pg << "REF O0, L0 /* 1: */;" << endl;
    //    pg << "RETURN /* 2: */;|" << endl;
    
    Calculator calc(0);

    try {
        calc.assemble(pg.str().c_str());
    }
    catch (FennelExcn& ex) {
        BOOST_FAIL("Assemble exception " << ex.getMessage()<< pg.str());
    }

    TupleDataWithBuffer outTuple(calc.getOutputRegisterDescriptor());
    TupleDataWithBuffer inTuple(calc.getInputRegisterDescriptor());

    calc.bind(&inTuple, &outTuple);
    calc.exec();
    printOutput(outTuple, calc);    
    
}

FENNEL_UNIT_TEST_SUITE(CalcExtDateTimeTest);

