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
    void checkWarnings(Calculator& calc, string expected);

    void testCalcExtMathLogarithms();
    void testCalcExtMathLogarithmsFails();
    void testCalcExtMathPow();
    void testCalcExtMathPowFails();
    void testCalcExtMathAbs();

    void printOutput(TupleData const & tup,
                     Calculator const & calc);
    
    string mProgramPower;
public:
    explicit CalcExtMathTest()
        : TraceSource(this,"CalcExtMathTest")
    {
        srand(time(NULL));
        CalcInit::instance();
        FENNEL_UNIT_TEST_CASE(CalcExtMathTest, testCalcExtMathLogarithms);
        FENNEL_UNIT_TEST_CASE(CalcExtMathTest, testCalcExtMathLogarithmsFails);
        FENNEL_UNIT_TEST_CASE(CalcExtMathTest, testCalcExtMathAbs);
        FENNEL_UNIT_TEST_CASE(CalcExtMathTest, testCalcExtMathPow);
        FENNEL_UNIT_TEST_CASE(CalcExtMathTest, testCalcExtMathPowFails);


        //~ Programs used by more than one function -------------------------
        ostringstream pg;

        pg << "O d;" << endl;
        pg << "L d;" << endl;
        pg << "C %s, %s;" << endl;
        pg << "V %s, %s;" << endl;
        pg << "T;" << endl;
        pg << "CALL 'POW(L0, C0, C1);" << endl;	
        pg << "REF O0, L0;" << endl;

        mProgramPower = pg.str();
    }
     
    virtual ~CalcExtMathTest()
    {
    }
};

// for nitty-gritty debugging. sadly, doesn't use BOOST_MESSAGE.
void
CalcExtMathTest::printOutput(TupleData const & tup,
                             Calculator const & calc)
{
#if 0
    TuplePrinter tuplePrinter;
    tuplePrinter.print(cout, calc.getOutputRegisterDescriptor(), tup);
    cout << endl;
#endif
}


void 
CalcExtMathTest::checkWarnings(Calculator& calc, string expected)
{
    try {
        calc.exec();
    } catch(...) {
        BOOST_FAIL("An exception was thrown while running program");
    }
    
    int i = calc.warnings().find(expected);

    if ( i < 0) {
        string msg ="Unexpected or no warning found\n";
        msg += "Expected: ";
        msg += expected;
        msg += "\nActual:  ";
        msg += calc.warnings();
	
        BOOST_FAIL(msg);
    }   
}

void
CalcExtMathTest::testCalcExtMathLogarithms()
{
    ostringstream pg("");
    
    pg << "O d, d;" << endl;
    pg << "L d, d;" << endl;
    pg << "C d, d;" << endl;
    pg << "V 2.71828, 10.0;" << endl;
    pg << "T;" << endl;
    pg << "CALL 'LN(L0, C0);" << endl;
    pg << "CALL 'LOG10(L1, C1);" << endl;
    pg << "REF O0, L0;" << endl;
    pg << "REF O1, L1;" << endl;

    Calculator calc;
    
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

    for(int i=0;i<2;i++) {
        BOOST_CHECK(fabs(*(reinterpret_cast<double*>
                           (const_cast<PBuffer>(outTuple[i].pData)))-1.0)<0.0001);
    }

}
    

void
CalcExtMathTest::testCalcExtMathLogarithmsFails()
{
    char buff[1024];
    const char* pg = 
        "O d;\n" 
        "L d;\n" 
        "C %s;\n" 
        "V %s;\n"
        "T;\n" 
        "CALL '%s(L0, C0);\n"
        "REF O0, L0;\n";

    char* tests[][3] = {
        {"LN","s8","0"},{"LN","d","0.0"},{"LN","s8","-1"},{"LN","d","-1.0"},
        {"LOG10","s8","0"},{"LOG10","d","0.0"},{"LOG10","s8","-1"},{"LOG10","d","-1.0"}
    };

    int n = sizeof(tests) / sizeof(tests[0]);
    for( int i=0; i<n; i++ ) {
        Calculator calc;
        sprintf(buff, pg, tests[i][1], tests[i][2], tests[i][0]);
        try {
            calc.assemble(buff);
        } catch (FennelExcn& ex) {
            BOOST_FAIL("Assemble exception " << ex.getMessage() << ex.what());
        }

        TupleDataWithBuffer outTuple(calc.getOutputRegisterDescriptor());
        TupleDataWithBuffer inTuple(calc.getInputRegisterDescriptor());

        calc.bind(&inTuple, &outTuple);
        checkWarnings(calc,"22023");
        if ( !outTuple.containsNull() ) {
            BOOST_FAIL("Result should be NULL");
        }
    }
}

void
CalcExtMathTest::testCalcExtMathAbs()
{
    ostringstream pg("");
    
    pg << "O d, s8;" << endl;
    pg << "L d, s8;" << endl;
    pg << "C d, s8;" << endl;
    pg << "V -10.0, -10;" << endl;
    pg << "T;" << endl;
    pg << "CALL 'ABS(L0, C0);" << endl;
    pg << "CALL 'ABS(L1, C1);" << endl;
    pg << "REF O0, L0;" << endl;
    pg << "REF O1, L1;" << endl;
    // BOOST_MESSAGE(pg.str());

    Calculator calc;
    
    try {
        calc.assemble(pg.str().c_str());
    }
    catch (FennelExcn& ex) {
        BOOST_FAIL("Assemble exception " << ex.getMessage() << pg.str());
    }

    TupleDataWithBuffer outTuple(calc.getOutputRegisterDescriptor());
    TupleDataWithBuffer inTuple(calc.getInputRegisterDescriptor());

    calc.bind(&inTuple, &outTuple);
    calc.exec();
    printOutput(outTuple, calc);


    BOOST_CHECK(fabs(*(reinterpret_cast<double*>
                       (const_cast<PBuffer>(outTuple[0].pData)))-10)<0.0001);
    
    BOOST_CHECK_EQUAL(*(reinterpret_cast<int*>
                        (const_cast<PBuffer>(outTuple[1].pData))),10);
}

void
CalcExtMathTest::testCalcExtMathPow()
{
    char buff[1024];


    char* tests[][4] = {
        {"d","d","2.0","2.2"}
        ,{"d","d","2.0","-2.2"} 
        ,{"d","d","-2.0","2.0"} 
    };

    double results[] = { 4.5947934, 0.21763764, 4};
    int n = sizeof(results) / sizeof(results[0]);
    assert( n == ( sizeof(tests) / sizeof(tests[0]) ));
    for( int i=0; i<n; i++) {
        sprintf(buff, mProgramPower.c_str(),
                tests[i][0], tests[i][1], tests[i][2], tests[i][3]);

        Calculator calc;
        try {
            calc.assemble(buff);
        }
        catch (FennelExcn& ex) {
            BOOST_FAIL("Assemble exception " << ex.getMessage() << ex.what() << buff);
        }

        TupleDataWithBuffer outTuple(calc.getOutputRegisterDescriptor());
        TupleDataWithBuffer inTuple(calc.getInputRegisterDescriptor());

        calc.bind(&inTuple, &outTuple);
        calc.exec();
        printOutput(outTuple, calc);

        BOOST_CHECK(fabs(*(reinterpret_cast<double*>
                           (const_cast<PBuffer>(outTuple[0].pData)))-results[i])<0.00001);
    }

}

void
CalcExtMathTest::testCalcExtMathPowFails()
{
    char buff[1024];

    char* tests[][4] = {
        {"d","d","0.0","-1.0"}
        ,{"d","d","-2.0","2.2"} 
        ,{"d","d","-2.0","-2.2"} 
    };
    
    int n = sizeof(tests) / sizeof(tests[0]);
    for( int i=0; i<n; i++ ) {
        Calculator calc;
        sprintf(buff, mProgramPower.c_str(),
                tests[i][0], tests[i][1], tests[i][2], tests[i][3]);
	
        try {
            calc.assemble(buff);
        } catch (FennelExcn& ex) {
            BOOST_FAIL("Assemble exception " << ex.getMessage() << ex.what());
        }

        TupleDataWithBuffer outTuple(calc.getOutputRegisterDescriptor());
        TupleDataWithBuffer inTuple(calc.getInputRegisterDescriptor());

        calc.bind(&inTuple, &outTuple);
        checkWarnings(calc,"22023");
        if ( !outTuple.containsNull() ) {
            BOOST_FAIL("Result should be NULL");
        }
    }
}


FENNEL_UNIT_TEST_SUITE(CalcExtMathTest);

