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


class CalcExtRegExpTest : virtual public TestBase, public TraceSource
{
    void testCalcExtRegExpLikeAVarChar();
    void testCalcExtRegExpLikeAChar();
    void testCalcExtRegExpSimilarAVarChar();
    void testCalcExtRegExpSimilarAChar();

    void likeHelper(TupleDataWithBuffer const & outTuple,
                    bool const * exp,
                    int validoutputs,
                    int nulloutputs,
                    deque<CalcMessage> dq);

    void similarHelper(TupleDataWithBuffer const & outTuple,
                       bool const * exp,
                       int validoutputs,
                       int nulloutputs,
                       deque<CalcMessage> dq);

    int cmpTupStr(TupleDatum const & tup,
                  char const * const str);
    int cmpTupInt(TupleDatum const & tup,
                  int val);
    int cmpTupBool(TupleDatum const & tup,
                   bool val);
    int cmpTupNull(TupleDatum const & tup);
    void printOutput(TupleData const & tup,
                     Calculator const & calc);
    void refLocalOutput(ostringstream& pg, 
                        int count);
    static char* truncErr;
    static char* substrErr;
    
public:
    explicit CalcExtRegExpTest()
        : TraceSource(this,"CalcExtRegExpTest")
    {
        srand(time(NULL));
        CalcInit::instance();
        FENNEL_UNIT_TEST_CASE(CalcExtRegExpTest, testCalcExtRegExpLikeAVarChar);
        FENNEL_UNIT_TEST_CASE(CalcExtRegExpTest, testCalcExtRegExpLikeAChar);
        FENNEL_UNIT_TEST_CASE(CalcExtRegExpTest, testCalcExtRegExpSimilarAVarChar);
        FENNEL_UNIT_TEST_CASE(CalcExtRegExpTest, testCalcExtRegExpSimilarAChar);
    }
     
    virtual ~CalcExtRegExpTest()
    {
    }
};

char *
CalcExtRegExpTest::truncErr = "22001";

char *
CalcExtRegExpTest::substrErr = "22011";

int
CalcExtRegExpTest::cmpTupStr(TupleDatum const & tup,
                             char const * const str)
{
    int len = strlen(str);
    BOOST_CHECK_EQUAL(len, tup.cbData);
    return strncmp(reinterpret_cast<char *>
                   (const_cast<PBuffer>(tup.pData)),
                   str,
                   len);
}

int
CalcExtRegExpTest::cmpTupInt(TupleDatum const & tup,
                             int val)
{
    return *(reinterpret_cast<int*>
             (const_cast<PBuffer>(tup.pData))) - val;
}

int
CalcExtRegExpTest::cmpTupBool(TupleDatum const & tup,
                              bool val)
{
    return *(reinterpret_cast<bool*>
             (const_cast<PBuffer>(tup.pData))) != val;
}

int
CalcExtRegExpTest::cmpTupNull(TupleDatum const & tup)
{
    if ((const_cast<PBuffer>(tup.pData)) == NULL) return 1;
    return 0;
}

// for nitty-gritty debugging. sadly, doesn't use BOOST_MESSAGE.
void
CalcExtRegExpTest::printOutput(TupleData const & tup,
                               Calculator const & calc)
{
#if 0
    TuplePrinter tuplePrinter;
    tuplePrinter.print(cout, calc.getOutputRegisterDescriptor(), tup);
    cout << endl;
#endif
}

// copy-by-reference locals into identical output register
void
CalcExtRegExpTest::refLocalOutput(ostringstream& pg, 
                                  int count)
{
    int i;
    
    for (i = 0; i < count; i++) {
        pg << "REF O" << i << ", L" << i << ";" << endl;
    }
}

void
CalcExtRegExpTest::likeHelper(TupleDataWithBuffer const & outTuple,
                              bool const * exp,
                              int validoutputs,
                              int nulloutputs,
                              deque<CalcMessage> dq)
{
    int i;
    deque<CalcMessage>::iterator iter = dq.begin();
    deque<CalcMessage>::iterator end = dq.end();
    
    for (i = 0; i < validoutputs; i++) {
        if (cmpTupBool(outTuple[i], exp[i])) {
            BOOST_MESSAGE("error on output [" << i << "]");
        }
        BOOST_CHECK_EQUAL(0, cmpTupBool(outTuple[i], exp[i]));
    }

    for (i = validoutputs; i < nulloutputs; i++) {
        if (!cmpTupNull(outTuple[i])) {
            BOOST_MESSAGE("error on output [" << i << "]");
        }
        BOOST_CHECK_EQUAL(1, cmpTupNull(outTuple[i]));
    }
    BOOST_CHECK(iter != end);
    BOOST_CHECK_EQUAL(iter->mPc, nulloutputs);
    BOOST_CHECK_EQUAL(0, strcmp(iter->mStr, "22019"));
    iter++;
    BOOST_CHECK(iter != end);

    BOOST_CHECK_EQUAL(iter->mPc, nulloutputs + 1);
    BOOST_CHECK_EQUAL(0, strcmp(iter->mStr, "22025"));
    iter++;
    BOOST_CHECK(iter == end);
                      
}



void
CalcExtRegExpTest::testCalcExtRegExpLikeAVarChar()
{
    ostringstream pg(""), outloc(""), constants("");
    int i;
    int outputs = 20;

    bool exp[40];
    BOOST_REQUIRE((sizeof(exp) / sizeof(bool)) >= outputs);

    for (i = 0; i < outputs - 1; i++) {
        outloc << "bo, ";
    }
    outloc << "bo;" << endl;
    // strings in [0-9], null in [10]

    for (i = 0; i <= 9; i++) {
        constants << "vc,5, ";
    }
    constants << "vc,5;" << endl;

    pg << "O " << outloc.str();
    pg << "L " << outloc.str();
    pg << "C " << constants.str();
    pg << "V 0x" << stringToHex("_");  // 0
    pg << ", 0x" << stringToHex("%");  // 1 
    pg << ", 0x" << stringToHex("=");  // 2
    pg << ", 0x" << stringToHex("=_"); // 3
    pg << ", 0x" << stringToHex("=%"); // 4
    pg << ", 0x" << stringToHex("ab"); // 5
    pg << ", 0x" << stringToHex("ba"); // 6
    pg << ", 0x" << stringToHex("a%"); // 7
    pg << ", 0x" << stringToHex("a_"); // 8
    pg << ", 0x" << stringToHex("");   // 9
    pg << ",;" << endl;                // 10
    pg << "T;" << endl;

    // try to intersperse true and false returns to
    // catch off-by-one errors in checking results.

    // (result, matchValue, pattern, escape)
    // no escape
    pg << "CALL 'strLikeA(L0, C5, C1, C9);" << endl; exp[0] = true;
    pg << "CALL 'strLikeA(L1, C5, C0, C9);" << endl; exp[1] = false;
    pg << "CALL 'strLikeA(L2, C5, C7, C9);" << endl; exp[2] = true;
    pg << "CALL 'strLikeA(L3, C6, C7, C9);" << endl; exp[3] = false;
    pg << "CALL 'strLikeA(L4, C5, C8, C9);" << endl; exp[4] = true;
    pg << "CALL 'strLikeA(L5, C6, C8, C9);" << endl; exp[5] = false;
    pg << "CALL 'strLikeA(L6, C0, C0, C9);" << endl; exp[6] = true;
    pg << "CALL 'strLikeA(L7, C0, C1, C9);" << endl; exp[7] = true;

    // escape
    pg << "CALL 'strLikeA(L8, C5, C0, C2);" << endl; exp[8] = false;
    pg << "CALL 'strLikeA(L9, C5, C1, C2);" << endl; exp[9] = true;
    pg << "CALL 'strLikeA(L10, C5, C3, C2);" << endl; exp[10] = false;
    pg << "CALL 'strLikeA(L11, C0, C3, C2);" << endl; exp[11] = true;
    pg << "CALL 'strLikeA(L12, C5, C4, C2);" << endl; exp[12] = false;
    pg << "CALL 'strLikeA(L13, C0, C4, C2);" << endl; exp[13] = false;
    pg << "CALL 'strLikeA(L14, C1, C4, C2);" << endl; exp[14] = true;
    int validoutputs = 15;

    // null cases
    pg << "CALL 'strLikeA(L15, C10, C1, C9);" << endl; 
    pg << "CALL 'strLikeA(L16, C5, C10, C9);" << endl;
    pg << "CALL 'strLikeA(L17, C5, C1, C10);" << endl;
    int nulloutputs = 18;
    
    // exception cases
    // 22019 -- invalid escape character, >1 char in escape
    pg << "CALL 'strLikeA(L18, C5, C1, C4);" << endl;
    // 22025 -- invalid escape sequence, end pattern w/escape
    pg << "CALL 'strLikeA(L19, C5, C2, C2);" << endl;


    refLocalOutput(pg, outputs);

    Calculator calc;
    
    try {
        calc.assemble(pg.str().c_str());
    }
    catch (FennelExcn& ex) {
        BOOST_MESSAGE("Assemble exception " << ex.getMessage());
        BOOST_MESSAGE(pg.str());
        BOOST_FAIL("assembler error");
    }

    TupleDataWithBuffer outTuple(calc.getOutputRegisterDescriptor());
    TupleDataWithBuffer inTuple(calc.getInputRegisterDescriptor());

    calc.bind(&inTuple, &outTuple);
    calc.exec();
    printOutput(outTuple, calc);

    likeHelper(outTuple, exp, validoutputs, nulloutputs,
               calc.mWarnings);
    

    // run twice to check that cached regex is, at least at first
    // glance, working correctly.
    calc.exec();
    likeHelper(outTuple, exp, validoutputs, nulloutputs,
               calc.mWarnings);
}

void
CalcExtRegExpTest::testCalcExtRegExpLikeAChar()
{
    ostringstream pg("");

    pg << "O bo, bo;" << endl;
    pg << "L bo, bo;" << endl;
    pg << "C c,1, c,2, c,0;" << endl;
    pg << "V 0x" << stringToHex("%");  // 0
    pg << ", 0x" << stringToHex("ab"); // 1 
    pg << ", 0x" << stringToHex("");   // 2
    pg << ";" << endl;
    pg << "T;" << endl;

    // (result, matchValue, pattern, escape)
    pg << "CALL 'strLikeA(L0, C1, C0, C2);" << endl;
    pg << "CALL 'strLikeA(L1, C0, C1, C2);" << endl;

    refLocalOutput(pg, 2);

    Calculator calc;
    
    try {
        calc.assemble(pg.str().c_str());
    }
    catch (FennelExcn& ex) {
        BOOST_MESSAGE("Assemble exception " << ex.getMessage());
        BOOST_MESSAGE(pg.str());
        BOOST_FAIL("assembler error");
    }

    TupleDataWithBuffer outTuple(calc.getOutputRegisterDescriptor());
    TupleDataWithBuffer inTuple(calc.getInputRegisterDescriptor());

    calc.bind(&inTuple, &outTuple);
    calc.exec();
    printOutput(outTuple, calc);

    BOOST_CHECK(calc.mWarnings.begin() == calc.mWarnings.end());
    BOOST_CHECK_EQUAL(0, cmpTupBool(outTuple[0], true));
    BOOST_CHECK_EQUAL(0, cmpTupBool(outTuple[1], false));
    
    // run twice to check that cached regex is, at least at first
    // glance, working correctly.
    calc.exec();
    BOOST_CHECK(calc.mWarnings.begin() == calc.mWarnings.end());
    BOOST_CHECK_EQUAL(0, cmpTupBool(outTuple[0], true));
    BOOST_CHECK_EQUAL(0, cmpTupBool(outTuple[1], false));

    
}

void
CalcExtRegExpTest::similarHelper(TupleDataWithBuffer const & outTuple,
                                 bool const * exp,
                                 int validoutputs,
                                 int nulloutputs,
                                 deque<CalcMessage> dq)
{
    int i;
    deque<CalcMessage>::iterator iter = dq.begin();
    deque<CalcMessage>::iterator end = dq.end();
    
    for (i = 0; i < validoutputs; i++) {
        if (cmpTupBool(outTuple[i], exp[i])) {
            BOOST_MESSAGE("error on output [" << i << "]");
        }
        BOOST_CHECK_EQUAL(0, cmpTupBool(outTuple[i], exp[i]));
    }

    for (i = validoutputs; i < nulloutputs; i++) {
        if (!cmpTupNull(outTuple[i])) {
            BOOST_MESSAGE("error on output [" << i << "]");
        }
        BOOST_CHECK_EQUAL(1, cmpTupNull(outTuple[i]));
    }
    int exceptionPc = nulloutputs;

    BOOST_CHECK(iter != end);
    BOOST_CHECK_EQUAL(iter->mPc, exceptionPc++);
    BOOST_CHECK_EQUAL(0, strcmp(iter->mStr, "2200B"));
    iter++;
    BOOST_CHECK(iter != end);

    BOOST_CHECK_EQUAL(iter->mPc, exceptionPc++);
    BOOST_CHECK_EQUAL(0, strcmp(iter->mStr, "22019"));
    iter++;
    BOOST_CHECK(iter != end);

    BOOST_CHECK_EQUAL(iter->mPc, exceptionPc++);
    BOOST_CHECK_EQUAL(0, strcmp(iter->mStr, "2201B"));
    iter++;
    BOOST_CHECK(iter != end);

    BOOST_CHECK_EQUAL(iter->mPc, exceptionPc++);
    BOOST_CHECK_EQUAL(0, strcmp(iter->mStr, "2200C"));
    iter++;

    BOOST_CHECK_EQUAL(iter->mPc, exceptionPc++);
    BOOST_CHECK_EQUAL(0, strcmp(iter->mStr, "2201B"));
    iter++;
    BOOST_CHECK(iter == end);
                      
}


void
CalcExtRegExpTest::testCalcExtRegExpSimilarAVarChar()
{
    ostringstream pg(""), outloc(""), constants("");
    int i;
    int outputs = 23;

    bool exp[40];
    BOOST_REQUIRE((sizeof(exp) / sizeof(bool)) >= outputs);

    for (i = 0; i < outputs - 1; i++) {
        outloc << "bo, ";
    }
    outloc << "bo;" << endl;
    // strings in [0-9], null in [10]

    for (i = 0; i <= 14; i++) {
        constants << "vc,20, ";
    }
    constants << "vc,20;" << endl;

    pg << "O " << outloc.str();
    pg << "L " << outloc.str();
    pg << "C " << constants.str();
    pg << "V 0x" << stringToHex("_");  // 0
    pg << ", 0x" << stringToHex("%");  // 1 
    pg << ", 0x" << stringToHex("=");  // 2
    pg << ", 0x" << stringToHex("=_"); // 3
    pg << ", 0x" << stringToHex("=%"); // 4
    pg << ", 0x" << stringToHex("ab"); // 5
    pg << ", 0x" << stringToHex("ba"); // 6
    pg << ", 0x" << stringToHex("a%"); // 7
    pg << ", 0x" << stringToHex("a_"); // 8
    pg << ", 0x" << stringToHex("");   // 9
    pg << ", 0x" << stringToHex(":");  // 10
    pg << ", 0x" << stringToHex("[[:ALPHA:]]");   // 11
    pg << ", 0x" << stringToHex("=a"); // 12
    pg << ", 0x" << stringToHex("%a"); // 13
    pg << ", 0x" << stringToHex("{}"); // 14
    pg << ",;" << endl;                // 15
    pg << "T;" << endl;

    // try to intersperse true and false returns to
    // catch off-by-one errors in checking results.

    // (result, matchValue, pattern, escape)
    // no escape
    pg << "CALL 'strSimilarA(L0, C5, C1, C9);" << endl; exp[0] = true;
    pg << "CALL 'strSimilarA(L1, C5, C0, C9);" << endl; exp[1] = false;
    pg << "CALL 'strSimilarA(L2, C5, C7, C9);" << endl; exp[2] = true;
    pg << "CALL 'strSimilarA(L3, C6, C7, C9);" << endl; exp[3] = false;
    pg << "CALL 'strSimilarA(L4, C5, C8, C9);" << endl; exp[4] = true;
    pg << "CALL 'strSimilarA(L5, C6, C8, C9);" << endl; exp[5] = false;
    pg << "CALL 'strSimilarA(L6, C0, C0, C9);" << endl; exp[6] = true;
    pg << "CALL 'strSimilarA(L7, C0, C1, C9);" << endl; exp[7] = true;

    // escape
    pg << "CALL 'strSimilarA(L8, C5, C0, C2);" << endl; exp[8] = false;
    pg << "CALL 'strSimilarA(L9, C5, C1, C2);" << endl; exp[9] = true;
    pg << "CALL 'strSimilarA(L10, C5, C3, C2);" << endl; exp[10] = false;
    pg << "CALL 'strSimilarA(L11, C0, C3, C2);" << endl; exp[11] = true;
    pg << "CALL 'strSimilarA(L12, C5, C4, C2);" << endl; exp[12] = false;
    pg << "CALL 'strSimilarA(L13, C0, C4, C2);" << endl; exp[13] = false;
    pg << "CALL 'strSimilarA(L14, C1, C4, C2);" << endl; exp[14] = true;
    int validoutputs = 15;

    // null cases
    pg << "CALL 'strSimilarA(L15, C15, C1, C9);" << endl; 
    pg << "CALL 'strSimilarA(L16, C5, C15, C9);" << endl;
    pg << "CALL 'strSimilarA(L17, C5, C1, C15);" << endl;
    int nulloutputs = 18;
    
    // exception cases
    // 2200B -- escape character conflict (: as escape)
    pg << "CALL 'strSimilarA(L18, C5, C11, C10);" << endl;
    // 22019 -- invalid escape character, >1 char in escape
    pg << "CALL 'strSimilarA(L19, C5, C1, C4);" << endl;
    // 2201B -- invalid regular expression, end pattern w/escape
    pg << "CALL 'strSimilarA(L20, C5, C2, C2);" << endl;
    // 2201C -- invalid use of escape character, special char is escape & used
    pg << "CALL 'strSimilarA(L21, C5, C13, C1);" << endl;
    // 2201B -- invalid regular expression, caught by regex, not SqlSimilarPrep
    pg << "CALL 'strSimilarA(L22, C5, C14, C1);" << endl;


    refLocalOutput(pg, outputs);

    Calculator calc;
    
    try {
        calc.assemble(pg.str().c_str());
    }
    catch (FennelExcn& ex) {
        BOOST_MESSAGE("Assemble exception " << ex.getMessage());
        BOOST_MESSAGE(pg.str());
        BOOST_FAIL("assembler error");
    }

    TupleDataWithBuffer outTuple(calc.getOutputRegisterDescriptor());
    TupleDataWithBuffer inTuple(calc.getInputRegisterDescriptor());

    calc.bind(&inTuple, &outTuple);
    calc.exec();
    printOutput(outTuple, calc);

    similarHelper(outTuple, exp, validoutputs, nulloutputs,
                  calc.mWarnings);
    

    // run twice to check that cached regex is, at least at first
    // glance, working correctly.
    calc.exec();
    similarHelper(outTuple, exp, validoutputs, nulloutputs,
                  calc.mWarnings);
}

void
CalcExtRegExpTest::testCalcExtRegExpSimilarAChar()
{
    ostringstream pg("");

    pg << "O bo, bo;" << endl;
    pg << "L bo, bo;" << endl;
    pg << "C c,1, c,2, c,0;" << endl;
    pg << "V 0x" << stringToHex("%");  // 0
    pg << ", 0x" << stringToHex("ab"); // 1 
    pg << ", 0x" << stringToHex("");   // 2
    pg << ";" << endl;
    pg << "T;" << endl;

    // (result, matchValue, pattern, escape)
    pg << "CALL 'strSimilarA(L0, C1, C0, C2);" << endl;
    pg << "CALL 'strSimilarA(L1, C0, C1, C2);" << endl;

    refLocalOutput(pg, 2);

    Calculator calc;
    
    try {
        calc.assemble(pg.str().c_str());
    }
    catch (FennelExcn& ex) {
        BOOST_MESSAGE("Assemble exception " << ex.getMessage());
        BOOST_MESSAGE(pg.str());
        BOOST_FAIL("assembler error");
    }

    TupleDataWithBuffer outTuple(calc.getOutputRegisterDescriptor());
    TupleDataWithBuffer inTuple(calc.getInputRegisterDescriptor());

    calc.bind(&inTuple, &outTuple);
    calc.exec();
    printOutput(outTuple, calc);

    BOOST_CHECK(calc.mWarnings.begin() == calc.mWarnings.end());
    BOOST_CHECK_EQUAL(0, cmpTupBool(outTuple[0], true));
    BOOST_CHECK_EQUAL(0, cmpTupBool(outTuple[1], false));
    
    // run twice to check that cached regex is, at least at first
    // glance, working correctly.
    calc.exec();
    BOOST_CHECK(calc.mWarnings.begin() == calc.mWarnings.end());
    BOOST_CHECK_EQUAL(0, cmpTupBool(outTuple[0], true));
    BOOST_CHECK_EQUAL(0, cmpTupBool(outTuple[1], false));

    
}


FENNEL_UNIT_TEST_SUITE(CalcExtRegExpTest);

