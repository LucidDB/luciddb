/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2004-2005 Disruptive Tech
// Copyright (C) 2005-2005 The Eigenbase Project
//
// This program is free software; you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation; either version 2 of the License, or
// (at your option) any later Eigenbase-approved version.
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
#include "fennel/disruptivetech/calc/StringToHex.h"
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
            BOOST_MESSAGE("error on valid output [" << i << "]");
        }
        BOOST_CHECK_EQUAL(0, cmpTupBool(outTuple[i], exp[i]));
    }

    for (i = validoutputs; i < nulloutputs; i++) {
        if (!cmpTupNull(outTuple[i])) {
            BOOST_MESSAGE("error on null output [" << i << "]");
        }
        BOOST_CHECK_EQUAL(1, cmpTupNull(outTuple[i]));
    }
    BOOST_CHECK(iter != end);
    BOOST_CHECK_EQUAL(iter->pc, nulloutputs);
    BOOST_CHECK_EQUAL(0, strcmp(iter->str, "22019"));
    iter++;
    BOOST_CHECK(iter != end);

    BOOST_CHECK(iter != end);
    BOOST_CHECK_EQUAL(iter->pc, nulloutputs + 1);
    BOOST_CHECK_EQUAL(0, strcmp(iter->str, "22019"));
    iter++;
    BOOST_CHECK(iter != end);

    BOOST_CHECK_EQUAL(iter->pc, nulloutputs + 2);
    BOOST_CHECK_EQUAL(0, strcmp(iter->str, "22025"));
    iter++;
    BOOST_CHECK(iter == end);
                      
}



void
CalcExtRegExpTest::testCalcExtRegExpLikeAVarChar()
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
    pg << "CALL 'strLikeA3(L0, C5, C1);" << endl; exp[0] = true;
    pg << "CALL 'strLikeA3(L1, C5, C0);" << endl; exp[1] = false;
    pg << "CALL 'strLikeA3(L2, C5, C7);" << endl; exp[2] = true;
    pg << "CALL 'strLikeA3(L3, C6, C7);" << endl; exp[3] = false;
    pg << "CALL 'strLikeA3(L4, C5, C8);" << endl; exp[4] = true;
    pg << "CALL 'strLikeA3(L5, C6, C8);" << endl; exp[5] = false;
    pg << "CALL 'strLikeA3(L6, C0, C0);" << endl; exp[6] = true;
    pg << "CALL 'strLikeA3(L7, C0, C1);" << endl; exp[7] = true;

    // escape
    pg << "CALL 'strLikeA4(L8, C5, C0, C2);" << endl; exp[8] = false;
    pg << "CALL 'strLikeA4(L9, C5, C1, C2);" << endl; exp[9] = true;
    pg << "CALL 'strLikeA4(L10, C5, C3, C2);" << endl; exp[10] = false;
    pg << "CALL 'strLikeA4(L11, C0, C3, C2);" << endl; exp[11] = true;
    pg << "CALL 'strLikeA4(L12, C5, C4, C2);" << endl; exp[12] = false;
    pg << "CALL 'strLikeA4(L13, C0, C4, C2);" << endl; exp[13] = false;
    pg << "CALL 'strLikeA4(L14, C1, C4, C2);" << endl; exp[14] = true;
    int validoutputs = 15;

    // null cases no escape
    pg << "CALL 'strLikeA3(L15, C10, C1);" << endl; 
    pg << "CALL 'strLikeA3(L16, C5, C10);" << endl;

    // null cases escape
    pg << "CALL 'strLikeA4(L17, C10, C1, C2);" << endl; 
    pg << "CALL 'strLikeA4(L18, C5, C10, C2);" << endl;
    pg << "CALL 'strLikeA4(L19, C5, C1, C10);" << endl;
    int nulloutputs = 20;
    
    // exception cases
    // 22019 -- invalid escape character, >1 char in escape
    pg << "CALL 'strLikeA4(L20, C5, C1, C4);" << endl;
    // 22019 -- invalid escape character, 0 char in escape
    pg << "CALL 'strLikeA4(L21, C5, C1, C9);" << endl;
    // 22025 -- invalid escape sequence, end pattern w/escape
    pg << "CALL 'strLikeA4(L22, C5, C2, C2);" << endl;


    refLocalOutput(pg, outputs);

    Calculator calc(0);
    
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

    pg << "O bo, bo, bo, bo, bo, bo, bo, bo;" << endl;
    pg << "L bo, bo, bo, bo, bo, bo, bo, bo;" << endl;
    pg << "C c,1, c,2, c,1, vc,1, vc,2, vc,1;" << endl;
    pg << "V 0x" << stringToHex("%");  // 0
    pg << ", 0x" << stringToHex("ab"); // 1 
    pg << ", 0x" << stringToHex("=");  // 2
    pg << ", 0x" << stringToHex("%");  // 3
    pg << ", 0x" << stringToHex("ab"); // 4 
    pg << ", 0x" << stringToHex("=");  // 5
    pg << ";" << endl;
    pg << "T;" << endl;

    // all char case: (result, matchValue, pattern, escape)
    pg << "CALL 'strLikeA4(L0, C1, C0, C2);" << endl;  // true
    pg << "CALL 'strLikeA4(L1, C0, C1, C2);" << endl;  // false

    // all char case: (result, matchValue, pattern)
    pg << "CALL 'strLikeA3(L2, C1, C0);" << endl;  // true
    pg << "CALL 'strLikeA3(L3, C0, C1);" << endl;  // false

    // mixed char/var cases: (result, matchValue, pattern, escape)
    pg << "CALL 'strLikeA4(L4, C1, C3, C2);" << endl;  // true
    pg << "CALL 'strLikeA4(L5, C4, C0, C2);" << endl;  // true

    // all char case: (result, matchValue, pattern)
    pg << "CALL 'strLikeA3(L6, C1, C3);" << endl;  // true
    pg << "CALL 'strLikeA3(L7, C4, C0);" << endl;  // true

    refLocalOutput(pg, 8);

    Calculator calc(0);
    
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

    int i;
    BOOST_CHECK(calc.mWarnings.begin() == calc.mWarnings.end());
    for (i = 0; i < 8; i++) {
        if (i == 1 || i == 3) {
            BOOST_CHECK_EQUAL(0, cmpTupBool(outTuple[i], false));
        } else {
            BOOST_CHECK_EQUAL(0, cmpTupBool(outTuple[i], true));
        }
    }
    
    // run twice to check that cached regex is, at least at first
    // glance, working correctly.
    calc.exec();
    BOOST_CHECK(calc.mWarnings.begin() == calc.mWarnings.end());
    for (i = 0; i < 8; i++) {
        if (i == 1 || i == 3) {
            BOOST_CHECK_EQUAL(0, cmpTupBool(outTuple[i], false));
        } else {
            BOOST_CHECK_EQUAL(0, cmpTupBool(outTuple[i], true));
        }
    }
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
            BOOST_MESSAGE("error on valid output [" << i << "]");
        }
        BOOST_CHECK_EQUAL(0, cmpTupBool(outTuple[i], exp[i]));
    }

    for (i = validoutputs; i < nulloutputs; i++) {
        if (!cmpTupNull(outTuple[i])) {
            BOOST_MESSAGE("error on null output [" << i << "]");
        }
        BOOST_CHECK_EQUAL(1, cmpTupNull(outTuple[i]));
    }
    int exceptionPc = nulloutputs;

    BOOST_CHECK(iter != end);
    BOOST_CHECK_EQUAL(iter->pc, exceptionPc++);
    BOOST_CHECK_EQUAL(0, strcmp(iter->str, "2200B"));
    iter++;
    BOOST_CHECK(iter != end);

    BOOST_CHECK_EQUAL(iter->pc, exceptionPc++);
    BOOST_CHECK_EQUAL(0, strcmp(iter->str, "22019"));
    iter++;
    BOOST_CHECK(iter != end);

    BOOST_CHECK_EQUAL(iter->pc, exceptionPc++);
    BOOST_CHECK_EQUAL(0, strcmp(iter->str, "22019"));
    iter++;
    BOOST_CHECK(iter != end);

    BOOST_CHECK_EQUAL(iter->pc, exceptionPc++);
    BOOST_CHECK_EQUAL(0, strcmp(iter->str, "2201B"));
    iter++;
    BOOST_CHECK(iter != end);

    BOOST_CHECK_EQUAL(iter->pc, exceptionPc++);
    BOOST_CHECK_EQUAL(0, strcmp(iter->str, "2200C"));
    iter++;

    BOOST_CHECK_EQUAL(iter->pc, exceptionPc++);
    BOOST_CHECK_EQUAL(0, strcmp(iter->str, "2201B"));
    iter++;
    BOOST_CHECK(iter == end);
}


void
CalcExtRegExpTest::testCalcExtRegExpSimilarAVarChar()
{
    ostringstream pg(""), outloc(""), constants("");
    int i;
    int outputs = 26;

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
    pg << "CALL 'strSimilarA3(L0, C5, C1);" << endl; exp[0] = true;
    pg << "CALL 'strSimilarA3(L1, C5, C0);" << endl; exp[1] = false;
    pg << "CALL 'strSimilarA3(L2, C5, C7);" << endl; exp[2] = true;
    pg << "CALL 'strSimilarA3(L3, C6, C7);" << endl; exp[3] = false;
    pg << "CALL 'strSimilarA3(L4, C5, C8);" << endl; exp[4] = true;
    pg << "CALL 'strSimilarA3(L5, C6, C8);" << endl; exp[5] = false;
    pg << "CALL 'strSimilarA3(L6, C0, C0);" << endl; exp[6] = true;
    pg << "CALL 'strSimilarA3(L7, C0, C1);" << endl; exp[7] = true;

    // escape
    pg << "CALL 'strSimilarA4(L8, C5, C0, C2);" << endl; exp[8] = false;
    pg << "CALL 'strSimilarA4(L9, C5, C1, C2);" << endl; exp[9] = true;
    pg << "CALL 'strSimilarA4(L10, C5, C3, C2);" << endl; exp[10] = false;
    pg << "CALL 'strSimilarA4(L11, C0, C3, C2);" << endl; exp[11] = true;
    pg << "CALL 'strSimilarA4(L12, C5, C4, C2);" << endl; exp[12] = false;
    pg << "CALL 'strSimilarA4(L13, C0, C4, C2);" << endl; exp[13] = false;
    pg << "CALL 'strSimilarA4(L14, C1, C4, C2);" << endl; exp[14] = true;
    int validoutputs = 15;

    // null cases
    pg << "CALL 'strSimilarA3(L15, C15, C1);" << endl; 
    pg << "CALL 'strSimilarA3(L16, C5, C15);" << endl;

    pg << "CALL 'strSimilarA4(L17, C15, C1, C9);" << endl; 
    pg << "CALL 'strSimilarA4(L18, C5, C15, C9);" << endl;
    pg << "CALL 'strSimilarA4(L19, C5, C1, C15);" << endl;
    int nulloutputs = 20;
    
    // exception cases
    // 2200B -- escape character conflict (: as escape)
    pg << "CALL 'strSimilarA4(L20, C5, C11, C10);" << endl;
    // 22019 -- invalid escape character, >1 char in escape
    pg << "CALL 'strSimilarA4(L21, C5, C1, C4);" << endl;
    // 22019 -- invalid escape character, 0 char in escape
    pg << "CALL 'strSimilarA4(L22, C5, C1, C9);" << endl;
    // 2201B -- invalid regular expression, end pattern w/escape
    pg << "CALL 'strSimilarA4(L23, C5, C2, C2);" << endl;
    // 2201C -- invalid use of escape character, special char is escape & used
    pg << "CALL 'strSimilarA4(L24, C5, C13, C1);" << endl;
    // 2201B -- invalid regular expression, caught by regex, not SqlSimilarPrep
    pg << "CALL 'strSimilarA4(L25, C5, C14, C1);" << endl;


    refLocalOutput(pg, outputs);

    Calculator calc(0);
    
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

    pg << "O bo, bo, bo, bo, bo, bo, bo, bo;" << endl;
    pg << "L bo, bo, bo, bo, bo, bo, bo, bo;" << endl;
    pg << "C c,1, c,2, c,1, vc,1, vc,2, vc,1 ;" << endl;
    pg << "V 0x" << stringToHex("%");  // 0
    pg << ", 0x" << stringToHex("ab"); // 1 
    pg << ", 0x" << stringToHex("=");  // 2
    pg << ", 0x" << stringToHex("%");  // 3
    pg << ", 0x" << stringToHex("ab"); // 4 
    pg << ", 0x" << stringToHex("=");  // 5
    pg << ";" << endl;
    pg << "T;" << endl;

    // all char case: (result, matchValue, pattern, escape)
    pg << "CALL 'strSimilarA4(L0, C1, C0, C2);" << endl;
    pg << "CALL 'strSimilarA4(L1, C0, C1, C2);" << endl;

    // all char case: (result, matchValue, pattern)
    pg << "CALL 'strSimilarA3(L2, C1, C0);" << endl;
    pg << "CALL 'strSimilarA3(L3, C0, C1);" << endl;

    // mixed char/var cases: (result, matchValue, pattern, escape)
    pg << "CALL 'strSimilarA4(L4, C1, C3, C2);" << endl;  // true
    pg << "CALL 'strSimilarA4(L5, C4, C0, C2);" << endl;  // true

    // all char case: (result, matchValue, pattern)
    pg << "CALL 'strSimilarA3(L6, C1, C3);" << endl;  // true
    pg << "CALL 'strSimilarA3(L7, C4, C0);" << endl;  // true

    refLocalOutput(pg, 8);

    Calculator calc(0);
    
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

    int i;
    BOOST_CHECK(calc.mWarnings.begin() == calc.mWarnings.end());
    for (i = 0; i < 8; i++) {
        if (i == 1 || i == 3) {
            BOOST_CHECK_EQUAL(0, cmpTupBool(outTuple[i], false));
        } else {
            BOOST_CHECK_EQUAL(0, cmpTupBool(outTuple[i], true));
        }
    }
    
    // run twice to check that cached regex is, at least at first
    // glance, working correctly.
    calc.exec();
    BOOST_CHECK(calc.mWarnings.begin() == calc.mWarnings.end());
    for (i = 0; i < 8; i++) {
        if (i == 1 || i == 3) {
            BOOST_CHECK_EQUAL(0, cmpTupBool(outTuple[i], false));
        } else {
            BOOST_CHECK_EQUAL(0, cmpTupBool(outTuple[i], true));
        }
    }
}


FENNEL_UNIT_TEST_SUITE(CalcExtRegExpTest);

