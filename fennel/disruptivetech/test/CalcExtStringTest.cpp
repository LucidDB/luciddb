/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2004-2005 Disruptive Tech
// Copyright (C) 2005-2005 The Eigenbase Project
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
#include "fennel/disruptivetech/calc/StringToHex.h"
#include "fennel/common/FennelExcn.h"

#include <boost/test/test_tools.hpp>
#include <boost/scoped_array.hpp>
#include <string>
#include <limits>


using namespace fennel;
using namespace std;


class CalcExtStringTest : virtual public TestBase, public TraceSource
{
    void testCalcExtStringCatA2();
    void testCalcExtStringCatA3();
    void testCalcExtStringCmpA();
    void testCalcExtStringCmpOct();
    void testCalcExtStringLenBitA();
    void testCalcExtStringLenCharA();
    void testCalcExtStringLenOctA();
    void testCalcExtStringOverlayA4();
    void testCalcExtStringOverlayA5();
    void testCalcExtStringPosA();
    void testCalcExtStringSubStringA3();
    void testCalcExtStringSubStringA4();
    void testCalcExtStringToANull();
    void testCalcExtStringToLower();
    void testCalcExtStringToUpper();
    void testCalcExtStringTrim();

    int cmpTupStr(TupleDatum const & tup,
                  char const * const str);
    int cmpTupInt(TupleDatum const & tup,
                  int val);
    int cmpTupNull(TupleDatum const & tup);
    void printOutput(TupleData const & tup,
                     Calculator const & calc);
    void refLocalOutput(ostringstream& pg, 
                        int count);
    static char* truncErr;
    static char* substrErr;
    
public:
    explicit CalcExtStringTest()
        : TraceSource(shared_from_this(),"CalcExtStringTest")
    {
        srand(time(NULL));
        CalcInit::instance();
        FENNEL_UNIT_TEST_CASE(CalcExtStringTest, testCalcExtStringCatA2);
        FENNEL_UNIT_TEST_CASE(CalcExtStringTest, testCalcExtStringCatA3);
        FENNEL_UNIT_TEST_CASE(CalcExtStringTest, testCalcExtStringCmpA);
        FENNEL_UNIT_TEST_CASE(CalcExtStringTest, testCalcExtStringCmpOct);
        FENNEL_UNIT_TEST_CASE(CalcExtStringTest, testCalcExtStringLenBitA);
        FENNEL_UNIT_TEST_CASE(CalcExtStringTest, testCalcExtStringLenCharA);
        FENNEL_UNIT_TEST_CASE(CalcExtStringTest, testCalcExtStringLenOctA);
        FENNEL_UNIT_TEST_CASE(CalcExtStringTest, testCalcExtStringOverlayA4);
        FENNEL_UNIT_TEST_CASE(CalcExtStringTest, testCalcExtStringOverlayA5);
        FENNEL_UNIT_TEST_CASE(CalcExtStringTest, testCalcExtStringPosA);
        FENNEL_UNIT_TEST_CASE(CalcExtStringTest, testCalcExtStringSubStringA3);
        FENNEL_UNIT_TEST_CASE(CalcExtStringTest, testCalcExtStringSubStringA4);
        FENNEL_UNIT_TEST_CASE(CalcExtStringTest, testCalcExtStringToANull);
        FENNEL_UNIT_TEST_CASE(CalcExtStringTest, testCalcExtStringToLower);
        FENNEL_UNIT_TEST_CASE(CalcExtStringTest, testCalcExtStringToUpper);
        FENNEL_UNIT_TEST_CASE(CalcExtStringTest, testCalcExtStringTrim);

    }
     
    virtual ~CalcExtStringTest()
    {
    }
};

char *
CalcExtStringTest::truncErr = "22001";

char *
CalcExtStringTest::substrErr = "22011";

int
CalcExtStringTest::cmpTupStr(TupleDatum const & tup,
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
CalcExtStringTest::cmpTupInt(TupleDatum const & tup,
                             int val)
{
    return *(reinterpret_cast<int*>
             (const_cast<PBuffer>(tup.pData))) - val;
}

int
CalcExtStringTest::cmpTupNull(TupleDatum const & tup)
{
    if ((const_cast<PBuffer>(tup.pData)) == NULL) return 1;
    return 0;
}

// for nitty-gritty debugging. sadly, doesn't use BOOST_MESSAGE.
void
CalcExtStringTest::printOutput(TupleData const & tup,
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
CalcExtStringTest::refLocalOutput(ostringstream& pg, 
                                  int count)
{
    int i;
    
    for (i = 0; i < count; i++) {
        pg << "REF O" << i << ", L" << i << ";" << endl;
    }
}


void
CalcExtStringTest::testCalcExtStringCatA2()
{
    ostringstream pg(""), outloc("");
    int i;

    for (i = 0; i <= 2; i++) {
        outloc << "vc,5, ";
    }
    outloc << "c,5, ";
    outloc << "vc,1, c,1;" << endl;    // right truncate

    pg << "O " << outloc.str();
    pg << "L " << outloc.str();
    pg << "C vc,5, vc,2, vc,2, ";  // varchar data[0-2]
    pg << "c,5, ";                 // char data[3] 
    pg << "vc,5, c,5;" << endl;    // nulls[4-5]
    pg << "V 0x" << stringToHex("AB");
    pg << ", 0x" << stringToHex("CD");
    pg << ", 0x" << stringToHex("");
    pg << ", 0x" << stringToHex("GHIJ ");
    pg << ",,;" << endl;
    pg << "T;" << endl;
    // varchar common case
    // first, clear output string
    pg << "CALL 'strCpyA(L0, C2);" << endl;
    pg << "CALL 'strCatA2(L0, C0);" << endl;
    // append to first string
    pg << "CALL 'strCatA2(L0, C1);" << endl;
    // zero length case
    // first, clear output string
    pg << "CALL 'strCpyA(L1, C2);" << endl;
    pg << "CALL 'strCatA2(L1, C2);" << endl;
    // varchar null case
    pg << "CALL 'strCatA2(L2, C4);" << endl;
    // varchar right truncate
    pg << "CALL 'strCatA2(L4, C0);" << endl;

    // char common case: can't test fixed common case w/o
    // using strCatAF3 as well to have length set 
    // correctly. tested elsewhere
    // char null case
    pg << "CALL 'strCatA2(L3, C5);" << endl;
    // char right truncate
    pg << "CALL 'strCatA2(L5, C3);" << endl;
    // make output available
    refLocalOutput(pg, 6);

    Calculator calc(0);
    
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
    printOutput(outTuple, calc);
    deque<CalcMessage>::iterator iter = calc.mWarnings.begin();

    // varchar common case
    BOOST_CHECK_EQUAL(0, cmpTupStr(outTuple[0], "ABCD"));
    // varchar zero length case
    BOOST_CHECK_EQUAL(0, cmpTupStr(outTuple[1], ""));
    // varchar null case
    BOOST_CHECK_EQUAL(1, cmpTupNull(outTuple[2]));
    // varchar right truncation
    BOOST_CHECK_EQUAL(iter->pc, 6);
    BOOST_CHECK_EQUAL(0, strcmp(iter->str, truncErr));
    iter++;
    // char null case
    BOOST_CHECK_EQUAL(1, cmpTupNull(outTuple[3]));
    // char right truncation
    BOOST_CHECK_EQUAL(iter->pc, 8);
    BOOST_CHECK_EQUAL(0, strcmp(iter->str, truncErr));
    iter++;
    BOOST_CHECK(iter == calc.mWarnings.end());
}
void
CalcExtStringTest::testCalcExtStringCatA3()
{
    ostringstream pg(""), outloc("");
    int i;

    for (i = 0; i <= 3; i++) {
        outloc << "vc,5, ";
    }
    for (i = 4; i <= 6; i++) {
        outloc << "c,5, ";
    }
    outloc << "vc,1, c,1;" << endl;    // right truncate

    pg << "O " << outloc.str();
    pg << "L " << outloc.str();
    pg << "C vc,5, vc,2, vc,2, ";  // varchar data[0-2]
    pg << "c,3, c,2, ";            // char data[3-4]
    pg << "vc,5, c,5;" << endl;    // nulls[5-6]
    pg << "V 0x" << stringToHex("AB");
    pg << ", 0x" << stringToHex("CD");
    pg << ", 0x" << stringToHex("");
    pg << ", 0x" << stringToHex("GHI");
    pg << ", 0x" << stringToHex("JK");
    pg << ",,;" << endl;
    pg << "T;" << endl;
    // varchar common case
    pg << "CALL 'strCatA3(L0, C0, C1);" << endl;
    // zero length case
    pg << "CALL 'strCatA3(L1, C2, C2);" << endl;
    // varchar null cases
    pg << "CALL 'strCatA3(L2, C5, C1);" << endl;
    pg << "CALL 'strCatA3(L3, C0, C5);" << endl;
    // varchar right truncate
    pg << "CALL 'strCatA3(L7, C0, C0);" << endl;

    // char common case
    pg << "CALL 'strCatA3(L4, C3, C4);" << endl;
    // char null cases
    pg << "CALL 'strCatA3(L5, C6, C4);" << endl;
    pg << "CALL 'strCatA3(L6, C3, C6);" << endl;
    // char right truncate
    pg << "CALL 'strCatA3(L8, C3, C4);" << endl;
    // make output available
    refLocalOutput(pg, 9);

    Calculator calc(0);
    
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
    printOutput(outTuple, calc);
    deque<CalcMessage>::iterator iter = calc.mWarnings.begin();

    // varchar common case
    BOOST_CHECK_EQUAL(0, cmpTupStr(outTuple[0], "ABCD"));
    // varchar zero length case
    BOOST_CHECK_EQUAL(0, cmpTupStr(outTuple[1], ""));
    // varchar null cases
    BOOST_CHECK_EQUAL(1, cmpTupNull(outTuple[2]));
    BOOST_CHECK_EQUAL(1, cmpTupNull(outTuple[3]));
    // varchar right truncation
    BOOST_CHECK_EQUAL(iter->pc, 4);
    BOOST_CHECK_EQUAL(0, strcmp(iter->str, truncErr));
    iter++;
    // char common case
    BOOST_CHECK_EQUAL(0, cmpTupStr(outTuple[4], "GHIJK"));
    // char null cases
    BOOST_CHECK_EQUAL(1, cmpTupNull(outTuple[5]));
    BOOST_CHECK_EQUAL(1, cmpTupNull(outTuple[6]));
    // char right truncation
    BOOST_CHECK_EQUAL(iter->pc, 8);
    BOOST_CHECK_EQUAL(0, strcmp(iter->str, truncErr));
    iter++;
    BOOST_CHECK(iter == calc.mWarnings.end());
}

void
CalcExtStringTest::testCalcExtStringCmpA()
{
    ostringstream pg(""), outloc("");
    int i;

    for (i = 0; i <= 11; i++) {
        outloc << "s4, ";
    }
    outloc << "s4;" << endl;

    pg << "O " << outloc.str();
    pg << "L " << outloc.str();
    pg << "C vc,2, vc,2, vc,2, "; // varchar data[0-2]
    pg << "c,2, c,2, c,2, ";      // char data[3-5]
    pg << "vc,5, c,5;" << endl;  // nulls[6-7]
    pg << "V 0x" << stringToHex("AB");
    pg << ", 0x" << stringToHex("CD");
    pg << ", 0x" << stringToHex("EF");
    pg << ", 0x" << stringToHex("GH");
    pg << ", 0x" << stringToHex("IJ");
    pg << ", 0x" << stringToHex("EF");
    pg << ",,;" << endl;
    pg << "T;" << endl;
    // varchar common cases
    pg << "CALL 'strCmpA(L0, C0, C0);" << endl;
    pg << "CALL 'strCmpA(L1, C1, C0);" << endl;
    pg << "CALL 'strCmpA(L2, C0, C1);" << endl;
    // varchar null cases
    pg << "CALL 'strCmpA(L3, C6, C0);" << endl;
    pg << "CALL 'strCmpA(L4, C0, C6);" << endl;
    // char common cases
    pg << "CALL 'strCmpA(L5, C3, C3);" << endl;
    pg << "CALL 'strCmpA(L6, C4, C3);" << endl;
    pg << "CALL 'strCmpA(L7, C3, C4);" << endl;
    // char null cases
    pg << "CALL 'strCmpA(L8, C7, C2);" << endl;
    pg << "CALL 'strCmpA(L9, C2, C7);" << endl;
    // mixed common cases
    pg << "CALL 'strCmpA(L10, C2, C5);" << endl;
    pg << "CALL 'strCmpA(L11, C0, C3);" << endl;
    pg << "CALL 'strCmpA(L12, C3, C0);" << endl;

    // make output available
    refLocalOutput(pg, 13);

    Calculator calc(0);
    
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
    printOutput(outTuple, calc);

    // varchar common cases
    BOOST_CHECK_EQUAL(0, cmpTupInt(outTuple[0], 0));
    BOOST_CHECK_EQUAL(0, cmpTupInt(outTuple[1], 1));
    BOOST_CHECK_EQUAL(0, cmpTupInt(outTuple[2], -1));
    // varchar null cases
    BOOST_CHECK_EQUAL(1, cmpTupNull(outTuple[3]));
    BOOST_CHECK_EQUAL(1, cmpTupNull(outTuple[4]));
    // char common case
    BOOST_CHECK_EQUAL(0, cmpTupInt(outTuple[5], 0));
    BOOST_CHECK_EQUAL(0, cmpTupInt(outTuple[6], 1));
    BOOST_CHECK_EQUAL(0, cmpTupInt(outTuple[7], -1));
    // char null cases
    BOOST_CHECK_EQUAL(1, cmpTupNull(outTuple[8]));
    BOOST_CHECK_EQUAL(1, cmpTupNull(outTuple[9]));
    // mixed cases
    BOOST_CHECK_EQUAL(0, cmpTupInt(outTuple[10], 0));
    BOOST_CHECK_EQUAL(0, cmpTupInt(outTuple[11], -1));
    BOOST_CHECK_EQUAL(0, cmpTupInt(outTuple[12], 1));
}

void
CalcExtStringTest::testCalcExtStringCmpOct()
{
    ostringstream pg(""), outloc("");
    int i;

    for (i = 0; i <= 11; i++) {
        outloc << "s4, ";
    }
    outloc << "s4;" << endl;

    pg << "O " << outloc.str();
    pg << "L " << outloc.str();
    pg << "C vb,1, vb,1, vb,1, "; // varbinary data[0-2]
    //disabling binaries for now since they seem to be failing in the assembler
    pg << "b,1, b,1, b,1, ";      // binary data[3-5]
    pg << "vb,0, b,0; " << endl;  // nulls[6-7]
    pg << "V 0xAA";
    pg << ", 0xBB";
    pg << ", 0xCC";
    pg << ", 0xDD";
    pg << ", 0xEE";
    pg << ", 0xCC";
    pg << ",,;" << endl;

    pg << "T;" << endl;
    // varchar common cases
    pg << "CALL 'strCmpOct(L0, C0, C0);" << endl;
    pg << "CALL 'strCmpOct(L1, C1, C0);" << endl;
    pg << "CALL 'strCmpOct(L2, C0, C1);" << endl;
    // varchar null cases
    pg << "CALL 'strCmpOct(L3, C6, C0);" << endl;
    pg << "CALL 'strCmpOct(L4, C0, C6);" << endl;
    // char common cases
    pg << "CALL 'strCmpOct(L5, C3, C3);" << endl;
    pg << "CALL 'strCmpOct(L6, C4, C3);" << endl;
    pg << "CALL 'strCmpOct(L7, C3, C4);" << endl;
    // char null cases
    pg << "CALL 'strCmpOct(L8, C7, C3);" << endl;
    pg << "CALL 'strCmpOct(L9, C3, C7);" << endl;
    // mixed common cases
    pg << "CALL 'strCmpOct(L10, C2, C2);" << endl;
    pg << "CALL 'strCmpOct(L11, C0, C3);" << endl;
    pg << "CALL 'strCmpOct(L12, C3, C0);" << endl;
    // make output available
    refLocalOutput(pg, 13);

    Calculator calc(0);
    
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
    printOutput(outTuple, calc);

    // varchar common cases
    BOOST_CHECK_EQUAL(0, cmpTupInt(outTuple[0], 0));
    BOOST_CHECK_EQUAL(0, cmpTupInt(outTuple[1], 1));
    BOOST_CHECK_EQUAL(0, cmpTupInt(outTuple[2], -1));
    // varchar null cases
    BOOST_CHECK_EQUAL(1, cmpTupNull(outTuple[3]));
    BOOST_CHECK_EQUAL(1, cmpTupNull(outTuple[4]));
    // char common case
    BOOST_CHECK_EQUAL(0, cmpTupInt(outTuple[5], 0));
    BOOST_CHECK_EQUAL(0, cmpTupInt(outTuple[6], 1));
    BOOST_CHECK_EQUAL(0, cmpTupInt(outTuple[7], -1));
    // char null cases
    BOOST_CHECK_EQUAL(1, cmpTupNull(outTuple[8]));
    BOOST_CHECK_EQUAL(1, cmpTupNull(outTuple[9]));
    // check mixed cases
    BOOST_CHECK_EQUAL(0, cmpTupInt(outTuple[10], 0));
    BOOST_CHECK_EQUAL(0, cmpTupInt(outTuple[11], -1));
    BOOST_CHECK_EQUAL(0, cmpTupInt(outTuple[12], 1));
}

void
CalcExtStringTest::testCalcExtStringLenBitA()
{
    ostringstream pg(""), outloc("");
    int i;

    for (i = 0; i <= 8; i++) {
        outloc << "s4, ";
    }
    outloc << "s4;" << endl;

    pg << "O " << outloc.str();
    pg << "L " << outloc.str();
    pg << "C vc,2, vc,2, ";      // varchar data[0-1]
    pg << "c,2, c,2, ";          // char data[2-3]
    pg << "vc,5, c,5;" << endl;  // nulls[4-5]
    pg << "V 0x" << stringToHex("AB");
    pg << ", 0x" << stringToHex("");
    pg << ", 0x" << stringToHex("GH");
    pg << ", 0x" << stringToHex("  ");
    pg << ",,;" << endl;
    pg << "T;" << endl;
    // varchar common cases
    pg << "CALL 'strLenBitA(L0, C0);" << endl;
    pg << "CALL 'strLenBitA(L1, C1);" << endl;
    // varchar null case
    pg << "CALL 'strLenBitA(L2, C4);" << endl;
    // char common cases
    pg << "CALL 'strLenBitA(L3, C2);" << endl;
    pg << "CALL 'strLenBitA(L4, C3);" << endl;
    // char null case
    pg << "CALL 'strLenBitA(L5, C5);" << endl;
    // make output available
    refLocalOutput(pg, 6);

    Calculator calc(0);
    
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
    printOutput(outTuple, calc);

    // varchar common cases
    BOOST_CHECK_EQUAL(0, cmpTupInt(outTuple[0], 16));
    BOOST_CHECK_EQUAL(0, cmpTupInt(outTuple[1], 0));
    // varchar null case
    BOOST_CHECK_EQUAL(1, cmpTupNull(outTuple[2]));
    // char common case
    BOOST_CHECK_EQUAL(0, cmpTupInt(outTuple[3], 16));
    BOOST_CHECK_EQUAL(0, cmpTupInt(outTuple[4], 16));
    // char null case
    BOOST_CHECK_EQUAL(1, cmpTupNull(outTuple[5]));
}

void
CalcExtStringTest::testCalcExtStringLenCharA()
{
    ostringstream pg(""), outloc("");
    int i;

    for (i = 0; i <= 8; i++) {
        outloc << "s4, ";
    }
    outloc << "s4;" << endl;

    pg << "O " << outloc.str();
    pg << "L " << outloc.str();
    pg << "C vc,2, vc,2, ";      // varchar data[0-1]
    pg << "c,2, c,2, ";          // char data[2-3]
    pg << "vc,5, c,5;" << endl;  // nulls[4-5]
    pg << "V 0x" << stringToHex("AB");
    pg << ", 0x" << stringToHex("");
    pg << ", 0x" << stringToHex("GH");
    pg << ", 0x" << stringToHex("  ");
    pg << ",,;" << endl;
    pg << "T;" << endl;
    // varchar common cases
    pg << "CALL 'strLenCharA(L0, C0);" << endl;
    pg << "CALL 'strLenCharA(L1, C1);" << endl;
    // varchar null case
    pg << "CALL 'strLenCharA(L2, C4);" << endl;
    // char common cases
    pg << "CALL 'strLenCharA(L3, C2);" << endl;
    pg << "CALL 'strLenCharA(L4, C3);" << endl;
    // char null case
    pg << "CALL 'strLenCharA(L5, C5);" << endl;
    // make output available
    refLocalOutput(pg, 10);

    Calculator calc(0);
    
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
    printOutput(outTuple, calc);

    // varchar common cases
    BOOST_CHECK_EQUAL(0, cmpTupInt(outTuple[0], 2));
    BOOST_CHECK_EQUAL(0, cmpTupInt(outTuple[1], 0));
    // varchar null case
    BOOST_CHECK_EQUAL(1, cmpTupNull(outTuple[2]));
    // char common case
    BOOST_CHECK_EQUAL(0, cmpTupInt(outTuple[3], 2));
    BOOST_CHECK_EQUAL(0, cmpTupInt(outTuple[4], 2));
    // char null case
    BOOST_CHECK_EQUAL(1, cmpTupNull(outTuple[5]));
}

void
CalcExtStringTest::testCalcExtStringLenOctA()
{
    ostringstream pg(""), outloc("");
    int i;

    for (i = 0; i <= 8; i++) {
        outloc << "s4, ";
    }
    outloc << "s4;" << endl;

    pg << "O " << outloc.str();
    pg << "L " << outloc.str();
    pg << "C vc,2, vc,2, ";      // varchar data[0-1]
    pg << "c,2, c,2, ";          // char data[2-3]
    pg << "vc,5, c,5;" << endl;  // nulls[4-5]
    pg << "V 0x" << stringToHex("AB");
    pg << ", 0x" << stringToHex("");
    pg << ", 0x" << stringToHex("GH");
    pg << ", 0x" << stringToHex("  ");
    pg << ",,;" << endl;
    pg << "T;" << endl;
    // varchar common cases
    pg << "CALL 'strLenOctA(L0, C0);" << endl;
    pg << "CALL 'strLenOctA(L1, C1);" << endl;
    // varchar null case
    pg << "CALL 'strLenOctA(L2, C4);" << endl;
    // char common cases
    pg << "CALL 'strLenOctA(L3, C2);" << endl;
    pg << "CALL 'strLenOctA(L4, C3);" << endl;
    // char null case
    pg << "CALL 'strLenOctA(L5, C5);" << endl;
    // make output available
    refLocalOutput(pg, 6);

    Calculator calc(0);
    
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
    printOutput(outTuple, calc);

    // varchar common cases
    BOOST_CHECK_EQUAL(0, cmpTupInt(outTuple[0], 2));
    BOOST_CHECK_EQUAL(0, cmpTupInt(outTuple[1], 0));
    // varchar null case
    BOOST_CHECK_EQUAL(1, cmpTupNull(outTuple[2]));
    // char common case
    BOOST_CHECK_EQUAL(0, cmpTupInt(outTuple[3], 2));
    BOOST_CHECK_EQUAL(0, cmpTupInt(outTuple[4], 2));
    // char null case
    BOOST_CHECK_EQUAL(1, cmpTupNull(outTuple[5]));
}

void
CalcExtStringTest::testCalcExtStringOverlayA4()
{
    ostringstream pg(""), outloc("");
    int i;

    for (i = 0; i <= 9; i++) {
        outloc << "vc,5, ";
    }
    outloc << "vc,1, vc,1;" << endl;          // right truncate

    pg << "O " << outloc.str();
    pg << "L " << outloc.str();
    pg << "C vc,5, vc,2, c,5, c,2, s4, "; // data[0-4] 
    pg << "s4, ";                         // negative[5]
    pg << "vc,5, c,5, s4;" << endl;       // nulls[6-8]
    pg << "V 0x" << stringToHex("ABCD");
    pg << ", 0x" << stringToHex("EF");
    pg << ", 0x" << stringToHex("GHIJ ");
    pg << ", 0x" << stringToHex("KL");
    pg << ", 2,-2,,,;" << endl;
    pg << "T;" << endl;
    // varchar common case
    pg << "CALL 'strOverlayA4(L0, C0, C1, C4);" << endl;
    // varhcar null cases
    pg << "CALL 'strOverlayA4(L1, C6, C1, C4);" << endl;
    pg << "CALL 'strOverlayA4(L2, C0, C6, C4);" << endl;
    pg << "CALL 'strOverlayA4(L3, C0, C1, C8);" << endl;
    // varchar substring error
    pg << "CALL 'strOverlayA4(L4, C0, C1, C5);" << endl;
    // varchar right truncate
    pg << "CALL 'strOverlayA4(L10, C0, C1, C4);" << endl;

    // char common case
    pg << "CALL 'strOverlayA4(L5, C2, C3, C4);" << endl;
    // char null cases
    pg << "CALL 'strOverlayA4(L6, C7, C3, C4);" << endl;
    pg << "CALL 'strOverlayA4(L7, C2, C7, C4);" << endl;
    pg << "CALL 'strOverlayA4(L8, C2, C3, C8);" << endl;
    // varchar substring error
    pg << "CALL 'strOverlayA4(L9, C2, C3, C5);" << endl;
    // char right truncate
    pg << "CALL 'strOverlayA4(L11, C2, C3, C4);" << endl;
    // make output available
    refLocalOutput(pg, 12);

    Calculator calc(0);
    
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
    printOutput(outTuple, calc);
    deque<CalcMessage>::iterator iter = calc.mWarnings.begin();

    // varchar common case
    BOOST_CHECK_EQUAL(0, cmpTupStr(outTuple[0], "AEFD"));
    // varchar null cases
    for (i = 1; i <= 3; i++) {
        BOOST_CHECK_EQUAL(1, cmpTupNull(outTuple[i]));
    }
    // varchar substr errors
    BOOST_CHECK_EQUAL(iter->pc, 4);
    BOOST_CHECK_EQUAL(0, strcmp(iter->str, substrErr));
    iter++;
    // varchar right truncation
    BOOST_CHECK_EQUAL(iter->pc, 5);
    BOOST_CHECK_EQUAL(0, strcmp(iter->str, truncErr));
    iter++;
    // char common case
    BOOST_CHECK_EQUAL(0, cmpTupStr(outTuple[5], "GKLJ "));
    // char null cases
    for (i = 6; i <= 8; i++) {
        BOOST_CHECK_EQUAL(1, cmpTupNull(outTuple[i]));
    }
    // char substr errors
    BOOST_CHECK_EQUAL(iter->pc, 10);
    BOOST_CHECK_EQUAL(0, strcmp(iter->str, substrErr));
    iter++;
    // varchar right truncation
    BOOST_CHECK_EQUAL(iter->pc, 11);
    BOOST_CHECK_EQUAL(0, strcmp(iter->str, truncErr));
    iter++;
    BOOST_CHECK(iter == calc.mWarnings.end());
}

void
CalcExtStringTest::testCalcExtStringOverlayA5()
{
    ostringstream pg(""), outloc("");
    int i;

    for (i = 0; i <= 13; i++) {
        outloc << "vc,5, ";
    }
    outloc << "vc,1, vc,1;" << endl;             // right truncate

    pg << "O " << outloc.str();
    pg << "L " << outloc.str();
    pg << "C vc,5, vc,2, c,5, c,2, s4, s4, ";// data [0-5]
    pg << "s4, ";                            // negative [6]
    pg << "vc,5, c,5, s4;" << endl;          // nulls [7-9]
    pg << "V 0x" << stringToHex("ABCD");
    pg << ", 0x" << stringToHex("EF");
    pg << ", 0x" << stringToHex("GHIJ ");
    pg << ", 0x" << stringToHex("KL");
    pg << ", 1, 2,-2,,,;" << endl;
    pg << "T;" << endl;
    // varchar common case
    pg << "CALL 'strOverlayA5(L0, C0, C1, C4, C5);" << endl;
    // varchar null cases
    pg << "CALL 'strOverlayA5(L1, C7, C1, C4, C5);" << endl;
    pg << "CALL 'strOverlayA5(L2, C0, C7, C4, C5);" << endl;
    pg << "CALL 'strOverlayA5(L3, C0, C1, C9, C5);" << endl;
    pg << "CALL 'strOverlayA5(L4, C0, C1, C4, C9);" << endl;
    // varchar substring error
    pg << "CALL 'strOverlayA5(L5, C0, C1, C6, C5);" << endl;
    pg << "CALL 'strOverlayA5(L6, C0, C1, C4, C6);" << endl;
    // varchar right truncate
    pg << "CALL 'strOverlayA5(L14, C0, C1, C4, C5);" << endl;

    // char common case
    pg << "CALL 'strOverlayA5(L7, C2, C3, C4, C5);" << endl;
    // char null cases
    pg << "CALL 'strOverlayA5(L8, C8, C3, C4, C5);" << endl;
    pg << "CALL 'strOverlayA5(L9, C2, C8, C4, C5);" << endl;
    pg << "CALL 'strOverlayA5(L10, C2, C3, C9, C5);" << endl;
    pg << "CALL 'strOverlayA5(L11, C2, C3, C4, C9);" << endl;
    // char substring error
    pg << "CALL 'strOverlayA5(L12, C2, C3, C6, C5);" << endl;
    pg << "CALL 'strOverlayA5(L13, C2, C3, C4, C6);" << endl;
    // char right truncate
    pg << "CALL 'strOverlayA5(L15, C2, C3, C4, C5);" << endl;
    // make output available
    refLocalOutput(pg, 16);

    Calculator calc(0);
    
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
    printOutput(outTuple, calc);
    deque<CalcMessage>::iterator iter = calc.mWarnings.begin();

    // varchar common case
    BOOST_CHECK_EQUAL(0, cmpTupStr(outTuple[0], "EFCD"));
    // varchar null cases
    for (i = 1; i <= 4; i++) {
        BOOST_CHECK_EQUAL(1, cmpTupNull(outTuple[i]));
    }
    // varchar substr errors
    BOOST_CHECK_EQUAL(iter->pc, 5);
    BOOST_CHECK_EQUAL(0, strcmp(iter->str, substrErr));
    iter++;
    BOOST_CHECK_EQUAL(iter->pc, 6);
    BOOST_CHECK_EQUAL(0, strcmp(iter->str, substrErr));
    iter++;
    // varchar right truncation
    BOOST_CHECK_EQUAL(iter->pc, 7);
    BOOST_CHECK_EQUAL(0, strcmp(iter->str, truncErr));
    iter++;
    // char common case
    BOOST_CHECK_EQUAL(0, cmpTupStr(outTuple[7], "KLIJ "));
    // char null cases
    for (i = 8; i <= 11; i++) {
        BOOST_CHECK_EQUAL(1, cmpTupNull(outTuple[i]));
    }
    // char substr errors
    BOOST_CHECK_EQUAL(iter->pc, 13);
    BOOST_CHECK_EQUAL(0, strcmp(iter->str, substrErr));
    iter++;
    BOOST_CHECK_EQUAL(iter->pc, 14);
    BOOST_CHECK_EQUAL(0, strcmp(iter->str, substrErr));
    iter++;
    // char right truncation
    BOOST_CHECK_EQUAL(iter->pc, 15);
    BOOST_CHECK_EQUAL(0, strcmp(iter->str, truncErr));
    iter++;
    BOOST_CHECK(iter == calc.mWarnings.end());
}

void
CalcExtStringTest::testCalcExtStringPosA()
{
    ostringstream pg(""), outloc("");
    int i;
    
    for (i = 0; i <= 7; i++) {
        outloc << "s4, ";
    }
    outloc << "s4;" << endl;

    pg << "O " << outloc.str();
    pg << "L " << outloc.str();
    pg << "C vc,5, vc,4, vc,4, ";   // vc data[0-2]
    pg << "c,5, c,2, c,2, ";         // c data[3-5]
    pg << "vc,5, c,5";              // nulls[6-7]
    pg << ";" << endl;
    pg << "V 0x" << stringToHex("ABCD");
    pg << ", 0x" << stringToHex("BC");
    pg << ", 0x" << stringToHex("XX");
    pg << ", 0x" << stringToHex("GHIJ ");
    pg << ", 0x" << stringToHex("HI");
    pg << ", 0x" << stringToHex("XX");
    pg << ",,;" << endl;
    pg << "T;" << endl;
    // varchar common
    pg << "CALL 'strPosA(L0, C1, C0);" << endl;
    pg << "CALL 'strPosA(L1, C2, C0);" << endl;
    // varchar null 
    pg << "CALL 'strPosA(L2, C1, C6);" << endl;
    pg << "CALL 'strPosA(L3, C6, C0);" << endl;
    // char common
    pg << "CALL 'strPosA(L4, C4, C3);" << endl;
    pg << "CALL 'strPosA(L5, C5, C3);" << endl;
    // char null
    pg << "CALL 'strPosA(L6, C4, C7);" << endl;
    pg << "CALL 'strPosA(L7, C7, C3);" << endl;
    // make output available
    refLocalOutput(pg, 8);

    Calculator calc(0);
    
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
    printOutput(outTuple, calc);

    BOOST_CHECK_EQUAL(0, cmpTupInt(outTuple[0], 2));
    BOOST_CHECK_EQUAL(0, cmpTupInt(outTuple[1], 0));
    BOOST_CHECK_EQUAL(1, cmpTupNull(outTuple[2]));
    BOOST_CHECK_EQUAL(1, cmpTupNull(outTuple[3]));
    BOOST_CHECK_EQUAL(0, cmpTupInt(outTuple[4], 2));
    BOOST_CHECK_EQUAL(0, cmpTupInt(outTuple[5], 0));
    BOOST_CHECK_EQUAL(1, cmpTupNull(outTuple[6]));
    BOOST_CHECK_EQUAL(1, cmpTupNull(outTuple[7]));
}

void
CalcExtStringTest::testCalcExtStringSubStringA3()
{
    ostringstream pg(""), outloc("");
    int i;

    for (i = 0; i <= 5; i++) {
        outloc << "vc,5, ";
    }
    outloc << "vc,1, vc,1;" << endl;    // right truncate

    pg << "O " << outloc.str();
    pg << "L " << outloc.str();
    pg << "C vc,5, c,5, s4, ";      // data[0-2]
    pg << "vc,5, c,5, s4;" << endl; // nulls[3-5]
    
    pg << "V 0x" << stringToHex("ABCD");
    pg << ", 0x" << stringToHex("GHIJ ");
    pg << ", 2,,,;" << endl;
    pg << "T;" << endl;
    // varchar common case
    pg << "CALL 'strSubStringA3(L0, C0, C2);" << endl;
    // varchar null cases
    pg << "CALL 'strSubStringA3(L1, C3, C2);" << endl;
    pg << "CALL 'strSubStringA3(L2, C0, C5);" << endl;
    // substring error not possible if len is unspecified
    // varchar right trunaction
    pg << "CALL 'strSubStringA3(L6, C0, C2);" << endl;
    // char common case
    pg << "CALL 'strSubStringA3(L3, C1, C2);" << endl;
    // char null cases
    pg << "CALL 'strSubStringA3(L4, C4, C2);" << endl;
    pg << "CALL 'strSubStringA3(L5, C1, C5);" << endl;
    // substring error not possible if len is unspecified
    // char right trunaction
    pg << "CALL 'strSubStringA3(L7, C1, C2);" << endl;
    // make output available
    refLocalOutput(pg, 8);

    Calculator calc(0);
    
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
    printOutput(outTuple, calc);
    deque<CalcMessage>::iterator iter = calc.mWarnings.begin();

    // varchar common case
    BOOST_CHECK_EQUAL(0, cmpTupStr(outTuple[0], "BCD"));
    // varchar null cases
    BOOST_CHECK_EQUAL(1, cmpTupNull(outTuple[1]));
    BOOST_CHECK_EQUAL(1, cmpTupNull(outTuple[2]));
    // varchar right truncation
    BOOST_CHECK_EQUAL(iter->pc, 3);
    BOOST_CHECK_EQUAL(0, strcmp(iter->str, truncErr));
    iter++;
    // char common case
    BOOST_CHECK_EQUAL(0, cmpTupStr(outTuple[3], "HIJ "));
    // char null cases
    BOOST_CHECK_EQUAL(1, cmpTupNull(outTuple[4]));
    BOOST_CHECK_EQUAL(1, cmpTupNull(outTuple[5]));
    // varchar right truncation
    BOOST_CHECK_EQUAL(iter->pc, 7);
    BOOST_CHECK_EQUAL(0, strcmp(iter->str, truncErr));
    iter++;
    BOOST_CHECK(iter == calc.mWarnings.end());
}

void
CalcExtStringTest::testCalcExtStringSubStringA4()
{
    ostringstream pg(""), outloc("");
    int i;

    for (i = 0; i <= 11; i++) {
        outloc << "vc,5, ";
    }
    outloc << "vc,1, vc,1;" << endl;    // right truncate

    pg << "O " << outloc.str();
    pg << "L " << outloc.str();
    pg << "C vc,5, c,5, s4, s4, ";  // data[0-3]
    pg << "s4, ";                   // negative[4]
    pg << "vc,5, c,5, s4;" << endl; // nulls[5-7]
    pg << "V 0x" << stringToHex("ABCD");
    pg << ", 0x" << stringToHex("GHIJ ");
    pg << ", 1, 2, -2,,,;" << endl;
    pg << "T;" << endl;
    // varchar common case
    pg << "CALL 'strSubStringA4(L0, C0, C3, C2);" << endl;
    pg << "CALL 'strSubStringA4(L1, C0, C2, C3);" << endl;
    // varchar null cases
    pg << "CALL 'strSubStringA4(L2, C5, C3, C2);" << endl;
    pg << "CALL 'strSubStringA4(L3, C0, C7, C2);" << endl;
    pg << "CALL 'strSubStringA4(L4, C0, C3, C7);" << endl;
    // varchar substring error
    pg << "CALL 'strSubStringA4(L5, C0, C3, C4);" << endl;
    // varchar right trunaction
    pg << "CALL 'strSubStringA4(L12, C0, C2, C3);" << endl;
    // char common case
    pg << "CALL 'strSubStringA4(L6, C1, C3, C2);" << endl;
    pg << "CALL 'strSubStringA4(L7, C1, C2, C3);" << endl;
    // char null cases
    pg << "CALL 'strSubStringA4(L8, C6, C3, C2);" << endl;
    pg << "CALL 'strSubStringA4(L9, C1, C7, C2);" << endl;
    pg << "CALL 'strSubStringA4(L10, C1, C3, C7);" << endl;
    // varchar substring error
    pg << "CALL 'strSubStringA4(L11, C1, C3, C4);" << endl;
    // char right trunaction
    pg << "CALL 'strSubStringA4(L13, C1, C2, C3);" << endl;
    // make output available
    refLocalOutput(pg, 14);

    Calculator calc(0);
    
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
    printOutput(outTuple, calc);
    deque<CalcMessage>::iterator iter = calc.mWarnings.begin();

    // varchar common case
    BOOST_CHECK_EQUAL(0, cmpTupStr(outTuple[0], "B"));
    BOOST_CHECK_EQUAL(0, cmpTupStr(outTuple[1], "AB"));
    // varchar null cases
    BOOST_CHECK_EQUAL(1, cmpTupNull(outTuple[2]));
    BOOST_CHECK_EQUAL(1, cmpTupNull(outTuple[3]));
    BOOST_CHECK_EQUAL(1, cmpTupNull(outTuple[4]));
    // varchar substring error
    BOOST_CHECK_EQUAL(iter->pc, 5);
    BOOST_CHECK_EQUAL(0, strcmp(iter->str, substrErr));
    iter++;
    // varchar right truncation
    BOOST_CHECK_EQUAL(iter->pc, 6);
    BOOST_CHECK_EQUAL(0, strcmp(iter->str, truncErr));
    iter++;
    // char common case
    BOOST_CHECK_EQUAL(0, cmpTupStr(outTuple[6], "H"));
    BOOST_CHECK_EQUAL(0, cmpTupStr(outTuple[7], "GH"));
    // char null cases
    BOOST_CHECK_EQUAL(1, cmpTupNull(outTuple[8]));
    BOOST_CHECK_EQUAL(1, cmpTupNull(outTuple[9]));
    BOOST_CHECK_EQUAL(1, cmpTupNull(outTuple[10]));
    // char substring error
    BOOST_CHECK_EQUAL(iter->pc, 12);
    BOOST_CHECK_EQUAL(0, strcmp(iter->str, substrErr));
    iter++;
    // char right truncation
    BOOST_CHECK_EQUAL(iter->pc, 13);
    BOOST_CHECK_EQUAL(0, strcmp(iter->str, truncErr));
    iter++;
    BOOST_CHECK(iter == calc.mWarnings.end());
}


// Test that string operatings attempting to write into
// a null string result in a null, and not some other error/problem.
void
CalcExtStringTest::testCalcExtStringToANull()
{
    ostringstream pg(""), outloc(""), outlocchar("");
    int i;

    for (i = 0; i <= 4; i++) {  // [0-4] char
        outlocchar << "c,5, ";
    }

    for (i = 5; i <= 13; i++) { // [5-14] varchar
        outloc << "vc,5, ";
    }
    outloc << "vc,5;" << endl;

    pg << "O " << outlocchar.str() << outloc.str();
    pg << "L " << outlocchar.str() << outloc.str();
    pg << "C vc,5, c,5, s4, vc,5, c,5;" << endl;    // data[0-2], null [3-4]
    pg << "V 0x" << stringToHex(" abc ");   // vc const
    pg << ", 0x" << stringToHex(" hij ");   // char const
    pg << ", 1,,;" << endl;
    pg << "T;" << endl;

    for (i = 0; i <= 14; i++) {
        pg << "TONULL L" << i << ";" << endl;
    }

    // char cases
    pg << "CALL 'strCatA2(L0, C1);" << endl;
    pg << "CALL 'strCatA3(L1, C1, C1);" << endl;
    pg << "CALL 'strCpyA(L2, C1);" << endl;
    pg << "CALL 'strToLowerA(L3, C1);" << endl;
    pg << "CALL 'strToUpperA(L4, C1);" << endl;

    // varchar cases
    pg << "CALL 'strCatA2(L5, C0);" << endl;
    pg << "CALL 'strCatA3(L6, C0, C0);" << endl;
    pg << "CALL 'strCpyA(L7, C0);" << endl;
    pg << "CALL 'strOverlayA4(L8, C0, C0, C2);" << endl;
    pg << "CALL 'strOverlayA5(L9, C0, C0, C2, C2);" << endl;
    pg << "CALL 'strSubStringA3(L10, C0, C2);" << endl;
    pg << "CALL 'strSubStringA4(L11, C0, C2, C2);" << endl;
    pg << "CALL 'strToLowerA(L12, C0);" << endl;
    pg << "CALL 'strToUpperA(L13, C0);" << endl;
    pg << "CALL 'strTrimA(L14, C0, C0, C2, C2);" << endl; // trim both

    // make output available
    refLocalOutput(pg, 15);
    Calculator calc(0);
    
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
    printOutput(outTuple, calc);

    for (i = 0; i <= 14 ;i++) {
        BOOST_CHECK_EQUAL(1, cmpTupNull(outTuple[i]));
    }

    deque<CalcMessage>::iterator iter = calc.mWarnings.begin();
    deque<CalcMessage>::iterator end = calc.mWarnings.end();

    BOOST_CHECK(iter == end);
}


void
CalcExtStringTest::testCalcExtStringToLower()
{
    ostringstream pg(""), outloc("");
    int i;

    for (i = 0; i <= 1; i++) {
        outloc << "vc,5, ";
    }
    for (i = 1; i <= 2; i++) {
        outloc << "c,5, ";
    }
    outloc << "vc,1;" << endl;      // right truncate

    pg << "O " << outloc.str();
    pg << "L " << outloc.str();
    pg << "C vc,5, c,5, ";      // data[0-1]
    pg << "vc,5, c,5;" << endl; // nulls[2-3]
    pg << "V 0x" << stringToHex("ABC");
    pg << ", 0x" << stringToHex("GHIJ ");
    pg << ",,;" << endl;
    pg << "T;" << endl;
    // varchar common case
    pg << "CALL 'strToLowerA(L0, C0);" << endl;
    // varchar null case
    pg << "CALL 'strToLowerA(L1, C2);" << endl;
    // varchar right truncation
    pg << "CALL 'strToLowerA(L4, C0);" << endl;
    // char common case
    pg << "CALL 'strToLowerA(L2, C1);" << endl;
    // char null case
    pg << "CALL 'strToLowerA(L3, C3);" << endl;
    // right truncation not possible in fixed width, as both 
    // strings must be same length by definition.
    // make output available
    refLocalOutput(pg, 5);

    Calculator calc(0);
    
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
    printOutput(outTuple, calc);
    deque<CalcMessage>::iterator iter = calc.mWarnings.begin();

    // varchar common case
    BOOST_CHECK_EQUAL(0, cmpTupStr(outTuple[0], "abc"));
    // varchar null case
    BOOST_CHECK_EQUAL(1, cmpTupNull(outTuple[1]));
    // varchar right truncation
    BOOST_CHECK_EQUAL(iter->pc, 2);
    BOOST_CHECK_EQUAL(0, strcmp(iter->str, truncErr));
    iter++;
    BOOST_CHECK(iter == calc.mWarnings.end());
    // char common case
    BOOST_CHECK_EQUAL(0, cmpTupStr(outTuple[2], "ghij "));
    // varchar null case
    BOOST_CHECK_EQUAL(1, cmpTupNull(outTuple[3]));
}

void
CalcExtStringTest::testCalcExtStringToUpper()
{
    ostringstream pg(""), outloc("");
    int i;

    for (i = 0; i <= 1; i++) {
        outloc << "vc,5, ";
    }
    for (i = 1; i <= 2; i++) {
        outloc << "c,5, ";
    }
    outloc << "vc,1;" << endl;      // right truncate

    pg << "O " << outloc.str();
    pg << "L " << outloc.str();
    pg << "C vc,5, c,5, ";      // data[0-1]
    pg << "vc,5, c,5;" << endl; // nulls[2-3]
    pg << "V 0x" << stringToHex("abc");
    pg << ", 0x" << stringToHex("ghij ");
    pg << ",,;" << endl;
    pg << "T;" << endl;
    // varchar common case
    pg << "CALL 'strToUpperA(L0, C0);" << endl;
    // varchar null case
    pg << "CALL 'strToUpperA(L1, C2);" << endl;
    // varchar right truncation
    pg << "CALL 'strToUpperA(L4, C0);" << endl;
    // char common case
    pg << "CALL 'strToUpperA(L2, C1);" << endl;
    // char null case
    pg << "CALL 'strToUpperA(L3, C3);" << endl;
    // right truncation not possible in fixed width, as both 
    // strings must be same length by definition.
    // make output available
    refLocalOutput(pg, 5);

    Calculator calc(0);
    
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
    printOutput(outTuple, calc);
    deque<CalcMessage>::iterator iter = calc.mWarnings.begin();

    // varchar common case
    BOOST_CHECK_EQUAL(0, cmpTupStr(outTuple[0], "ABC"));
    // varchar null case
    BOOST_CHECK_EQUAL(1, cmpTupNull(outTuple[1]));
    // varchar right truncation
    BOOST_CHECK_EQUAL(iter->pc, 2);
    BOOST_CHECK_EQUAL(0, strcmp(iter->str, truncErr));
    iter++;
    BOOST_CHECK(iter == calc.mWarnings.end());
    // char common case
    BOOST_CHECK_EQUAL(0, cmpTupStr(outTuple[2], "GHIJ "));
    // varchar null case
    BOOST_CHECK_EQUAL(1, cmpTupNull(outTuple[3]));
}

void
CalcExtStringTest::testCalcExtStringTrim()
{
    ostringstream pg(""), outloc("");
    int i;

    for (i = 0; i <= 28; i++) {
        outloc << "vc,10, ";
    }
    outloc << "vc,10;" << endl;

    pg << "O " << outloc.str();
    pg << "L " << outloc.str();
    pg << "C vc,5, c,5, s4, s4, ";         // data[0-3]
    pg << "vc,5, c,5, s4, ";               // nulls[4-6]
    pg << "c,1, vc,1, vc,1, vc,2, vc,1, "; // trimchar[7-11]
    pg << "vc,10;" << endl;                // data[12]
    pg << "V 0x" << stringToHex(" abc ");   // vc const
    pg << ", 0x" << stringToHex(" hij ");   // char const
    pg << ", 1, 0,,,";
    pg << ", 0x" << stringToHex(" ");  // space trimchar char
    pg << ", 0x" << stringToHex(" ");  // space trimchar varchar
    pg << ", 0x" << stringToHex("");   // invalid zero length trimchar
    pg << ", 0x" << stringToHex(" a"); // invalid two char length trimchar
    pg << ", 0x" << stringToHex("x");  // x as trimchar
    pg << ", 0x" << stringToHex("xx pqr xx"); // data[12]
    pg << ";" << endl;
    pg << "T;" << endl;

    // all varchar common cases
    pg << "CALL 'strTrimA(L0, C0, C8, C2, C2);" << endl; // trim both
    pg << "CALL 'strTrimA(L1, C0, C8, C2, C3);" << endl; // trim left
    pg << "CALL 'strTrimA(L2, C0, C8, C3, C2);" << endl; // trim right
    pg << "CALL 'strTrimA(L3, C0, C8, C3, C3);" << endl; // trim none
    // all varchar null cases
    pg << "CALL 'strTrimA(L4, C0, C4, C2, C2);" << endl;
    pg << "CALL 'strTrimA(L5, C4, C8, C2, C2);" << endl;
    pg << "CALL 'strTrimA(L6, C0, C8, C6, C2);" << endl;
    pg << "CALL 'strTrimA(L7, C0, C8, C2, C6);" << endl;
    // all char common cases
    pg << "CALL 'strTrimA(L8, C1, C7, C2, C2);" << endl; // trim both
    pg << "CALL 'strTrimA(L9, C1, C7, C2, C3);" << endl; // trim left
    pg << "CALL 'strTrimA(L10, C1, C7, C3, C2);" << endl; // trim right
    pg << "CALL 'strTrimA(L11, C1, C7, C3, C3);" << endl; // trim none
    // all char null cases
    pg << "CALL 'strTrimA(L12, C5, C7, C2, C2);" << endl;    
    pg << "CALL 'strTrimA(L13, C1, C5, C2, C2);" << endl;
    pg << "CALL 'strTrimA(L14, C1, C7, C6, C2);" << endl;
    pg << "CALL 'strTrimA(L15, C1, C7, C2, C6);" << endl;
    // mixed varchar/char common cases
    pg << "CALL 'strTrimA(L16, C0, C7, C2, C2);" << endl; // trim both
    pg << "CALL 'strTrimA(L17, C1, C7, C2, C2);" << endl; 
    pg << "CALL 'strTrimA(L18, C0, C8, C2, C2);" << endl; 
    pg << "CALL 'strTrimA(L19, C1, C8, C2, C2);" << endl; 
    // mixed varchar/char null cases
    pg << "CALL 'strTrimA(L20, C4, C7, C2, C2);" << endl; // vc,vcN,c
    pg << "CALL 'strTrimA(L21, C0, C5, C2, C2);" << endl; // vc,vc,cN
    pg << "CALL 'strTrimA(L22, C1, C4, C6, C2);" << endl; // vc,c,vcN
    pg << "CALL 'strTrimA(L23, C5, C8, C2, C6);" << endl; // vc,cN,vc

    // An error is thrown in the extended instruction (as opposed to
    // in the string library), so it needs to be tested
    // here. (Exceptions thrown in the string library are tested in
    // the string library unit test>)

    // invalid trim characters
    pg << "CALL 'strTrimA(L24, C0, C9, C2, C2);" << endl; // zero char length trimchar
    pg << "CALL 'strTrimA(L25, C0, C10, C2, C2);" << endl; // two char length trimchar

    // all varchar common cases with other trim char
    pg << "CALL 'strTrimA(L26, C12, C11, C2, C2);" << endl; // trim both
    pg << "CALL 'strTrimA(L27, C12, C11, C2, C3);" << endl; // trim left
    pg << "CALL 'strTrimA(L28, C12, C11, C3, C2);" << endl; // trim right
    pg << "CALL 'strTrimA(L29, C12, C11, C3, C3);" << endl; // trim none

    // make output available
    refLocalOutput(pg, 30);

    Calculator calc(0);
    
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
    printOutput(outTuple, calc);
    BOOST_MESSAGE("Calculator Warnings: |" << calc.warnings() << "|");

    // varchar common cases
    BOOST_CHECK_EQUAL(0, cmpTupStr(outTuple[0], "abc"));
    BOOST_CHECK_EQUAL(0, cmpTupStr(outTuple[1], "abc "));
    BOOST_CHECK_EQUAL(0, cmpTupStr(outTuple[2], " abc"));
    BOOST_CHECK_EQUAL(0, cmpTupStr(outTuple[3], " abc "));
    // varchar null cases
    BOOST_CHECK_EQUAL(1, cmpTupNull(outTuple[4]));
    BOOST_CHECK_EQUAL(1, cmpTupNull(outTuple[5]));
    BOOST_CHECK_EQUAL(1, cmpTupNull(outTuple[6]));
    BOOST_CHECK_EQUAL(1, cmpTupNull(outTuple[7]));

    // char common cases
    BOOST_CHECK_EQUAL(0, cmpTupStr(outTuple[8], "hij"));
    BOOST_CHECK_EQUAL(0, cmpTupStr(outTuple[9], "hij "));
    BOOST_CHECK_EQUAL(0, cmpTupStr(outTuple[10], " hij"));
    BOOST_CHECK_EQUAL(0, cmpTupStr(outTuple[11], " hij "));
    // char null cases
    BOOST_CHECK_EQUAL(1, cmpTupNull(outTuple[12]));
    BOOST_CHECK_EQUAL(1, cmpTupNull(outTuple[13]));
    BOOST_CHECK_EQUAL(1, cmpTupNull(outTuple[14]));
    BOOST_CHECK_EQUAL(1, cmpTupNull(outTuple[15]));

    // mixed varchar/char common cases
    BOOST_CHECK_EQUAL(0, cmpTupStr(outTuple[16], "abc"));
    BOOST_CHECK_EQUAL(0, cmpTupStr(outTuple[17], "hij"));
    BOOST_CHECK_EQUAL(0, cmpTupStr(outTuple[18], "abc"));
    BOOST_CHECK_EQUAL(0, cmpTupStr(outTuple[19], "hij"));
    // mixed varchar/char null cases
    BOOST_CHECK_EQUAL(1, cmpTupNull(outTuple[20]));
    BOOST_CHECK_EQUAL(1, cmpTupNull(outTuple[21]));
    BOOST_CHECK_EQUAL(1, cmpTupNull(outTuple[22]));
    BOOST_CHECK_EQUAL(1, cmpTupNull(outTuple[23]));

    // check warning from invalid trim character
    deque<CalcMessage>::iterator iter = calc.mWarnings.begin();
    deque<CalcMessage>::iterator end = calc.mWarnings.end();

    BOOST_CHECK(iter != end);
    BOOST_CHECK_EQUAL(iter->pc, 24);
    BOOST_CHECK_EQUAL(0, strcmp(iter->str, "22027"));
    iter++;
    BOOST_CHECK_EQUAL(iter->pc, 25);
    BOOST_CHECK_EQUAL(0, strcmp(iter->str, "22027"));
    iter++;
    BOOST_CHECK(iter == end);

    // varchar common cases with other trim char
    BOOST_CHECK_EQUAL(0, cmpTupStr(outTuple[26], " pqr "));
    BOOST_CHECK_EQUAL(0, cmpTupStr(outTuple[27], " pqr xx"));
    BOOST_CHECK_EQUAL(0, cmpTupStr(outTuple[28], "xx pqr "));
    BOOST_CHECK_EQUAL(0, cmpTupStr(outTuple[29], "xx pqr xx"));

}


FENNEL_UNIT_TEST_SUITE(CalcExtStringTest);

