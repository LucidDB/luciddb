/*
// $Id$
// Fennel is a relational database kernel.
// Copyright (C) 2004-2004 Disruptive Tech
//
// This program is free software; you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation; either version 2 of the License, or
// (at your option) any later version.
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
#include <math.h>


using namespace fennel;
using namespace std;

class CalcExtCastTest : virtual public TestBase, public TraceSource
{
    void testCalcExtCastStringToChar();
    void testCalcExtCastStringToVarChar();
    void testCalcExtCastExactToChar();
    void testCalcExtCastExactToVarChar();
    void testCalcExtCastBigExactToString();
    void testCalcExtCastExactToStringTruncates();
    void testCalcExtCastCharToExact();
    void testCalcExtCastVarCharToExact();
    void testCalcExtCastStringToExactFails();
    void testCalcExtCastStringToApprox();
    void testCalcExtCastApproxToString();

    // TODO: Move these calc-test utils up to a new base class.
    // TODO: Clearer boost error messages for wrong value in tuple.
    // REVIEW mb: Might be clearer if tests always printed calc outputs.
    int cmpTupStr(TupleDatum const & tup, char const * const str);
    int cmpTupStr(TupleDatum const & tup, const string& str);
    int cmpTupInt(TupleDatum const & tup, int val);
    int cmpTupInt64(TupleDatum const & tup, int64_t val);
    int cmpTupNull(TupleDatum const & tup);
    int cmpTupDouble(TupleDatum const & tup, double val);
    void printOutput(TupleData const & tup, Calculator const & calc);
    void refLocalOutput(ostringstream& pg, int count);
    string minInt64String();
    string maxInt64String();
    string rpad(string s, int size, char pad = ' ');

    static const bool verbose = true; // print more test output

    static const char* truncErr;
    static const char* invalidCharErr;

public:
    explicit CalcExtCastTest()
        : TraceSource(this,"CalcExtCastTest")
    {
        srand(time(NULL));
        CalcInit::instance();
        FENNEL_UNIT_TEST_CASE(CalcExtCastTest, testCalcExtCastStringToChar);
        FENNEL_UNIT_TEST_CASE(CalcExtCastTest, testCalcExtCastStringToVarChar);
        FENNEL_UNIT_TEST_CASE(CalcExtCastTest, testCalcExtCastExactToVarChar);
        FENNEL_UNIT_TEST_CASE(CalcExtCastTest, testCalcExtCastExactToChar);
        FENNEL_UNIT_TEST_CASE(CalcExtCastTest, testCalcExtCastBigExactToString);
        FENNEL_UNIT_TEST_CASE(CalcExtCastTest, testCalcExtCastExactToStringTruncates); // errors
        FENNEL_UNIT_TEST_CASE(CalcExtCastTest, testCalcExtCastCharToExact);
        FENNEL_UNIT_TEST_CASE(CalcExtCastTest, testCalcExtCastVarCharToExact);
        FENNEL_UNIT_TEST_CASE(CalcExtCastTest, testCalcExtCastStringToExactFails);
        FENNEL_UNIT_TEST_CASE(CalcExtCastTest, testCalcExtCastStringToApprox);
        FENNEL_UNIT_TEST_CASE(CalcExtCastTest, testCalcExtCastApproxToString);
    }
     
    virtual ~CalcExtCastTest()
    {
    }
};

const char * CalcExtCastTest::truncErr = "22001";
const char * CalcExtCastTest::invalidCharErr = "22018";

// returns the minimum int64_t value, as a string.
string CalcExtCastTest::minInt64String()
{
    ostringstream os("");
    os << dec << numeric_limits<int64_t>::min(); 
    return os.str();
}

// returns the maximum int64_t value, as a string.
string CalcExtCastTest::maxInt64String()
{
    ostringstream os("");
    os << dec << numeric_limits<int64_t>::max(); 
    return os.str();
}

// right-pads a string to desired size
string 
CalcExtCastTest::rpad(string s, int size, char pad)
{
    int n = size - s.size();
    if (n > 0)
        s.append(n, pad);
    return s;
}

int
CalcExtCastTest::cmpTupInt(TupleDatum const & tup, int val)
{
    if (cmpTupNull(tup)) return 1;
    return *(reinterpret_cast<int*>
             (const_cast<PBuffer>(tup.pData))) - val;
}

int
CalcExtCastTest::cmpTupDouble(TupleDatum const & tup, double val)
{
    if (cmpTupNull(tup)) return 1;
    double tval = * reinterpret_cast<double*>
        (const_cast<PBuffer>(tup.pData));
    if (fabs(tval - val) < 0.00001) return 0;
    return (tval > val)? 1 : -1;
}

int
CalcExtCastTest::cmpTupInt64(TupleDatum const & tup,
                             int64_t val)
{
    if (cmpTupNull(tup)) return 1;
    return *(reinterpret_cast<int64_t*>
             (const_cast<PBuffer>(tup.pData))) - val;
}

int
CalcExtCastTest::cmpTupStr(TupleDatum const & tup,
                           const string& s)
{
    return cmpTupStr(tup, s.c_str());
}

int
CalcExtCastTest::cmpTupStr(TupleDatum const & tup,
                           char const * const str)
{
    if (cmpTupNull(tup)) return 1;
    int len = strlen(str);
    BOOST_CHECK_EQUAL(len, tup.cbData);
    return strncmp(reinterpret_cast<char *>
                   (const_cast<PBuffer>(tup.pData)),
                   str,
                   len);
}

int
CalcExtCastTest::cmpTupNull(TupleDatum const & tup)
{
    return ((const_cast<PBuffer>(tup.pData)) == NULL)? 1 : 0;
}

// for nitty-gritty debugging. sadly, doesn't use BOOST_MESSAGE.
void
CalcExtCastTest::printOutput(TupleData const & tup,
                               Calculator const & calc)
{
    if (verbose) {
        TuplePrinter tuplePrinter;
        tuplePrinter.print(cout, calc.getOutputRegisterDescriptor(), tup);
        cout << endl;
    }
}


// copy-by-reference locals into identical output register
void
CalcExtCastTest::refLocalOutput(ostringstream& pg, 
                                  int count)
{
    int i;
    
    for (i = 0; i < count; i++) {
        pg << "REF O" << i << ", L" << i << ";" << endl;
    }
}


void
CalcExtCastTest::testCalcExtCastStringToChar()
{
    ostringstream pg(""), outloc("");

    outloc << "c,5, c,5, c,5, c,5, c,5, c,5, c,5, c,5;" << endl;

    pg << "O " << outloc.str();
    pg << "L " << outloc.str();
    pg << "C vc,3, vc,8, c,3, c,8, vc,8, c,8, vc,8, c,8;" << endl;
    pg << "V 0x" << stringToHex("ABC");
    pg << ", 0x" << stringToHex("DEFGH");
    pg << ", 0x" << stringToHex("ZYX");
    pg << ", 0x" << stringToHex("WVUTS   ");
    pg << ", 0x" << stringToHex("IJKLMNOP");
    pg << ", 0x" << stringToHex("RQPONMLK");
    pg << ",,;" << endl;
    pg << "T;" << endl;

    // vc(3) -> c(5)
    pg << "CALL 'castA(L0, C0);" << endl;

    // vc(8), length5 -> c(5)
    pg << "CALL 'castA(L1, C1);" << endl;
    
    // c(3) -> c(5)
    pg << "CALL 'castA(L2, C2);" << endl;

    // c(8) -> c(5)
    pg << "CALL 'castA(L3, C3);" << endl;

    // vc(8), length 8 -> c(5) = 22001
    pg << "CALL 'castA(L4, C4);" << endl;

    // c(8) -> c(5) = 22001
    pg << "CALL 'castA(L5, C5);" << endl;

    // null vc(8) -> c(5)
    pg << "CALL 'castA(L6, C6);" << endl;

    // null c(8) -> c(5)
    pg << "CALL 'castA(L7, C7);" << endl;

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

    // vc(3) -> c(5)
    BOOST_CHECK_EQUAL(0, cmpTupStr(outTuple[0], "ABC  "));

    // vc(8), length 5 -> c(5)
    BOOST_CHECK_EQUAL(0, cmpTupStr(outTuple[1], "DEFGH"));

    // c(3) - > c(5)
    BOOST_CHECK_EQUAL(0, cmpTupStr(outTuple[2], "ZYX  "));

    // c(8) -> c(5)
    BOOST_CHECK_EQUAL(0, cmpTupStr(outTuple[3], "WVUTS"));

    // vc(8), length 8 -> c(5) = 22001
    BOOST_CHECK_EQUAL(0, cmpTupStr(outTuple[4], "IJKLM"));
    // TODO: when 22001 is thrown as a warning, re-enable this
    // BOOST_CHECK_EQUAL(iter->pc, 4);
    // BOOST_CHECK_EQUAL(0, strcmp(iter->str, truncErr));
    // iter++;

    // c(8) -> c(5) = 22001
    BOOST_CHECK_EQUAL(0, cmpTupStr(outTuple[5], "RQPON"));
    // TODO: when 22001 is thrown as a warning, re-enable this
    // BOOST_CHECK_EQUAL(iter->pc, 5);
    // BOOST_CHECK_EQUAL(0, strcmp(iter->str, truncErr));
    // iter++;

    // null vc(8) -> c(5)
    BOOST_CHECK_EQUAL(1, cmpTupNull(outTuple[6]));

    // null c(8) -> c(5)
    BOOST_CHECK_EQUAL(1, cmpTupNull(outTuple[7]));

    BOOST_CHECK(iter == calc.mWarnings.end());
}


void
CalcExtCastTest::testCalcExtCastStringToVarChar()
{
    ostringstream pg(""), outloc("");

    outloc << "vc,5, vc,5, vc,5, vc,5, vc,5, vc,5, vc,5, vc,5;" << endl;

    pg << "O " << outloc.str();
    pg << "L " << outloc.str();
    pg << "C vc,3, vc,8, c,3, c,8, vc,8, c,8, vc,8, c,8;" << endl;
    pg << "V 0x" << stringToHex("ABC");
    pg << ", 0x" << stringToHex("DEFGH");
    pg << ", 0x" << stringToHex("ZYX");
    pg << ", 0x" << stringToHex("WVUTS   ");
    pg << ", 0x" << stringToHex("IJKLMNOP");
    pg << ", 0x" << stringToHex("RQPONMLK");
    pg << ",,;" << endl;
    pg << "T;" << endl;

    // vc(3) -> vc(5)
    pg << "CALL 'castA(L0, C0);" << endl;

    // vc(8), length5 -> vc(5)
    pg << "CALL 'castA(L1, C1);" << endl;
    
    // c(3) -> vc(5)
    pg << "CALL 'castA(L2, C2);" << endl;

    // c(8) -> vc(5)
    pg << "CALL 'castA(L3, C3);" << endl;

    // vc(8), length 8 -> vc(5) = 22001
    pg << "CALL 'castA(L4, C4);" << endl;

    // c(8) -> vc(5) = 22001
    pg << "CALL 'castA(L5, C5);" << endl;

    // null vc(8) -> vc(5)
    pg << "CALL 'castA(L6, C6);" << endl;

    // null c(8) -> vc(5)
    pg << "CALL 'castA(L7, C7);" << endl;

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

    // vc(3) -> vc(5)
    BOOST_CHECK_EQUAL(0, cmpTupStr(outTuple[0], "ABC"));

    // vc(8), length 5 -> vc(5)
    BOOST_CHECK_EQUAL(0, cmpTupStr(outTuple[1], "DEFGH"));

    // c(3) - > vc(5)
    BOOST_CHECK_EQUAL(0, cmpTupStr(outTuple[2], "ZYX"));

    // c(8) -> vc(5)
    BOOST_CHECK_EQUAL(0, cmpTupStr(outTuple[3], "WVUTS"));

    // vc(8), length 8 -> vc(5) = 22001
    BOOST_CHECK_EQUAL(0, cmpTupStr(outTuple[4], "IJKLM"));
    // TODO: when 22001 is thrown as a warning, re-enable this
    // BOOST_CHECK_EQUAL(iter->pc, 4);
    // BOOST_CHECK_EQUAL(0, strcmp(iter->str, truncErr));
    // iter++;

    // c(8) -> vc(5) = 22001
    BOOST_CHECK_EQUAL(0, cmpTupStr(outTuple[5], "RQPON"));
    // TODO: when 22001 is thrown as a warning, re-enable this
    // BOOST_CHECK_EQUAL(iter->pc, 5);
    // BOOST_CHECK_EQUAL(0, strcmp(iter->str, truncErr));
    // iter++;

    // null vc(8) -> vc(5)
    BOOST_CHECK_EQUAL(1, cmpTupNull(outTuple[6]));

    // null c(8) -> vc(5)
    BOOST_CHECK_EQUAL(1, cmpTupNull(outTuple[7]));

    BOOST_CHECK(iter == calc.mWarnings.end());
}


// cast exact numbers to char strings
void
CalcExtCastTest::testCalcExtCastExactToChar()
{
    // int8 test values: (null, 0, 10, -10),  cast to CHAR(3) and CHAR(16)
    ostringstream pg(""), outloc("");
    outloc <<  "c,3, c,3, c,3, c,3, c,16, c,16, c,16, c,16;" << endl;
    pg << "O " << outloc.str();
    pg << "L " << outloc.str();
    pg << "C s8, s8, s8, s8;" << endl;
    pg << "V , 0, 10, -10;" << endl;
    pg << "T;" << endl;
    // cast all to CHAR(3)
    for (int i=0; i < 4; i++)
        pg << "CALL 'castA(L" << i << ", C" << i << ");" << endl;
    // cast all to CHAR(16)
    for (int i=0; i < 4; i++)
        pg << "CALL 'castA(L" << (i+4) << ", C" << i << ");" << endl;
    refLocalOutput(pg, 8);      // make output available

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

    // check results:
    BOOST_CHECK(cmpTupNull(outTuple[0]));
    BOOST_CHECK_EQUAL(0, cmpTupStr(outTuple[1], "0  "));
    BOOST_CHECK_EQUAL(0, cmpTupStr(outTuple[2], "10 "));
    BOOST_CHECK_EQUAL(0, cmpTupStr(outTuple[3], "-10"));
    BOOST_CHECK(cmpTupNull(outTuple[4]));
    BOOST_CHECK_EQUAL(0, cmpTupStr(outTuple[5], "0               "));
    BOOST_CHECK_EQUAL(0, cmpTupStr(outTuple[6], "10              "));
    BOOST_CHECK_EQUAL(0, cmpTupStr(outTuple[7], "-10             "));
    BOOST_CHECK(iter == calc.mWarnings.end());
}

// cast exact numbers to varchar strings
void
CalcExtCastTest::testCalcExtCastExactToVarChar()
{
    // int8 test values: (null, 0, 10, -10), cast to VARCHAR(3) and VARCHAR(16).
    ostringstream pg(""), outloc("");
    outloc <<  "vc,3, vc,3, vc,3, vc,3, vc,16, vc,16, vc,16, vc,16;" << endl;
    pg << "O " << outloc.str();
    pg << "L " << outloc.str();
    pg << "C s8, s8, s8, s8;" << endl;
    pg << "V , 0, 10, -10;" << endl;
    pg << "T;" << endl;
    // cast all to VARCHAR(3)
    for (int i=0; i < 4; i++)
        pg << "CALL 'castA(L" << i << ", C" << i << ");" << endl;
    // cast all to VARCHAR(16)
    for (int i=0; i < 4; i++)
        pg << "CALL 'castA(L" << (i+4) << ", C" << i << ");" << endl;
    refLocalOutput(pg, 8);      // make output available

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

    // check results:
    BOOST_CHECK(cmpTupNull(outTuple[0]));
    BOOST_CHECK_EQUAL(0, cmpTupStr(outTuple[1], "0"));
    BOOST_CHECK_EQUAL(0, cmpTupStr(outTuple[2], "10"));
    BOOST_CHECK_EQUAL(0, cmpTupStr(outTuple[3], "-10"));
    BOOST_CHECK(cmpTupNull(outTuple[4]));
    BOOST_CHECK_EQUAL(0, cmpTupStr(outTuple[5], "0"));
    BOOST_CHECK_EQUAL(0, cmpTupStr(outTuple[6], "10"));
    BOOST_CHECK_EQUAL(0, cmpTupStr(outTuple[7], "-10"));
    BOOST_CHECK(iter == calc.mWarnings.end());
}

// cast large exact numbers to strings
void
CalcExtCastTest::testCalcExtCastBigExactToString()
{
    // int8 test values: (MAX, MIN) cast to CHAR(32) and VARCHAR(32);
    ostringstream pg(""), outloc("");
    outloc <<  "vc,32, vc,32, c,32, c,32;" << endl;
    pg << "O " << outloc.str();
    pg << "L " << outloc.str();
    pg << "C s8, s8;" << endl;
    pg << "V " << maxInt64String() << ", " << minInt64String() << ";" << endl;
    pg << "T;" << endl;
    pg << "CALL 'castA(L0, C0);" << endl;
    pg << "CALL 'castA(L1, C1);" << endl;
    pg << "CALL 'castA(L2, C0);" << endl;
    pg << "CALL 'castA(L3, C1);" << endl;
    refLocalOutput(pg, 4);      // make output available

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

    // check results:
    BOOST_CHECK_EQUAL(0, cmpTupStr(outTuple[0], maxInt64String()));
    BOOST_CHECK_EQUAL(0, cmpTupStr(outTuple[1], minInt64String()));
    BOOST_CHECK_EQUAL(0, cmpTupStr(outTuple[2], rpad(maxInt64String(), 32)));
    BOOST_CHECK_EQUAL(0, cmpTupStr(outTuple[3], rpad(minInt64String(), 32)));
    BOOST_CHECK(iter == calc.mWarnings.end());
}

// cast exact numbers to strings; truncates
void
CalcExtCastTest::testCalcExtCastExactToStringTruncates()
{
    // int8 test values (1666, -1666) cast to VARCHAR(3) and CHAR(3)
    ostringstream pg(""), outloc("");
    outloc <<  "vc,3, vc,3, c,3, c,3;" << endl;
    pg << "O " << outloc.str();
    pg << "L " << outloc.str();
    pg << "C s8, s8;" << endl;
    pg << "V 1666, -1666;" << endl;
    pg << "T;" << endl;
    pg << "CALL 'castA(L0, C0);" << endl;
    pg << "CALL 'castA(L1, C1);" << endl;
    pg << "CALL 'castA(L2, C0);" << endl;
    pg << "CALL 'castA(L3, C1);" << endl;
    refLocalOutput(pg, 4);      // make output available

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

    // check results:
    // 1666 -> vc(3) = truncates
    // dtbug : last char of truncated value is a nul.
    // BOOST_CHECK_EQUAL(0, cmpTupStr(outTuple[0], "166"));
    BOOST_CHECK_EQUAL(iter->pc, 0);
    BOOST_CHECK_EQUAL(0, strcmp(iter->str, truncErr));
    iter++;

    // -1666 -> vc(3) = truncates
    // BOOST_CHECK_EQUAL(0, cmpTupStr(outTuple[0], "-16"));
    BOOST_CHECK_EQUAL(iter->pc, 1);
    BOOST_CHECK_EQUAL(0, strcmp(iter->str, truncErr));
    iter++;

    // 1666 -> c(3) = truncates
    // BOOST_CHECK_EQUAL(0, cmpTupStr(outTuple[0], "166"));
    BOOST_CHECK_EQUAL(iter->pc, 2);
    BOOST_CHECK_EQUAL(0, strcmp(iter->str, truncErr));
    iter++;

    // -1666 -> c(3) = truncates
    // BOOST_CHECK_EQUAL(0, cmpTupStr(outTuple[0], "-16"));
    BOOST_CHECK_EQUAL(iter->pc, 3);
    BOOST_CHECK_EQUAL(0, strcmp(iter->str, truncErr));
    iter++;
    BOOST_CHECK(iter == calc.mWarnings.end());
}

void CalcExtCastTest::testCalcExtCastVarCharToExact()
{
    // test values: null; 123, -123, MAX and MIN; same with trailing spaces.
    ostringstream pg(""), outloc("");
    outloc << "s8, s8, s8, s8, s8, s8, s8, s8, s8;" << endl;
    pg << "O " << outloc.str();
    pg << "L " << outloc.str();
    pg << "C vc,4, vc,4, vc,4, vc,20, vc,21, vc,8, vc,8, vc,32, vc,32;" << endl;
    pg << "V "                  // a null
       << ", 0x" << stringToHex("123") << ", 0x" << stringToHex("-123")
       << ", 0x" << stringToHex(maxInt64String())
       << ", 0x" << stringToHex(minInt64String())
       << ", 0x" << stringToHex("123     ") << ", 0x" << stringToHex("-123    ")
       << ", 0x" << stringToHex(rpad(maxInt64String(), 32))
       << ", 0x" << stringToHex(rpad(minInt64String(), 32))
       << ";" << endl;
    pg << "T;" << endl;
    for (int i = 0; i < 9; i++)
        pg << "CALL 'castA(L"<<i<<",C"<<i<<");"<< endl;
    refLocalOutput(pg, 9);      // make output available

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

    BOOST_CHECK_EQUAL(1, cmpTupNull(outTuple[0]));
    BOOST_CHECK_EQUAL(0, cmpTupInt(outTuple[1],  123));
    BOOST_CHECK_EQUAL(0, cmpTupInt(outTuple[2], -123));
    BOOST_CHECK_EQUAL(0, cmpTupInt64(outTuple[3], numeric_limits<int64_t>::max()));
    BOOST_CHECK_EQUAL(0, cmpTupInt64(outTuple[4], numeric_limits<int64_t>::min()));
    BOOST_CHECK_EQUAL(0, cmpTupInt(outTuple[5],  123));
    BOOST_CHECK_EQUAL(0, cmpTupInt(outTuple[6], -123));
    BOOST_CHECK_EQUAL(0, cmpTupInt64(outTuple[7], numeric_limits<int64_t>::max()));
    BOOST_CHECK_EQUAL(0, cmpTupInt64(outTuple[8], numeric_limits<int64_t>::min()));
    BOOST_CHECK(iter == calc.mWarnings.end());
}

void CalcExtCastTest::testCalcExtCastCharToExact()
{
    // test values: null; 123, -123, MAX and MIN; same with trailing spaces.
    ostringstream pg(""), outloc("");
    outloc << "s8, s8, s8, s8, s8, s8, s8, s8, s8;" << endl;
    pg << "O " << outloc.str();
    pg << "L " << outloc.str();
    pg << "C c,3, c,3, c,4, c,19, c,20, c,8, c,8, c,32, c,32;" << endl;
    pg << "V "                  // a null
       << ", 0x" << stringToHex("123") << ", 0x" << stringToHex("-123")
       << ", 0x" << stringToHex(maxInt64String())
       << ", 0x" << stringToHex(minInt64String())
       << ", 0x" << stringToHex("123     ") << ", 0x" << stringToHex("-123    ")
       << ", 0x" << stringToHex(rpad(maxInt64String(), 32))
       << ", 0x" << stringToHex(rpad(minInt64String(), 32))
       << ";" << endl;
    pg << "T;" << endl;
    for (int i = 0; i < 9; i++)
        pg << "CALL 'castA(L"<<i<<",C"<<i<<");"<< endl;
    refLocalOutput(pg, 9);      // make output available

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

    BOOST_CHECK_EQUAL(1, cmpTupNull(outTuple[0]));
    BOOST_CHECK_EQUAL(0, cmpTupInt(outTuple[1],  123));
    BOOST_CHECK_EQUAL(0, cmpTupInt(outTuple[2], -123));
    BOOST_CHECK_EQUAL(0, cmpTupInt64(outTuple[3], numeric_limits<int64_t>::max()));
    BOOST_CHECK_EQUAL(0, cmpTupInt64(outTuple[4], numeric_limits<int64_t>::min()));
    BOOST_CHECK_EQUAL(0, cmpTupInt(outTuple[5],  123));
    BOOST_CHECK_EQUAL(0, cmpTupInt(outTuple[6], -123));
    BOOST_CHECK_EQUAL(0, cmpTupInt64(outTuple[7], numeric_limits<int64_t>::max()));
    BOOST_CHECK_EQUAL(0, cmpTupInt64(outTuple[8], numeric_limits<int64_t>::min()));
    BOOST_CHECK(iter == calc.mWarnings.end());
}

void CalcExtCastTest::testCalcExtCastStringToExactFails()
{
    // test invalid values abc, 12z
    ostringstream pg(""), outloc("");
    outloc << "s8, s8, s8, s8;" << endl;
    pg << "O " << outloc.str();
    pg << "L " << outloc.str();
    pg << "C vc,3, vc,3, c,3, c,3;" << endl;
    pg << "V 0x" << stringToHex("abc") << ", 0x" << stringToHex("12z")
       << ", 0x" << stringToHex("abc") << ", 0x" << stringToHex("12z") << ";" << endl;
    pg << "T;" << endl;
    pg << "CALL 'castA(L0, C0);" << endl;
    pg << "CALL 'castA(L1, C1);" << endl;
    pg << "CALL 'castA(L2, C0);" << endl;
    pg << "CALL 'castA(L3, C1);" << endl;
    refLocalOutput(pg, 4);      // make output available
    
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

    // all the casts should have failed
    deque<CalcMessage>::iterator iter = calc.mWarnings.begin();
    for (int pc = 0; pc < 4; pc++, iter++) {
        BOOST_CHECK_EQUAL(iter->pc, pc);
        BOOST_CHECK_EQUAL(0, strcmp(iter->str, invalidCharErr));
    }
    BOOST_CHECK(iter == calc.mWarnings.end());
}

void CalcExtCastTest::testCalcExtCastStringToApprox()
{
    // test values: null, 0, 0.0, .0, 1.98, -1.98, 0.001, 0.00100
    // as varchar(8), as trim chars, and as char(16).
    ostringstream pg(""), outloc("");
    outloc << "d, d, d, d, d, d, d, d, "
           << "d, d, d, d, d, d, d, d, "
           << "d, d, d, d, d, d, d, d;" << endl;
    pg << "O " << outloc.str();
    pg << "L " << outloc.str();
    pg << "C "
       << "vc,8, vc,8, vc,8, vc,8, vc,8, vc,8, vc,8, vc,8, "
       << "c,1, c,1, c,3, c,2, c,4, c,5, c,5, c,7, "
       << "c,16, c,16, c,16, c,16, c,16, c,16, c,16, c,16;" << endl;
    pg << "V ";
    for (int i = 0; i < 2; i++) {
        pg << ","
           << "0x" << stringToHex("0")       << ","
           << "0x" << stringToHex("0.0")     << ","
           << "0x" << stringToHex(".0")      << ","
           << "0x" << stringToHex("1.98")    << ","
           << "0x" << stringToHex("-1.98")   << ","
           << "0x" << stringToHex("0.001")   << ","
           << "0x" << stringToHex("0.00100") << ",";
    }
    // same values padded to char(16)
    pg << ","
       << "0x" << stringToHex("0               ") << ","
       << "0x" << stringToHex("0.0             ") << ","
       << "0x" << stringToHex(".0              ") << ","
       << "0x" << stringToHex("1.98            ") << ","
       << "0x" << stringToHex("-1.98           ") << ","
       << "0x" << stringToHex("0.001           ") << ","
       << "0x" << stringToHex("0.00100         ") << ";" << endl;
    pg << "T;" << endl;
    for (int i=0; i<8; i++)
        pg << "CALL 'castA(L" << i << ", C" << i << ");" << endl;
    for (int i=0; i<8; i++)
        pg << "CALL 'castA(L" << (i+8) << ", C" << i << ");" << endl;
    for (int i=0; i<8; i++)
        pg << "CALL 'castA(L" << (i+16) << ", C" << i << ");" << endl;
    refLocalOutput(pg, 24);     // make output available

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
    // check 3 identical sets of 8 values
    for (int pass = 0, i = 0; pass < 3; pass++) {
        BOOST_CHECK(cmpTupNull(outTuple[i++]));
        BOOST_CHECK_EQUAL(0, cmpTupDouble(outTuple[i++], 0));
        BOOST_CHECK_EQUAL(0, cmpTupDouble(outTuple[i++], 0));
        BOOST_CHECK_EQUAL(0, cmpTupDouble(outTuple[i++], 0));
        BOOST_CHECK_EQUAL(0, cmpTupDouble(outTuple[i++], 1.98));
        BOOST_CHECK_EQUAL(0, cmpTupDouble(outTuple[i++], -1.98));
        BOOST_CHECK_EQUAL(0, cmpTupDouble(outTuple[i++], 0.001));
        BOOST_CHECK_EQUAL(0, cmpTupDouble(outTuple[i++], 0.001));
    }
    BOOST_CHECK(iter == calc.mWarnings.end());
}

void CalcExtCastTest::testCalcExtCastApproxToString()
{
    // double test values: null, 0, +1.98, -1.98, +0.001, -0.001

    // TODO: at present calc/SqlString always produces a string with 16 digits
    // plus exponents. When it's fixed to produce a shorter string, shorten the
    // output registers
    ostringstream pg(""), outloc("");
    outloc << "vc,32, vc,32, vc,32, vc,32, vc,32, vc,32, "
           << "c,32, c,32, c,32, c,32, c,32, c,32;" << endl;
    pg << "O " << outloc.str();
    pg << "L " << outloc.str();
    pg << "C d,d,d,d,d,d;" << endl;
    pg << "V , 0.0, 1.98, -1.98, 0.001, -0.001;" << endl;
    pg << "T;" << endl;
    for (int i=0; i<6; i++)
        pg << "CALL 'castA(L"<<i<<", C"<<i<<");" << endl;
    for (int i=0; i<6; i++)
        pg << "CALL 'castA(L"<<(i+6)<<", C"<<i<<");" << endl;
    refLocalOutput(pg, 12);     // make output available
    // cerr << "testCalcExtCastApproxToString Program:\n" << pg.str() << endl;

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
    
    int i = 0;
    // first the varchars
    // TODO: fix these strings when calc/SqlString produces terser results.
    BOOST_CHECK(cmpTupNull(outTuple[i++]));
    BOOST_CHECK_EQUAL(0, cmpTupStr(outTuple[i++], "0E0"));
    BOOST_CHECK_EQUAL(0, cmpTupStr(outTuple[i++], "1.9800000000000000E+00"));
    BOOST_CHECK_EQUAL(0, cmpTupStr(outTuple[i++], "-1.9800000000000000E+00"));
    BOOST_CHECK_EQUAL(0, cmpTupStr(outTuple[i++], "1.0000000000000000E-03"));
    BOOST_CHECK_EQUAL(0, cmpTupStr(outTuple[i++], "-1.0000000000000000E-03"));
    // then the char(6)s
    BOOST_CHECK(cmpTupNull(outTuple[i++]));
    BOOST_CHECK_EQUAL(0, cmpTupStr(outTuple[i++], "0E0                             "));
    BOOST_CHECK_EQUAL(0, cmpTupStr(outTuple[i++], "1.9800000000000000E+00          "));
    BOOST_CHECK_EQUAL(0, cmpTupStr(outTuple[i++], "-1.9800000000000000E+00         "));
    BOOST_CHECK_EQUAL(0, cmpTupStr(outTuple[i++], "1.0000000000000000E-03          "));
    BOOST_CHECK_EQUAL(0, cmpTupStr(outTuple[i++], "-1.0000000000000000E-03         "));
    BOOST_CHECK(iter == calc.mWarnings.end());
}

FENNEL_UNIT_TEST_SUITE(CalcExtCastTest);

