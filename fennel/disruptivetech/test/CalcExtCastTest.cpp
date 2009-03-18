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
    void testCalcExtCastBooleanToChar();
    void testCalcExtCastBooleanToVarChar();
    void testCalcExtCastExactToChar();
    void testCalcExtCastExactToVarChar();
    void testCalcExtCastDecimalToChar();
    void testCalcExtCastDecimalToVarChar();
    void testCalcExtCastBigExactToString();
    void testCalcExtCastExactToStringTruncates();
    void testCalcExtCastDecimalToStringTruncates();
    void testCalcExtCastCharToBoolean();
    void testCalcExtCastVarCharToBoolean();
    void testCalcExtCastCharToExact();
    void testCalcExtCastVarCharToExact();
    void testCalcExtCastCharToDecimal();
    void testCalcExtCastVarCharToDecimal();
    void testCalcExtCastStringToExactFails();
    void testCalcExtCastStringToDecimalFails();
    void testCalcExtCastStringToDecimalMinMax();
    void testCalcExtCastStringToDecimalRange();
    void testCalcExtCastStringToApprox();
    void testCalcExtCastApproxToString();

    // TODO: Move these calc-test utils up to a new base class.
    // TODO: Clearer boost error messages for wrong value in tuple.
    // REVIEW mb: Might be clearer if tests always printed calc outputs.
    int cmpTupStr(TupleDatum const & tup, char const * const str);
    int cmpTupStr(TupleDatum const & tup, const string& str);
    int cmpTupBool(TupleDatum const & tup, bool val);
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
    static const char* outOfRangeErr;

public:
    explicit CalcExtCastTest()
        : TraceSource(shared_from_this(),"CalcExtCastTest")
    {
        srand(time(NULL));
        CalcInit::instance();
        FENNEL_UNIT_TEST_CASE(CalcExtCastTest, testCalcExtCastStringToChar);
        FENNEL_UNIT_TEST_CASE(CalcExtCastTest, testCalcExtCastStringToVarChar);
        FENNEL_UNIT_TEST_CASE(CalcExtCastTest, testCalcExtCastBooleanToVarChar);
        FENNEL_UNIT_TEST_CASE(CalcExtCastTest, testCalcExtCastBooleanToChar);
        FENNEL_UNIT_TEST_CASE(CalcExtCastTest, testCalcExtCastExactToVarChar);
        FENNEL_UNIT_TEST_CASE(CalcExtCastTest, testCalcExtCastExactToChar);
        FENNEL_UNIT_TEST_CASE(CalcExtCastTest, testCalcExtCastDecimalToChar);
        FENNEL_UNIT_TEST_CASE(CalcExtCastTest, testCalcExtCastDecimalToVarChar);
        FENNEL_UNIT_TEST_CASE(CalcExtCastTest, testCalcExtCastBigExactToString);
        FENNEL_UNIT_TEST_CASE(CalcExtCastTest, testCalcExtCastExactToStringTruncates); // errors
        FENNEL_UNIT_TEST_CASE(CalcExtCastTest, testCalcExtCastDecimalToStringTruncates); // errors
        FENNEL_UNIT_TEST_CASE(CalcExtCastTest, testCalcExtCastCharToBoolean);
        FENNEL_UNIT_TEST_CASE(CalcExtCastTest, testCalcExtCastVarCharToBoolean);
        FENNEL_UNIT_TEST_CASE(CalcExtCastTest, testCalcExtCastCharToExact);
        FENNEL_UNIT_TEST_CASE(CalcExtCastTest, testCalcExtCastVarCharToExact);
        FENNEL_UNIT_TEST_CASE(CalcExtCastTest, testCalcExtCastCharToDecimal);
        FENNEL_UNIT_TEST_CASE(CalcExtCastTest, testCalcExtCastVarCharToDecimal);
        FENNEL_UNIT_TEST_CASE(CalcExtCastTest, testCalcExtCastStringToExactFails);
        FENNEL_UNIT_TEST_CASE(CalcExtCastTest, testCalcExtCastStringToDecimalFails);
        FENNEL_UNIT_TEST_CASE(CalcExtCastTest, testCalcExtCastStringToDecimalMinMax);
        FENNEL_UNIT_TEST_CASE(CalcExtCastTest, testCalcExtCastStringToDecimalRange);
        FENNEL_UNIT_TEST_CASE(CalcExtCastTest, testCalcExtCastStringToApprox);
        FENNEL_UNIT_TEST_CASE(CalcExtCastTest, testCalcExtCastApproxToString);
    }

    virtual ~CalcExtCastTest()
    {
    }
};

const char * CalcExtCastTest::truncErr = "22001";
const char * CalcExtCastTest::invalidCharErr = "22018";
const char * CalcExtCastTest::outOfRangeErr = "22003";

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
CalcExtCastTest::cmpTupBool(TupleDatum const & tup, bool val)
{
    if (cmpTupNull(tup)) return 0;
    return *(reinterpret_cast<bool*>
             (const_cast<PBuffer>(tup.pData))) == val;
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

// cast booleans to char strings
void
CalcExtCastTest::testCalcExtCastBooleanToChar()
{
    // int8 test values: (null, true, false),
    // cast to CHAR(3) and CHAR(4) and CHAR(5)
    ostringstream pg(""), outloc("");
    outloc <<  "c,3, c,3, c,3, c,4, c,4, c,4, c,5, c,5, c,5;" << endl;
    pg << "O " << outloc.str();
    pg << "L " << outloc.str();
    pg << "C bo, bo, bo;" << endl;
    pg << "V , 1, 0;" << endl;
    pg << "T;" << endl;
    // cast all to CHAR(3)
    for (int i=0; i < 3; i++)
        pg << "CALL 'castA(L" << i << ", C" << i << ");" << endl;
    // cast all to CHAR(4)
    for (int i=0; i < 3; i++)
        pg << "CALL 'castA(L" << (i+3) << ", C" << i << ");" << endl;
    // cast all to CHAR(5)
    for (int i=0; i < 3; i++)
        pg << "CALL 'castA(L" << (i+6) << ", C" << i << ");" << endl;
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

    // check results:
    BOOST_CHECK(cmpTupNull(outTuple[0]));

    // true -> c(3) = invalid char
    BOOST_CHECK_EQUAL(iter->pc, 1);
    BOOST_CHECK_EQUAL(0, strcmp(iter->str, invalidCharErr));
    iter++;

    // false -> c(3) = invalid char
    BOOST_CHECK_EQUAL(iter->pc, 2);
    BOOST_CHECK_EQUAL(0, strcmp(iter->str, invalidCharErr));
    iter++;

    BOOST_CHECK(cmpTupNull(outTuple[3]));
    BOOST_CHECK_EQUAL(0, cmpTupStr(outTuple[4], "TRUE"));

    // false -> c(4) = invalid char
    BOOST_CHECK_EQUAL(iter->pc, 5);
    BOOST_CHECK_EQUAL(0, strcmp(iter->str, invalidCharErr));
    iter++;

    BOOST_CHECK(cmpTupNull(outTuple[6]));
    BOOST_CHECK_EQUAL(0, cmpTupStr(outTuple[7], "TRUE "));
    BOOST_CHECK_EQUAL(0, cmpTupStr(outTuple[8], "FALSE"));
    BOOST_CHECK(iter == calc.mWarnings.end());
}

// cast booleans to varchar strings
void
CalcExtCastTest::testCalcExtCastBooleanToVarChar()
{
    // int8 test values: (null, true, false),
    // cast to VARCHAR(3) and VARCHAR(4) and VARCHAR(5)
    ostringstream pg(""), outloc("");
    outloc <<  "vc,3, vc,3, vc,3, vc,4, vc,4, vc,4, vc,5, vc,5, vc,5;" << endl;
    pg << "O " << outloc.str();
    pg << "L " << outloc.str();
    pg << "C bo, bo, bo;" << endl;
    pg << "V , 1, 0;" << endl;
    pg << "T;" << endl;
    // cast all to VARCHAR(3)
    for (int i=0; i < 3; i++)
        pg << "CALL 'castA(L" << i << ", C" << i << ");" << endl;
    // cast all to VARCHAR(4)
    for (int i=0; i < 3; i++)
        pg << "CALL 'castA(L" << (i+3) << ", C" << i << ");" << endl;
    // cast all to VARCHAR(5)
    for (int i=0; i < 3; i++)
        pg << "CALL 'castA(L" << (i+6) << ", C" << i << ");" << endl;
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

    // check results:
    BOOST_CHECK(cmpTupNull(outTuple[0]));

    // true -> vc(3) = invalid char
    BOOST_CHECK_EQUAL(iter->pc, 1);
    BOOST_CHECK_EQUAL(0, strcmp(iter->str, invalidCharErr));
    iter++;

    // false -> vc(3) = invalid char
    BOOST_CHECK_EQUAL(iter->pc, 2);
    BOOST_CHECK_EQUAL(0, strcmp(iter->str, invalidCharErr));
    iter++;

    BOOST_CHECK(cmpTupNull(outTuple[3]));
    BOOST_CHECK_EQUAL(0, cmpTupStr(outTuple[4], "TRUE"));

    // false -> vc(4) = invalid char
    BOOST_CHECK_EQUAL(iter->pc, 5);
    BOOST_CHECK_EQUAL(0, strcmp(iter->str, invalidCharErr));
    iter++;

    BOOST_CHECK(cmpTupNull(outTuple[6]));
    BOOST_CHECK_EQUAL(0, cmpTupStr(outTuple[7], "TRUE"));
    BOOST_CHECK_EQUAL(0, cmpTupStr(outTuple[8], "FALSE"));
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

// cast decimal numbers (with precision/scale) to char strings
void
CalcExtCastTest::testCalcExtCastDecimalToChar()
{
    // decimal(5,2) test values: (null, 0, 10, -10.90, 4.30, -.09, .30),
    // cast to CHAR(6) and CHAR(16)

    // decimal(5,0) test values: (null, 0, 1000, -1090, 430, -9, 30),
    // cast to CHAR(5) and CHAR(16)

    // decimal(5,-2) test values: (null, 0, 100000, -109000, 43000, -900, 3000),
    // cast to CHAR(7) and CHAR(16)

    ostringstream pg(""), outloc("");
    outloc <<  "c,6, c,6, c,6, c,6, c,6, c,6, c,6, "
           <<  "c,16, c,16, c,16, c,16, c,16, c,16, c,16, "
           <<  "c,5, c,5, c,5, c,5, c,5, c,5, c,5, "
           <<  "c,16, c,16, c,16, c,16, c,16, c,16, c,16, "
           <<  "c,7, c,7, c,7, c,7, c,7, c,7, c,7, "
           <<  "c,16, c,16, c,16, c,16, c,16, c,16, c,16;" << endl;
    pg << "O " << outloc.str();
    pg << "L " << outloc.str();
    pg << "C s8, s8, s8, s8, s8, s8, s8, s4, s4, s4, s4;" << endl;
    pg << "V , 0, 1000, -1090, 430, -9, 30, 5, 2, 0, -2;" << endl;
    pg << "T;" << endl;
    // cast decimal(5,2) to CHAR(6)
    for (int i=0; i < 7; i++)
        pg << "CALL 'castA(L" << i << ", C" << i << ", C7, C8 );" << endl;
    // cast decimal(5,2) to CHAR(16)
    for (int i=0; i < 7; i++)
        pg << "CALL 'castA(L" << (i+7) << ", C" << i << ", C7, C8 );" << endl;

    // cast decimal(5,0) to CHAR(5)
    for (int i=0; i < 7; i++)
        pg << "CALL 'castA(L" << (i+14) << ", C" << i << ", C7, C9 );" << endl;
    // cast decimal(5,0) to CHAR(16)
    for (int i=0; i < 7; i++)
        pg << "CALL 'castA(L" << (i+21) << ", C" << i << ", C7, C9 );" << endl;

    // cast decimal(5,-2) to CHAR(7)
    for (int i=0; i < 7; i++)
        pg << "CALL 'castA(L" << (i+28) << ", C" << i << ", C7, C10 );" << endl;
    // cast decimal(5,-2) to CHAR(16)
    for (int i=0; i < 7; i++)
        pg << "CALL 'castA(L" << (i+35) << ", C" << i << ", C7, C10 );" << endl;

    refLocalOutput(pg, 7*6);      // make output available

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
    // decimal(5,2)
    BOOST_CHECK(cmpTupNull(outTuple[0]));
    BOOST_CHECK_EQUAL(0, cmpTupStr(outTuple[1], ".00   "));
    BOOST_CHECK_EQUAL(0, cmpTupStr(outTuple[2], "10.00 "));
    BOOST_CHECK_EQUAL(0, cmpTupStr(outTuple[3], "-10.90"));
    BOOST_CHECK_EQUAL(0, cmpTupStr(outTuple[4], "4.30  "));
    BOOST_CHECK_EQUAL(0, cmpTupStr(outTuple[5], "-.09  "));
    BOOST_CHECK_EQUAL(0, cmpTupStr(outTuple[6], ".30   "));
    BOOST_CHECK(cmpTupNull(outTuple[7]));
    BOOST_CHECK_EQUAL(0, cmpTupStr(outTuple[8],  ".00             "));
    BOOST_CHECK_EQUAL(0, cmpTupStr(outTuple[9],  "10.00           "));
    BOOST_CHECK_EQUAL(0, cmpTupStr(outTuple[10], "-10.90          "));
    BOOST_CHECK_EQUAL(0, cmpTupStr(outTuple[11], "4.30            "));
    BOOST_CHECK_EQUAL(0, cmpTupStr(outTuple[12], "-.09            "));
    BOOST_CHECK_EQUAL(0, cmpTupStr(outTuple[13], ".30             "));

    // decimal(5,0)
    BOOST_CHECK(cmpTupNull(outTuple[14]));
    BOOST_CHECK_EQUAL(0, cmpTupStr(outTuple[15], "0    "));
    BOOST_CHECK_EQUAL(0, cmpTupStr(outTuple[16], "1000 "));
    BOOST_CHECK_EQUAL(0, cmpTupStr(outTuple[17], "-1090"));
    BOOST_CHECK_EQUAL(0, cmpTupStr(outTuple[18], "430  "));
    BOOST_CHECK_EQUAL(0, cmpTupStr(outTuple[19], "-9   "));
    BOOST_CHECK_EQUAL(0, cmpTupStr(outTuple[20], "30   "));
    BOOST_CHECK(cmpTupNull(outTuple[21]));
    BOOST_CHECK_EQUAL(0, cmpTupStr(outTuple[22], "0               "));
    BOOST_CHECK_EQUAL(0, cmpTupStr(outTuple[23], "1000            "));
    BOOST_CHECK_EQUAL(0, cmpTupStr(outTuple[24], "-1090           "));
    BOOST_CHECK_EQUAL(0, cmpTupStr(outTuple[25], "430             "));
    BOOST_CHECK_EQUAL(0, cmpTupStr(outTuple[26], "-9              "));
    BOOST_CHECK_EQUAL(0, cmpTupStr(outTuple[27], "30              "));

    // decimal(5,-2)
    BOOST_CHECK(cmpTupNull(outTuple[28]));
    BOOST_CHECK_EQUAL(0, cmpTupStr(outTuple[29], "0      "));
    BOOST_CHECK_EQUAL(0, cmpTupStr(outTuple[30], "100000 "));
    BOOST_CHECK_EQUAL(0, cmpTupStr(outTuple[31], "-109000"));
    BOOST_CHECK_EQUAL(0, cmpTupStr(outTuple[32], "43000  "));
    BOOST_CHECK_EQUAL(0, cmpTupStr(outTuple[33], "-900   "));
    BOOST_CHECK_EQUAL(0, cmpTupStr(outTuple[34], "3000   "));
    BOOST_CHECK(cmpTupNull(outTuple[35]));
    BOOST_CHECK_EQUAL(0, cmpTupStr(outTuple[36], "0               "));
    BOOST_CHECK_EQUAL(0, cmpTupStr(outTuple[37], "100000          "));
    BOOST_CHECK_EQUAL(0, cmpTupStr(outTuple[38], "-109000         "));
    BOOST_CHECK_EQUAL(0, cmpTupStr(outTuple[39], "43000           "));
    BOOST_CHECK_EQUAL(0, cmpTupStr(outTuple[40], "-900            "));
    BOOST_CHECK_EQUAL(0, cmpTupStr(outTuple[41], "3000            "));
}

// cast decimal numbers (with precision/scale) to varchar strings
void
CalcExtCastTest::testCalcExtCastDecimalToVarChar()
{
    // decimal(5,2) test values: (null, 0, 10, -10.90, 4.30, -.09, .30),
    // cast to VARCHAR(6) and VARCHAR(16)

    // decimal(5,0) test values: (null, 0, 1000, -1090, 430, -9, 30),
    // cast to VARCHAR(5) and VARCHAR(16)

    // decimal(5,-2) test values: (null, 0, 100000, -109000, 43000, -900, 3000),
    // cast to VARCHAR(7) and VARCHAR(16)

    ostringstream pg(""), outloc("");
    outloc <<  "vc,6, vc,6, vc,6, vc,6, vc,6, vc,6, vc,6, "
           <<  "vc,16, vc,16, vc,16, vc,16, vc,16, vc,16, vc,16, "
           <<  "vc,5, vc,5, vc,5, vc,5, vc,5, vc,5, vc,5, "
           <<  "vc,16, vc,16, vc,16, vc,16, vc,16, vc,16, vc,16, "
           <<  "vc,7, vc,7, vc,7, vc,7, vc,7, vc,7, vc,7, "
           <<  "vc,16, vc,16, vc,16, vc,16, vc,16, vc,16, vc,16;" << endl;
    pg << "O " << outloc.str();
    pg << "L " << outloc.str();
    pg << "C s8, s8, s8, s8, s8, s8, s8, s4, s4, s4, s4;" << endl;
    pg << "V , 0, 1000, -1090, 430, -9, 30, 5, 2, 0, -2;" << endl;
    pg << "T;" << endl;
    // cast decimal(5,2) to VARCHAR(6)
    for (int i=0; i < 7; i++)
        pg << "CALL 'castA(L" << i << ", C" << i << ", C7, C8 );" << endl;
    // cast decimal(5,2) to VARCHAR(16)
    for (int i=0; i < 7; i++)
        pg << "CALL 'castA(L" << (i+7) << ", C" << i << ", C7, C8 );" << endl;

    // cast decimal(5,0) to VARCHAR(5)
    for (int i=0; i < 7; i++)
        pg << "CALL 'castA(L" << (i+14) << ", C" << i << ", C7, C9 );" << endl;
    // cast decimal(5,0) to VARCHAR(16)
    for (int i=0; i < 7; i++)
        pg << "CALL 'castA(L" << (i+21) << ", C" << i << ", C7, C9 );" << endl;

    // cast decimal(5,-2) to VARCHAR(7)
    for (int i=0; i < 7; i++)
        pg << "CALL 'castA(L" << (i+28) << ", C" << i << ", C7, C10 );" << endl;
    // cast decimal(5,-2) to VARCHAR(16)
    for (int i=0; i < 7; i++)
        pg << "CALL 'castA(L" << (i+35) << ", C" << i << ", C7, C10 );" << endl;

    refLocalOutput(pg, 7*6);      // make output available

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
    // decimal(5,2)
    BOOST_CHECK(cmpTupNull(outTuple[0]));
    BOOST_CHECK_EQUAL(0, cmpTupStr(outTuple[1], ".00"));
    BOOST_CHECK_EQUAL(0, cmpTupStr(outTuple[2], "10.00"));
    BOOST_CHECK_EQUAL(0, cmpTupStr(outTuple[3], "-10.90"));
    BOOST_CHECK_EQUAL(0, cmpTupStr(outTuple[4], "4.30"));
    BOOST_CHECK_EQUAL(0, cmpTupStr(outTuple[5], "-.09"));
    BOOST_CHECK_EQUAL(0, cmpTupStr(outTuple[6], ".30"));
    BOOST_CHECK(cmpTupNull(outTuple[7]));
    BOOST_CHECK_EQUAL(0, cmpTupStr(outTuple[8],  ".00"));
    BOOST_CHECK_EQUAL(0, cmpTupStr(outTuple[9],  "10.00"));
    BOOST_CHECK_EQUAL(0, cmpTupStr(outTuple[10], "-10.90"));
    BOOST_CHECK_EQUAL(0, cmpTupStr(outTuple[11], "4.30"));
    BOOST_CHECK_EQUAL(0, cmpTupStr(outTuple[12], "-.09"));
    BOOST_CHECK_EQUAL(0, cmpTupStr(outTuple[13], ".30"));

    // decimal(5,0)
    BOOST_CHECK(cmpTupNull(outTuple[14]));
    BOOST_CHECK_EQUAL(0, cmpTupStr(outTuple[15], "0"));
    BOOST_CHECK_EQUAL(0, cmpTupStr(outTuple[16], "1000"));
    BOOST_CHECK_EQUAL(0, cmpTupStr(outTuple[17], "-1090"));
    BOOST_CHECK_EQUAL(0, cmpTupStr(outTuple[18], "430"));
    BOOST_CHECK_EQUAL(0, cmpTupStr(outTuple[19], "-9"));
    BOOST_CHECK_EQUAL(0, cmpTupStr(outTuple[20], "30"));
    BOOST_CHECK(cmpTupNull(outTuple[21]));
    BOOST_CHECK_EQUAL(0, cmpTupStr(outTuple[22], "0"));
    BOOST_CHECK_EQUAL(0, cmpTupStr(outTuple[23], "1000"));
    BOOST_CHECK_EQUAL(0, cmpTupStr(outTuple[24], "-1090"));
    BOOST_CHECK_EQUAL(0, cmpTupStr(outTuple[25], "430"));
    BOOST_CHECK_EQUAL(0, cmpTupStr(outTuple[26], "-9"));
    BOOST_CHECK_EQUAL(0, cmpTupStr(outTuple[27], "30"));

    // decimal(5,-2)
    BOOST_CHECK(cmpTupNull(outTuple[28]));
    BOOST_CHECK_EQUAL(0, cmpTupStr(outTuple[29], "0"));
    BOOST_CHECK_EQUAL(0, cmpTupStr(outTuple[30], "100000"));
    BOOST_CHECK_EQUAL(0, cmpTupStr(outTuple[31], "-109000"));
    BOOST_CHECK_EQUAL(0, cmpTupStr(outTuple[32], "43000"));
    BOOST_CHECK_EQUAL(0, cmpTupStr(outTuple[33], "-900"));
    BOOST_CHECK_EQUAL(0, cmpTupStr(outTuple[34], "3000"));
    BOOST_CHECK(cmpTupNull(outTuple[35]));
    BOOST_CHECK_EQUAL(0, cmpTupStr(outTuple[36], "0"));
    BOOST_CHECK_EQUAL(0, cmpTupStr(outTuple[37], "100000"));
    BOOST_CHECK_EQUAL(0, cmpTupStr(outTuple[38], "-109000"));
    BOOST_CHECK_EQUAL(0, cmpTupStr(outTuple[39], "43000"));
    BOOST_CHECK_EQUAL(0, cmpTupStr(outTuple[40], "-900"));
    BOOST_CHECK_EQUAL(0, cmpTupStr(outTuple[41], "3000"));
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
    BOOST_CHECK_EQUAL(0, cmpTupStr(outTuple[0], "166"));
    BOOST_CHECK_EQUAL(iter->pc, 0);
    BOOST_CHECK_EQUAL(0, strcmp(iter->str, truncErr));
    iter++;

    // -1666 -> vc(3) = truncates
    BOOST_CHECK_EQUAL(0, cmpTupStr(outTuple[1], "-16"));
    BOOST_CHECK_EQUAL(iter->pc, 1);
    BOOST_CHECK_EQUAL(0, strcmp(iter->str, truncErr));
    iter++;

    // 1666 -> c(3) = truncates
    BOOST_CHECK_EQUAL(0, cmpTupStr(outTuple[2], "166"));
    BOOST_CHECK_EQUAL(iter->pc, 2);
    BOOST_CHECK_EQUAL(0, strcmp(iter->str, truncErr));
    iter++;

    // -1666 -> c(3) = truncates
    BOOST_CHECK_EQUAL(0, cmpTupStr(outTuple[3], "-16"));
    BOOST_CHECK_EQUAL(iter->pc, 3);
    BOOST_CHECK_EQUAL(0, strcmp(iter->str, truncErr));
    iter++;
    BOOST_CHECK(iter == calc.mWarnings.end());
}


// cast decimal numbers to strings; truncates
void
CalcExtCastTest::testCalcExtCastDecimalToStringTruncates()
{
    // decimal(5,2) test values: (-10.9, -.09, .30),
    // decimal(5,0) test values: (-1090, -9, 30),
    // decimal(5,-2) test values: (-109000, -900, 3000),

    // cast to VARCHAR(3) and CHAR(3)
    ostringstream pg(""), outloc("");
    outloc <<  "vc,3, vc,3, vc,3, c,3, c,3, c,3, "
           <<  "vc,3, vc,3, vc,3, c,3, c,3, c,3, "
           <<  "vc,3, vc,3, vc,3, c,3, c,3, c,3;" << endl;
    pg << "O " << outloc.str();
    pg << "L " << outloc.str();
    pg << "C s8, s8, s8, s4, s4, s4, s4;" << endl;
    pg << "V -1090, -9, 30, 5, 2, 0, -2;" << endl;
    pg << "T;" << endl;

    pg << "CALL 'castA(L0, C0, C3, C4);" << endl;
    pg << "CALL 'castA(L1, C1, C3, C4);" << endl;
    pg << "CALL 'castA(L2, C2, C3, C4);" << endl;
    pg << "CALL 'castA(L3, C0, C3, C4);" << endl;
    pg << "CALL 'castA(L4, C1, C3, C4);" << endl;
    pg << "CALL 'castA(L5, C2, C3, C4);" << endl;

    pg << "CALL 'castA(L6, C0, C3, C5);" << endl;
    pg << "CALL 'castA(L7, C1, C3, C5);" << endl;
    pg << "CALL 'castA(L8, C2, C3, C5);" << endl;
    pg << "CALL 'castA(L9, C0, C3, C5);" << endl;
    pg << "CALL 'castA(L10, C1, C3, C5);" << endl;
    pg << "CALL 'castA(L11, C2, C3, C5);" << endl;

    pg << "CALL 'castA(L12, C0, C3, C6);" << endl;
    pg << "CALL 'castA(L13, C1, C3, C6);" << endl;
    pg << "CALL 'castA(L14, C2, C3, C6);" << endl;
    pg << "CALL 'castA(L15, C0, C3, C6);" << endl;
    pg << "CALL 'castA(L16, C1, C3, C6);" << endl;
    pg << "CALL 'castA(L17, C2, C3, C6);" << endl;

    refLocalOutput(pg, 18);      // make output available

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
    // decimal(5,2)
    // -10.9 -> vc(3) = truncates
    BOOST_CHECK_EQUAL(iter->pc, 0);
    BOOST_CHECK_EQUAL(0, strcmp(iter->str, truncErr));
    iter++;

    // -.09 -> vc(3) = truncates
    BOOST_CHECK_EQUAL(iter->pc, 1);
    BOOST_CHECK_EQUAL(0, strcmp(iter->str, truncErr));
    iter++;

    // .30 - > vc(3) = ok
    BOOST_CHECK_EQUAL(0, cmpTupStr(outTuple[2], ".30"));

    // -10.9 -> c(3) = truncates
    BOOST_CHECK_EQUAL(iter->pc, 3);
    BOOST_CHECK_EQUAL(0, strcmp(iter->str, truncErr));
    iter++;

    // -.09 -> c(3) = truncates
    BOOST_CHECK_EQUAL(iter->pc, 4);
    BOOST_CHECK_EQUAL(0, strcmp(iter->str, truncErr));
    iter++;

    // .30 - > c(3) = ok
    BOOST_CHECK_EQUAL(0, cmpTupStr(outTuple[5], ".30"));

    // decimal(5,0)
    // -1090 -> vc(3) = truncates
    BOOST_CHECK_EQUAL(iter->pc, 6);
    BOOST_CHECK_EQUAL(0, strcmp(iter->str, truncErr));
    iter++;

    // -9 -> vc(3) = ok
    BOOST_CHECK_EQUAL(0, cmpTupStr(outTuple[7], "-9"));

    // 30 - > vc(3) = ok
    BOOST_CHECK_EQUAL(0, cmpTupStr(outTuple[8], "30"));

    // -1090 -> c(3) = truncates
    BOOST_CHECK_EQUAL(iter->pc, 9);
    BOOST_CHECK_EQUAL(0, strcmp(iter->str, truncErr));
    iter++;

    // -9 -> c(3) = ok
    BOOST_CHECK_EQUAL(0, cmpTupStr(outTuple[10], "-9 "));

    // 30 - > c(3) = ok
    BOOST_CHECK_EQUAL(0, cmpTupStr(outTuple[11], "30 "));

    // decimal(5,-2)
    // -109000 -> vc(3) = truncates
    BOOST_CHECK_EQUAL(iter->pc, 12);
    BOOST_CHECK_EQUAL(0, strcmp(iter->str, truncErr));
    iter++;

    // -900 -> vc(3) = truncates
    BOOST_CHECK_EQUAL(iter->pc, 13);
    BOOST_CHECK_EQUAL(0, strcmp(iter->str, truncErr));
    iter++;

    // 3000 -> vc(3) = truncates
    BOOST_CHECK_EQUAL(iter->pc, 14);
    BOOST_CHECK_EQUAL(0, strcmp(iter->str, truncErr));
    iter++;

    // -109000 -> c(3) = truncates
    BOOST_CHECK_EQUAL(iter->pc, 15);
    BOOST_CHECK_EQUAL(0, strcmp(iter->str, truncErr));
    iter++;

    // -900 -> c(3) = truncates
    BOOST_CHECK_EQUAL(iter->pc, 16);
    BOOST_CHECK_EQUAL(0, strcmp(iter->str, truncErr));
    iter++;

    // 3000 -> c(3) = truncates
    BOOST_CHECK_EQUAL(iter->pc, 17);
    BOOST_CHECK_EQUAL(0, strcmp(iter->str, truncErr));
    iter++;

    BOOST_CHECK(iter == calc.mWarnings.end());
}

void CalcExtCastTest::testCalcExtCastCharToBoolean()
{
    // test values: null, true, false, unknown, invalid; same with spaces.
    ostringstream pg(""), outloc("");
    outloc << "bo, bo, bo, bo, bo, bo, bo, bo;" << endl;
    pg << "O " << outloc.str();
    pg << "L " << outloc.str();
    pg << "C c,4, c,4, c,8, c,5, c,9, c,7, c,11, c,13;" << endl;
    pg << "V "                  // a null
       << ", 0x" << stringToHex("tRUe")
       << ", 0x" << stringToHex("  true  ")
       << ", 0x" << stringToHex("faLSe")
       << ", 0x" << stringToHex("  FALSE  ")
       << ", 0x" << stringToHex("UnknowN")
       << ", 0x" << stringToHex("  UnknowN  ")
       << ", 0x" << stringToHex("  Invalid    ")
       << ";" << endl;
    pg << "T;" << endl;
    for (int i = 0; i < 8; i++)
        pg << "CALL 'castA(L"<<i<<",C"<<i<<");"<< endl;
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

    BOOST_CHECK_EQUAL(1, cmpTupNull(outTuple[0]));
    BOOST_CHECK_EQUAL(1, cmpTupBool(outTuple[1], true));
    BOOST_CHECK_EQUAL(1, cmpTupBool(outTuple[2], true));
    BOOST_CHECK_EQUAL(1, cmpTupBool(outTuple[3], false));
    BOOST_CHECK_EQUAL(1, cmpTupBool(outTuple[4], false));

    // TODO: SQL2003 says 'unknown' should be treated as null
    // For now, unknown -> bool = invalid char
    BOOST_CHECK_EQUAL(iter->pc, 5);
    BOOST_CHECK_EQUAL(0, strcmp(iter->str, invalidCharErr));
    iter++;

    BOOST_CHECK_EQUAL(iter->pc, 6);
    BOOST_CHECK_EQUAL(0, strcmp(iter->str, invalidCharErr));
    iter++;

    // invalid -> bool = invalid char
    BOOST_CHECK_EQUAL(iter->pc, 7);
    BOOST_CHECK_EQUAL(0, strcmp(iter->str, invalidCharErr));
    iter++;

    BOOST_CHECK(iter == calc.mWarnings.end());
}

void CalcExtCastTest::testCalcExtCastVarCharToBoolean()
{
    // test values: null, true, false, unknown, invalid; same with spaces.
    ostringstream pg(""), outloc("");
    outloc << "bo, bo, bo, bo, bo, bo, bo, bo;" << endl;
    pg << "O " << outloc.str();
    pg << "L " << outloc.str();
    pg << "C vc,4, vc,8, vc,8, vc,9, vc,9, vc,7, vc,11, vc,13;" << endl;
    pg << "V "                  // a null
       << ", 0x" << stringToHex("tRUe")
       << ", 0x" << stringToHex("  true  ")
       << ", 0x" << stringToHex("faLSe")
       << ", 0x" << stringToHex("  FALSE  ")
       << ", 0x" << stringToHex("UnknowN")
       << ", 0x" << stringToHex("  UnknowN  ")
       << ", 0x" << stringToHex("  Invalid    ")
       << ";" << endl;
    pg << "T;" << endl;
    for (int i = 0; i < 8; i++)
        pg << "CALL 'castA(L"<<i<<",C"<<i<<");"<< endl;
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

    BOOST_CHECK_EQUAL(1, cmpTupNull(outTuple[0]));
    BOOST_CHECK_EQUAL(1, cmpTupBool(outTuple[1], true));
    BOOST_CHECK_EQUAL(1, cmpTupBool(outTuple[2], true));
    BOOST_CHECK_EQUAL(1, cmpTupBool(outTuple[3], false));
    BOOST_CHECK_EQUAL(1, cmpTupBool(outTuple[4], false));

    // TODO: SQL2003 says 'unknown' should be treated as null
    // For now, unknown -> bool = invalid char
    BOOST_CHECK_EQUAL(iter->pc, 5);
    BOOST_CHECK_EQUAL(0, strcmp(iter->str, invalidCharErr));
    iter++;

    BOOST_CHECK_EQUAL(iter->pc, 6);
    BOOST_CHECK_EQUAL(0, strcmp(iter->str, invalidCharErr));
    iter++;

    // invalid -> bool = invalid char
    BOOST_CHECK_EQUAL(iter->pc, 7);
    BOOST_CHECK_EQUAL(0, strcmp(iter->str, invalidCharErr));
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

void CalcExtCastTest::testCalcExtCastVarCharToDecimal()
{
    // test values: null, 99.99, -105.0e-3, 950.00, 234.446
    // same with trailing spaces.to
    // decimal(5, 2)
    // decimal(5, 0)
    // decimal(5, -2)
    ostringstream pg(""), outloc("");
    outloc << "s8, s8, s8, s8, s8, s8, s8, s8, s8, "
           << "s8, s8, s8, s8, s8, s8, s8, s8, s8, "
           << "s8, s8, s8, s8, s8, s8, s8, s8, s8;"
           << endl;
    pg << "O " << outloc.str();
    pg << "L " << outloc.str();
    pg << "C vc,1, vc,5, vc,9, vc,6, vc,7, vc,10, vc,10, vc,10, vc,10, "
       << "  s4, s4, s4, s4;" << endl;
    pg << "V "                  // a null
       << ", 0x" << stringToHex("99.99")
       << ", 0x" << stringToHex("-105.0e-3")
       << ", 0x" << stringToHex("950.00")
       << ", 0x" << stringToHex("234.446")
       << ", 0x" << stringToHex("99.99     ")
       << ", 0x" << stringToHex("-105.0e-3 ")
       << ", 0x" << stringToHex("950.00    ")
       << ", 0x" << stringToHex("234.446   ")
       << ", 5, 2, 0, -2;" << endl;
    pg << "T;" << endl;

    for (int i = 0; i < 9; i++)
        pg << "CALL 'castA(L"<<i<<",C"<<i<<",C9, C10);"<< endl;
    for (int i = 0; i < 9; i++)
        pg << "CALL 'castA(L"<<(i+9)<<",C"<<i<<",C9, C11);"<< endl;
    for (int i = 0; i < 9; i++)
        pg << "CALL 'castA(L"<<(i+18)<<",C"<<i<<",C9, C12);"<< endl;

    refLocalOutput(pg, 9*3);      // make output available

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

    // decimal(5,2)
    BOOST_CHECK_EQUAL(1, cmpTupNull(outTuple[0]));
    BOOST_CHECK_EQUAL(0, cmpTupInt(outTuple[1],  9999));
    BOOST_CHECK_EQUAL(0, cmpTupInt(outTuple[2],  -11));
    BOOST_CHECK_EQUAL(0, cmpTupInt(outTuple[3],  95000));
    BOOST_CHECK_EQUAL(0, cmpTupInt(outTuple[4],  23445));
    BOOST_CHECK_EQUAL(0, cmpTupInt(outTuple[5],  9999));
    BOOST_CHECK_EQUAL(0, cmpTupInt(outTuple[6],  -11));
    BOOST_CHECK_EQUAL(0, cmpTupInt(outTuple[7],  95000));
    BOOST_CHECK_EQUAL(0, cmpTupInt(outTuple[8],  23445));

    // decimal(5,0)
    BOOST_CHECK_EQUAL(1, cmpTupNull(outTuple[9]));
    BOOST_CHECK_EQUAL(0, cmpTupInt(outTuple[10],  100));
    BOOST_CHECK_EQUAL(0, cmpTupInt(outTuple[11],  0));
    BOOST_CHECK_EQUAL(0, cmpTupInt(outTuple[12],  950));
    BOOST_CHECK_EQUAL(0, cmpTupInt(outTuple[13],  234));
    BOOST_CHECK_EQUAL(0, cmpTupInt(outTuple[14],  100));
    BOOST_CHECK_EQUAL(0, cmpTupInt(outTuple[15],  0));
    BOOST_CHECK_EQUAL(0, cmpTupInt(outTuple[16],  950));
    BOOST_CHECK_EQUAL(0, cmpTupInt(outTuple[17],  234));

    // decimal(5,-2)
    BOOST_CHECK_EQUAL(1, cmpTupNull(outTuple[18]));
    BOOST_CHECK_EQUAL(0, cmpTupInt(outTuple[19],  1));
    BOOST_CHECK_EQUAL(0, cmpTupInt(outTuple[20],  0));
    BOOST_CHECK_EQUAL(0, cmpTupInt(outTuple[21],  10));
    BOOST_CHECK_EQUAL(0, cmpTupInt(outTuple[22],  2));
    BOOST_CHECK_EQUAL(0, cmpTupInt(outTuple[23],  1));
    BOOST_CHECK_EQUAL(0, cmpTupInt(outTuple[24],  0));
    BOOST_CHECK_EQUAL(0, cmpTupInt(outTuple[25],  10));
    BOOST_CHECK_EQUAL(0, cmpTupInt(outTuple[26],  2));

    BOOST_CHECK(iter == calc.mWarnings.end());
}

void CalcExtCastTest::testCalcExtCastCharToDecimal()
{
    // test values: null, .8987, -0005.2, +980, 0.000000000000355e14
    // same with trailing spaces.to
    // decimal(5, 2)
    // decimal(5, 0)
    // decimal(5, -2)
    ostringstream pg(""), outloc("");
    outloc << "s8, s8, s8, s8, s8, s8, s8, s8, s8, "
           << "s8, s8, s8, s8, s8, s8, s8, s8, s8, "
           << "s8, s8, s8, s8, s8, s8, s8, s8, s8;"
           << endl;
    pg << "O " << outloc.str();
    pg << "L " << outloc.str();
    pg << "C c,1, c,5, c,7, c,4, c,20, c,10, c,10, c,10, c,32, "
       << "  s4, s4, s4, s4;" << endl;
    pg << "V "                  // a null
       << ", 0x" << stringToHex(".8987")
       << ", 0x" << stringToHex("-0005.2")
       << ", 0x" << stringToHex("+980")
       << ", 0x" << stringToHex("0.000000000000355e14")
       << ", 0x" << stringToHex(".8987     ")
       << ", 0x" << stringToHex("-0005.2   ")
       << ", 0x" << stringToHex("+980      ")
       << ", 0x" << stringToHex("0.000000000000355e14            ")
       << ", 5, 2, 0, -2;" << endl;
    pg << "T;" << endl;

    for (int i = 0; i < 9; i++)
        pg << "CALL 'castA(L"<<i<<",C"<<i<<",C9, C10);"<< endl;
    for (int i = 0; i < 9; i++)
        pg << "CALL 'castA(L"<<(i+9)<<",C"<<i<<",C9, C11);"<< endl;
    for (int i = 0; i < 9; i++)
        pg << "CALL 'castA(L"<<(i+18)<<",C"<<i<<",C9, C12);"<< endl;

    refLocalOutput(pg, 9*3);      // make output available

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

    // decimal(5,2)
    BOOST_CHECK_EQUAL(1, cmpTupNull(outTuple[0]));
    BOOST_CHECK_EQUAL(0, cmpTupInt(outTuple[1],  90));
    BOOST_CHECK_EQUAL(0, cmpTupInt(outTuple[2],  -520));
    BOOST_CHECK_EQUAL(0, cmpTupInt(outTuple[3],  98000));
    BOOST_CHECK_EQUAL(0, cmpTupInt(outTuple[4],  3550));
    BOOST_CHECK_EQUAL(0, cmpTupInt(outTuple[5],  90));
    BOOST_CHECK_EQUAL(0, cmpTupInt(outTuple[6],  -520));
    BOOST_CHECK_EQUAL(0, cmpTupInt(outTuple[7],  98000));
    BOOST_CHECK_EQUAL(0, cmpTupInt(outTuple[8],  3550));

    // decimal(5,0)
    BOOST_CHECK_EQUAL(1, cmpTupNull(outTuple[9]));
    BOOST_CHECK_EQUAL(0, cmpTupInt(outTuple[10],  1));
    BOOST_CHECK_EQUAL(0, cmpTupInt(outTuple[11],  -5));
    BOOST_CHECK_EQUAL(0, cmpTupInt(outTuple[12],  980));
    BOOST_CHECK_EQUAL(0, cmpTupInt(outTuple[13],  36));
    BOOST_CHECK_EQUAL(0, cmpTupInt(outTuple[14],  1));
    BOOST_CHECK_EQUAL(0, cmpTupInt(outTuple[15],  -5));
    BOOST_CHECK_EQUAL(0, cmpTupInt(outTuple[16],  980));
    BOOST_CHECK_EQUAL(0, cmpTupInt(outTuple[17],  36));

    // decimal(5,-2)
    BOOST_CHECK_EQUAL(1, cmpTupNull(outTuple[18]));
    BOOST_CHECK_EQUAL(0, cmpTupInt(outTuple[19],  0));
    BOOST_CHECK_EQUAL(0, cmpTupInt(outTuple[20],  0));
    BOOST_CHECK_EQUAL(0, cmpTupInt(outTuple[21],  10));
    BOOST_CHECK_EQUAL(0, cmpTupInt(outTuple[22],  0));
    BOOST_CHECK_EQUAL(0, cmpTupInt(outTuple[23],  0));
    BOOST_CHECK_EQUAL(0, cmpTupInt(outTuple[24],  0));
    BOOST_CHECK_EQUAL(0, cmpTupInt(outTuple[25],  10));
    BOOST_CHECK_EQUAL(0, cmpTupInt(outTuple[26],  0));

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

void CalcExtCastTest::testCalcExtCastStringToDecimalFails()
{
    // test invalid values: 12c, 34.54.243, 342.342e453.23, 234e 23
    // cast to decimal(5,2)
    ostringstream pg(""), outloc("");
    outloc << "s8, s8, s8, s8, s8, s8, s8, s8;" << endl;
    pg << "O " << outloc.str();
    pg << "L " << outloc.str();
    pg << "C vc,3, vc,9, vc,14, vc,7, c,3, c,9, c,14, c,7, "
       << "  s4, s4;" << endl;
    pg << "V 0x" << stringToHex("12c")
       << ", 0x" << stringToHex("34.54.243")
       << ", 0x" << stringToHex("342.342e453.23")
       << ", 0x" << stringToHex("234e 23")
       << ", 0x" << stringToHex("12c")
       << ", 0x" << stringToHex("34.54.243")
       << ", 0x" << stringToHex("342.342e453.23")
       << ", 0x" << stringToHex("234e 23")
       << ", 5, 2;" << endl;
    pg << "T;" << endl;

    for (int i = 0; i < 8; i++)
        pg << "CALL 'castA(L"<<i<<",C"<<i<<",C8, C9);"<< endl;

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

    // all the casts should have failed
    deque<CalcMessage>::iterator iter = calc.mWarnings.begin();
    for (int pc = 0; pc < 8; pc++, iter++) {
        BOOST_CHECK_EQUAL(iter->pc, pc);
        BOOST_CHECK_EQUAL(0, strcmp(iter->str, invalidCharErr));
    }
    BOOST_CHECK(iter == calc.mWarnings.end());
}

void CalcExtCastTest::testCalcExtCastStringToDecimalMinMax()
{
    // test values: MIN, MAX,
    //              9223372036854775808 (MAX+1),
    //              9223372036854775807.12345
    //              9223372036854775807.9
    //              -9223372036854775809
    //              -9223372036854775807.9
    //              -9223372036854775808.9
    //              9323415432153452535
    //              9.78E+18
    //              9.78E+20
    // cast to decimal(19, 0)
    // cast to decimal(9, -10)
    ostringstream pg(""), outloc("");
    outloc << "s8, s8, s8, s8, s8, s8, s8, s8, s8, s8, s8, "
           << "s8, s8, s8, s8, s8, s8, s8, s8, s8, s8, s8;" << endl;
    pg << "O " << outloc.str();
    pg << "L " << outloc.str();
    pg << "C vc,30, vc,30, vc,30, vc,30, vc,30, "
       << "  vc,30, vc,30, vc,30, vc,30, vc,30, vc,30, "
       << "  s4, s4, s4, s4;" << endl;
    pg << "V 0x" << stringToHex(minInt64String())
       << ", 0x" << stringToHex(maxInt64String())
       << ", 0x" << stringToHex("9223372036854775808")
       << ", 0x" << stringToHex("9223372036854775807.12345")
       << ", 0x" << stringToHex("9223372036854775807.9")
       << ", 0x" << stringToHex("-9223372036854775809")
       << ", 0x" << stringToHex("-9223372036854775807.9")
       << ", 0x" << stringToHex("-9223372036854775808.9")
       << ", 0x" << stringToHex("9323415432153452535")
       << ", 0x" << stringToHex("9.78E+18")
       << ", 0x" << stringToHex("9.78E+20")
       << ", 19, 0, 9, -10;" << endl;
    pg << "T;" << endl;

    for (int i = 0; i < 11; i++)
        pg << "CALL 'castA(L"<<i<<",C"<<i<<",C11, C12);"<< endl;

    for (int i = 0; i < 11; i++)
        pg << "CALL 'castA(L"<<(i+11)<<",C"<<i<<",C13, C14);"<< endl;

    refLocalOutput(pg, 22);      // make output available

    Calculator calc(0);
    try {
        calc.assemble(pg.str().c_str());
    }
    catch (FennelExcn& ex) {
        cout << ex.getMessage();
        BOOST_MESSAGE("Assemble exception " << ex.getMessage());
        BOOST_MESSAGE(pg.str());
        BOOST_REQUIRE(0);
    }

    TupleDataWithBuffer outTuple(calc.getOutputRegisterDescriptor());
    TupleDataWithBuffer inTuple(calc.getInputRegisterDescriptor());
    calc.bind(&inTuple, &outTuple);
    calc.exec();
    printOutput(outTuple, calc);

    // check results:
    deque<CalcMessage>::iterator iter = calc.mWarnings.begin();

    // decimal(19,0)
    // MIN -> decimal(19,0) = ok
    BOOST_CHECK_EQUAL(0, cmpTupInt64(outTuple[0], std::numeric_limits<int64_t>::min()));

    // MAX -> decimal(19,0) = ok
    BOOST_CHECK_EQUAL(0, cmpTupInt64(outTuple[1], std::numeric_limits<int64_t>::max()));

    // MAX + 1 -> decimal(19,0) = out of range
    BOOST_CHECK_EQUAL(iter->pc, 2);
    BOOST_CHECK_EQUAL(0, strcmp(iter->str, outOfRangeErr));
    iter++;

    // MAX.12345 -> decimal(19, 0) = ok
    BOOST_CHECK_EQUAL(0, cmpTupInt64(outTuple[3], std::numeric_limits<int64_t>::max()));

    // MAX.9 -> decimal(19, 0) = out of range
    BOOST_CHECK_EQUAL(iter->pc, 4);
    BOOST_CHECK_EQUAL(0, strcmp(iter->str, outOfRangeErr));
    iter++;

    // MIN - 1 -> decimal(19,0) = out of range
    BOOST_CHECK_EQUAL(iter->pc, 5);
    BOOST_CHECK_EQUAL(0, strcmp(iter->str, outOfRangeErr));
    iter++;

    // (MIN+1).9 -> decimal(19,0) = ok
    BOOST_CHECK_EQUAL(0, cmpTupInt64(outTuple[6], std::numeric_limits<int64_t>::min()));

    // MIN.9 -> decimal(19,0) = out of range
    BOOST_CHECK_EQUAL(iter->pc, 7);
    BOOST_CHECK_EQUAL(0, strcmp(iter->str, outOfRangeErr));
    iter++;

    // 9323415432153452535 -> decimal(19,0) = out of range
    BOOST_CHECK_EQUAL(iter->pc, 8);
    BOOST_CHECK_EQUAL(0, strcmp(iter->str, outOfRangeErr));
    iter++;

    // 9.78E+18 -> decimal(19,0) = out of range
    BOOST_CHECK_EQUAL(iter->pc, 9);
    BOOST_CHECK_EQUAL(0, strcmp(iter->str, outOfRangeErr));
    iter++;

    // 9.78E+20 -> decimal(19,0) = out of range
    BOOST_CHECK_EQUAL(iter->pc, 10);
    BOOST_CHECK_EQUAL(0, strcmp(iter->str, outOfRangeErr));
    iter++;

    // decimal(9,-10)
    int64_t factor = 1;
    for (int i = 0; i < 10; i++) {
        factor *= 10;
    }
    int64_t smax = std::numeric_limits<int64_t>::max()/factor + 1;
    int64_t smin = std::numeric_limits<int64_t>::min()/factor - 1;

    // MIN -> decimal(9,-10) = ok
    BOOST_CHECK_EQUAL(0, cmpTupInt64(outTuple[11], smin));

    // MAX -> decimal(9,-10) = ok
    BOOST_CHECK_EQUAL(0, cmpTupInt64(outTuple[12], smax));

    // MAX + 1 -> decimal(9,-10) = ok
    BOOST_CHECK_EQUAL(0, cmpTupInt64(outTuple[13], smax));

    // MAX.12345 -> decimal(19,-10) = ok
    BOOST_CHECK_EQUAL(0, cmpTupInt64(outTuple[14], smax));

    // MAX.9 -> decimal(19,-10) = ok
    BOOST_CHECK_EQUAL(0, cmpTupInt64(outTuple[15], smax));

    // MIN - 1 -> decimal(9,-10) = ok
    BOOST_CHECK_EQUAL(0, cmpTupInt64(outTuple[16], smin));

    // (MIN+1).9 -> decimal(9,-10) = ok
    BOOST_CHECK_EQUAL(0, cmpTupInt64(outTuple[17], smin));

    // MIN.9 -> decimal(9,-10) = ok
    BOOST_CHECK_EQUAL(0, cmpTupInt64(outTuple[18], smin));

    // 9323415432153452535 -> decimal(9,-10) = ok
    BOOST_CHECK_EQUAL(0, cmpTupInt64(outTuple[19], 932341543ll));

    // 9.78E+18 -> decimal(9,-10) = ok
    BOOST_CHECK_EQUAL(0, cmpTupInt64(outTuple[20], 978000000ll));

    // 9.78E+20 -> decimal(9,-10) = out of range
    BOOST_CHECK_EQUAL(iter->pc, 21);
    BOOST_CHECK_EQUAL(0, strcmp(iter->str, outOfRangeErr));
    iter++;

    BOOST_CHECK(iter == calc.mWarnings.end());
}


void CalcExtCastTest::testCalcExtCastStringToDecimalRange()
{
    // test values: 1000 999.999, 999.991 9.99999e2 9.9999e2, 99999999990000000000e-20
    // cast to decimal(5, 2)
    ostringstream pg(""), outloc("");
    outloc << "s8, s8, s8, s8, s8, s8;" << endl;
    pg << "O " << outloc.str();
    pg << "L " << outloc.str();
    pg << "C vc,30, vc,30, vc,30, vc,30, vc,30, vc,30, "
       << "  s4, s4;" << endl;
    pg << "V 0x" << stringToHex("1000")
       << ", 0x" << stringToHex("999.999")
       << ", 0x" << stringToHex("999.991")
       << ", 0x" << stringToHex("9.99999e2")
       << ", 0x" << stringToHex("9.9999e2")
       << ", 0x" << stringToHex("99999999990000000000e-20")
       << ", 5, 2;" << endl;
    pg << "T;" << endl;

    for (int i = 0; i < 6; i++)
        pg << "CALL 'castA(L"<<i<<",C"<<i<<",C6, C7);"<< endl;

    refLocalOutput(pg, 6);      // make output available

    Calculator calc(0);
    try {
        calc.assemble(pg.str().c_str());
    }
    catch (FennelExcn& ex) {
        cout << ex.getMessage();
        BOOST_MESSAGE("Assemble exception " << ex.getMessage());
        BOOST_MESSAGE(pg.str());
        BOOST_REQUIRE(0);
    }

    TupleDataWithBuffer outTuple(calc.getOutputRegisterDescriptor());
    TupleDataWithBuffer inTuple(calc.getInputRegisterDescriptor());
    calc.bind(&inTuple, &outTuple);
    calc.exec();
    printOutput(outTuple, calc);

    // check results:
    deque<CalcMessage>::iterator iter = calc.mWarnings.begin();

    // decimal(5,2)
    // 1000 -> decimal(5,2) = out of range
    BOOST_CHECK_EQUAL(iter->pc, 0);
    BOOST_CHECK_EQUAL(0, strcmp(iter->str, outOfRangeErr));
    iter++;

    // 999.999 -> decimal(5,2) = out of range
    BOOST_CHECK_EQUAL(iter->pc, 1);
    BOOST_CHECK_EQUAL(0, strcmp(iter->str, outOfRangeErr));
    iter++;

    // 999.991 -> decimal(5,2) = ok
    BOOST_CHECK_EQUAL(0, cmpTupInt(outTuple[2], 99999));

    // 9.99999e2 -> decimal(5,2) = out of range
    BOOST_CHECK_EQUAL(iter->pc, 3);
    BOOST_CHECK_EQUAL(0, strcmp(iter->str, outOfRangeErr));
    iter++;

    // 9.9999e2 -> decimal(5,2) = ok
    BOOST_CHECK_EQUAL(0, cmpTupInt(outTuple[4], 99999));

    // 99999999990000000000e-20 -> decimal(5,2) = ok
    BOOST_CHECK_EQUAL(0, cmpTupInt(outTuple[5], 100));
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

    ostringstream pg(""), outloc("");
    outloc << "vc,16, vc,16, vc,16, vc,16, vc,16, vc,16, "
           << "c,16, c,16, c,16, c,16, c,16, c,16;" << endl;
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
    BOOST_CHECK_EQUAL(0, cmpTupStr(outTuple[i++], "1.98E0"));
    BOOST_CHECK_EQUAL(0, cmpTupStr(outTuple[i++], "-1.98E0"));
    BOOST_CHECK_EQUAL(0, cmpTupStr(outTuple[i++], "1E-3"));
    BOOST_CHECK_EQUAL(0, cmpTupStr(outTuple[i++], "-1E-3"));
    // then the char(16)s
    BOOST_CHECK(cmpTupNull(outTuple[i++]));
    BOOST_CHECK_EQUAL(0, cmpTupStr(outTuple[i++], "0E0             "));
    BOOST_CHECK_EQUAL(0, cmpTupStr(outTuple[i++], "1.98E0          "));
    BOOST_CHECK_EQUAL(0, cmpTupStr(outTuple[i++], "-1.98E0         "));
    BOOST_CHECK_EQUAL(0, cmpTupStr(outTuple[i++], "1E-3            "));
    BOOST_CHECK_EQUAL(0, cmpTupStr(outTuple[i++], "-1E-3           "));
    BOOST_CHECK(iter == calc.mWarnings.end());
}

FENNEL_UNIT_TEST_SUITE(CalcExtCastTest);

// End CalcExtCastTest.cpp
