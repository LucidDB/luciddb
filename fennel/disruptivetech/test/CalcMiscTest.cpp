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


using namespace fennel;
using namespace std;


class CalcMiscTest : virtual public TestBase, public TraceSource
{
    void testCalcStatusReg();
    void testCalcStatusRegZero();
    void testCalcRefInst();
    void testCalcReturn();
    void testCalcRaise();
    void testCalcContinueOnException();

public:
    explicit CalcMiscTest()
        : TraceSource(shared_from_this(),"CalcMiscTest")
    {
        srand(time(NULL));
        CalcInit::instance();
        FENNEL_UNIT_TEST_CASE(CalcMiscTest, testCalcStatusReg);
        FENNEL_UNIT_TEST_CASE(CalcMiscTest, testCalcStatusRegZero);
        FENNEL_UNIT_TEST_CASE(CalcMiscTest, testCalcRefInst);
        FENNEL_UNIT_TEST_CASE(CalcMiscTest, testCalcReturn);
        FENNEL_UNIT_TEST_CASE(CalcMiscTest, testCalcRaise);
        FENNEL_UNIT_TEST_CASE(CalcMiscTest, testCalcContinueOnException);
    }

    virtual ~CalcMiscTest()
    {
    }
};

void
CalcMiscTest::testCalcStatusReg()
{
    ostringstream pg("");

    pg << "L u2;" << endl;
    pg << "O u2;" << endl;
    pg << "S u2, u2, u2;" << endl;
    pg << "C u2, u2, u2;" << endl;
    pg << "V 4, 5, 6;" << endl;
    pg << "T;" << endl;
    pg << "MOVE S 0, C 0;" << endl;
    pg << "MOVE L 0, C 1;" << endl;
    pg << "REF  O 0, C 2;" << endl;
    pg << "MOVE S 1, L 0;" << endl;
    pg << "MOVE S 2, O 0;" << endl;

    // BOOST_MESSAGE(pg.str());

    Calculator calc(0);

    try {
        calc.assemble(pg.str().c_str());
    }
    catch (FennelExcn& ex) {
        BOOST_MESSAGE("Assemble exception " << ex.getMessage());
        BOOST_REQUIRE(0);
    }

    TupleDataWithBuffer outTuple(calc.getOutputRegisterDescriptor());
    TupleDataWithBuffer inTuple(calc.getInputRegisterDescriptor());

    calc.bind(&inTuple, &outTuple);
    calc.exec();

    TupleData const * const statusTuple = calc.getStatusRegister();
#if 0
    TupleDescriptor statusDesc = calc.getStatusRegisterDescriptor();
    TuplePrinter tuplePrinter;
    tuplePrinter.print(cout, statusDesc, *statusTuple);
    cout << endl;
#endif

    BOOST_CHECK_EQUAL(*(reinterpret_cast<uint16_t *>
                        (const_cast<PBuffer>((*statusTuple)[0].pData))),
                      4);
    BOOST_CHECK_EQUAL(*(reinterpret_cast<uint16_t *>
                        (const_cast<PBuffer>((*statusTuple)[1].pData))),
                      5);
    BOOST_CHECK_EQUAL(*(reinterpret_cast<uint16_t *>
                        (const_cast<PBuffer>((*statusTuple)[2].pData))),
                      6);

}

void
CalcMiscTest::testCalcStatusRegZero()
{
    ostringstream pg("");

    pg << "L u2;" << endl;
    pg << "O u2;" << endl;
    pg << "S u2, u2;" << endl;
    pg << "C u2, u2;" << endl;
    pg << "V 1, 2;" << endl;
    pg << "T;" << endl;
    pg << "MOVE L 0, S 0;" << endl;
    pg << "ADD S 0, L 0, C 0;" << endl;
    pg << "MOVE L 0, S 1;" << endl;
    pg << "ADD S 1, L 0, C 1;" << endl;

    // BOOST_MESSAGE(pg.str());

    Calculator calc(0);

    try {
        calc.assemble(pg.str().c_str());
    }
    catch (FennelExcn& ex) {
        BOOST_MESSAGE("Assemble exception " << ex.getMessage());
        BOOST_REQUIRE(0);
    }

    TupleDataWithBuffer outTuple(calc.getOutputRegisterDescriptor());
    TupleDataWithBuffer inTuple(calc.getInputRegisterDescriptor());

    calc.bind(&inTuple, &outTuple);

    TupleData const * const statusTuple = calc.getStatusRegister();

    for(int i = 1; i <= 3; i++) {
        calc.exec();

        BOOST_CHECK_EQUAL(*(reinterpret_cast<uint16_t *>
                            (const_cast<PBuffer>((*statusTuple)[0].pData))),
                          i);
        BOOST_CHECK_EQUAL(*(reinterpret_cast<uint16_t *>
                            (const_cast<PBuffer>((*statusTuple)[1].pData))),
                          i * 2);
    }

    calc.zeroStatusRegister();

    BOOST_CHECK_EQUAL(*(reinterpret_cast<uint16_t *>
                        (const_cast<PBuffer>((*statusTuple)[0].pData))),
                      0);
    BOOST_CHECK_EQUAL(*(reinterpret_cast<uint16_t *>
                        (const_cast<PBuffer>((*statusTuple)[1].pData))),
                      0);

    calc.exec();

    BOOST_CHECK_EQUAL(*(reinterpret_cast<uint16_t *>
                        (const_cast<PBuffer>((*statusTuple)[0].pData))),
                      1);
    BOOST_CHECK_EQUAL(*(reinterpret_cast<uint16_t *>
                        (const_cast<PBuffer>((*statusTuple)[1].pData))),
                      2);
}

void
CalcMiscTest::testCalcRefInst()
{
    ostringstream pg("");

    char const * const all =
        "bo, s1, u1, s2, u2, s4, u4, s8, u8, r, d, vc,2, c,2";
    //   0   1   2   3   4   5   6   7   8   9  10 11    12
    int const numTypes = 13;
    char regs[] = { 'I', 'L', 'C' };
    int const numRegSets = 3;

    // TODO: Add binary and varbinary once supported.
    pg << "I " << all << ";" << endl;
    pg << "O " << all << ", " << endl;
    pg << "  " << all << ", " << endl;
    pg << "  " << all << ";" << endl;
    pg << "L " << all << ";" << endl;
    pg << "C " << all << ";" << endl;
    pg << "V 0, 1, 2, 3, 4, 5, 6, 7, 8, 9.0, 10.0, 0x";
    pg << stringToHex("11") << ", 0x" << stringToHex("12") << ";" << endl;
    pg << "T;" << endl;

    int outReg = 0, regSet, regFrom;
    // copy constants to local
    for (regFrom = 0; regFrom < numTypes; regFrom++) {
        pg << "MOVE L" << regFrom << ", C" << regFrom << ";" << endl;
    }

    // have output refer to other three sets
    for (regSet = 0; regSet < numRegSets; regSet++) {
        for (regFrom = 0; regFrom < numTypes; regFrom++) {
            pg << "REF O" << outReg++ << ", " << regs[regSet];
            pg << regFrom << ";" << endl;
        }
    }

    //    BOOST_MESSAGE(pg.str());

    Calculator calc(0);

    try {
        calc.assemble(pg.str().c_str());
    }
    catch (FennelExcn& ex) {
        BOOST_MESSAGE("Assemble exception " << ex.getMessage());
        BOOST_REQUIRE(0);
    }

    TupleDataWithBuffer outTuple(calc.getOutputRegisterDescriptor());
    TupleDataWithBuffer inTuple(calc.getInputRegisterDescriptor());

    calc.bind(&inTuple, &outTuple);
    calc.exec();

    outReg = 0;
    for (regSet = 0; regSet < numRegSets; regSet++) {
        for (regFrom = 0; regFrom < numTypes; regFrom++) {
            switch (regs[regSet]) {
            case 'I':
                // verify that pointers are identical
                BOOST_CHECK_EQUAL(outTuple[outReg].pData,
                                  inTuple[regFrom].pData);
                break;
            case 'L':
            case 'C':
                // no trivial way to verify that pointers
                // are identical w/o breaking object encapsulation
                // of calculator. instead, see if data matches up.
                // should be sufficent.
                if (regFrom <= 8) {
                    // integer natives cast correctly w/o help
                    // (at least on x86!)
                    BOOST_CHECK_EQUAL(*(outTuple[outReg].pData),
                                      regFrom);
                } else if (regFrom == 9) {
                    // real (float)
                    BOOST_CHECK_EQUAL(*(reinterpret_cast<float const *>
                                        (outTuple[outReg].pData)),
                                      static_cast<float>(regFrom));
                } else if (regFrom == 10) {
                    // double
                    BOOST_CHECK_EQUAL(*(reinterpret_cast<double const*>
                                        (outTuple[outReg].pData)),
                                      static_cast<double>(regFrom));
                } else if (regFrom == 11) {
                    // varchar string
                    BOOST_CHECK_EQUAL(0,
                                      strncmp(reinterpret_cast<char const *>
                                              (outTuple[outReg].pData),
                                              "11",
                                              2));

                } else if (regFrom == 12) {
                    // char string
                    BOOST_CHECK_EQUAL(0,
                                      strncmp(reinterpret_cast<char const *>
                                              (outTuple[outReg].pData),
                                              "12",
                                              2));
                } else {
                    BOOST_FAIL("logic error");
                }
                break;
            }
            outReg++;
        }
    }
}

void
CalcMiscTest::testCalcReturn()
{
    ostringstream pg("");

    pg << "S u4;" << endl;
    pg << "C u4, u4, u4;" << endl;
    pg << "V 4, 5, 6;" << endl;
    pg << "T;" << endl;
    pg << "MOVE S 0, C 0;" << endl;
    pg << "RETURN;" << endl;
    pg << "MOVE S 0, C 1;" << endl;

    // BOOST_MESSAGE(pg.str());

    Calculator calc(0);

    try {
        calc.assemble(pg.str().c_str());
    }
    catch (FennelExcn& ex) {
        BOOST_MESSAGE("Assemble exception " << ex.getMessage());
        BOOST_REQUIRE(0);
    }

    TupleDataWithBuffer outTuple(calc.getOutputRegisterDescriptor());
    TupleDataWithBuffer inTuple(calc.getInputRegisterDescriptor());

    calc.bind(&inTuple, &outTuple);
    calc.exec();

    TupleData const * const statusTuple = calc.getStatusRegister();
#if 0
    TupleDescriptor statusDesc = calc.getStatusRegisterDescriptor();
    TuplePrinter tuplePrinter;
    tuplePrinter.print(cout, statusDesc, *statusTuple);
    cout << endl;
#endif

    BOOST_CHECK_EQUAL(*(reinterpret_cast<uint32_t *>
                        (const_cast<PBuffer>((*statusTuple)[0].pData))),
                      4);
}


void
CalcMiscTest::testCalcRaise()
{
    ostringstream pg("");

    pg << "I u4;" << endl;
    pg << "S u4;" << endl;
    pg << "C u4, u4, vc,5, vc,5;" << endl;
    pg << "V 4, 5, 0x" << stringToHex("12345") << ",;" << endl;
    pg << "T;" << endl;
    pg << "MOVE S0, C0;" << endl;
    pg << "RAISE C2;" << endl;
    pg << "RAISE C3;" << endl; // null should induce no-op mode;
    pg << "MOVE S0, C1;" << endl;
    pg << "RETURN;" << endl;

    //    BOOST_MESSAGE(pg.str());

    Calculator calc(0);

    try {
        calc.assemble(pg.str().c_str());
    }
    catch (FennelExcn& ex) {
        BOOST_MESSAGE("Assemble exception " << ex.getMessage());
        BOOST_REQUIRE(0);
    }

    TupleDataWithBuffer outTuple(calc.getOutputRegisterDescriptor());
    TupleDataWithBuffer inTuple(calc.getInputRegisterDescriptor());

    calc.bind(&inTuple, &outTuple);
    calc.exec();

    TupleData const * const statusTuple = calc.getStatusRegister();
#if 0
    TupleDescriptor statusDesc = calc.getStatusRegisterDescriptor();
    TuplePrinter tuplePrinter;
    tuplePrinter.print(cout, statusDesc, *statusTuple);
    cout << endl;
#endif

    BOOST_CHECK_EQUAL(*(reinterpret_cast<uint32_t *>
                        (const_cast<PBuffer>((*statusTuple)[0].pData))),
                      5);

    deque<CalcMessage>::iterator iter = calc.mWarnings.begin();
    deque<CalcMessage>::iterator end = calc.mWarnings.end();

    //BOOST_MESSAGE("warnings: |" << calc.warnings() << "|");

    BOOST_CHECK(iter != end);
    BOOST_CHECK_EQUAL(iter->pc, 1);
    BOOST_CHECK_EQUAL(0, strcmp(iter->str, "12345"));
    iter++;
    BOOST_CHECK(iter == end);
}

void
CalcMiscTest::testCalcContinueOnException()
{
    ostringstream pg("");

    pg << "I u4;" << endl;
    pg << "S u4;" << endl;
    pg << "C u4, u4, vc,5, vc,5;" << endl;
    pg << "V 4, 5, 0x" << stringToHex("12345") << ",;" << endl;
    pg << "T;" << endl;
    pg << "MOVE S0, C0;" << endl;
    pg << "RAISE C2;" << endl;
    pg << "MOVE S0, C1;" << endl;
    pg << "RETURN;" << endl;

    BOOST_MESSAGE(pg.str());

    Calculator calc(0);

    try {
        calc.assemble(pg.str().c_str());
    }
    catch (FennelExcn& ex) {
        BOOST_MESSAGE("Assemble exception " << ex.getMessage());
        BOOST_REQUIRE(0);
    }

    TupleDataWithBuffer outTuple(calc.getOutputRegisterDescriptor());
    TupleDataWithBuffer inTuple(calc.getInputRegisterDescriptor());

    calc.bind(&inTuple, &outTuple);
    // run default mode, which continues after exception
    calc.exec();

    TupleData const * const statusTuple = calc.getStatusRegister();
#if 0
    TupleDescriptor statusDesc = calc.getStatusRegisterDescriptor();
    TuplePrinter tuplePrinter;
    tuplePrinter.print(cout, statusDesc, *statusTuple);
    cout << endl;
#endif

    BOOST_CHECK_EQUAL(*(reinterpret_cast<uint32_t *>
                        (const_cast<PBuffer>((*statusTuple)[0].pData))),
                      5);

    deque<CalcMessage>::iterator iter = calc.mWarnings.begin();
    deque<CalcMessage>::iterator end = calc.mWarnings.end();

    BOOST_MESSAGE("warnings: |" << calc.warnings() << "|");

    BOOST_CHECK(iter != end);
    BOOST_CHECK_EQUAL(iter->pc, 1);
    BOOST_CHECK_EQUAL(0, strcmp(iter->str, "12345"));
    iter++;
    BOOST_CHECK(iter == end);

    // change mode to return on exception
    calc.continueOnException(false);
    calc.exec();

    BOOST_CHECK_EQUAL(*(reinterpret_cast<uint32_t *>
                        (const_cast<PBuffer>((*statusTuple)[0].pData))),
                      4);

    iter = calc.mWarnings.begin();
    end = calc.mWarnings.end();

    BOOST_MESSAGE("warnings: |" << calc.warnings() << "|");

    BOOST_CHECK(iter != end);
    BOOST_CHECK_EQUAL(iter->pc, 1);
    BOOST_CHECK_EQUAL(0, strcmp(iter->str, "12345"));
    iter++;
    BOOST_CHECK(iter == end);

}

FENNEL_UNIT_TEST_SUITE(CalcMiscTest);

// End CalcMiscTest.cpp
