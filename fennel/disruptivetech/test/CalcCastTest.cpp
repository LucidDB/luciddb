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


class CalcCastTest : virtual public TestBase, public TraceSource
{
    typedef void (*SetFunction)(TupleDataWithBuffer& inTuple);
    typedef void (*CheckFunction)(TupleDataWithBuffer const & inTuple,
                                  int numSrcTypes,
                                  deque<CalcMessage>& warnings,
                                  vector<int> const & expectedWarnings);

    static void testExe(SetFunction setF,
                        CheckFunction checkF,
                        char const * const srcTypeStr,
                        int numSrcTypes,
                        char const * const destTypeStr,
                        int numDestTypes,
                        int warningCombos[][2],
                        bool roundValues);

    static bool shouldWarn(int warningCombos[][2],
                           int srcIdx,
                           int destIdx);

    static void checkWarnings(deque<CalcMessage>& warnings,
                              vector<int> const & expectedWarnings);

    void PassAll();
    static void PassAllSet(TupleDataWithBuffer& inTuple);
    static void PassAllCheck(TupleDataWithBuffer const & outTuple,
                             int numSrcTypes,
                             deque<CalcMessage>& warnings,
                             vector<int> const & expectedWarnings);

    void NegValues();
    static void NegValuesSet(TupleDataWithBuffer& inTuple);
    static void NegValuesCheck(TupleDataWithBuffer const & outTuple,
                               int numSrcTypes,
                               deque<CalcMessage>& warnings,
                               vector<int> const & expectedWarnings);

    void Round();
    static void RoundSet(TupleDataWithBuffer& inTuple);
    static void RoundCheck(TupleDataWithBuffer const & outTuple,
                           int numSrcTypes,
                           deque<CalcMessage>& warnings,
                           vector<int> const & expectedWarnings);


    void Overflow();
    static void OverflowSet(TupleDataWithBuffer& inTuple);
    static void OverflowCheck(TupleDataWithBuffer const & outTuple,
                              int numSrcTypes,
                              deque<CalcMessage>& warnings,
                              vector<int> const & expectedWarnings);

    void Underflow();
    static void UnderflowSet(TupleDataWithBuffer& inTuple);
    static void UnderflowCheck(TupleDataWithBuffer const & outTuple,
                               int numSrcTypes,
                               deque<CalcMessage>& warnings,
                               vector<int> const & expectedWarnings);

    void testRoundInstruction();

public:
    explicit CalcCastTest()
        : TraceSource(shared_from_this(),"CalcCastTest")
    {
        srand(time(NULL));
        CalcInit::instance();
        FENNEL_UNIT_TEST_CASE(CalcCastTest, testRoundInstruction);
        FENNEL_UNIT_TEST_CASE(CalcCastTest, PassAll);
        FENNEL_UNIT_TEST_CASE(CalcCastTest, NegValues);
        FENNEL_UNIT_TEST_CASE(CalcCastTest, Round);
        FENNEL_UNIT_TEST_CASE(CalcCastTest, Overflow);
        FENNEL_UNIT_TEST_CASE(CalcCastTest, Underflow);
    }

    virtual ~CalcCastTest()
    {
    }
};

// test rounding away from zero
void
CalcCastTest::testRoundInstruction()
{
    ostringstream pg("");
    int idx;

    const char * const all =
        "s1, u1, s2, u2, s4, u4, s8, u8, r, r, r, r, r, d, d, d, d, d";
    //   0   1   2   3   4   5   6   7   8  9  10 11 12 13 14 15 16 17

    pg << "O " << all << ";" << endl;
    pg << "L " << all << ";" << endl;
    pg << "C " << all << ";" << endl;
    pg << "V 2, 2, 2, 2, 2, 2, 2, 2,";
    pg << "  -0.5, -0.25, 0.0, 0.25, 0.5,";
    pg << "  -0.5, -0.25, 0.0, 0.25, 0.5;" << endl;
    pg << "T;" << endl;

    for (idx = 0; idx < 18; idx++) {
        pg << "ROUND L" << idx << ", C" << idx << ";" << endl;
        pg << "REF   O" << idx << ", L" << idx << ";" << endl;
    }

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
    calc.exec();

    int64_t exact = 2;
    idx = 0;

    BOOST_CHECK_EQUAL(*(reinterpret_cast<int8_t *>
                        (const_cast<PBuffer>(outTuple[idx++].pData))),
                      exact);

    BOOST_CHECK_EQUAL(*(reinterpret_cast<uint8_t *>
                        (const_cast<PBuffer>(outTuple[idx++].pData))),
                      exact);

    BOOST_CHECK_EQUAL(*(reinterpret_cast<int16_t *>
                        (const_cast<PBuffer>(outTuple[idx++].pData))),
                      exact);

    BOOST_CHECK_EQUAL(*(reinterpret_cast<uint16_t *>
                        (const_cast<PBuffer>(outTuple[idx++].pData))),
                      exact);

    BOOST_CHECK_EQUAL(*(reinterpret_cast<int32_t *>
                        (const_cast<PBuffer>(outTuple[idx++].pData))),
                      exact);

    BOOST_CHECK_EQUAL(*(reinterpret_cast<uint32_t *>
                        (const_cast<PBuffer>(outTuple[idx++].pData))),
                      exact);

    BOOST_CHECK_EQUAL(*(reinterpret_cast<int64_t *>
                        (const_cast<PBuffer>(outTuple[idx++].pData))),
                      exact);

    BOOST_CHECK_EQUAL(*(reinterpret_cast<uint64_t *>
                        (const_cast<PBuffer>(outTuple[idx++].pData))),
                      exact);

    BOOST_CHECK_EQUAL(*(reinterpret_cast<float *>
                        (const_cast<PBuffer>(outTuple[idx++].pData))),
                      -1.0);

    BOOST_CHECK_EQUAL(*(reinterpret_cast<float *>
                        (const_cast<PBuffer>(outTuple[idx++].pData))),
                      0.0);

    BOOST_CHECK_EQUAL(*(reinterpret_cast<float *>
                        (const_cast<PBuffer>(outTuple[idx++].pData))),
                      0.0);

    BOOST_CHECK_EQUAL(*(reinterpret_cast<float *>
                        (const_cast<PBuffer>(outTuple[idx++].pData))),
                      0.0);

    BOOST_CHECK_EQUAL(*(reinterpret_cast<float *>
                        (const_cast<PBuffer>(outTuple[idx++].pData))),
                      1.0);

    BOOST_CHECK_EQUAL(*(reinterpret_cast<double *>
                        (const_cast<PBuffer>(outTuple[idx++].pData))),
                      -1.0);

    BOOST_CHECK_EQUAL(*(reinterpret_cast<double *>
                        (const_cast<PBuffer>(outTuple[idx++].pData))),
                      0.0);

    BOOST_CHECK_EQUAL(*(reinterpret_cast<double *>
                        (const_cast<PBuffer>(outTuple[idx++].pData))),
                      0.0);

    BOOST_CHECK_EQUAL(*(reinterpret_cast<double *>
                        (const_cast<PBuffer>(outTuple[idx++].pData))),
                      0.0);

    BOOST_CHECK_EQUAL(*(reinterpret_cast<double *>
                        (const_cast<PBuffer>(outTuple[idx++].pData))),
                      1.0);

}

void
CalcCastTest::testExe(SetFunction setF,
                      CheckFunction checkF,
                      char const * const srcTypeStr,
                      int numSrcTypes,
                      char const * const destTypeStr,
                      int numDestTypes,
                      int warningCombos[][2],
                      bool roundValues)
{
    ostringstream pg(""), typeProduct("");

    int destIdx, srcIdx, idx, pc;
    bool warn;
    vector<int> expectedWarnings;

    // create a dest array with one row for each src type
    for (idx = 0; idx < numSrcTypes; idx++ ) {
        if (idx != 0) {
            typeProduct << "," << endl << "  ";
        }
        typeProduct << destTypeStr;
    }
    typeProduct << ";" << endl;

    pg << "L " << typeProduct.str();
    pg << "O " << typeProduct.str();
    pg << "I " << srcTypeStr << ";" << endl;
    // Slightly abusive use of status register for rounding.
    pg << "S " << srcTypeStr << ";" << endl;
    pg << "T;" << endl;

    if (roundValues) {
        for (srcIdx = 0; srcIdx < numSrcTypes; srcIdx++) {
            pg << "ROUND S" << srcIdx << ", I" << srcIdx << ";" << endl;
        }
    }

    destIdx = 0;
    pc = 0;
    for (srcIdx = 0; srcIdx < numSrcTypes; srcIdx++) {
        for (idx = 0; idx < numDestTypes; idx++) {
            warn = shouldWarn(warningCombos, srcIdx, idx);
            if (warn) {
                expectedWarnings.push_back(pc);
                BOOST_MESSAGE("ShouldWarn PC=" << pc << " size=" << expectedWarnings.size());
            }
            pg << "CAST L" << destIdx;
            if (roundValues) {
                pg << ", S";
            } else {
                pg << ", I";
            }
            pg << srcIdx << ";";
            pg << "    /* " << pc++ << (warn ? " WARN" : "");
            pg << " src=" << srcIdx << " dest=" << idx <<  "  */" << endl;
            pg << "REF  O" << destIdx << ", L" << destIdx << ";";
            pg << "    /* " << pc++ <<  " */" << endl;
            destIdx++;
        }
    }

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

    setF(inTuple);

    calc.exec();
    BOOST_MESSAGE("Calculator Warnings: |" << calc.warnings() << "|");
    checkF(outTuple, numSrcTypes, calc.mWarnings, expectedWarnings);
}

bool
CalcCastTest::shouldWarn(int warningCombos[][2],
                         int srcIdx,
                         int destIdx)
{
    int idx = 0;
    // Yeah, so this is O(n^2). It's good enough for now, and probably forever.
    while (warningCombos[idx][0] >= 0 &&
           warningCombos[idx][1] >= 0) {
        if (warningCombos[idx][0] == srcIdx &&
            warningCombos[idx][1] == destIdx) {
            return true;
        }
        idx++;
    }
    return false;
}

void
CalcCastTest::checkWarnings(deque<CalcMessage>& warnings,
                            vector<int> const & expectedWarnings)
{
    deque<CalcMessage>::iterator iter = warnings.begin();
    deque<CalcMessage>::iterator end = warnings.end();

    int idx = 0;
    ostringstream ew("");
    ew << "Expected Warnings: (PC=) |";
    while(idx < expectedWarnings.size()) {
        ew << expectedWarnings[idx++] << ", ";
    }
    ew << "|" << endl;
    BOOST_MESSAGE(ew.str());

    idx = 0;
    while(idx < expectedWarnings.size()) {
        BOOST_CHECK(iter != end);
        BOOST_CHECK_EQUAL(iter->pc, expectedWarnings[idx]);
        BOOST_CHECK_EQUAL(0, strcmp(iter->str, "22003"));
        iter++;
        idx++;
    }
    BOOST_CHECK(iter == end);
}

void
CalcCastTest::PassAllSet(TupleDataWithBuffer& inTuple)
{
    uint64_t exact = 10;
    double approx = 20.0;
    int idx = 0;

    *(reinterpret_cast<int8_t *>
      (const_cast<PBuffer>(inTuple[idx++].pData))) = exact;
    *(reinterpret_cast<uint8_t *>
      (const_cast<PBuffer>(inTuple[idx++].pData))) = exact;
    *(reinterpret_cast<int16_t *>
      (const_cast<PBuffer>(inTuple[idx++].pData))) = exact;
    *(reinterpret_cast<uint16_t *>
      (const_cast<PBuffer>(inTuple[idx++].pData))) = exact;
    *(reinterpret_cast<int32_t *>
      (const_cast<PBuffer>(inTuple[idx++].pData))) = exact;
    *(reinterpret_cast<uint32_t *>
      (const_cast<PBuffer>(inTuple[idx++].pData))) = exact;
    *(reinterpret_cast<int64_t *>
      (const_cast<PBuffer>(inTuple[idx++].pData))) = exact;
    *(reinterpret_cast<uint64_t *>
      (const_cast<PBuffer>(inTuple[idx++].pData))) = exact;

    *(reinterpret_cast<float *>
      (const_cast<PBuffer>(inTuple[idx++].pData))) = approx;
    *(reinterpret_cast<double *>
      (const_cast<PBuffer>(inTuple[idx++].pData))) = approx;

}

void
CalcCastTest::PassAllCheck(TupleDataWithBuffer const & outTuple,
                           int numSrcTypes,
                           deque<CalcMessage>& warnings,
                           vector<int> const & expectedWarnings)
{
    checkWarnings(warnings, expectedWarnings);

    uint64_t exact;
    double approx;
    int i, idx = 0;

    for (i = 0; i < numSrcTypes; i++) {
        if (i < 8) {
            exact = 10;
            approx = 10;
        } else {
            exact = 20;
            approx = 20;
        }
        BOOST_CHECK_EQUAL(*(reinterpret_cast<int8_t *>
                            (const_cast<PBuffer>(outTuple[idx++].pData))),
                          exact);

        BOOST_CHECK_EQUAL(*(reinterpret_cast<uint8_t *>
                            (const_cast<PBuffer>(outTuple[idx++].pData))),
                          exact);

        BOOST_CHECK_EQUAL(*(reinterpret_cast<int16_t *>
                            (const_cast<PBuffer>(outTuple[idx++].pData))),
                          exact);

        BOOST_CHECK_EQUAL(*(reinterpret_cast<uint16_t *>
                            (const_cast<PBuffer>(outTuple[idx++].pData))),
                          exact);

        BOOST_CHECK_EQUAL(*(reinterpret_cast<int32_t *>
                            (const_cast<PBuffer>(outTuple[idx++].pData))),
                          exact);

        BOOST_CHECK_EQUAL(*(reinterpret_cast<uint32_t *>
                            (const_cast<PBuffer>(outTuple[idx++].pData))),
                          exact);

        BOOST_CHECK_EQUAL(*(reinterpret_cast<int64_t *>
                            (const_cast<PBuffer>(outTuple[idx++].pData))),
                          exact);

        BOOST_CHECK_EQUAL(*(reinterpret_cast<uint64_t *>
                            (const_cast<PBuffer>(outTuple[idx++].pData))),
                          exact);

        BOOST_CHECK_EQUAL(*(reinterpret_cast<float *>
                            (const_cast<PBuffer>(outTuple[idx++].pData))),
                          approx);

        BOOST_CHECK_EQUAL(*(reinterpret_cast<double *>
                            (const_cast<PBuffer>(outTuple[idx++].pData))),
                          approx);
    }
}


void
CalcCastTest::PassAll()
{
    char const * const all =
        "s1, u1, s2, u2, s4, u4, s8, u8, r, d";
    //   0   1   2   3   4   5   6   7   8  9

    int warningCombos[][2] = {
        // src index from just above, then dest index from just above
        {  -1, -1 }   // sentinal
    };

    testExe(CalcCastTest::PassAllSet,
            CalcCastTest::PassAllCheck,
            all, 10,
            all, 10,
            warningCombos,
            false);
}


void
CalcCastTest::NegValuesSet(TupleDataWithBuffer& inTuple)
{
    int64_t exact = -10;
    double approx = -20.0;
    int idx = 0;

    *(reinterpret_cast<int8_t *>
      (const_cast<PBuffer>(inTuple[idx++].pData))) = exact;
    *(reinterpret_cast<int16_t *>
      (const_cast<PBuffer>(inTuple[idx++].pData))) = exact;
    *(reinterpret_cast<int32_t *>
      (const_cast<PBuffer>(inTuple[idx++].pData))) = exact;
    *(reinterpret_cast<int64_t *>
      (const_cast<PBuffer>(inTuple[idx++].pData))) = exact;

    *(reinterpret_cast<float *>
      (const_cast<PBuffer>(inTuple[idx++].pData))) = approx;
    *(reinterpret_cast<double *>
      (const_cast<PBuffer>(inTuple[idx++].pData))) = approx;
}

void
CalcCastTest::NegValuesCheck(TupleDataWithBuffer const & outTuple,
                             int numSrcTypes,
                             deque<CalcMessage>& warnings,
                             vector<int> const & expectedWarnings)
{
    checkWarnings(warnings, expectedWarnings);

    int64_t exact = -10;
    double approx = -20.0;
    int i, idx = 0;

    for (i = 0; i < numSrcTypes; i++) {
        if (i < 4) {
            exact = -10;
            approx = -10;
        } else {
            exact = -20;
            approx = -20;
        }
        BOOST_CHECK_EQUAL(*(reinterpret_cast<int8_t *>
                            (const_cast<PBuffer>(outTuple[idx++].pData))),
                          exact);

        // uint8_t always generates a warning
        idx++;

        BOOST_CHECK_EQUAL(*(reinterpret_cast<int16_t *>
                            (const_cast<PBuffer>(outTuple[idx++].pData))),
                          exact);

        // uint16_t always generates a warning
        idx++;

        BOOST_CHECK_EQUAL(*(reinterpret_cast<int32_t *>
                            (const_cast<PBuffer>(outTuple[idx++].pData))),
                          exact);

        // uint32_t always generates a warning
        idx++;

        BOOST_CHECK_EQUAL(*(reinterpret_cast<int64_t *>
                            (const_cast<PBuffer>(outTuple[idx++].pData))),
                          exact);

        // uint64_t always generates a warning
        idx++;

        BOOST_CHECK_EQUAL(*(reinterpret_cast<float *>
                            (const_cast<PBuffer>(outTuple[idx++].pData))),
                          approx);

        BOOST_CHECK_EQUAL(*(reinterpret_cast<double *>
                            (const_cast<PBuffer>(outTuple[idx++].pData))),
                          approx);
    }
}


void
CalcCastTest::NegValues()
{
    char const * const src =
        "s1, s2, s4, s8, r, d";
    //   0   1   2   3   4  5

    char const * const dest =
        "s1, u1, s2, u2, s4, u4, s8, u8, r, d";
    //   0   1   2   3   4   5   6   7   8  9

    // combinations of types above that should generate warnings
    int warningCombos[][2] = {
        // src index from just above, then dest index from just above
        {  0,  1  },  // s1 -> u1
        {  0,  3  },  // s1 -> u2
        {  0,  5  },
        {  0,  7  },
        {  1,  1  },  // s2 -> u1
        {  1,  3  },  // s2 -> u2
        {  1,  5  },
        {  1,  7  },
        {  2,  1  },  // s4 -> u1
        {  2,  3  },  // s4 -> u2
        {  2,  5  },
        {  2,  7  },
        {  3,  1  },  // s8 -> u1
        {  3,  3  },  // s8 -> u2
        {  3,  5  },
        {  3,  7  },
        {  4,  1  },  // r -> u1
        {  4,  3  },  // r -> u2
        {  4,  5  },
        {  4,  7  },
        {  5,  1  },  // d -> u1
        {  5,  3  },  // d -> u2
        {  5,  5  },
        {  5,  7  },
        {  -1, -1 }   // sentinal
    };

    testExe(CalcCastTest::NegValuesSet,
            CalcCastTest::NegValuesCheck,
            src, 6,
            dest, 10,
            warningCombos,
            false);
}


void
CalcCastTest::RoundSet(TupleDataWithBuffer& inTuple)
{
    // expect 20, 21, 21
    double val[3] = { 20.2, 20.5, 20.7 };

    int idx = 0, group;

    for (group = 0; group < 3; group++) {
        *(reinterpret_cast<float *>
          (const_cast<PBuffer>(inTuple[idx++].pData))) = val[group];
        *(reinterpret_cast<double *>
          (const_cast<PBuffer>(inTuple[idx++].pData))) = val[group];
    }
}

void
CalcCastTest::RoundCheck(TupleDataWithBuffer const & outTuple,
                         int numSrcTypes,
                         deque<CalcMessage>& warnings,
                         vector<int> const & expectedWarnings)
{
    checkWarnings(warnings, expectedWarnings);

    int64_t valE[3] = { 20, 21, 21 }, exact;
    int group, srcI, idx = 0;

    for (group = 0; group < 3; group++) {
        exact = valE[group];
        for (srcI = 0; srcI < 2; srcI++) {
            BOOST_CHECK_EQUAL(*(reinterpret_cast<int8_t *>
                                (const_cast<PBuffer>(outTuple[idx++].pData))),
                              exact);

            BOOST_CHECK_EQUAL(*(reinterpret_cast<uint8_t *>
                                (const_cast<PBuffer>(outTuple[idx++].pData))),
                              exact);

            BOOST_CHECK_EQUAL(*(reinterpret_cast<int16_t *>
                                (const_cast<PBuffer>(outTuple[idx++].pData))),
                              exact);

            BOOST_CHECK_EQUAL(*(reinterpret_cast<uint16_t *>
                                (const_cast<PBuffer>(outTuple[idx++].pData))),
                              exact);

            BOOST_CHECK_EQUAL(*(reinterpret_cast<int32_t *>
                                (const_cast<PBuffer>(outTuple[idx++].pData))),
                              exact);

            BOOST_CHECK_EQUAL(*(reinterpret_cast<uint32_t *>
                                (const_cast<PBuffer>(outTuple[idx++].pData))),
                              exact);

            BOOST_CHECK_EQUAL(*(reinterpret_cast<int64_t *>
                                (const_cast<PBuffer>(outTuple[idx++].pData))),
                              exact);

            BOOST_CHECK_EQUAL(*(reinterpret_cast<uint64_t *>
                                (const_cast<PBuffer>(outTuple[idx++].pData))),
                              exact);

        }
    }
}


void
CalcCastTest::Round()
{
    char const * const src =
        "r, d, r, d, r, d";
    //   0  1  2  3  4  5

    char const * const dest =
        "s1, u1, s2, u2, s4, u4, s8, u8";
    //   0   1   2   3   4   5   6   7

    // combinations of types above that should generate warnings
    int warningCombos[][2] = {
        // src index from just above, then dest index from just above
        {  -1, -1 }   // sentinal
    };

    testExe(CalcCastTest::RoundSet,
            CalcCastTest::RoundCheck,
            src, 6,
            dest, 8,
            warningCombos,
            true);
}


void
CalcCastTest::OverflowSet(TupleDataWithBuffer& inTuple)
{
    int idx = 0;

    *(reinterpret_cast<int8_t *>
      (const_cast<PBuffer>(inTuple[idx++].pData))) =
        std::numeric_limits<int8_t>::max();
    *(reinterpret_cast<uint8_t *>
      (const_cast<PBuffer>(inTuple[idx++].pData))) =
        std::numeric_limits<uint8_t>::max();
    *(reinterpret_cast<int16_t *>
      (const_cast<PBuffer>(inTuple[idx++].pData))) =
        std::numeric_limits<int16_t>::max();
    *(reinterpret_cast<uint16_t *>
      (const_cast<PBuffer>(inTuple[idx++].pData))) =
        std::numeric_limits<uint16_t>::max();
    *(reinterpret_cast<int32_t *>
      (const_cast<PBuffer>(inTuple[idx++].pData))) =
        std::numeric_limits<int32_t>::max();
    *(reinterpret_cast<uint32_t *>
      (const_cast<PBuffer>(inTuple[idx++].pData))) =
        std::numeric_limits<uint32_t>::max();
    *(reinterpret_cast<int64_t *>
      (const_cast<PBuffer>(inTuple[idx++].pData))) =
        std::numeric_limits<int64_t>::max();
    *(reinterpret_cast<uint64_t *>
      (const_cast<PBuffer>(inTuple[idx++].pData))) =
        std::numeric_limits<uint64_t>::max();
    *(reinterpret_cast<float *>
      (const_cast<PBuffer>(inTuple[idx++].pData))) =
        std::numeric_limits<float>::max();
    *(reinterpret_cast<double *>
      (const_cast<PBuffer>(inTuple[idx++].pData))) =
        std::numeric_limits<double>::max();
}

void
CalcCastTest::OverflowCheck(TupleDataWithBuffer const & outTuple,
                            int numSrcTypes,
                            deque<CalcMessage>& warnings,
                            vector<int> const & expectedWarnings)
{
    checkWarnings(warnings, expectedWarnings);

    // TODO: Checking that the non-warning values are correct would be
    // rather complex. Punt for now.
}


void
CalcCastTest::Overflow()
{
    char const * const src =
        "s1, u1, s2, u2, s4, u4, s8, u8, r, d";
    //   0   1   2   3   4   5   6   7   8  9

    char const * const dest =
        "s1, u1, s2, u2, s4, u4, s8, u8, r, d";
    //   0   1   2   3   4   5   6   7   8  9

    // combinations of types above that should generate warnings
    int warningCombos[][2] = {
        // src index from just above, then dest index from just above
        {  1,  0  },  // u1 -> s1
        {  2,  0  },  // s2 -> s1
        {  2,  1  },  // s2 -> u1
        {  3,  0  },  // u2 -> s1
        {  3,  1  },  // u2 -> u1
        {  3,  2  },  // u2 -> s2
        {  4,  0  },  // s4 -> s1
        {  4,  1  },  // s4 -> u1
        {  4,  2  },  // s4 -> s2
        {  4,  3  },  // s4 -> s2
        {  5,  0  },  // u4 -> s1
        {  5,  1  },  // u4 -> u1
        {  5,  2  },  // u4 -> s2
        {  5,  3  },  // u4 -> s2
        {  5,  4  },  // u4 -> s2
        {  6,  0  },  // etc...
        {  6,  1  },
        {  6,  2  },
        {  6,  3  },
        {  6,  4  },
        {  6,  5  },
        {  7,  0  },
        {  7,  1  },
        {  7,  2  },
        {  7,  3  },
        {  7,  4  },
        {  7,  5  },
        {  7,  6  },  // u8 -> s8
        // REVIEW jvs 14-Aug-2005:  When I upgraded
        // to boost 1.33, the semantics for these two changed.
        // Loss of precision isn't the same as overflow, so
        // maybe the new semantics are better?
        /*
        {  7,  8  },  // u8 -> r (loss of precision)
        {  7,  9  },  // u8 -> d (loss of precision)
        */
        {  8,  0  },
        {  8,  1  },
        {  8,  2  },
        {  8,  3  },
        {  8,  4  },
        {  8,  5  },
        {  8,  6  },  // man, you'd think I'da automated this by now.
        {  8,  7  },
        {  9,  0  },
        {  9,  1  },
        {  9,  2  },
        {  9,  3  },
        {  9,  4  },
        {  9,  5  },
        {  9,  6  },
        {  9,  7  },
        {  9,  8  },  // d -> r

        { -1, -1  }   // sentinal
    };

    testExe(CalcCastTest::OverflowSet,
            CalcCastTest::OverflowCheck,
            src, 10,
            dest, 10,
            warningCombos,
            false);
}



void
CalcCastTest::UnderflowSet(TupleDataWithBuffer& inTuple)
{

    int idx = 0;

    *(reinterpret_cast<int8_t *>
      (const_cast<PBuffer>(inTuple[idx++].pData))) =
        std::numeric_limits<int8_t>::min();
    *(reinterpret_cast<uint8_t *>
      (const_cast<PBuffer>(inTuple[idx++].pData))) =
        std::numeric_limits<uint8_t>::min();
    *(reinterpret_cast<int16_t *>
      (const_cast<PBuffer>(inTuple[idx++].pData))) =
        std::numeric_limits<int16_t>::min();
    *(reinterpret_cast<uint16_t *>
      (const_cast<PBuffer>(inTuple[idx++].pData))) =
        std::numeric_limits<uint16_t>::min();
    *(reinterpret_cast<int32_t *>
      (const_cast<PBuffer>(inTuple[idx++].pData))) =
        std::numeric_limits<int32_t>::min();
    *(reinterpret_cast<uint32_t *>
      (const_cast<PBuffer>(inTuple[idx++].pData))) =
        std::numeric_limits<uint32_t>::min();
    *(reinterpret_cast<int64_t *>
      (const_cast<PBuffer>(inTuple[idx++].pData))) =
        std::numeric_limits<int64_t>::min();
    *(reinterpret_cast<uint64_t *>
      (const_cast<PBuffer>(inTuple[idx++].pData))) =
        std::numeric_limits<uint64_t>::min();

    // Note: min() for approx type is the smallest positive
    // number, not the most negative number. Unary minus of max()
    // gives smallest possible (very negative) number.
    *(reinterpret_cast<float *>
      (const_cast<PBuffer>(inTuple[idx++].pData))) =
        -std::numeric_limits<float>::max();
    *(reinterpret_cast<double *>
      (const_cast<PBuffer>(inTuple[idx++].pData))) =
        -std::numeric_limits<double>::max();
}

void
CalcCastTest::UnderflowCheck(TupleDataWithBuffer const & outTuple,
                             int numSrcTypes,
                             deque<CalcMessage>& warnings,
                             vector<int> const & expectedWarnings)
{
    checkWarnings(warnings, expectedWarnings);

    // TODO: Checking that the non-warning values are correct would be
    // rather complex. Punt for now.
}


void
CalcCastTest::Underflow()
{
    char const * const src =
        "s1, u1, s2, u2, s4, u4, s8, u8, r, d";
    //   0   1   2   3   4   5   6   7   8  9

    char const * const dest =
        "s1, u1, s2, u2, s4, u4, s8, u8, r, d";
    //   0   1   2   3   4   5   6   7   8  9

    // combinations of types above that should generate warnings
    int warningCombos[][2] = {
        // src index from just above, then dest index from just above
        {  0,  1  },  // s1 -> u1
        {  0,  3  },  // s1 -> u2
        {  0,  5  },  // s1 -> u4
        {  0,  7  },  // s1 -> u8

        {  2,  0  },  // s2 -> s1
        {  2,  1  },  // s2 -> u1
        {  2,  3  },  // s2 -> u2
        {  2,  5  },  // s2 -> u4
        {  2,  7  },  // s2 -> u8

        {  4,  0  },  // s4 -> s1
        {  4,  1  },  // s4 -> u1
        {  4,  2  },  // s4 -> s2
        {  4,  3  },  // s4 -> u2
        {  4,  5  },  // s4 -> u4
        {  4,  7  },  // s4 -> u8

        {  6,  0  },  // s8 -> s1
        {  6,  1  },  // s8 -> u1
        {  6,  2  },  // s8 -> s2
        {  6,  3  },  // s8 -> u2
        {  6,  4  },  // s8 -> s4
        {  6,  5  },  // s8 -> u4
        {  6,  7  },  // s8 -> u8

        {  8,  0  },  // r -> s1
        {  8,  1  },  // r -> u1
        {  8,  2  },  // r -> s2
        {  8,  3  },  // r -> u2
        {  8,  4  },  // r -> s4
        {  8,  5  },  // r -> u4
        {  8,  6  },  // r -> s8
        {  8,  7  },  // r -> u8

        {  9,  0  },  // d -> s1
        {  9,  1  },  // d -> u1
        {  9,  2  },  // d -> s2
        {  9,  3  },  // d -> u2
        {  9,  4  },  // d -> s4
        {  9,  5  },  // d -> u4
        {  9,  6  },  // d -> s8
        {  9,  7  },  // d -> u8
        {  9,  8  },  // d -> r

        { -1, -1  }   // sentinal
    };

    testExe(CalcCastTest::UnderflowSet,
            CalcCastTest::UnderflowCheck,
            src, 10,
            dest, 10,
            warningCombos,
            false);
}


FENNEL_UNIT_TEST_SUITE(CalcCastTest);

// End CalcCastTest.cpp
