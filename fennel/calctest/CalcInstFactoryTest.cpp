/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005-2009 The Eigenbase Project
// Copyright (C) 2004-2009 SQLstream, Inc.
// Copyright (C) 2009-2009 LucidEra, Inc.
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
#include "fennel/calculator/CalcCommon.h"
#include "fennel/calculator/StringToHex.h"
#include "fennel/common/FennelExcn.h"

#include <boost/test/test_tools.hpp>
#include <boost/scoped_array.hpp>
#include <string>
#include <limits>


using namespace fennel;
using namespace std;



class CalcInstFactoryTest : virtual public TestBase, public TraceSource
{
    void testBool();
    void testBoolNative();
    void testBoolPointer();
    void testIntegralNative();
    void testIntegralPointer();
    void testJump();
    void testNativeNative();
    void testPointerIntegral();
    void testPointerPointer();
    void testReturn();

    static char const * const all;
    static char const * const nativeNotBool;
    static char const * const nativeNotBoolValues;
    static char const * const pointerArray;
    static char const * const nativeIntegral;
    static char const * const nativeIntegralValues;

public:
    explicit CalcInstFactoryTest()
        : TraceSource(shared_from_this(),"CalcInstFactoryTest")
    {
        srand(time(NULL));
        CalcInit::instance();
        FENNEL_UNIT_TEST_CASE(CalcInstFactoryTest, testBool);
        FENNEL_UNIT_TEST_CASE(CalcInstFactoryTest, testBoolNative);
        FENNEL_UNIT_TEST_CASE(CalcInstFactoryTest, testBoolPointer);
        FENNEL_UNIT_TEST_CASE(CalcInstFactoryTest, testIntegralNative);
        FENNEL_UNIT_TEST_CASE(CalcInstFactoryTest, testIntegralPointer);
        FENNEL_UNIT_TEST_CASE(CalcInstFactoryTest, testJump);
        FENNEL_UNIT_TEST_CASE(CalcInstFactoryTest, testNativeNative);
        FENNEL_UNIT_TEST_CASE(CalcInstFactoryTest, testPointerIntegral);
        FENNEL_UNIT_TEST_CASE(CalcInstFactoryTest, testPointerPointer);
        FENNEL_UNIT_TEST_CASE(CalcInstFactoryTest, testReturn);
    }

    virtual ~CalcInstFactoryTest()
    {
    }
};

char const * const
CalcInstFactoryTest::all =
"s1, u1, s2, u2, s4, u4, s8, u8, bo, r, d, c, vc, b, vb";
// 0   1   2   3   4   5   6   7   8   9  10 11 12  13 14

char const * const
CalcInstFactoryTest::nativeNotBool =
"s1, u1, s2, u2, s4, u4, s8, u8, r, d";
// 0   1   2   3   4   5   6   7   8  9
char const * const
CalcInstFactoryTest::nativeNotBoolValues =
"1, 2, 3, 4, 5, 6, 7, 8, 9.0, 10.0";
// 0  1  2  3  4  5  6  7  8    9

char const * const
CalcInstFactoryTest::pointerArray =
"vc,1, c,1, u4, bo, bo";
// 0   1    2   3   4

char const * const
CalcInstFactoryTest::nativeIntegral =
"s1, u1, s2, u2, s4, u4, s8, u8";
// 0   1   2   3   4   5   6   7
char const * const
CalcInstFactoryTest::nativeIntegralValues =
"1, 2, 3, 4, 5, 6, 7, 8";
// 0  1  2  3  4  5  6  7



void
CalcInstFactoryTest::testBool()
{
    ostringstream pg("");

    const char* Bool[][2] = {
        { "OR", "3" },
        { "AND", "3" },
        { "NOT", "2" },
        { "MOVE", "2" },
        { "REF", "2" },
        { "IS", "3" },
        { "ISNOT", "3" },
        { "EQ", "3" },
        { "NE", "3" },
        { "GT", "3" },
        { "GE", "3" },
        { "LT", "3" },
        { "LE", "3" },
        { "ISNULL", "2" },
        { "ISNOTNULL", "2" },
        { "TONULL", "1" },
        { "", "" }};

    pg << "O bo;" << endl;
    pg << "C bo,bo;" << endl;
    pg << "V 1,0;" << endl;
    pg << "T;" << endl;

    int inst;
    for (inst = 0; *(Bool[inst][0]); inst++) {
        BOOST_MESSAGE(Bool[inst][0]);
        pg << Bool[inst][0] << " O0";
        if (atoi(Bool[inst][1]) >= 2) {
            pg << ", C0";
        }
        if (atoi(Bool[inst][1]) >= 3) {
            pg << ", C1";
        }
        pg << ";" << endl;
    }

    BOOST_MESSAGE(pg.str());

    Calculator calc(0);

    try {
        calc.assemble(pg.str().c_str());
    } catch (FennelExcn& ex) {
        BOOST_MESSAGE("Assemble exception " << ex.getMessage());
        BOOST_REQUIRE(0);
    }

    // Don't bother executing the instructions.
}

void
CalcInstFactoryTest::testBoolNative()
{
    ostringstream pg("");

    const char* boolnative[][2] = {
       { "EQ", "3" },
       { "NE" , "3" },
       { "GT", "3" },
       { "GE", "3" },
       { "LT", "3" },
       { "LE", "3" },
       { "ISNULL", "2" },
       { "ISNOTNULL", "2" },
       { "", "" }};

    pg << "O " << nativeNotBool << ", bo;" << endl;
    pg << "C " << nativeNotBool << ";" << endl;
    pg << "V " << nativeNotBoolValues << ";" << endl;
    pg << "T;" << endl;

    int inst, type;
    for (inst = 0; *(boolnative[inst][0]); inst++) {
        BOOST_MESSAGE(boolnative[inst][0]);
        for (type = 0; type <= 9; type++) {
            pg << boolnative[inst][0] << " O10"; // always bool
            if (atoi(boolnative[inst][1]) >= 2) {
                pg << ", C" << type;
            }
            if (atoi(boolnative[inst][1]) >= 3) {
                pg << ", C" << type;
            }
            pg << ";" << endl;
        }
    }

    BOOST_MESSAGE(pg.str());

    Calculator calc(0);

    try {
        calc.assemble(pg.str().c_str());
    } catch (FennelExcn& ex) {
        BOOST_MESSAGE("Assemble exception " << ex.getMessage());
        BOOST_REQUIRE(0);
    }

    // Don't bother executing the instructions.
}

void
CalcInstFactoryTest::testIntegralNative()
{
    ostringstream pg("");

    const char* integralnative[][2] = {
        { "MOD", "3" },
        { "AND", "3" },
        { "OR", "3" },
        { "SHFL", "3" },
        { "SHFR", "3" },
        { "", "" }};

    pg << "O " << nativeIntegral << ";" << endl;
    pg << "C " << nativeIntegral << ";" << endl;
    pg << "V " << nativeIntegralValues << ";" << endl;
    pg << "T;" << endl;

    int inst, type;
    for (inst = 0; *(integralnative[inst][0]); inst++) {
        BOOST_MESSAGE(integralnative[inst][0]);
        for (type = 0; type <= 7; type++) {
            pg << integralnative[inst][0] << " O" << type;
            if (atoi(integralnative[inst][1]) >= 2) {
                pg << ", C" << type;
            }
            if (atoi(integralnative[inst][1]) >= 3) {
                pg << ", C" << type;
            }
            pg << ";" << endl;
        }
    }

    BOOST_MESSAGE(pg.str());

    Calculator calc(0);

    try {
        calc.assemble(pg.str().c_str());
    } catch (FennelExcn& ex) {
        BOOST_MESSAGE("Assemble exception " << ex.getMessage());
        BOOST_REQUIRE(0);
    }

    // Don't bother executing the instructions.
}

void
CalcInstFactoryTest::testIntegralPointer()
{
    ostringstream pg("");

    const char* integralpointer[][2] = {
       { "GETS", "2", },
       { "GETMS" , "2", },
       { "", "" }};

    pg << "O " << pointerArray << ";" << endl;
    pg << "C " << pointerArray << ";" << endl;
    pg << "V 0x" << stringToHex("a") << ", 0x" << stringToHex("b") <<
        ", 1, 0, 1;" << endl;
    pg << "T;" << endl;

    int inst, type;
    for (inst = 0; *(integralpointer[inst][0]); inst++) {
        BOOST_MESSAGE(integralpointer[inst][0]);
        for (type = 0; type <= 1; type++) {
            pg << integralpointer[inst][0] << " O2";
            if (atoi(integralpointer[inst][1]) >= 2) {
                pg << ", C" << type;
            }
            if (atoi(integralpointer[inst][1]) >= 3) {
                pg << ", C" << type;
            }
            pg << ";" << endl;
        }
    }

    BOOST_MESSAGE(pg.str());

    Calculator calc(0);

    try {
        calc.assemble(pg.str().c_str());
    } catch (FennelExcn& ex) {
        BOOST_MESSAGE("Assemble exception " << ex.getMessage());
        BOOST_REQUIRE(0);
    }

    // Don't bother executing the instructions.
}

void
CalcInstFactoryTest::testPointerIntegral()
{
    ostringstream pg("");

    const char* pointerintegral[][2] = {
       { "PUTS", "2", },
       { "", "" }};

    pg << "O " << pointerArray << ";" << endl;
    pg << "C " << pointerArray << ";" << endl;
    pg << "V 0x" << stringToHex("a") << ", 0x" << stringToHex("b") <<
        ", 1, 0, 1;" << endl;
    pg << "T;" << endl;

    int inst, type;
    for (inst = 0; *(pointerintegral[inst][0]); inst++) {
        BOOST_MESSAGE(pointerintegral[inst][0]);
        for (type = 0; type <= 1; type++) {
            pg << pointerintegral[inst][0] << " O" << type;
            pg << ", C2;" << endl;
        }
    }

    BOOST_MESSAGE(pg.str());

    Calculator calc(0);

    try {
        calc.assemble(pg.str().c_str());
    } catch (FennelExcn& ex) {
        BOOST_MESSAGE("Assemble exception " << ex.getMessage());
        BOOST_REQUIRE(0);
    }

    // Don't bother executing the instructions.
}

void
CalcInstFactoryTest::testBoolPointer()
{
    ostringstream pg("");

    const char* boolpointer[][2] = {
       { "EQ", "3", },
       { "NE" , "3", },
       { "GT", "3", },
       { "GE", "3", },
       { "LT", "3", },
       { "LE", "3", },
       { "ISNULL", "2", },
       { "ISNOTNULL", "2", },
       { "", "" }};

    pg << "O " << pointerArray << ";" << endl;
    pg << "C " << pointerArray << ";" << endl;
    pg << "V 0x" << stringToHex("a") << ", 0x" << stringToHex("b") <<
        ", 1, 0, 1;" << endl;
    pg << "T;" << endl;

    int inst, type;
    for (inst = 0; *(boolpointer[inst][0]); inst++) {
        BOOST_MESSAGE(boolpointer[inst][0]);
        for (type = 0; type <= 1; type++) {
            pg << boolpointer[inst][0] << " O3";
            if (atoi(boolpointer[inst][1]) >= 2) {
                pg << ", C" << type;
            }
            if (atoi(boolpointer[inst][1]) >= 3) {
                pg << ", C" << type;
            }
            pg << ";" << endl;
        }
    }

    BOOST_MESSAGE(pg.str());

    Calculator calc(0);

    try {
        calc.assemble(pg.str().c_str());
    } catch (FennelExcn& ex) {
        BOOST_MESSAGE("Assemble exception " << ex.getMessage());
        BOOST_REQUIRE(0);
    }

    // Don't bother executing the instructions.
}

void
CalcInstFactoryTest::testJump()
{
    ostringstream pg("");

    const char* Jump[][2] = {
        { "JMP", "0" },
        { "JMPT", "1" },
        { "JMPF", "1" },
        { "JMPN", "1" },
        { "JMPNN", "1" },
        { "", "" }};

    pg << "O bo;" << endl;
    pg << "C bo,bo;" << endl;
    pg << "V 1,0;" << endl;
    pg << "T;" << endl;

    int inst;
    for (inst = 0; *(Jump[inst][0]); inst++) {
        pg << Jump[inst][0] << " @1";
        if (atoi(Jump[inst][1]) >= 1) {
            pg << ", C0";
        }
        pg << ";" << endl;
    }

    BOOST_MESSAGE("|" << pg.str() << "|");

    Calculator calc(0);

    try {
        calc.assemble(pg.str().c_str());
    } catch (FennelExcn& ex) {
        BOOST_MESSAGE("Assemble exception " << ex.getMessage());
        BOOST_REQUIRE(0);
    }

    // Don't bother executing the instructions.
}

void
CalcInstFactoryTest::testNativeNative()
{
    ostringstream pg("");

    const char* nativenative[][2] = {
       { "ADD", "3" },
       { "SUB" , "3" },
       { "MUL", "3" },
       { "DIV", "3" },
       { "NEG", "2" },
       { "MOVE", "2" },
       { "REF", "2" },
       { "TONULL", "1" },
       { "", "" }};

    pg << "O " << nativeNotBool << ";" << endl;
    pg << "C " << nativeNotBool << ";" << endl;
    pg << "V " << nativeNotBoolValues << ";" << endl;
    pg << "T;" << endl;

    int inst, type;
    for (inst = 0; *(nativenative[inst][0]); inst++) {
        BOOST_MESSAGE(nativenative[inst][0]);
        for (type = 0; type <= 9; type++) {
            pg << nativenative[inst][0] << " O" << type;
            if (atoi(nativenative[inst][1]) >= 2) {
                pg << ", C" << type;
            }
            if (atoi(nativenative[inst][1]) >= 3) {
                pg << ", C" << type;
            }
            pg << ";" << endl;
        }
    }

    BOOST_MESSAGE(pg.str());

    Calculator calc(0);

    try {
        calc.assemble(pg.str().c_str());
    } catch (FennelExcn& ex) {
        BOOST_MESSAGE("Assemble exception " << ex.getMessage());
        BOOST_REQUIRE(0);
    }

    // Don't bother executing the instructions.
}

void
CalcInstFactoryTest::testPointerPointer()
{
    ostringstream pg("");

    const char* pointerpointer[][3] = {
       { "ADD", "2", "1" },
       { "SUB" , "2", "1" },
       { "MOVE", "2", "0" },
       { "REF", "2", "0" },
       { "TONULL", "1", "0" },
       { "", "" }};

    pg << "O " << pointerArray << ";" << endl;
    pg << "C " << pointerArray << ";" << endl;
    pg << "V 0x" << stringToHex("a") << ", 0x" << stringToHex("b") <<
        ", 1, 0, 1;" << endl;
    pg << "T;" << endl;

    int inst, type;
    for (inst = 0; *(pointerpointer[inst][0]); inst++) {
        BOOST_MESSAGE(pointerpointer[inst][0]);
        for (type = 0; type <= 1; type++) {
            pg << pointerpointer[inst][0] << " O" << type;
            if (atoi(pointerpointer[inst][1]) >= 2) {
                pg << ", C" << type;
            }
            if (atoi(pointerpointer[inst][2]) >= 1) {
                pg << ", C2";
            }
            pg << ";" << endl;
        }
    }

    BOOST_MESSAGE(pg.str());

    Calculator calc(0);

    try {
        calc.assemble(pg.str().c_str());
    } catch (FennelExcn& ex) {
        BOOST_MESSAGE("Assemble exception " << ex.getMessage());
        BOOST_REQUIRE(0);
    }

    // Don't bother executing the instructions.
}

void
CalcInstFactoryTest::testReturn()
{
    ostringstream pg("");

    pg << "O bo;" << endl;
    pg << "C bo,bo;" << endl;
    pg << "V 1,0;" << endl;
    pg << "T;" << endl;
    pg << "RETURN;" << endl;

    BOOST_MESSAGE(pg.str());

    Calculator calc(0);

    try {
        calc.assemble(pg.str().c_str());
    } catch (FennelExcn& ex) {
        BOOST_MESSAGE("Assemble exception " << ex.getMessage());
        BOOST_REQUIRE(0);
    }

    // Don't bother executing the instructions.
}

FENNEL_UNIT_TEST_SUITE(CalcInstFactoryTest);

// End CalcInstFactoryTest.cpp
