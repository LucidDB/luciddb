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
#include "fennel/common/FennelExcn.h"

#include <boost/test/test_tools.hpp>
#include <boost/scoped_array.hpp>
#include <string>
#include <limits>


using namespace fennel;
using namespace std;


class CalcExtDynamicVariableTest : virtual public TestBase, public TraceSource
{
    void testCalcExtDynamicVariable();

    void printOutput(TupleData const & tup, Calculator const & calc);

public:
    explicit CalcExtDynamicVariableTest()
        : TraceSource(this,"CalcExtDynamicVariableTest")
    {
        srand(time(NULL));
        CalcInit::instance();
        FENNEL_UNIT_TEST_CASE(CalcExtDynamicVariableTest, testCalcExtDynamicVariable);
    }
     
    virtual ~CalcExtDynamicVariableTest()
    {
    }
};

// for nitty-gritty debugging. sadly, doesn't use BOOST_MESSAGE.
void
CalcExtDynamicVariableTest::printOutput(TupleData const & tup,
                                        Calculator const & calc)
{
    if (true) {
        TuplePrinter tuplePrinter;
        tuplePrinter.print(cout, calc.getOutputRegisterDescriptor(), tup);
        cout << endl;
    }
}

void
CalcExtDynamicVariableTest::testCalcExtDynamicVariable()
{
    ostringstream pg("");
    char* typesArray[] = {"s4", "u4", "s8", "u8", "s1", "u1", "s2","u2", "bo", "r", "d", "c,4", "vc,4", "b,4"};
    const uint N = sizeof(typesArray)/sizeof(typesArray[0]);
    string types;
    for (int i=0; i<N; i++) {
        if (i>0) {
            types += ", ";
        }
        types += typesArray[i];
    }
    pg << "O " << types << ";" << endl;
    pg << "L " << types << ";" << endl;
    pg << "C ";
    for (int i=0; i<N; i++) {
        if (i>0) {
            pg << ", ";
        }
        pg << "s4";
    }
    pg << ";" << endl;
    pg << "V ";
    for (int i=0; i<N; i++) {
        if (i>0) {
            pg << ", ";
        }
        pg << i;
    }
    pg << ";" << endl;
    pg << "T;" << endl;
    for (int i=0; i<N; i++) {
        pg << "CALL 'dynamicVariable(L" << i << ", C" << i << ");" << endl;
    }
    for (int i=0; i<N; i++) {
        pg << "REF O" << i << ", L" << i << ";" << endl;
    }

    DynamicParamManager dpm;
    Calculator calc(&dpm);

    try {
        calc.assemble(pg.str().c_str());
    }
    catch (FennelExcn& ex) {
        BOOST_FAIL("Assemble exception " << ex.getMessage()<< pg.str());
    }

    // Setup dynamic variables
    TupleDescriptor outTupleDesc = calc.getOutputRegisterDescriptor();
    TupleData dynamicData(outTupleDesc);

    int32_t data0 = -321;
    dynamicData[0].pData = (PConstBuffer) &data0;
    dpm.createParam(0, outTupleDesc[0]);
    dpm.setParam(0, dynamicData[0]);

    uint32_t data1 = 622;
    dynamicData[1].pData = (PConstBuffer) &data1;
    dpm.createParam(1, outTupleDesc[1]);
    dpm.setParam(1, dynamicData[1]);

    int64_t data2 = 0xFFFFFFFFffffffffLL;
    dynamicData[2].pData = (PConstBuffer) &data2;
    dpm.createParam(2, outTupleDesc[2]);
    dpm.setParam(2, dynamicData[2]);

    uint64_t data3 = 0x8000000000000000ULL;
    dynamicData[3].pData = (PConstBuffer) &data3;
    dpm.createParam(3, outTupleDesc[3]);
    dpm.setParam(3, dynamicData[3]);

    int8_t data4 = 0xFF;
    dynamicData[4].pData = (PConstBuffer) &data4;
    dpm.createParam(4, outTupleDesc[4]);
    dpm.setParam(4, dynamicData[4]);

    uint8_t data5 = 128;
    dynamicData[5].pData = (PConstBuffer) &data5;
    dpm.createParam(5, outTupleDesc[5]);
    dpm.setParam(5, dynamicData[5]);
    
    int16_t data6 = 0xFFFF;
    dynamicData[6].pData = (PConstBuffer) &data6;
    dpm.createParam(6, outTupleDesc[6]);
    dpm.setParam(6, dynamicData[6]);

    uint16_t data7 = 0x8000;
    dynamicData[7].pData = (PConstBuffer) &data7;
    dpm.createParam(7, outTupleDesc[7]);
    dpm.setParam(7, dynamicData[7]);

    bool data8 = true;
    dynamicData[8].pData = (PConstBuffer) &data8;
    dpm.createParam(8, outTupleDesc[8]);
    dpm.setParam(8, dynamicData[8]);

    float data9 = 3.14f;
    dynamicData[9].pData = (PConstBuffer) &data9;
    dpm.createParam(9, outTupleDesc[9]);
    dpm.setParam(9, dynamicData[9]);

    float data10 = 3.14e300;
    dynamicData[10].pData = (PConstBuffer) &data10;
    dpm.createParam(10, outTupleDesc[10]);
    dpm.setParam(10, dynamicData[10]);

    char* data11 = "abc";
    dynamicData[11].pData = (PConstBuffer) data11;
    dynamicData[11].cbData = strlen(data11);
    dpm.createParam(11, outTupleDesc[11]);
    dpm.setParam(11, dynamicData[11]);

    char* data12 = "def";
    dynamicData[12].pData = (PConstBuffer) data12;
    dynamicData[12].cbData = strlen(data12);
    dpm.createParam(12, outTupleDesc[12]);
    dpm.setParam(12, dynamicData[12]);

    char* data13 = "ghi";
    dynamicData[13].pData = (PConstBuffer) data13;
    dynamicData[13].cbData = strlen(data13);
    dpm.createParam(13, outTupleDesc[13]);
    dpm.setParam(13, dynamicData[13]);

    TupleDataWithBuffer outTuple(calc.getOutputRegisterDescriptor());
    TupleDataWithBuffer inTuple(calc.getInputRegisterDescriptor());

    calc.bind(&inTuple, &outTuple);
    calc.exec();

    //    printOutput(outTuple, calc);
    BOOST_CHECK(*(reinterpret_cast<int32_t*>(const_cast<PBuffer>(outTuple[0].pData)))==-321);
    BOOST_CHECK(*(reinterpret_cast<uint32_t*>(const_cast<PBuffer>(outTuple[1].pData)))==622);
    BOOST_CHECK(*(reinterpret_cast<int64_t*>(const_cast<PBuffer>(outTuple[2].pData)))==-1);
    BOOST_CHECK(*(reinterpret_cast<uint64_t*>(const_cast<PBuffer>(outTuple[3].pData)))==0x8000000000000000ULL);
    BOOST_CHECK(*(reinterpret_cast<int8_t*>(const_cast<PBuffer>(outTuple[4].pData)))==-1);
    BOOST_CHECK(*(reinterpret_cast<uint8_t*>(const_cast<PBuffer>(outTuple[5].pData)))==128);
    BOOST_CHECK(*(reinterpret_cast<int16_t*>(const_cast<PBuffer>(outTuple[6].pData)))==-1);
    BOOST_CHECK(*(reinterpret_cast<uint16_t*>(const_cast<PBuffer>(outTuple[7].pData)))==32768);
    BOOST_CHECK(*(reinterpret_cast<bool*>(const_cast<PBuffer>(outTuple[8].pData)))==true);
    BOOST_CHECK(*(reinterpret_cast<float*>(const_cast<PBuffer>(outTuple[9].pData)))-3.14<0.0001);
    BOOST_CHECK(*(reinterpret_cast<double*>(const_cast<PBuffer>(outTuple[10].pData)))-3.14e300<0.0001);

    BOOST_CHECK(!memcmp("abc",outTuple[11].pData,3));
    BOOST_CHECK(!memcmp("def",outTuple[12].pData,3));
    BOOST_CHECK(!memcmp("ghi",outTuple[13].pData,3));
}
    
FENNEL_UNIT_TEST_SUITE(CalcExtDynamicVariableTest);

