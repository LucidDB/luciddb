/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2006-2007 Disruptive Tech
// Copyright (C) 2006-2007 The Eigenbase Project
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

#include "fennel/tuple/TupleData.h"
#include "fennel/tuple/TupleDataWithBuffer.h"
#include "fennel/tuple/TuplePrinter.h"
#include "fennel/disruptivetech/calc/CalcCommon.h"
#include "fennel/common/FennelExcn.h"

#include <boost/test/floating_point_comparison.hpp>

#include <boost/test/test_tools.hpp>
#include <boost/scoped_array.hpp>
#include <string>
#include <limits>
#include <vector>

using namespace fennel;
using namespace std;


#define TEST_DATA_INDEX 0
#define MIN_INDEX 1
#define MAX_INDEX 2
#define SUM_INDEX 3
#define FV_INDEX 4
#define LV_INDEX 5

#define SAMPLE_SIZE 10


static const int64_t INT_TEST_MIN =  2; // min data value below
static const int64_t INT_TEST_MAX =  105; // max data value below

static int64_t intTestData[][SAMPLE_SIZE] =
{
    {12,  33, 52, 14, 10, 63,  5,  2, 49,105},  // test data values
    {12,  12, 12, 12, 10, 10,  5,  2,  2,  2},  // running MIN values
    {12,  33, 52, 52, 52, 63, 63, 63, 63,105},  // running MAX values
    {12,  45, 97,111,121,184,189,191,240,345},  // running SUM of test values
    {12,  12, 12, 12, 12, 12, 12, 12, 12, 12},  // running FIRST_VALUE values
    {105,105,105,105,105,105,105,105,105,105},  // running LAST_VALUE values
};


static const double DBL_TEST_MIN = 1.5; // min data value below
static const double DBL_TEST_MAX = 874.5; // max data value below

static double dblTestData[][SAMPLE_SIZE] =
{
    { 63.5, 63.1, 92.9,  1.5,  6.3, 38.5, 23.1, 874.5,  44.7, 498.0}, // data values
    { 63.5, 63.1, 63.1,  1.5,  1.5,  1.5,  1.5,   1.5,   1.5,   1.5}, // running MIN
    { 63.5, 63.5, 92.9, 92.9, 92.9, 92.9, 92.9, 874.5, 874.5, 874.5}, // running MAX
    { 63.5,126.6,219.5,221.0,227.3,265.8,288.9,1163.4,1208.1,1706.1}, // running SUM
    { 63.5, 63.5, 63.5, 63.5, 63.5, 63.5, 63.5,  63.5,  63.5,  63.5}, // running FIRST_VALUE
    {498.0,498.0,490.0,498.0,498.0,498.0,498.0, 498.0, 498.0, 498.0}, // running LAST_VALUE
};

#define STR_SAMPLE_SIZE 4

static const char *str1 = "abcd";
static const char *str2 = "qrst";
static const char *str3 = "abc ";
static const char *str4 = "noot";

static const char* strAddTestData[][STR_SAMPLE_SIZE] =
{
    {str1, str2, str3, str4},   // data values
    {str1, str1, str3, str3},   // running MIN
    {str1, str2, str2, str2},   // running MAX
    {NULL, NULL, NULL, NULL},   // (not used)
    {str1, str1, str1, str1},   // running FIRST_VALUE
    {str1, str2, str3, str4},   // running LAST_VALUE
};

static const char* strDropTestData[][STR_SAMPLE_SIZE] =
{
    {str1, str2, str3, str4},   // data values
    {str3, str3, str4, NULL},   // running MIN
    {str2, str4, str4, NULL},   // running MAX
    {NULL, NULL, NULL, NULL},   // (not used)
    {str2, str3, str4, NULL},   // running FIRST_VALUE
    {str4, str4, str4, NULL},   // running LAST_VALUE
};

static vector<TupleData*>  testTuples;



class CalcExtWinAggFuncTest : virtual public TestBase, public TraceSource
{
    void checkWarnings(Calculator& calc, string expected);

    void testCalcExtMinMaxInt();
    void testCalcExtMinMaxDbl();
    void testCalcExtMinMaxStr();
    
    
    void initWindowedAggDataBlock(
        TupleDataWithBuffer* outTuple,
        StandardTypeDescriptorOrdinal dType);

    void printOutput(
        TupleData const & tup,
        Calculator const & calc);
    
public:
    explicit CalcExtWinAggFuncTest()
        : TraceSource(shared_from_this(),"CalcExtWinAggFuncTest")
    {
        srand(time(NULL));
        CalcInit::instance();
        FENNEL_UNIT_TEST_CASE(CalcExtWinAggFuncTest, testCalcExtMinMaxInt);
        FENNEL_UNIT_TEST_CASE(CalcExtWinAggFuncTest, testCalcExtMinMaxDbl);
        FENNEL_UNIT_TEST_CASE(CalcExtWinAggFuncTest, testCalcExtMinMaxStr);

    }
     
    virtual ~CalcExtWinAggFuncTest()
    {
    }
};

// for nitty-gritty debugging. sadly, doesn't use BOOST_MESSAGE.
void
CalcExtWinAggFuncTest::printOutput(
    TupleData const & tup,
    Calculator const & calc)
{
#if 0
    TuplePrinter tuplePrinter;
    tuplePrinter.print(cout, calc.getOutputRegisterDescriptor(), tup);
    cout << endl;
#endif
}


void 
CalcExtWinAggFuncTest::checkWarnings(Calculator& calc, string expected)
{
    try {
        calc.exec();
    } catch(...) {
        BOOST_FAIL("An exception was thrown while running program");
    }
    
    int i = calc.warnings().find(expected);

    if (i < 0) {
        string msg ="Unexpected or no warning found\n";
        msg += "Expected: ";
        msg += expected;
        msg += "\nActual:  ";
        msg += calc.warnings();

        BOOST_FAIL(msg);
    }   
}


void
CalcExtWinAggFuncTest::initWindowedAggDataBlock(
    TupleDataWithBuffer* outTuple,
    StandardTypeDescriptorOrdinal dType)
{
    ostringstream pg("");

    // script to initialize the Windowed
    // agg functions.  Just pass in the Type
    // to be initialized.  It is supplied with
    // all subsequent calls
    if (dType == STANDARD_TYPE_INT_64) {
        pg << "I s8;" << endl;
    } else  if (dType == STANDARD_TYPE_DOUBLE) {
        pg << "I d;" << endl;
    } else if (dType == STANDARD_TYPE_VARCHAR) {
        pg << "I vc,4;" << endl;
    } else if (dType == STANDARD_TYPE_CHAR) {
        pg << "I c,4;" << endl;
    }

    pg << "O vb,4;" << endl;
    pg << "T;" << endl;
    pg << "CALL 'WinAggInit(O0,I0);" << endl;

    // Allocate
    Calculator calc(0);
    calc.outputRegisterByReference(false);
    
    // Assemble the script
    try {
        calc.assemble(pg.str().c_str());
    }
    catch (FennelExcn& ex) {
        BOOST_FAIL("Assemble exception " << ex.getMessage()<< pg.str());
    }

    
    outTuple->computeAndAllocate(calc.getOutputRegisterDescriptor());
    
    TupleDataWithBuffer inTuple(calc.getInputRegisterDescriptor());

    calc.bind(&inTuple, outTuple);
    
    calc.exec();
    printOutput(*outTuple, calc);
}

template <typename DTYPE>
void
WinAggAddTest(
    TupleDataWithBuffer* winAggTuple,
    DTYPE testData[][SAMPLE_SIZE],
    StandardTypeDescriptorOrdinal dType,
    void (*check)(TupleDataWithBuffer*,DTYPE[][SAMPLE_SIZE],int))
{
    ostringstream pg("");

    if (StandardTypeDescriptor::isExact(dType)) {
        pg << "O s8,s8,s8,s8,s8,s8,s8;" << endl;
        pg << "I s8,vb,4;" <<endl;
    } else if (StandardTypeDescriptor::isApprox(dType)) {
        pg << "O s8,d,d,d,d,d,d;" << endl;
        pg << "I d,vb,4;" <<endl;
    }
    pg << "T;" << endl;
    pg << "CALL 'WinAggAdd(I0,I1);" << endl;
    pg << "CALL 'WinAggCount(O0,I1);" << endl;
    pg << "CALL 'WinAggSum(O1,I1);" << endl;
    pg << "CALL 'WinAggAvg(O2,I1);" << endl;
    pg << "CALL 'WinAggMin(O3,I1);" << endl;
    pg << "CALL 'WinAggMax(O4,I1);" << endl;
    pg << "CALL 'WinAggFirstValue(O5,I1);" << endl;
    pg << "CALL 'WinAggLastValue(O6,I1);" << endl;

    // Allocate
    Calculator calc(0);
    calc.outputRegisterByReference(false);
    
    // Assemble the script
    try {
        calc.assemble(pg.str().c_str());
    }
    catch (FennelExcn& ex) {
        BOOST_FAIL("Assemble exception " << ex.getMessage()<< pg.str());
    }
    
    TupleDataWithBuffer outTuple(calc.getOutputRegisterDescriptor());

    for (int i=0; i < 10; i++) {
        TupleDataWithBuffer *inTuple = 
            new TupleDataWithBuffer(calc.getInputRegisterDescriptor());
        testTuples.push_back(inTuple);

        calc.bind(inTuple, &outTuple);
        
        // copy the Agg data block pointer into the input tuple
        (*inTuple)[1] = (*winAggTuple)[0];
        
        TupleDatum* pTD = &((*inTuple)[0]);
        pTD->pData = reinterpret_cast<PConstBuffer>(&testData[TEST_DATA_INDEX][i]);
    
        calc.exec();

        (*check)(&outTuple,testData,i);
    }
    assert(10 == *(reinterpret_cast<int64_t*>(const_cast<uint8_t*>(outTuple[0].pData))));
}


template <typename DTYPE>
void
WinAggDropTest(
    TupleDataWithBuffer* winAggTuple,
    DTYPE testData[][SAMPLE_SIZE],
    StandardTypeDescriptorOrdinal dType,
    void (*check)(TupleDataWithBuffer*,DTYPE[][SAMPLE_SIZE],int))
{
    ostringstream pg("");

    if (StandardTypeDescriptor::isExact(dType)) {
        pg << "O s8,s8,s8,s8,s8,s8,s8;" << endl;
        pg << "I s8,vb,4;" <<endl;
    } else if (StandardTypeDescriptor::isApprox(dType)) {
        pg << "O s8,d,d,d,d,d,d;" << endl;
        pg << "I d,vb,4;" <<endl;
    }
    pg << "T;" << endl;
    pg << "CALL 'WinAggDrop(I0,I1);" << endl;
    pg << "CALL 'WinAggCount(O0,I1);" << endl;
    pg << "CALL 'WinAggSum(O1,I1);" << endl;
    pg << "CALL 'WinAggAvg(O2,I1);" << endl;
    pg << "CALL 'WinAggMin(O3,I1);" << endl;
    pg << "CALL 'WinAggMax(O4,I1);" << endl;
    pg << "CALL 'WinAggFirstValue(O5,I1);" << endl;
    pg << "CALL 'WinAggLastValue(O6,I1);" << endl;

    // Allocate
    Calculator calc(0);
    calc.outputRegisterByReference(false);
    
    // Assemble the script
    try {
        calc.assemble(pg.str().c_str());
    }
    catch (FennelExcn& ex) {
        BOOST_FAIL("Assemble exception " << ex.getMessage()<< pg.str());
    }

    // Alloc tuples and buffer space
    TupleDataWithBuffer outTuple(calc.getOutputRegisterDescriptor());

    // Step backwards through the data table and remove each entry
    // from the window checking the function returns along the way.
    for (int i=SAMPLE_SIZE-1; i >=0 ; i--) {
        TupleData* inTuple = testTuples[i];
        TupleDatum* pTD = &(*inTuple)[0];

        calc.bind(inTuple, &outTuple);
        
        // copy the Agg data block pointer into the input tuple
        (*inTuple)[1] = (*winAggTuple)[0];
    
        pTD->pData = reinterpret_cast<PConstBuffer>(&testData[TEST_DATA_INDEX][i]);
    
        calc.exec();

        (*check)(&outTuple, testData, i);
    }
    assert(0 == *(reinterpret_cast<const int64_t*>(outTuple[0].pData)));
}

template <typename DTYPE>
void
WinAggAddTestStr(
    TupleDataWithBuffer* winAggTuple,
    DTYPE testData[][STR_SAMPLE_SIZE],
    StandardTypeDescriptorOrdinal dType,
    void (*check)(TupleDataWithBuffer*,DTYPE[][STR_SAMPLE_SIZE],int))
{
    ostringstream pg("");

    if (StandardTypeDescriptor::isVariableLenArray(dType)) {
        pg << "O s8, vc,4, vc,4, vc,4, vc,4;" << endl;
        pg << "I vc,4,vb,4;" <<endl;
    } else if (StandardTypeDescriptor::isArray(dType)) {
        pg << "O s8, c,4, c,4, c,4, c,4;" << endl;
        pg << "I c,4,vb,4;" <<endl;
    }
    pg << "T;" << endl;
    pg << "CALL 'WinAggAdd(I0,I1);" << endl;
    pg << "CALL 'WinAggCount(O0,I1);" << endl;
    pg << "CALL 'WinAggMin(O1,I1);" << endl;
    pg << "CALL 'WinAggMax(O2,I1);" << endl;
    pg << "CALL 'WinAggFirstValue(O3,I1);" << endl;
    pg << "CALL 'WinAggLastValue(O4,I1);" << endl;

    // Allocate
    Calculator calc(0);
    calc.outputRegisterByReference(false);
    
    // Assemble the script
    try {
        calc.assemble(pg.str().c_str());
    }
    catch (FennelExcn& ex) {
        BOOST_FAIL("Assemble exception " << ex.getMessage()<< pg.str());
    }
    
    TupleDataWithBuffer outTuple(calc.getOutputRegisterDescriptor());

    for (int i=0; i < STR_SAMPLE_SIZE; i++) {
        TupleDataWithBuffer *inTuple = new TupleDataWithBuffer(calc.getInputRegisterDescriptor());
        testTuples.push_back(inTuple);

        calc.bind(inTuple, &outTuple);
        
        // copy the Agg data block pointer into the input tuple
        (*inTuple)[1] = (*winAggTuple)[0];
        
        TupleDatum* pTD = &((*inTuple)[0]);
        pTD->pData = reinterpret_cast<PConstBuffer>(testData[TEST_DATA_INDEX][i]);
    
        calc.exec();

        (*check)(&outTuple,testData,i);
    }
    assert(4  == *(reinterpret_cast<int64_t*>(const_cast<uint8_t*>(outTuple[0].pData))));
}

template <typename DTYPE>
void
WinAggDropTestStr(
    TupleDataWithBuffer* winAggTuple,
    DTYPE testData[][STR_SAMPLE_SIZE],
    StandardTypeDescriptorOrdinal dType,
    void (*check)(TupleDataWithBuffer*,DTYPE[][STR_SAMPLE_SIZE],int))
{
    ostringstream pg("");

    if (StandardTypeDescriptor::isVariableLenArray(dType)) {
        pg << "O s8, vc,4, vc,4, vc,4, vc,4;" << endl;
        pg << "I vc,4,vb,4;" <<endl;
    } else if (StandardTypeDescriptor::isArray(dType)) {
        pg << "O s8, c,4, c,4, c,4, c,4;" << endl;
        pg << "I c,4,vb,4;" <<endl;
    }
    pg << "T;" << endl;
    pg << "CALL 'WinAggDrop(I0,I1);" << endl;
    pg << "CALL 'WinAggCount(O0,I1);" << endl;
    pg << "CALL 'WinAggMin(O1,I1);" << endl;
    pg << "CALL 'WinAggMax(O2,I1);" << endl;
    pg << "CALL 'WinAggFirstValue(O3,I1);" << endl;
    pg << "CALL 'WinAggLastValue(O4,I1);" << endl;

    // Allocate
    Calculator calc(0);
    calc.outputRegisterByReference(false);
    
    // Assemble the script
    try {
        calc.assemble(pg.str().c_str());
    }
    catch (FennelExcn& ex) {
        BOOST_FAIL("Assemble exception " << ex.getMessage()<< pg.str());
    }

    // Alloc tuples and buffer space
    TupleDataWithBuffer outTuple(calc.getOutputRegisterDescriptor());

    // Step forwards through the data table and remove each entry
    // from the window checking the function returns along the way.
    for (int i = 0; i < STR_SAMPLE_SIZE; i++) {
        TupleData* inTuple = testTuples[i];
        TupleDatum* pTD = &(*inTuple)[0];

        calc.bind(inTuple, &outTuple);
        
        // copy the Agg data block pointer into the input tuple
        (*inTuple)[1] = (*winAggTuple)[0];
    
        pTD->pData = reinterpret_cast<PConstBuffer>(testData[TEST_DATA_INDEX][i]);
    
        calc.exec();

        (*check)(&outTuple, testData, i);
    }
    assert(0 == *(reinterpret_cast<const int64_t*>(outTuple[0].pData)));
}

void checkAddInt(
    TupleDataWithBuffer* outTuple,
    int64_t testData[][SAMPLE_SIZE],
    int index)
{
    BOOST_CHECK_EQUAL(index+1, *(reinterpret_cast<const int64_t*>((*outTuple)[0].pData)));
    BOOST_CHECK_EQUAL(testData[SUM_INDEX][index], *(reinterpret_cast<const int64_t*>((*outTuple)[1].pData)));
    BOOST_CHECK_EQUAL(testData[MIN_INDEX][index], *(reinterpret_cast<const int64_t*>((*outTuple)[3].pData)));
    BOOST_CHECK_EQUAL(testData[MAX_INDEX][index], *(reinterpret_cast<const int64_t*>((*outTuple)[4].pData)));
    BOOST_CHECK_EQUAL(testData[FV_INDEX][index], *(reinterpret_cast<const int64_t*>((*outTuple)[5].pData)));
    BOOST_CHECK_EQUAL(testData[TEST_DATA_INDEX][index], *(reinterpret_cast<const int64_t*>((*outTuple)[6].pData)));
}

void checkDropInt(
    TupleDataWithBuffer* outTuple,
    int64_t testData[][SAMPLE_SIZE],
    int index)
{
    if (index > 0) {
        BOOST_CHECK_EQUAL(index, *(reinterpret_cast<const int64_t*>((*outTuple)[0].pData)));
        BOOST_CHECK_EQUAL(testData[SUM_INDEX][index-1], *(reinterpret_cast<const int64_t*>((*outTuple)[1].pData)));
        BOOST_CHECK_EQUAL(testData[MIN_INDEX][index-1], *(reinterpret_cast<const int64_t*>((*outTuple)[3].pData)));
        BOOST_CHECK_EQUAL(testData[MAX_INDEX][index-1], *(reinterpret_cast<const int64_t*>((*outTuple)[4].pData)));
        BOOST_CHECK_EQUAL(testData[TEST_DATA_INDEX][SAMPLE_SIZE - index], *(reinterpret_cast<const int64_t*>((*outTuple)[5].pData)));
        BOOST_CHECK_EQUAL(testData[LV_INDEX][index-1], *(reinterpret_cast<const int64_t*>((*outTuple)[6].pData)));
    }
}

void checkAddDbl(
    TupleDataWithBuffer* outTuple,
    double testData[][SAMPLE_SIZE],
    int index)
{
    int64_t rowCount = *(reinterpret_cast<const int64_t*>((*outTuple)[0].pData));
    BOOST_CHECK_EQUAL(index+1, rowCount);
    
    double tdSum = testData[SUM_INDEX][index];
    double calcSum = *(reinterpret_cast<const double*>((*outTuple)[1].pData));
    BOOST_CHECK_CLOSE(tdSum, calcSum, 0.1);
    
    double tdMin = testData[MIN_INDEX][index];
    double calcMin = *(reinterpret_cast<const double*>((*outTuple)[3].pData));
    BOOST_CHECK_EQUAL(tdMin, calcMin);
    
    double tdMax = testData[MAX_INDEX][index];
    double calcMax = *(reinterpret_cast<const double*>((*outTuple)[4].pData));
    BOOST_CHECK_EQUAL(tdMax, calcMax);
}

void checkDropDbl(
    TupleDataWithBuffer* outTuple,
    double testData[][SAMPLE_SIZE],
    int index)
{
    if (index > 0) {
        int64_t calcRowCount = *(reinterpret_cast<const int64_t*>((*outTuple)[0].pData));
        BOOST_CHECK_EQUAL(index, calcRowCount);

        double tdSum = testData[SUM_INDEX][index-1];
        double calcSum = *(reinterpret_cast<const double*>((*outTuple)[1].pData));
        BOOST_CHECK_CLOSE(tdSum, calcSum, 0.1);

        double tdMin = testData[MIN_INDEX][index-1];
        double calcMin = *(reinterpret_cast<const double*>((*outTuple)[3].pData));
        BOOST_CHECK_CLOSE(tdMin, calcMin, 0.1);

        double tdMax = testData[MAX_INDEX][index-1];
        double calcMax = *(reinterpret_cast<const double*>((*outTuple)[4].pData));
        BOOST_CHECK_CLOSE(tdMax, calcMax, 0.1);
    }
}

void checkDropStr(
    TupleDataWithBuffer* outTuple,
    char* testData[][STR_SAMPLE_SIZE],
    int index)
{
    
}

/// Helper function to compare a tuple with an expected string value.
void checkEqualStr(
    TupleDatum const &tuple,
    const char *expected)
{
    const char *rtnStr = reinterpret_cast<const char*>(const_cast<PBuffer>(tuple.pData));
    if (NULL != expected && NULL != rtnStr) {
        const char *rtnStrEnd = rtnStr + tuple.cbData;
        const char *expectedEnd = expected + strlen(expected);
    
        BOOST_CHECK_EQUAL_COLLECTIONS(rtnStr, rtnStrEnd, expected, expectedEnd);
    } else {
        BOOST_CHECK(expected == rtnStr);
    }
}

void checkStr(
    TupleDataWithBuffer* outTuple,
    const char* testData[][STR_SAMPLE_SIZE],
    int index)
{
    // check the MIN return value
    checkEqualStr((*outTuple)[1], testData[MIN_INDEX][index]);

    // check the MAX return value
    checkEqualStr((*outTuple)[2], testData[MAX_INDEX][index]);

    // check the FIRST_VALUE return value
    checkEqualStr((*outTuple)[3], testData[FV_INDEX][index]);

    // check the LAST_VALUE return value
    checkEqualStr((*outTuple)[4], testData[LV_INDEX][index]);
}

void
CalcExtWinAggFuncTest::testCalcExtMinMaxInt()
{
    // Clear the vector that holds the TupleData for the simulated window.
    testTuples.clear();
    
    // Test windowing integer type
    TupleDataWithBuffer intAggTuple;
    initWindowedAggDataBlock(&intAggTuple, STANDARD_TYPE_INT_64);
    WinAggAddTest(&intAggTuple, intTestData, STANDARD_TYPE_INT_64, checkAddInt);
    WinAggDropTest(&intAggTuple, intTestData, STANDARD_TYPE_INT_64, checkDropInt);
}

void
CalcExtWinAggFuncTest::testCalcExtMinMaxDbl()
{
    // Clear the vector that holds the TupleData for the simulated window.
    testTuples.clear();
    
    // windowing real type
    TupleDataWithBuffer dblAggTuple;
    initWindowedAggDataBlock(&dblAggTuple, STANDARD_TYPE_DOUBLE);
    WinAggAddTest(&dblAggTuple, dblTestData, STANDARD_TYPE_DOUBLE, checkAddDbl);
    WinAggDropTest(&dblAggTuple, dblTestData, STANDARD_TYPE_DOUBLE, checkDropDbl);
}

void
CalcExtWinAggFuncTest::testCalcExtMinMaxStr()
{
    testTuples.clear();
    
    // Test VARCHAR
    TupleDataWithBuffer vcAggTuple;
    initWindowedAggDataBlock(&vcAggTuple, STANDARD_TYPE_VARCHAR);
    WinAggAddTestStr(&vcAggTuple, strAddTestData, STANDARD_TYPE_VARCHAR, checkStr);
    WinAggDropTestStr(&vcAggTuple, strDropTestData, STANDARD_TYPE_VARCHAR, checkStr);
}



FENNEL_UNIT_TEST_SUITE(CalcExtWinAggFuncTest);

