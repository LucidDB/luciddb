/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005-2005 LucidEra, Inc.
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
#include "fennel/test/ExecStreamUnitTestBase.h"
//#include "fennel/tuple/StandardTypeDescriptor.h"
#include "fennel/exec/MockProducerExecStream.h"
#include "fennel/exec/ExecStreamEmbryo.h"

#include "fennel/exec/ExecStreamGraph.h"
#include "fennel/exec/ExecStreamGraphEmbryo.h"
#include "fennel/exec/ExecStreamScheduler.h"
#include "fennel/exec/ExecStream.h"
#include "fennel/exec/ScratchBufferExecStream.h"
#include "fennel/exec/ExecStreamEmbryo.h"
#include "fennel/exec/ExecStreamBufAccessor.h"
#include "fennel/exec/MockProducerExecStream.h"
#include "fennel/tuple/TuplePrinter.h"
#include "fennel/tuple/StandardTypeDescriptor.h"

#include <boost/test/test_tools.hpp>

#include "fennel/lucidera/flatfile/FlatFileBuffer.h"
#include "fennel/lucidera/flatfile/FlatFileParser.h"
#include "fennel/lucidera/flatfile/FlatFileExecStream.h"

using namespace fennel;

/**
 * StringExecStreamGenerator defines an interface for generating
 * a data stream of strings.
 */
class StringExecStreamGenerator
{
public:
    virtual ~StringExecStreamGenerator() {}
    
    /**
     * Generates one data value.
     *
     * @param iRow 0-based row number to generate
     */
    virtual const std::string &generateValue(uint iRow) = 0;
};

class StringExecStreamGeneratorImpl : public StringExecStreamGenerator
{
    std::vector<std::string> values;
    
public:
    void insert(const std::string &value)
    {
        values.push_back(value);
    }
    
    // Implement StringExecStreamGenerator
    const std::string &generateValue(uint iRow)
    {
        BOOST_CHECK(iRow < values.size());
        return values[iRow];
    }
};

class FlatFileExecStreamTest : public ExecStreamUnitTestBase
{
public:
    explicit FlatFileExecStreamTest()
    {
        FENNEL_UNIT_TEST_CASE(FlatFileExecStreamTest, testBuffer);
        FENNEL_UNIT_TEST_CASE(FlatFileExecStreamTest, testParser);
        FENNEL_UNIT_TEST_CASE(FlatFileExecStreamTest, testStream);
        FENNEL_UNIT_TEST_CASE(FlatFileExecStreamTest, testStreamCalc);
    }

    void testBuffer();
    void testParser();
    void testStream();
    void testStreamCalc();

    void verifyOutput(
        ExecStream &stream,
        uint nRowsExpected,
        StringExecStreamGenerator &generator);
};

void FlatFileExecStreamTest::testBuffer()
{
    FixedBuffer fixedBuffer[8];
    std::string path = "flatfile/buffer";
    
    FlatFileBuffer fileBuffer(path);
    fileBuffer.setStorage((char *) fixedBuffer, (uint)8);
    fileBuffer.open();
    BOOST_REQUIRE(fileBuffer.buf()==(char *)fixedBuffer);
    BOOST_REQUIRE(fileBuffer.size()==0);
    fileBuffer.fill();
    BOOST_REQUIRE(fileBuffer.size() == 8);
    BOOST_REQUIRE(strncmp(fileBuffer.buf(), "12345671", 8)==0);
    fileBuffer.fill(fileBuffer.buf()+7);
    BOOST_REQUIRE(fileBuffer.size() == 8);
    BOOST_REQUIRE(strncmp(fileBuffer.buf(), "12345676", 8)==0);
    fileBuffer.fill(fileBuffer.buf()+6);
    BOOST_REQUIRE(fileBuffer.size()==5);
    BOOST_REQUIRE(strncmp(fileBuffer.buf(), "7654\n", 5)==0);
    BOOST_REQUIRE(fileBuffer.readCompleted());
    fileBuffer.close();

    // TODO: test missing file, empty file
}

void FlatFileExecStreamTest::testParser()
{
    FlatFileParser parser(',', '\n', '"', '"');
    char buffer[128];

    strcpy(buffer, "");
    BOOST_CHECK_EQUAL(parser.trim(buffer, 0), 0);
    BOOST_CHECK_EQUAL(strncmp(buffer, "", 0), 0);
    strcpy(buffer, "aRobin");
    BOOST_CHECK_EQUAL(parser.trim(buffer, 6), 6);
    BOOST_CHECK_EQUAL(strncmp(buffer, "aRobin", 6), 0);
    strcpy(buffer, "   red breast in cage  ");
    BOOST_CHECK_EQUAL(parser.trim(buffer, 23), 18);
    BOOST_CHECK_EQUAL(strncmp(buffer, "red breast in cage", 18), 0);
    
    strcpy(buffer, "");
    BOOST_CHECK_EQUAL(parser.stripQuoting(buffer, 0, true), 0);
    BOOST_CHECK_EQUAL(strncmp(buffer, "", 0), 0);
    strcpy(buffer, "puts all");
    BOOST_CHECK_EQUAL(parser.stripQuoting(buffer, 8, true), 8);
    BOOST_CHECK_EQUAL(strncmp(buffer, "puts all", 8), 0);
    strcpy(buffer, "\"heaven\"");
    BOOST_CHECK_EQUAL(parser.stripQuoting(buffer, 8, true), 6);
    BOOST_CHECK_EQUAL(strncmp(buffer, "heaven", 6), 0);
    strcpy(buffer, "   \"in a\"  ");
    BOOST_CHECK_EQUAL(parser.stripQuoting(buffer, 11, true), 4);
    BOOST_CHECK_EQUAL(strncmp(buffer, "in a", 4), 0);
    strcpy(buffer, "   \"\"\"rage\"\"\"  ");
    BOOST_CHECK_EQUAL(parser.stripQuoting(buffer, 15, true), 6);
    BOOST_CHECK_EQUAL(strncmp(buffer, "\"rage\"", 6), 0);

    FlatFileColumnParseResult result;
    // quote/escapes are plain escapes if not column is not char type
    strcpy(buffer, "\"all that\"\n is gold, ");
    parser.scanColumn(buffer, 21, 128, false, result);
    BOOST_CHECK_EQUAL(result.type, FlatFileColumnParseResult::FIELD_DELIM);
    BOOST_CHECK_EQUAL(result.size, 19);
    BOOST_CHECK(result.next == buffer + 20);
    // quotes are valid for char columns
    strcpy(buffer, "\"does not glitter\"\n ");
    parser.scanColumn(buffer, 20, 128, true, result);
    BOOST_CHECK_EQUAL(result.type, FlatFileColumnParseResult::ROW_DELIM);
    BOOST_CHECK_EQUAL(result.size, 18);
    BOOST_CHECK(result.next == buffer + 19);
    // embedded quotes
    strcpy(buffer, "\"not all those who \"\"wander\"\"\", ");
    parser.scanColumn(buffer, 32, 128, true, result);
    BOOST_CHECK_EQUAL(result.type, FlatFileColumnParseResult::FIELD_DELIM);
    BOOST_CHECK_EQUAL(result.size, 30);
    BOOST_CHECK(result.next == buffer + 31);
    // fixed column type
    strcpy(buffer, " are lost  ");
    parser.scanColumn(buffer, 11, 9, true, result);
    BOOST_CHECK_EQUAL(result.type, FlatFileColumnParseResult::MAX_LENGTH);
    BOOST_CHECK_EQUAL(result.size, 9);
    BOOST_CHECK(result.next == buffer + 9);
    // imbalanced quote
    strcpy(buffer, "\"JRR, ");
    parser.scanColumn(buffer, 6, 128, true, result);
    BOOST_CHECK_EQUAL(result.type, FlatFileColumnParseResult::NO_DELIM);
    BOOST_CHECK_EQUAL(result.size, 6);
    BOOST_CHECK(result.next == buffer + 6);
    // data after quote
    strcpy(buffer, "\"Tolkien\"  , ");
    parser.scanColumn(buffer, 13, 128, true, result);
    BOOST_CHECK_EQUAL(result.type, FlatFileColumnParseResult::FIELD_DELIM);
    BOOST_CHECK_EQUAL(result.size, 11);
    BOOST_CHECK(result.next == buffer + 12);
    // fixed length exactly equal to buffer size
    strcpy(buffer, "some poems");
    parser.scanColumn(buffer, 10, 10, true, result);
    BOOST_CHECK_EQUAL(result.type, FlatFileColumnParseResult::NO_DELIM);
    BOOST_CHECK_EQUAL(result.size, 10);
    BOOST_CHECK(result.next == buffer + 10);
}

void FlatFileExecStreamTest::testStream()
{
    StandardTypeDescriptorFactory stdTypeFactory;
    TupleAttributeDescriptor attrDesc(
        stdTypeFactory.newDataType(STANDARD_TYPE_VARCHAR),
        false,
        32);
    
    FlatFileExecStreamParams flatfileParams;
    flatfileParams.scratchAccessor =
        pSegmentFactory->newScratchSegment(pCache,1);
     flatfileParams.outputTupleDesc.push_back(attrDesc);
    flatfileParams.outputTupleDesc.push_back(attrDesc);
    flatfileParams.dataFilePath = "flatfile/stream";
    flatfileParams.fieldDelim = ',';
    flatfileParams.rowDelim = '\n';
    flatfileParams.quoteChar = '"';
    flatfileParams.escapeChar = '\\';
    flatfileParams.header = false;
    
    ExecStreamEmbryo flatfileStreamEmbryo;
    flatfileStreamEmbryo.init(
        FlatFileExecStream::newFlatFileExecStream(), flatfileParams);
    flatfileStreamEmbryo.getStream()->setName("FlatFileExecStream");

    SharedExecStream pOutputStream = prepareSourceGraph(flatfileStreamEmbryo);
    StringExecStreamGeneratorImpl verifier;
    verifier.insert("[ 'No one', 'travels' ]");
    verifier.insert("[ 'Along this way', 'but I,' ]");
    verifier.insert("[ 'This', 'autumn evening.' ]");

    verifyOutput(
        *pOutputStream,
        3,
        verifier);
}

void FlatFileExecStreamTest::testStreamCalc()
{
    StandardTypeDescriptorFactory stdTypeFactory;
    TupleAttributeDescriptor attrDesc(
        stdTypeFactory.newDataType(STANDARD_TYPE_INT_32),
        false);
    
    FlatFileExecStreamParams flatfileParams;
    flatfileParams.scratchAccessor =
        pSegmentFactory->newScratchSegment(pCache,1);
    flatfileParams.outputTupleDesc.push_back(attrDesc);
    //flatfileParams.outputTupleDesc.push_back(attrDesc);
    flatfileParams.dataFilePath = "flatfile/integer";
    flatfileParams.fieldDelim = ',';
    flatfileParams.rowDelim = '\n';
    flatfileParams.quoteChar = '"';
    flatfileParams.escapeChar = '\\';
    flatfileParams.header = false;
    flatfileParams.calcProgram =
        std::string("O s4;\n") +
        "I vc,255;\n" +
        "L s8, s4, bo;\n" +
        "C bo, bo, vc,5;\n" +
        "V 1, 0, 0x3232303034 /* 22004 */;\n" +
        "T;\n" +
        "CALL 'castA(L0, I0) /* 0: CAST($0):BIGINT NOT NULL */;\n" +
        "CAST L1, L0 /* 1: CAST(CAST($0):BIGINT NOT NULL):INTEGER NOT NULL CAST($0):INTEGER NOT NULL */;\n" +
        "ISNULL L2, L1 /* 2: */;\n" +
        "JMPF @6, L2 /* 3: */;\n" +
        "RAISE C2 /* 4: */;\n" +
        "RETURN /* 5: */;\n" +
        "REF O0, L1 /* 6: */;\n" +
        "RETURN /* 7: */;";

    ExecStreamEmbryo flatfileStreamEmbryo;
    flatfileStreamEmbryo.init(
        FlatFileExecStream::newFlatFileExecStream(), flatfileParams);
    flatfileStreamEmbryo.getStream()->setName("FlatFileExecStream");

    SharedExecStream pOutputStream = prepareSourceGraph(flatfileStreamEmbryo);
    StringExecStreamGeneratorImpl verifier;
    verifier.insert("[ 123 ]");
    verifier.insert("[ 456 ]");
    verifier.insert("[ 777 ]");

    verifyOutput(
        *pOutputStream,
        3,
        verifier);
}

void FlatFileExecStreamTest::verifyOutput(
    ExecStream &stream,
    uint nRowsExpected,
    StringExecStreamGenerator &generator)
{
    // TODO:  assertions about output tuple, or better yet, use proper tuple
    // access
    
    pGraph->open();
    pScheduler->start();
    uint nRows = 0;
    for (;;) {
        ExecStreamBufAccessor &bufAccessor =
            pScheduler->readStream(stream);
        if (bufAccessor.getState() == EXECBUF_EOS) {
            break;
        }
        BOOST_REQUIRE(bufAccessor.isConsumptionPossible());
        const uint nCol = 
            bufAccessor.getConsumptionTupleAccessor().size();
        BOOST_REQUIRE(nCol == bufAccessor.getTupleDesc().size());
        BOOST_REQUIRE(nCol >= 1);
        TupleData inputTuple;
        inputTuple.compute(bufAccessor.getTupleDesc());
        std::ostringstream oss;
        TuplePrinter tuplePrinter;
        for (;;) {
            if (!bufAccessor.demandData()) {
                break;
            }
            BOOST_REQUIRE(nRows < nRowsExpected);
            bufAccessor.unmarshalTuple(inputTuple);
            tuplePrinter.print(oss,bufAccessor.getTupleDesc(),inputTuple);
            std::string actualValue = oss.str();
            oss.str("");
            const std::string &expectedValue = generator.generateValue(nRows);
            if (actualValue.compare(expectedValue)) {
                std::cout << "(Row) = (" << nRows << ")" << std::endl;
                BOOST_CHECK_EQUAL(expectedValue,actualValue);
                return;
            }
            bufAccessor.consumeTuple();
            ++nRows;
        }
    }
    BOOST_CHECK_EQUAL(nRowsExpected,nRows);
}

FENNEL_UNIT_TEST_SUITE(FlatFileExecStreamTest);

// End FlatFileStreamTest.cpp
