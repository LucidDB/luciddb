/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005 The Eigenbase Project
// Copyright (C) 2009 SQLstream, Inc.
// Copyright (C) 2005 Dynamo BI Corporation
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

#include "fennel/flatfile/FlatFileBuffer.h"
#include "fennel/flatfile/FlatFileParser.h"
#include "fennel/flatfile/FlatFileExecStream.h"

using namespace fennel;

class FlatFileExecStreamTest : public ExecStreamUnitTestBase
{
    void checkRead(
        FlatFileBuffer &buffer,
        const char *string);

    void checkTrim(
        FlatFileParser &parser,
        const char *string,
        const char *result);

    void checkStrip(
        FlatFileParser &parser,
        const char *string,
        const char *result);

    void checkColumnScan(
        FlatFileParser &parser,
        const char *string,
        FlatFileColumnParseResult::DelimiterType type,
        uint size,
        uint offset);

public:
    explicit FlatFileExecStreamTest()
    {
        FENNEL_UNIT_TEST_CASE(FlatFileExecStreamTest, testBuffer);
        FENNEL_UNIT_TEST_CASE(FlatFileExecStreamTest, testParser);
        FENNEL_UNIT_TEST_CASE(FlatFileExecStreamTest, testStream);
    }

    void testBuffer();
    void testParser();
    void testStream();
};

void FlatFileExecStreamTest::testBuffer()
{
    FixedBuffer fixedBuffer[8];
    std::string path = "flatfile/buffer";

    SharedFlatFileBuffer pFileBuffer;
    pFileBuffer.reset(new FlatFileBuffer(path), ClosableObjectDestructor());
    pFileBuffer->open();
    pFileBuffer->setStorage((char *) fixedBuffer, (uint)8);

    checkRead(*pFileBuffer, "12345671");
    BOOST_CHECK_EQUAL(pFileBuffer->getReadPtr(), (char *)fixedBuffer);

    pFileBuffer->setReadPtr(pFileBuffer->getReadPtr() + 7);
    checkRead(*pFileBuffer, "12345676");

    pFileBuffer->setReadPtr(pFileBuffer->getReadPtr() + 6);
    checkRead(*pFileBuffer, "7654\n");
    BOOST_CHECK(pFileBuffer->isComplete());
}

void FlatFileExecStreamTest::testParser()
{
    FlatFileParser parser(',', '\n', '"', '"');

    checkTrim(parser, "", "");
    checkTrim(parser, "aRobin", "aRobin");
    checkTrim(parser, "   red breast in cage  ", "red breast in cage");

    checkStrip(parser, "", "");
    checkStrip(parser, "puts all", "puts all");
    checkStrip(parser, "\"heaven\"", "heaven");
    checkStrip(parser, "   \"in a\"  ", "in a");
    checkStrip(parser, "   \"\"\"rage\"\"\"  ", "\"rage\"");

    // quote a delimiter
    checkColumnScan(
        parser, "\"all that\n is \"gold, ",
        FlatFileColumnParseResult::FIELD_DELIM, 19, 20);

    // quotes are valid for char columns
    checkColumnScan(
        parser, "\"does not, glitter\"\n ",
        FlatFileColumnParseResult::ROW_DELIM, 19, 20);

    // embedded quotes
    checkColumnScan(
        parser, "\"not all those who \"\"wander\"\"\", ",
        FlatFileColumnParseResult::FIELD_DELIM, 30, 31);

    // ends in escape
    checkColumnScan(
        parser, " are lost  \"",
        FlatFileColumnParseResult::NO_DELIM, 12, 12);

    // imbalanced quote
    checkColumnScan(
        parser, "\"JRR, ",
        FlatFileColumnParseResult::NO_DELIM, 6, 6);

    // data after quote
    checkColumnScan(
        parser, "\"Tolkien\"  , ",
        FlatFileColumnParseResult::FIELD_DELIM, 11, 12);

    // fixed length exactly equal to buffer size
    checkColumnScan(
        parser, "some poems",
        FlatFileColumnParseResult::NO_DELIM, 10, 10);
}

void FlatFileExecStreamTest::checkRead(
    FlatFileBuffer &buffer,
    const char *string)
{
    uint size = strlen(string);
    buffer.read();
    BOOST_CHECK_EQUAL(buffer.getEndPtr() - buffer.getReadPtr(), size);
    BOOST_CHECK_EQUAL(strncmp(buffer.getReadPtr(), string, size), 0);
}

void FlatFileExecStreamTest::checkTrim(
    FlatFileParser &parser,
    const char *string,
    const char *result)
{
    char buffer[128];
    assert(strlen(string) < sizeof(buffer));
    strcpy(buffer, string);

    uint size = strlen(result);
    BOOST_CHECK_EQUAL(parser.trim(buffer, strlen(buffer)), size);
    BOOST_CHECK_EQUAL(strncmp(buffer, result, size), 0);
}

void FlatFileExecStreamTest::checkStrip(
    FlatFileParser &parser,
    const char *string,
    const char *result)
{
    char buffer[128];
    assert(strlen(string) < sizeof(buffer));
    strcpy(buffer, string);

    uint size = strlen(result);
    BOOST_CHECK_EQUAL(parser.stripQuoting(buffer, strlen(buffer), true), size);
    BOOST_CHECK_EQUAL(strncmp(buffer, result, size), 0);
}

void FlatFileExecStreamTest::checkColumnScan(
    FlatFileParser &parser,
    const char *string,
    FlatFileColumnParseResult::DelimiterType type,
    uint size,
    uint offset)
{
    char buffer[128];
    assert(strlen(string) < sizeof(buffer));
    strcpy(buffer, string);

    FlatFileColumnParseResult result;
    parser.scanColumn(buffer, strlen(buffer), sizeof(buffer), result);

    BOOST_CHECK_EQUAL(result.type, type);
    BOOST_CHECK_EQUAL(result.size, size);
    BOOST_CHECK_EQUAL(result.next, buffer + offset);
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
        pSegmentFactory->newScratchSegment(pCache, 1);
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
    std::vector<std::string> expected;
    expected.push_back("[ 'No one', 'travels' ]");
    expected.push_back("[ 'Along this way', 'but I,' ]");
    expected.push_back("[ 'This', 'autumn evening.' ]");
    verifyStringOutput(*pOutputStream, 3, expected);
}

FENNEL_UNIT_TEST_SUITE(FlatFileExecStreamTest);

// End FlatFileExecStreamTest.cpp
