/*
// Licensed to DynamoBI Corporation (DynamoBI) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  DynamoBI licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at

//   http://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
*/

#include "fennel/common/CommonPreamble.h"
#include "fennel/lbm/LbmRidReader.h"
#include "fennel/lbm/LbmTupleReader.h"
#include "fennel/ldbtest/LbmExecStreamTestBase.h"

#include <boost/test/test_tools.hpp>

using namespace fennel;

/**
 * Test for bitmap tuple and rid reader classes. Currently a placeholder.
 */
class LbmReaderTest : public LbmExecStreamTestBase
{
public:
    explicit LbmReaderTest()
    {
        FENNEL_UNIT_TEST_CASE(LbmReaderTest, testSingleTupleReader);
    }

    void testSingleTupleReader();
};

void LbmReaderTest::testSingleTupleReader()
{
    LcsRid rid = LcsRid(123);
    bitmapTupleData[0].pData = (PConstBuffer) &rid;

    LbmSingleTupleReader tupleReader;
    tupleReader.init(bitmapTupleData);

    uint nRows = 0;
    while (true) {
        TupleData *pTuple;
        ExecStreamResult rc = tupleReader.read(pTuple);
        if (rc == EXECRC_EOS) {
            break;
        }
        BOOST_CHECK_EQUAL(rc, EXECRC_YIELD);
        int cmp = bitmapTupleDesc.compareTuples(bitmapTupleData, *pTuple);
        BOOST_CHECK_EQUAL(cmp, 0);
        nRows++;
    }
    BOOST_CHECK_EQUAL(nRows, 1);

    LbmTupleRidReader ridReader;
    ridReader.init(bitmapTupleData);
    BOOST_CHECK_EQUAL(ridReader.hasNext(), true);
    BOOST_CHECK_EQUAL(ridReader.peek(), LcsRid(123));
    ridReader.advance();
    BOOST_CHECK_EQUAL(ridReader.hasNext(), false);

    ridReader.init(bitmapTupleData);
    BOOST_CHECK_EQUAL(ridReader.hasNext(), true);
    BOOST_CHECK_EQUAL(ridReader.getNext(), LcsRid(123));
    BOOST_CHECK_EQUAL(ridReader.hasNext(), false);
}

FENNEL_UNIT_TEST_SUITE(LbmReaderTest);

// End LbmReaderTest.cpp
