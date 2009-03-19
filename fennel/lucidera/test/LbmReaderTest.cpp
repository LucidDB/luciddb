/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005-2009 LucidEra, Inc.
// Copyright (C) 2005-2009 The Eigenbase Project
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
#include "fennel/lucidera/bitmap/LbmRidReader.h"
#include "fennel/lucidera/bitmap/LbmTupleReader.h"
#include "fennel/lucidera/test/LbmExecStreamTestBase.h"

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
