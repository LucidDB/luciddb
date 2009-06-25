/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005-2009 The Eigenbase Project
// Copyright (C) 2009-2009 SQLstream, Inc.
// Copyright (C) 2004-2009 LucidEra, Inc.
// Portions Copyright (C) 2004-2009 John V. Sichi
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
#include "fennel/sorter/ExternalSortExecStream.h"
#include "fennel/tuple/StandardTypeDescriptor.h"
#include "fennel/exec/MockProducerExecStream.h"
#include "fennel/exec/ExecStreamEmbryo.h"
#include "fennel/exec/ExecStreamScheduler.h"
#include "fennel/exec/ExecStreamGraph.h"
#include "fennel/cache/Cache.h"

#include <boost/test/test_tools.hpp>

using namespace fennel;

class ExternalSortExecStreamTest : public ExecStreamUnitTestBase
{
    void testImpl(
        uint nRows,
        SharedMockProducerExecStreamGenerator pGenerator,
        MockProducerExecStreamGenerator &verifier,
        bool storeFinalRun = false,
        bool stopEarly = false,
        bool desc = false);

public:
    explicit ExternalSortExecStreamTest()
    {
        FENNEL_UNIT_TEST_CASE(
            ExternalSortExecStreamTest, testPresortedInMem);
        FENNEL_UNIT_TEST_CASE(
            ExternalSortExecStreamTest, testPresortedExternal);
        FENNEL_UNIT_TEST_CASE(
            ExternalSortExecStreamTest, testRandomInMem);
        FENNEL_UNIT_TEST_CASE(
            ExternalSortExecStreamTest, testRandomDescInMem);
        FENNEL_UNIT_TEST_CASE(
            ExternalSortExecStreamTest, testRandomExternal);
        FENNEL_UNIT_TEST_CASE(
            ExternalSortExecStreamTest, testRandomExternalStoreFinal);
        FENNEL_UNIT_TEST_CASE(
            ExternalSortExecStreamTest, testRandomExternalFault);
    }

    void testPresortedInMem();
    void testPresortedExternal();
    void testRandomInMem();
    void testRandomDescInMem();
    void testRandomExternal();
    void testRandomExternalStoreFinal();
    void testRandomExternalFault();

    virtual void testCaseSetUp();
};

void ExternalSortExecStreamTest::testCaseSetUp()
{
    ExecStreamUnitTestBase::testCaseSetUp();

    // lower the cache availability to force external sorts for
    // the larger datasets
    ExecStreamResourceQuantity quantity;
    quantity.nCachePages = 10;
    pResourceGovernor->setResourceAvailability(
        quantity, EXEC_RESOURCE_CACHE_PAGES);

    ExecStreamResourceKnobs knob;
    knob.expectedConcurrentStatements = 1;
    pResourceGovernor->setResourceKnob(
        knob, EXEC_KNOB_EXPECTED_CONCURRENT_STATEMENTS);
}

void ExternalSortExecStreamTest::testRandomInMem()
{
    SharedMockProducerExecStreamGenerator pGenerator(
        new PermutationGenerator(100));
    RampExecStreamGenerator verifier;
    testImpl(100, pGenerator, verifier);
}

void ExternalSortExecStreamTest::testRandomDescInMem()
{
    SharedMockProducerExecStreamGenerator pGenerator(
        new PermutationGenerator(100));
    std::vector< boost::shared_ptr<ColumnGenerator<int64_t> > > colGens;
    colGens.push_back(
        boost::shared_ptr< ColumnGenerator<int64_t> >(
            new SeqColumnGenerator(99, -1)));
    CompositeExecStreamGenerator verifier(colGens);
    testImpl(100, pGenerator, verifier, false, false, true);
}

void ExternalSortExecStreamTest::testRandomExternal()
{
    SharedMockProducerExecStreamGenerator pGenerator(
        new PermutationGenerator(10000));
    RampExecStreamGenerator verifier;
    testImpl(10000, pGenerator, verifier);
}

void ExternalSortExecStreamTest::testRandomExternalStoreFinal()
{
    SharedMockProducerExecStreamGenerator pGenerator(
        new PermutationGenerator(10000));
    RampExecStreamGenerator verifier;
    testImpl(10000, pGenerator, verifier, true);
}

void ExternalSortExecStreamTest::testRandomExternalFault()
{
    SharedMockProducerExecStreamGenerator pGenerator(
        new PermutationGenerator(10000));
    RampExecStreamGenerator verifier;
    // only read half the result set, and then abort
    testImpl(10000, pGenerator, verifier, true, true);
}

void ExternalSortExecStreamTest::testPresortedInMem()
{
    SharedMockProducerExecStreamGenerator pGenerator(
        new RampExecStreamGenerator());
    testImpl(100, pGenerator, *pGenerator);
}

void ExternalSortExecStreamTest::testPresortedExternal()
{
    SharedMockProducerExecStreamGenerator pGenerator(
        new RampExecStreamGenerator());
    testImpl(10000, pGenerator, *pGenerator);
}

void ExternalSortExecStreamTest::testImpl(
    uint nRows,
    SharedMockProducerExecStreamGenerator pGenerator,
    MockProducerExecStreamGenerator &verifier,
    bool storeFinalRun,
    bool stopEarly,
    bool desc)
{
    StandardTypeDescriptorFactory stdTypeFactory;
    TupleAttributeDescriptor attrDesc(
        stdTypeFactory.newDataType(STANDARD_TYPE_INT_64));

    MockProducerExecStreamParams mockParams;
    mockParams.outputTupleDesc.push_back(attrDesc);
    mockParams.nRows = nRows;
    mockParams.pGenerator = pGenerator;

    ExecStreamEmbryo mockStreamEmbryo;
    mockStreamEmbryo.init(new MockProducerExecStream(), mockParams);
    mockStreamEmbryo.getStream()->setName("MockProducerExecStream");

    ExternalSortExecStreamParams sortParams;
    sortParams.outputTupleDesc.push_back(attrDesc);
    sortParams.distinctness = DUP_ALLOW;
    sortParams.estimatedNumRows = nRows;
    sortParams.earlyClose = false;
    sortParams.pTempSegment = pRandomSegment;
    sortParams.pCacheAccessor = pCache;
    // 10 total cache pages, 5% in reserve ==> 9 scratch pages per stream graph
    sortParams.scratchAccessor =
        pSegmentFactory->newScratchSegment(pCache, 9);
    sortParams.keyProj.push_back(0);
    sortParams.storeFinalRun = storeFinalRun;
    sortParams.descendingKeyColumns.push_back(desc);

    ExecStreamEmbryo sortStreamEmbryo;
    sortStreamEmbryo.init(
        ExternalSortExecStream::newExternalSortExecStream(), sortParams);
    sortStreamEmbryo.getStream()->setName("ExternalSortExecStream");

    SharedExecStream pOutputStream = prepareTransformGraph(
        mockStreamEmbryo, sortStreamEmbryo);

    verifyOutput(
        *pOutputStream,
        stopEarly ? (mockParams.nRows / 2) : mockParams.nRows,
        verifier,
        stopEarly);

    if (stopEarly) {
        // simulate error cleanup
        pScheduler->stop();
        pGraph->close();
    }

    BOOST_CHECK_EQUAL(0, pRandomSegment->getAllocatedSizeInPages());
}

FENNEL_UNIT_TEST_SUITE(ExternalSortExecStreamTest);

// End ExternalSortExecStreamTest.cpp
