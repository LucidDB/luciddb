/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2005-2005 Disruptive Tech
// Copyright (C) 2005-2005 LucidEra, Inc.
// Portions Copyright (C) 2004-2005 John V. Sichi
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
#include "fennel/segment/ScratchMemExcn.h"
#include "fennel/exec/ExecStreamScheduler.h"
#include "fennel/exec/ExecStream.h"
#include "fennel/exec/ExecStreamGraph.h"
#include "fennel/exec/ExecStreamBufAccessor.h"
#include "fennel/exec/ExecStreamGovernor.h"
#include "fennel/exec/MockResourceExecStream.h"
#include "fennel/exec/BarrierExecStream.h"
#include "fennel/exec/ExecStreamEmbryo.h"
#include "fennel/tuple/StandardTypeDescriptor.h"

#include <boost/test/test_tools.hpp>

using namespace fennel;

/**
 * Testcase for the exec stream resource governor
 */
class ExecStreamGovernorTest : public ExecStreamUnitTestBase
{
    /**
     * Creates a confluence stream consisting of nProducer producers that
     * feed into a barrier stream.  Each producer takes as input min and opt
     * resource requirements, the setting type of the opt resource requirement,
     * and the expected resources to be allocated to each.
     *
     * @param nProducers number of producers
     * @param minReqts min resource requirements for each producer
     * @param optReqts opt resource requirements for each producer
     * @param optTypes opt resource setting type for each producer
     * @param expected expected resources that the resource governor will
     * allocate to each producer 
     * @param exception true if the test is expected to return an exception
     * indicating that scratch pages have been exhausted; default is false
     */
    void testGovernor(
        uint nProducers,
        std::vector<ExecStreamResourceQuantity> const &minReqts,
        std::vector<ExecStreamResourceQuantity> const &optReqts,
        std::vector<ExecStreamResourceSettingType> optTypes,
        std::vector<ExecStreamResourceQuantity> expected,
        bool exception = false);

public:
    explicit ExecStreamGovernorTest()
    {
        FENNEL_UNIT_TEST_CASE(ExecStreamGovernorTest, testOptLessAccurate);
        FENNEL_UNIT_TEST_CASE(ExecStreamGovernorTest, testOptLessEstimate);
        FENNEL_UNIT_TEST_CASE(ExecStreamGovernorTest, testOptEqualEstimate);
        FENNEL_UNIT_TEST_CASE(ExecStreamGovernorTest, testInBetween);
        FENNEL_UNIT_TEST_CASE(
            ExecStreamGovernorTest, testMinEqualAllocation);
        FENNEL_UNIT_TEST_CASE(
            ExecStreamGovernorTest, testMinGreaterAllocation);
        FENNEL_UNIT_TEST_CASE(
            ExecStreamGovernorTest, testMinGreaterAvailable);
        FENNEL_UNIT_TEST_CASE(ExecStreamGovernorTest, testReturnResources);
    }

    void testOptLessAccurate();
    void testOptLessEstimate();
    void testOptEqualEstimate();
    void testInBetween();
    void testMinGreaterAllocation();
    void testMinEqualAllocation();
    void testMinGreaterAvailable();
    void testReturnResources();

    virtual void testCaseSetUp();
};

void ExecStreamGovernorTest::testCaseSetUp()
{
    ExecStreamUnitTestBase::testCaseSetUp();

    // lower the total cache availability so the perGraphAllocation is 95
    ExecStreamResourceQuantity quantity;
    quantity.nCachePages = 100;
    pResourceGovernor->setResourceAvailability(
        quantity, EXEC_RESOURCE_CACHE_PAGES);

    ExecStreamResourceKnobs knob;
    knob.expectedConcurrentStatements = 1;
    pResourceGovernor->setResourceKnob(
        knob, EXEC_KNOB_EXPECTED_CONCURRENT_STATEMENTS);
}

/**
 * 2 streams; total optimum of streams < perGraphAllocation; both
 * streams have EXEC_RESOURCE_ACCURATE optimum settings so the optimum amount
 * will be allocated
 */
void ExecStreamGovernorTest::testOptLessAccurate()
{
    uint nProducers = 2;
    std::vector<ExecStreamResourceQuantity> minReqts;
    std::vector<ExecStreamResourceQuantity> optReqts;
    std::vector<ExecStreamResourceQuantity> expected;
    std::vector<ExecStreamResourceSettingType> optTypes;

    ExecStreamResourceQuantity quantity;
    ExecStreamResourceSettingType optType;

    // producer 1 - min=10, opt=15, optType=EXEC_RESOURCE_ACCURATE, expected=15
    quantity.nCachePages = 10;
    minReqts.push_back(quantity);
    quantity.nCachePages = 15;
    optReqts.push_back(quantity);
    expected.push_back(quantity);
    optType = EXEC_RESOURCE_ACCURATE;
    optTypes.push_back(optType);
    
    // producer 2 - min=20, opt=40, optType=EXEC_RESOURCE_ACCURATE, expected=40
    quantity.nCachePages = 20;
    minReqts.push_back(quantity);
    quantity.nCachePages = 40;
    optReqts.push_back(quantity);
    expected.push_back(quantity);
    optType = EXEC_RESOURCE_ACCURATE;
    optTypes.push_back(optType);
    
    testGovernor(nProducers, minReqts, optReqts, optTypes, expected);
}

/**
 * 4 streams -- total optimum < perGraphAllocation; but one stream has an
 * EXEC_RESOURCE_ESTIMATE optimum setting
 */
void ExecStreamGovernorTest::testOptLessEstimate()
{
    uint nProducers = 4;
    std::vector<ExecStreamResourceQuantity> minReqts;
    std::vector<ExecStreamResourceQuantity> optReqts;
    std::vector<ExecStreamResourceQuantity> expected;
    std::vector<ExecStreamResourceSettingType> optTypes;

    ExecStreamResourceQuantity quantity;
    ExecStreamResourceSettingType optType;

    // total opt is 80, but since one stream has an estimate setting, we
    // can allocate up to an excess of 15 to that stream

    // producer 1 - min=10, opt=11, optType=EXEC_RESOURCE_ACCURATE, expected=11
    quantity.nCachePages = 10;
    minReqts.push_back(quantity);
    quantity.nCachePages = 11;
    optReqts.push_back(quantity);
    expected.push_back(quantity);
    optType = EXEC_RESOURCE_ACCURATE;
    optTypes.push_back(optType);
    
    // producer 2 - min=15, opt=17, optType=EXEC_RESOURCE_ACCURATE, expected=17
    quantity.nCachePages = 15;
    minReqts.push_back(quantity);
    quantity.nCachePages = 17;
    optReqts.push_back(quantity);
    expected.push_back(quantity);
    optType = EXEC_RESOURCE_ACCURATE;
    optTypes.push_back(optType);
    
    // producer 3 - min=20, opt=23, optType=EXEC_RESOURCE_ESTIMATE, expected=38
    quantity.nCachePages = 20;
    minReqts.push_back(quantity);
    quantity.nCachePages = 23;
    optReqts.push_back(quantity);
    quantity.nCachePages = 38;
    expected.push_back(quantity);
    optType = EXEC_RESOURCE_ESTIMATE;
    optTypes.push_back(optType);

    // producer 4 - min=25, opt=29, optType=EXEC_RESOURCE_ACCURATE, expected=29
    quantity.nCachePages = 25;
    minReqts.push_back(quantity);
    quantity.nCachePages = 29;
    optReqts.push_back(quantity);
    expected.push_back(quantity);
    optType = EXEC_RESOURCE_ACCURATE;
    optTypes.push_back(optType);

    testGovernor(nProducers, minReqts, optReqts, optTypes, expected);
}

/**
 * 4 streams -- total opt = perGraphAllocation; 2 streams with
 * estimate optimum settings
 */
void ExecStreamGovernorTest::testOptEqualEstimate()
{
    uint nProducers = 4;
    std::vector<ExecStreamResourceQuantity> minReqts;
    std::vector<ExecStreamResourceQuantity> optReqts;
    std::vector<ExecStreamResourceQuantity> expected;
    std::vector<ExecStreamResourceSettingType> optTypes;

    ExecStreamResourceQuantity quantity;
    ExecStreamResourceSettingType optType;

    // total opt is 95 with two streams with estimate settings; all
    // streams will be allocated their optimum

    // producer 1 - min=10, opt=20, optType=EXEC_RESOURCE_ESTIMATE, expected=20
    quantity.nCachePages = 10;
    minReqts.push_back(quantity);
    quantity.nCachePages = 20;
    optReqts.push_back(quantity);
    expected.push_back(quantity);
    optType = EXEC_RESOURCE_ESTIMATE;
    optTypes.push_back(optType);
    
    // producer 2 - min=15, opt=17, optType=EXEC_RESOURCE_ACCURATE, expected=17
    quantity.nCachePages = 15;
    minReqts.push_back(quantity);
    quantity.nCachePages = 17;
    optReqts.push_back(quantity);
    expected.push_back(quantity);
    optType = EXEC_RESOURCE_ACCURATE;
    optTypes.push_back(optType);
    
    // producer 3 - min=20, opt=23, optType=EXEC_RESOURCE_ACCURATE, expected=23
    quantity.nCachePages = 20;
    minReqts.push_back(quantity);
    quantity.nCachePages = 23;
    optReqts.push_back(quantity);
    expected.push_back(quantity);
    optType = EXEC_RESOURCE_ACCURATE;
    optTypes.push_back(optType);

    // producer 4 - min=25, opt=35, optType=EXEC_RESOURCE_ESTIMATE, expected=35
    quantity.nCachePages = 25;
    minReqts.push_back(quantity);
    quantity.nCachePages = 35;
    optReqts.push_back(quantity);
    expected.push_back(quantity);
    optType = EXEC_RESOURCE_ESTIMATE;
    optTypes.push_back(optType);

    testGovernor(nProducers, minReqts, optReqts, optTypes, expected);
}

/**
 * 4 streams -- total min < perGraphAllocation < total opt; streams have
 * a mix of different optimum settings
 */
void ExecStreamGovernorTest::testInBetween()
{
    uint nProducers = 4;
    std::vector<ExecStreamResourceQuantity> minReqts;
    std::vector<ExecStreamResourceQuantity> optReqts;
    std::vector<ExecStreamResourceQuantity> expected;
    std::vector<ExecStreamResourceSettingType> optTypes;

    ExecStreamResourceQuantity quantity;
    ExecStreamResourceSettingType optType;

    // total min is 70; each stream will be assigned their min and an
    // excess of 25 will be divided across the streams

    // producer 1 - min=10, opt=25, optType=EXEC_RESOURCE_ACCURATE, expected=14
    quantity.nCachePages = 10;
    minReqts.push_back(quantity);
    quantity.nCachePages = 25;
    optReqts.push_back(quantity);
    quantity.nCachePages = 14;
    expected.push_back(quantity);
    optType = EXEC_RESOURCE_ACCURATE;
    optTypes.push_back(optType);
    
    // producer 2 - min=15, opt=31, optType=EXEC_RESOURCE_ESTIMATE, expected=19
    quantity.nCachePages = 15;
    minReqts.push_back(quantity);
    quantity.nCachePages = 31;
    optReqts.push_back(quantity);
    quantity.nCachePages = 19;
    expected.push_back(quantity);
    optType = EXEC_RESOURCE_ESTIMATE;
    optTypes.push_back(optType);
    
    // producer 3 - min=20, opt=0, optType=EXEC_RESOURCE_UNBOUNDED, expected=31
    quantity.nCachePages = 20;
    minReqts.push_back(quantity);
    quantity.nCachePages = 0;
    optReqts.push_back(quantity);
    quantity.nCachePages = 31;
    expected.push_back(quantity);
    optType = EXEC_RESOURCE_UNBOUNDED;
    optTypes.push_back(optType);

    // producer 4 - min=25, opt=42, optType=EXEC_RESOURCE_ESTIMATE, expected=29
    quantity.nCachePages = 25;
    minReqts.push_back(quantity);
    quantity.nCachePages = 42;
    optReqts.push_back(quantity);
    quantity.nCachePages = 29;
    expected.push_back(quantity);
    optType = EXEC_RESOURCE_ESTIMATE;
    optTypes.push_back(optType);

    testGovernor(nProducers, minReqts, optReqts, optTypes, expected);
}

/**
 * 2 streams; total min of streams > perGraphAllocation but less than the
 * amount available; so it should be possible to allocate the minimum
 */
void ExecStreamGovernorTest::testMinGreaterAllocation()
{
    // since this stream will be asking for more than the perGraphAllocation,
    // we need to allow for at least 2 stream graphs
    ExecStreamResourceQuantity quantity;
    quantity.nCachePages = 200;
    pResourceGovernor->setResourceAvailability(
        quantity, EXEC_RESOURCE_CACHE_PAGES);
    ExecStreamResourceKnobs knob;
    knob.expectedConcurrentStatements = 2;
    pResourceGovernor->setResourceKnob(
        knob, EXEC_KNOB_EXPECTED_CONCURRENT_STATEMENTS);

    uint nProducers = 2;
    std::vector<ExecStreamResourceQuantity> minReqts;
    std::vector<ExecStreamResourceQuantity> optReqts;
    std::vector<ExecStreamResourceQuantity> expected;
    std::vector<ExecStreamResourceSettingType> optTypes;

    ExecStreamResourceSettingType optType;

    // producer 1 - min=50, opt=50, optType=EXEC_RESOURCE_ACCURATE, expected=50
    quantity.nCachePages = 50;
    minReqts.push_back(quantity);
    optReqts.push_back(quantity);
    expected.push_back(quantity);
    optType = EXEC_RESOURCE_ACCURATE;
    optTypes.push_back(optType);
    
    // producer 2 - min=55, opt=55, optType=EXEC_RESOURCE_ACCURATE, expected=55
    quantity.nCachePages = 55;
    minReqts.push_back(quantity);
    optReqts.push_back(quantity);
    expected.push_back(quantity);
    optType = EXEC_RESOURCE_ACCURATE;
    optTypes.push_back(optType);
    
    testGovernor(nProducers, minReqts, optReqts, optTypes, expected);
}

/**
 * 2 streams; total min of streams = perGraphAllocation; so min is
 * assigned
 */
void ExecStreamGovernorTest::testMinEqualAllocation()
{
    uint nProducers = 2;
    std::vector<ExecStreamResourceQuantity> minReqts;
    std::vector<ExecStreamResourceQuantity> optReqts;
    std::vector<ExecStreamResourceQuantity> expected;
    std::vector<ExecStreamResourceSettingType> optTypes;

    ExecStreamResourceQuantity quantity;
    ExecStreamResourceSettingType optType;

    // producer 1 - min=50, opt=60, optType=EXEC_RESOURCE_ACCURATE,
    // expected=50
    quantity.nCachePages = 50;
    minReqts.push_back(quantity);
    quantity.nCachePages = 60;
    optReqts.push_back(quantity);
    quantity.nCachePages = 50;
    expected.push_back(quantity);
    optType = EXEC_RESOURCE_ACCURATE;
    optTypes.push_back(optType);
    
    // producer 2 - min=45, opt=50, optType=EXEC_RESOURCE_ACCURATE, expected=45
    quantity.nCachePages = 45;
    minReqts.push_back(quantity);
    quantity.nCachePages = 50;
    optReqts.push_back(quantity);
    quantity.nCachePages = 45;
    expected.push_back(quantity);
    optType = EXEC_RESOURCE_ACCURATE;
    optTypes.push_back(optType);
    
    testGovernor(nProducers, minReqts, optReqts, optTypes, expected);
}

/**
 * 2 streams; total min of streams > perGraphAllocation and also greater
 * than the amount available; so an exception should be returned
 */
void ExecStreamGovernorTest::testMinGreaterAvailable()
{
    uint nProducers = 2;
    std::vector<ExecStreamResourceQuantity> minReqts;
    std::vector<ExecStreamResourceQuantity> optReqts;
    std::vector<ExecStreamResourceQuantity> expected;
    std::vector<ExecStreamResourceSettingType> optTypes;

    ExecStreamResourceQuantity quantity;
    ExecStreamResourceSettingType optType;

    // producer 1 - min=50, opt=50, optType=EXEC_RESOURCE_ACCURATE,
    // expected=50
    quantity.nCachePages = 50;
    minReqts.push_back(quantity);
    optReqts.push_back(quantity);
    expected.push_back(quantity);
    optType = EXEC_RESOURCE_ACCURATE;
    optTypes.push_back(optType);
    
    // producer 2 - min=46, opt=46, optType=EXEC_RESOURCE_ACCURATE, expected=46
    quantity.nCachePages = 46;
    minReqts.push_back(quantity);
    optReqts.push_back(quantity);
    expected.push_back(quantity);
    optType = EXEC_RESOURCE_ACCURATE;
    optTypes.push_back(optType);
    
    testGovernor(nProducers, minReqts, optReqts, optTypes, expected, true);
}

/**
 * Execute two stream graphs in sequence.  The total min of the first is 90
 * and the second is 95.  Both should succeed, provided the resources used
 * by the first are successfully returned.
 */
void ExecStreamGovernorTest::testReturnResources()
{
    uint nProducers = 2;
    std::vector<ExecStreamResourceQuantity> minReqts;
    std::vector<ExecStreamResourceQuantity> optReqts;
    std::vector<ExecStreamResourceQuantity> expected;
    std::vector<ExecStreamResourceSettingType> optTypes;

    ExecStreamResourceQuantity quantity;
    ExecStreamResourceSettingType optType;

    // producer 1 - min=45, opt=45, optType=EXEC_RESOURCE_ACCURATE, expected=45
    quantity.nCachePages = 45;
    minReqts.push_back(quantity);
    optReqts.push_back(quantity);
    expected.push_back(quantity);
    optType = EXEC_RESOURCE_ACCURATE;
    optTypes.push_back(optType);
    
    // producer 2 - min=45, opt=45, optType=EXEC_RESOURCE_ACCURATE, expected=45
    quantity.nCachePages = 45;
    minReqts.push_back(quantity);
    optReqts.push_back(quantity);
    expected.push_back(quantity);
    optType = EXEC_RESOURCE_ACCURATE;
    optTypes.push_back(optType);
    
    testGovernor(nProducers, minReqts, optReqts, optTypes, expected);

    resetExecStreamTest();
    minReqts.clear();
    optReqts.clear();
    expected.clear();
    optTypes.clear();

    // producer 1 - min=45, opt=45, optType=EXEC_RESOURCE_ACCURATE, expected=45
    quantity.nCachePages = 45;
    minReqts.push_back(quantity);
    optReqts.push_back(quantity);
    expected.push_back(quantity);
    optType = EXEC_RESOURCE_ACCURATE;
    optTypes.push_back(optType);
    
    // producer 2 - min=50, opt=50, optType=EXEC_RESOURCE_ACCURATE, expected=50
    quantity.nCachePages = 50;
    minReqts.push_back(quantity);
    optReqts.push_back(quantity);
    expected.push_back(quantity);
    optType = EXEC_RESOURCE_ACCURATE;
    optTypes.push_back(optType);
    
    testGovernor(nProducers, minReqts, optReqts, optTypes, expected);
}

void ExecStreamGovernorTest::testGovernor(
    uint nProducers,
    std::vector<ExecStreamResourceQuantity> const &minReqts,
    std::vector<ExecStreamResourceQuantity> const &optReqts,
    std::vector<ExecStreamResourceSettingType> optTypes,
    std::vector<ExecStreamResourceQuantity> expected,
    bool exception)
{
    StandardTypeDescriptorFactory stdTypeFactory;
    TupleAttributeDescriptor int8AttrDesc(
        stdTypeFactory.newDataType(STANDARD_TYPE_INT_8));

    std::vector<ExecStreamEmbryo> producerStreamEmbryos;
    for (uint i = 0; i < nProducers; i++) {
        MockResourceExecStreamParams producerParams;
        producerParams.minReqt = minReqts[i];
        producerParams.optReqt = optReqts[i];
        producerParams.optTypeInput = optTypes[i];
        producerParams.expected = expected[i];

        // limit the number of pages that scratchAccessor can allocate to
        // the number of pages that this stream is expected to allocate
        producerParams.scratchAccessor =
            pSegmentFactory->newScratchSegment(pCache, expected[i].nCachePages);
        producerParams.pCacheAccessor = pCache;
        producerParams.outputTupleDesc.push_back(int8AttrDesc);

        ExecStreamEmbryo producerStreamEmbryo;
        producerStreamEmbryo.init(
            new MockResourceExecStream(), producerParams);
        std::ostringstream oss;
        oss << "MockResourceExecStream" << "#" << i;
        producerStreamEmbryo.getStream()->setName(oss.str());
        producerStreamEmbryos.push_back(producerStreamEmbryo);
    }

    BarrierExecStreamParams barrierParams;
    barrierParams.outputTupleDesc.push_back(int8AttrDesc);
    barrierParams.returnMode = BARRIER_RET_ANY_INPUT;

    ExecStreamEmbryo barrierStreamEmbryo;
    barrierStreamEmbryo.init(new BarrierExecStream(), barrierParams);
    barrierStreamEmbryo.getStream()->setName("BarrierExecStream");

    SharedExecStream pOutputStream = prepareConfluenceGraph(
        producerStreamEmbryos, barrierStreamEmbryo);

    int8_t expectedOutput = 1;
    TupleData expectedTuple;
    expectedTuple.compute(barrierParams.outputTupleDesc);
    expectedTuple[0].pData = (PConstBuffer) &expectedOutput;

    // if the testcase expects an exception to be returned, then test for it
    try {
        verifyConstantOutput(*pOutputStream, expectedTuple, 1);
        if (exception) {
            BOOST_FAIL("Cache memory not exhausted");
        }
    } catch (FennelExcn &ex) {
        std::string errMsg = ex.getMessage();
        if (errMsg.compare(ScratchMemExcn().getMessage()) != 0) {
            BOOST_FAIL("Wrong exception returned");
        }
    }
}

FENNEL_UNIT_TEST_SUITE(ExecStreamGovernorTest);

// End ExecStreamGovernorTest.cpp
