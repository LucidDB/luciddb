/*
// $Id$
// Fennel is a relational database kernel.
// Copyright (C) 2004-2004 John V. Sichi.
//
// This program is free software; you can redistribute it and/or
// modify it under the terms of the GNU Lesser General Public License
// as published by the Free Software Foundation; either version 2.1
// of the License, or (at your option) any later version.
// 
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Lesser General Public License for more details.
// 
// You should have received a copy of the GNU Lesser General Public License
// along with this program; if not, write to the Free Software
// Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
*/

#include "fennel/common/CommonPreamble.h"
#include "fennel/exec/DfsTreeExecStreamScheduler.h"
#include "fennel/exec/ExecStream.h"
#include "fennel/exec/ExecStreamGraphImpl.h"
#include "fennel/exec/ExecStreamBufAccessor.h"
#include "fennel/exec/MockProducerStream.h"
#include "fennel/exec/ScratchBufferStream.h"
#include "fennel/test/SegStorageTestBase.h"
#include "fennel/tuple/StandardTypeDescriptor.h"

#include <boost/test/test_tools.hpp>

using namespace fennel;

class SchedulerTest : virtual public SegStorageTestBase
{
    SharedExecStreamScheduler pScheduler;
    SharedExecStreamGraph pGraph;
    
public:
    explicit SchedulerTest()
    {
        FENNEL_UNIT_TEST_CASE(SchedulerTest,testDfsTree);
    }

    void testDfsTree();
    
    virtual void testCaseSetUp();
    virtual void testCaseTearDown();
};

void SchedulerTest::testDfsTree()
{
    pGraph.reset(new ExecStreamGraphImpl(),ClosableObjectDestructor());
    pScheduler.reset(new DfsTreeExecStreamScheduler(pGraph));

    SharedExecStream pStream1(new MockProducerStream());
    pStream1->setName("MockProducerStream");
    
    SharedExecStream pStream2(new ScratchBufferStream());
    pStream2->setName("ScratchBufferingStream");

    pGraph->addStream(pStream1);
    pGraph->addStream(pStream2);
    pGraph->addDataflow(
        pStream1->getStreamId(),
        pStream2->getStreamId());
    pGraph->addOutputDataflow(
        pStream2->getStreamId());
    pGraph->prepare(*pScheduler);
    
    StandardTypeDescriptorFactory stdTypeFactory;
    TupleAttributeDescriptor attrDesc(
        stdTypeFactory.newDataType(STANDARD_TYPE_INT_32));
    
    MockProducerStreamParams params1;
    params1.outputTupleDesc.push_back(attrDesc);
    params1.nRows = 100000;     // at least one page
    params1.enforceQuotas = true;
    static_cast<MockProducerStream *>(pStream1.get())->prepare(params1);
    
    ExecStreamParams params2;
    params2.scratchAccessor = pSegmentFactory->newScratchSegment(pCache,1);
    params2.enforceQuotas = true;
    pStream2->prepare(params2);

    pGraph->setScratchSegment(params2.scratchAccessor.pSegment);
    
    pGraph->open();
    pScheduler->start();

    uint nBytes = 0;
    for (;;) {
        ExecStreamBufAccessor &bufAccessor =
            pScheduler->readStream(*pStream2);
        if (bufAccessor.getState() == EXECBUF_EOS) {
            break;
        }
        BOOST_CHECK_EQUAL(EXECBUF_NEED_CONSUMPTION,bufAccessor.getState());
        nBytes += bufAccessor.getConsumptionAvailable();
        bufAccessor.consumeData(bufAccessor.getConsumptionEnd());
    }
    BOOST_CHECK_EQUAL(params1.nRows*sizeof(int32_t),nBytes);
}

void SchedulerTest::testCaseSetUp()
{
    SegStorageTestBase::testCaseSetUp();
    openStorage(DeviceMode::createNew);
}

void SchedulerTest::testCaseTearDown()
{
    if (pScheduler) {
        pScheduler->stop();
    }
    pGraph.reset();
    pScheduler.reset();
    SegStorageTestBase::testCaseTearDown();
}

FENNEL_UNIT_TEST_SUITE(SchedulerTest);

// End SchedulerTest.cpp
