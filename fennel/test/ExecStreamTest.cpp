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
#include "fennel/test/ExecStreamTestBase.h"
#include "fennel/exec/ExecStreamScheduler.h"
#include "fennel/exec/ExecStream.h"
#include "fennel/exec/ExecStreamGraph.h"
#include "fennel/exec/ExecStreamBufAccessor.h"
#include "fennel/exec/MockProducerStream.h"
#include "fennel/exec/ScratchBufferStream.h"
#include "fennel/exec/SegBufferStream.h"
#include "fennel/tuple/StandardTypeDescriptor.h"

#include <boost/test/test_tools.hpp>

using namespace fennel;

/**
 * ExecStreamTest tests various implementations of ExecStream.
 */
class ExecStreamTest : public ExecStreamTestBase
{
    void verifyZeroedOutput(ExecStream &stream,uint nBytesExpected);
    
public:
    explicit ExecStreamTest()
    {
        FENNEL_UNIT_TEST_CASE(ExecStreamTest,testScratchBufferStream);
        FENNEL_UNIT_TEST_CASE(ExecStreamTest,testSegBufferStream);
    }

    void testScratchBufferStream();
    void testSegBufferStream();
};

void ExecStreamTest::verifyZeroedOutput(
    ExecStream &stream,uint nBytesExpected)
{
    pGraph->open();
    pScheduler->start();
    uint nBytesTotal = 0;
    for (;;) {
        ExecStreamBufAccessor &bufAccessor =
            pScheduler->readStream(stream);
        if (bufAccessor.getState() == EXECBUF_EOS) {
            break;
        }
        BOOST_CHECK_EQUAL(EXECBUF_NEED_CONSUMPTION,bufAccessor.getState());
        uint nBytes = bufAccessor.getConsumptionAvailable();
        nBytesTotal += nBytes;
        for (uint i = 0; i < nBytes; ++i) {
            uint c = bufAccessor.getConsumptionStart()[i];
            if (c) {
                BOOST_CHECK_EQUAL(0,c);
                break;
            }
        }
        bufAccessor.consumeData(bufAccessor.getConsumptionEnd());
    }
    BOOST_CHECK_EQUAL(nBytesExpected,nBytesTotal);
}

void ExecStreamTest::testScratchBufferStream()
{
    MockProducerStream *pStreamImpl1 = new MockProducerStream();
    SharedExecStream pStream1(pStreamImpl1);
    pStream1->setName("MockProducerStream");
    
    ScratchBufferStream *pStreamImpl2 = new ScratchBufferStream();
    SharedExecStream pStream2(pStreamImpl2);
    pStream2->setName("ScratchBufferStream");

    prepareGraphTwoStreams(pStream1,pStream2);
    
    StandardTypeDescriptorFactory stdTypeFactory;
    TupleAttributeDescriptor attrDesc(
        stdTypeFactory.newDataType(STANDARD_TYPE_INT_32));
    
    MockProducerStreamParams params1;
    params1.outputTupleDesc.push_back(attrDesc);
    params1.nRows = 100000;     // at least one page
    params1.enforceQuotas = false;
    pStreamImpl1->prepare(params1);
    
    ExecStreamParams params2;
    params2.scratchAccessor =
        pSegmentFactory->newScratchSegment(pCache,1);
    params2.enforceQuotas = false;
    pStreamImpl2->prepare(params2);

    pGraph->setScratchSegment(params2.scratchAccessor.pSegment);
    
    verifyZeroedOutput(
        *pStream2,
        params1.nRows*sizeof(int32_t));
}

void ExecStreamTest::testSegBufferStream()
{
    MockProducerStream *pStreamImpl1 = new MockProducerStream();
    SharedExecStream pStream1(pStreamImpl1);
    pStream1->setName("MockProducerStream");
    
    SegBufferStream *pStreamImpl2 = new SegBufferStream();
    SharedExecStream pStream2(pStreamImpl2);
    pStream2->setName("SegBufferStream");

    prepareGraphTwoStreams(pStream1,pStream2);
    
    StandardTypeDescriptorFactory stdTypeFactory;
    TupleAttributeDescriptor attrDesc(
        stdTypeFactory.newDataType(STANDARD_TYPE_INT_32));
    
    MockProducerStreamParams params1;
    params1.outputTupleDesc.push_back(attrDesc);
    params1.nRows = 100000;     // at least one page
    params1.enforceQuotas = false;
    pStreamImpl1->prepare(params1);
    
    SegBufferStreamParams params2;
    params2.scratchAccessor.pSegment = pLinearSegment;
    params2.scratchAccessor.pCacheAccessor = pCache;
    params2.enforceQuotas = false;
    params2.multipass = false;
    pStreamImpl2->prepare(params2);

    verifyZeroedOutput(
        *pStream2,
        params1.nRows*sizeof(int32_t));
}

FENNEL_UNIT_TEST_SUITE(ExecStreamTest);

// End ExecStreamTest.cpp
