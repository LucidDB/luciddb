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
#include "fennel/exec/ExecStreamGraphImpl.h"
#include "fennel/exec/ExecStream.h"
#include "fennel/exec/ScratchBufferStream.h"
#include "fennel/exec/ExecStreamEmbryo.h"
#include "fennel/exec/ExecStreamBufAccessor.h"
#include "fennel/exec/DfsTreeExecStreamScheduler.h"

#include <boost/test/test_tools.hpp>

FENNEL_BEGIN_CPPFILE("$Id$");

void ExecStreamTestBase::prepareGraphTwoStreams(
    SharedExecStream pSourceStream,
    SharedExecStream pOutputStream)
{
    pGraph->addStream(pSourceStream);
    pGraph->addStream(pOutputStream);
    pGraph->addDataflow(
        pSourceStream->getStreamId(),
        pOutputStream->getStreamId());
    pGraph->addOutputDataflow(
        pOutputStream->getStreamId());
    pGraph->prepare(*pScheduler);
    decorateGraph();
}

SharedExecStream ExecStreamTestBase::prepareGraphTwoBufferedStreams(
    ExecStreamEmbryo &sourceStreamEmbryo,
    ExecStreamEmbryo &transformStreamEmbryo)
{
    SharedExecStream pSourceStream = sourceStreamEmbryo.getStream();
    SharedExecStream pTransformStream = transformStreamEmbryo.getStream();
    
    ScratchBufferStream *pBufStreamImpl1 = new ScratchBufferStream();
    SharedExecStream pBufStream1(pBufStreamImpl1);
    pBufStream1->setName("ScratchBufferStream1");

    ScratchBufferStream *pBufStreamImpl2 = new ScratchBufferStream();
    SharedExecStream pBufStream2(pBufStreamImpl2);
    pBufStream2->setName("ScratchBufferStream2");
    
    ScratchBufferStreamParams paramsScratch;
    paramsScratch.scratchAccessor =
        pSegmentFactory->newScratchSegment(pCache,2);
    paramsScratch.enforceQuotas = false;
    
    pGraph->addStream(pSourceStream);
    pGraph->addStream(pBufStream1);
    pGraph->addStream(pTransformStream);
    pGraph->addStream(pBufStream2);

    pGraph->addDataflow(
        pSourceStream->getStreamId(),
        pBufStream1->getStreamId());
    pGraph->addDataflow(
        pBufStream1->getStreamId(),
        pTransformStream->getStreamId());
    pGraph->addDataflow(
        pTransformStream->getStreamId(),
        pBufStream2->getStreamId());
    pGraph->addOutputDataflow(
        pBufStream2->getStreamId());
    pGraph->prepare(*pScheduler);
    decorateGraph();

    sourceStreamEmbryo.prepareStream();
    pBufStreamImpl1->prepare(paramsScratch);
    transformStreamEmbryo.prepareStream();
    pBufStreamImpl2->prepare(paramsScratch);
    
    pGraph->setScratchSegment(paramsScratch.scratchAccessor.pSegment);

    return pBufStream2;
}

void ExecStreamTestBase::testCaseSetUp()
{
    SegStorageTestBase::testCaseSetUp();
    openStorage(DeviceMode::createNew);
    pGraph.reset(new ExecStreamGraphImpl(),ClosableObjectDestructor());
    pScheduler.reset(
        new DfsTreeExecStreamScheduler(
            this,
            "DfsTreeExecStreamScheduler"));
}

void ExecStreamTestBase::testCaseTearDown()
{
    if (pScheduler) {
        pScheduler->stop();
    }
    pGraph.reset();
    pScheduler.reset();
    SegStorageTestBase::testCaseTearDown();
}

void ExecStreamTestBase::decorateGraph()
{
    std::vector<SharedExecStream> streams = pGraph->getSortedStreams();
    for (uint i = 0; i < streams.size(); ++i) {
        streams[i]->initTraceSource(this, streams[i]->getName());
    }
    pScheduler->addGraph(pGraph);
}

void ExecStreamTestBase::verifyConstantOutput(
    ExecStream &stream,
    uint nBytesExpected,
    uint byteExpected)
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
            if (c != byteExpected) {
                BOOST_CHECK_EQUAL(byteExpected,c);
                break;
            }
        }
        bufAccessor.consumeData(bufAccessor.getConsumptionEnd());
    }
    BOOST_CHECK_EQUAL(nBytesExpected,nBytesTotal);
}

FENNEL_END_CPPFILE("$Id$");

// End ExecStreamTestBase.cpp
