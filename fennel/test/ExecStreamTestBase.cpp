/*
// $Id$
// Fennel is a relational database kernel.
// Copyright (C) 2004-2004 John V. Sichi
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
#include "fennel/exec/ExecStreamGraph.h"
#include "fennel/exec/ExecStreamGraphEmbryo.h"
#include "fennel/exec/ExecStream.h"
#include "fennel/exec/ScratchBufferExecStream.h"
#include "fennel/exec/ExecStreamEmbryo.h"
#include "fennel/exec/ExecStreamBufAccessor.h"
#include "fennel/exec/DfsTreeExecStreamScheduler.h"
#include "fennel/exec/MockProducerExecStream.h"
#include "fennel/tuple/TuplePrinter.h"

#include <boost/test/test_tools.hpp>

FENNEL_BEGIN_CPPFILE("$Id$");

SharedExecStream ExecStreamTestBase::prepareTransformGraph(
    ExecStreamEmbryo &sourceStreamEmbryo,
    ExecStreamEmbryo &transformStreamEmbryo)
{
    pGraphEmbryo->saveStreamEmbryo(sourceStreamEmbryo);
    pGraphEmbryo->saveStreamEmbryo(transformStreamEmbryo);
    
    pGraphEmbryo->addDataflow(
        sourceStreamEmbryo.getStream()->getName(),
        transformStreamEmbryo.getStream()->getName());

    SharedExecStream pAdaptedStream =
        pGraphEmbryo->addAdapterFor(
            transformStreamEmbryo.getStream()->getName(),
            BUFPROV_PRODUCER);
    pGraph->addOutputDataflow(
        pAdaptedStream->getStreamId());

    pGraphEmbryo->prepareGraph(this, "");

    return pAdaptedStream;
}

SharedExecStream ExecStreamTestBase::prepareConfluenceGraph(
    ExecStreamEmbryo &sourceStreamEmbryo1,
    ExecStreamEmbryo &sourceStreamEmbryo2,
    ExecStreamEmbryo &confluenceStreamEmbryo)
{
    pGraphEmbryo->saveStreamEmbryo(sourceStreamEmbryo1);
    pGraphEmbryo->saveStreamEmbryo(sourceStreamEmbryo2);
    pGraphEmbryo->saveStreamEmbryo(confluenceStreamEmbryo);
    
    pGraphEmbryo->addDataflow(
        sourceStreamEmbryo1.getStream()->getName(),
        confluenceStreamEmbryo.getStream()->getName());

    pGraphEmbryo->addDataflow(
        sourceStreamEmbryo2.getStream()->getName(),
        confluenceStreamEmbryo.getStream()->getName());

    SharedExecStream pAdaptedStream =
        pGraphEmbryo->addAdapterFor(
            confluenceStreamEmbryo.getStream()->getName(),
            BUFPROV_PRODUCER);
    pGraph->addOutputDataflow(
        pAdaptedStream->getStreamId());

    pGraphEmbryo->prepareGraph(this, "");

    return pAdaptedStream;
}

void ExecStreamTestBase::testCaseSetUp()
{
    SegStorageTestBase::testCaseSetUp();
    openStorage(DeviceMode::createNew);
    pGraph = ExecStreamGraph::newExecStreamGraph();
    pScheduler.reset(
        new DfsTreeExecStreamScheduler(
            this,
            "DfsTreeExecStreamScheduler"));
    pGraphEmbryo.reset(
        new ExecStreamGraphEmbryo(
            pGraph,
            pScheduler,
            pCache,
            pSegmentFactory,
            true));
}

void ExecStreamTestBase::testCaseTearDown()
{
    if (pScheduler) {
        pScheduler->stop();
    }
    pGraph.reset();
    pScheduler.reset();
    pGraphEmbryo.reset();
    SegStorageTestBase::testCaseTearDown();
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
        BOOST_REQUIRE(bufAccessor.isConsumptionPossible());
        uint nBytes = bufAccessor.getConsumptionAvailable();
        nBytesTotal += nBytes;
        for (uint i = 0; i < nBytes; ++i) {
            uint c = bufAccessor.getConsumptionStart()[i];
            if (c != byteExpected) {
                BOOST_CHECK_EQUAL(byteExpected,c);
                return;
            }
        }
        bufAccessor.consumeData(bufAccessor.getConsumptionEnd());
    }
    BOOST_CHECK_EQUAL(nBytesExpected,nBytesTotal);
}

void ExecStreamTestBase::verifyOutput(
    ExecStream &stream,
    uint nRowsExpected,
    MockProducerExecStreamGenerator &generator)
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
        for (;;) {
            if (!bufAccessor.demandData()) {
                break;
            }
            BOOST_REQUIRE(nRows < nRowsExpected);
            int64_t actualValue = *reinterpret_cast<int64_t const *>(
                bufAccessor.getConsumptionStart());
            bufAccessor.consumeData(
                bufAccessor.getConsumptionStart() + sizeof(actualValue));
            int64_t expectedValue = generator.generateValue(nRows);
            ++nRows;
            if (expectedValue != actualValue) {
                BOOST_CHECK_EQUAL(expectedValue,actualValue);
                return;
            }
        }
    }
    BOOST_CHECK_EQUAL(nRowsExpected,nRows);
}

void ExecStreamTestBase::verifyConstantOutput(
    ExecStream &stream, 
    const TupleData  &expectedTuple,
    uint nRowsExpected)
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

        if (!bufAccessor.demandData()) {
            break;
        }
        BOOST_REQUIRE(nRows < nRowsExpected);

        TupleData actualTuple;
        actualTuple.compute(bufAccessor.getTupleDesc());
        bufAccessor.unmarshalTuple(actualTuple);

        int c = bufAccessor.getTupleDesc().compareTuples(
                               expectedTuple, actualTuple);
        bufAccessor.consumeTuple();
        ++nRows;
        if (c) {
#if 1 
            TupleDescriptor statusDesc = bufAccessor.getTupleDesc();
            TuplePrinter tuplePrinter;
            tuplePrinter.print(std::cout, statusDesc, actualTuple);
            tuplePrinter.print(std::cout, statusDesc, expectedTuple);
            std::cout << std::endl;
#endif
            BOOST_CHECK_EQUAL(0,c);
            break;
        }
    }
    BOOST_CHECK_EQUAL(nRowsExpected, nRows);
}

FENNEL_END_CPPFILE("$Id$");

// End ExecStreamTestBase.cpp
