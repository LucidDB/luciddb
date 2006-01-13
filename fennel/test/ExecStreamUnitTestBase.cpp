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
#include "fennel/exec/ExecStreamGraph.h"
#include "fennel/exec/ExecStreamGraphEmbryo.h"
#include "fennel/exec/ExecStreamScheduler.h"
#include "fennel/exec/ExecStream.h"
#include "fennel/exec/ScratchBufferExecStream.h"
#include "fennel/exec/ExecStreamEmbryo.h"
#include "fennel/exec/ExecStreamBufAccessor.h"
#include "fennel/exec/MockProducerExecStream.h"
#include "fennel/tuple/TuplePrinter.h"

#include <boost/test/test_tools.hpp>

FENNEL_BEGIN_CPPFILE("$Id$");

SharedExecStream ExecStreamUnitTestBase::prepareSourceGraph(
    ExecStreamEmbryo &sourceStreamEmbryo)
{
    std::vector<ExecStreamEmbryo> transforms;
    return prepareTransformGraph(sourceStreamEmbryo, transforms);
}

SharedExecStream ExecStreamUnitTestBase::prepareTransformGraph(
    ExecStreamEmbryo &sourceStreamEmbryo,
    ExecStreamEmbryo &transformStreamEmbryo)
{
    std::vector<ExecStreamEmbryo> transforms;
    transforms.push_back(transformStreamEmbryo);
    return prepareTransformGraph(sourceStreamEmbryo, transforms);
}

SharedExecStream ExecStreamUnitTestBase::prepareTransformGraph(
    ExecStreamEmbryo &sourceStreamEmbryo,
    std::vector<ExecStreamEmbryo> &transforms)
{
    pGraphEmbryo->saveStreamEmbryo(sourceStreamEmbryo);
    std::vector<ExecStreamEmbryo>::iterator it;
    
    // save all transforms
    for (it = transforms.begin(); it != transforms.end(); ++it) {
        pGraphEmbryo->saveStreamEmbryo(*it);
    }

    // connect streams in a cascade
    ExecStreamEmbryo& previousStream = sourceStreamEmbryo;
    for (it = transforms.begin(); it != transforms.end(); ++it) {
        pGraphEmbryo->addDataflow(previousStream.getStream()->getName(),
                                  (*it).getStream()->getName());
        previousStream = *it;
    }

    SharedExecStream pAdaptedStream =
        pGraphEmbryo->addAdapterFor(previousStream.getStream()->getName(), 0,
                                    BUFPROV_PRODUCER);
    pGraph->addOutputDataflow(pAdaptedStream->getStreamId());

    pGraphEmbryo->prepareGraph(shared_from_this(), "");
    return pAdaptedStream;
}

SharedExecStream ExecStreamUnitTestBase::prepareConfluenceGraph(
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
            confluenceStreamEmbryo.getStream()->getName(), 0, 
            BUFPROV_PRODUCER);
    pGraph->addOutputDataflow(
        pAdaptedStream->getStreamId());

    pGraphEmbryo->prepareGraph(shared_from_this(), "");

    return pAdaptedStream;
}

SharedExecStream ExecStreamUnitTestBase::prepareDAG(
    ExecStreamEmbryo &srcStreamEmbryo,
    ExecStreamEmbryo &splitterStreamEmbryo,
    std::vector<ExecStreamEmbryo> &interStreamEmbryos,
    ExecStreamEmbryo &destStreamEmbryo)
{
    std::vector<ExecStreamEmbryo>::iterator it;
    
    pGraphEmbryo->saveStreamEmbryo(srcStreamEmbryo);
    pGraphEmbryo->saveStreamEmbryo(splitterStreamEmbryo);

    // save all intermediate stream embryos
    for (it = interStreamEmbryos.begin(); it != interStreamEmbryos.end();
            ++it) {
        pGraphEmbryo->saveStreamEmbryo(*it);
    }

    pGraphEmbryo->saveStreamEmbryo(destStreamEmbryo);
    
    pGraphEmbryo->addDataflow(
        srcStreamEmbryo.getStream()->getName(),
        splitterStreamEmbryo.getStream()->getName());

    // connect all inter streams to src and dest
    for (it = interStreamEmbryos.begin(); it != interStreamEmbryos.end();
            ++it) {
        pGraphEmbryo->addDataflow(splitterStreamEmbryo.getStream()->getName(),
                                  (*it).getStream()->getName());
        pGraphEmbryo->addDataflow((*it).getStream()->getName(),
                                  destStreamEmbryo.getStream()->getName());
    }

    SharedExecStream pAdaptedStream =
        pGraphEmbryo->addAdapterFor(
            destStreamEmbryo.getStream()->getName(), 0, 
            BUFPROV_PRODUCER);
    pGraph->addOutputDataflow(
        pAdaptedStream->getStreamId());

    pGraphEmbryo->prepareGraph(shared_from_this(), "");

    return pAdaptedStream;
}

void ExecStreamUnitTestBase::testCaseSetUp()
{
    ExecStreamTestBase::testCaseSetUp();
    openRandomSegment();
    pGraph = newStreamGraph();
    pGraphEmbryo = newStreamGraphEmbryo(pGraph);
}

void ExecStreamUnitTestBase::resetExecStreamTest()
{
    if (pScheduler) {
        pScheduler->stop();
    }
    tearDownExecStreamTest();
                
    pScheduler.reset(newScheduler());
    pGraph = newStreamGraph();
    pGraphEmbryo = newStreamGraphEmbryo(pGraph);
}

// refines ExecStreamTestBase::testCaseTearDown()
void ExecStreamUnitTestBase::tearDownExecStreamTest()
{
    pGraph.reset();
    pGraphEmbryo.reset();
}

void ExecStreamUnitTestBase::verifyConstantOutput(
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

void ExecStreamUnitTestBase::verifyOutput(
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
        const uint nCol = 
            bufAccessor.getConsumptionTupleAccessor().size();
        BOOST_REQUIRE(nCol == bufAccessor.getTupleDesc().size());
        BOOST_REQUIRE(nCol >= 1);
        TupleData inputTuple;
        inputTuple.compute(bufAccessor.getTupleDesc());
        for (;;) {
            if (!bufAccessor.demandData()) {
                break;
            }
            BOOST_REQUIRE(nRows < nRowsExpected);
            bufAccessor.unmarshalTuple(inputTuple);
            for (int col=0;col<nCol;++col) {
                int64_t actualValue = 
                    *reinterpret_cast<int64_t const *>(inputTuple[col].pData);
                int64_t expectedValue = generator.generateValue(nRows, col);
                if (actualValue != expectedValue) {
                    std::cout << "(Row, Col) = (" << nRows << ", " << col <<")"
                              << std::endl;
                    BOOST_CHECK_EQUAL(expectedValue,actualValue);
                    return;
                }
            }
            bufAccessor.consumeTuple();
            ++nRows;
        }
    }
    BOOST_CHECK_EQUAL(nRowsExpected,nRows);
}

void ExecStreamUnitTestBase::verifyConstantOutput(
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

