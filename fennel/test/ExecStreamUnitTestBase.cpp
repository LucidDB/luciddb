/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005-2009 The Eigenbase Project
// Copyright (C) 2005-2009 SQLstream, Inc.
// Copyright (C) 2005-2009 LucidEra, Inc.
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
#include "fennel/exec/ExecStreamGraph.h"
#include "fennel/exec/ExecStreamGraphEmbryo.h"
#include "fennel/exec/ExecStreamScheduler.h"
#include "fennel/exec/ExecStream.h"
#include "fennel/exec/ScratchBufferExecStream.h"
#include "fennel/exec/ExecStreamEmbryo.h"
#include "fennel/exec/ExecStreamBufAccessor.h"
#include "fennel/exec/MockProducerExecStream.h"
#include "fennel/tuple/TuplePrinter.h"
#include "fennel/cache/QuotaCacheAccessor.h"

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
    std::vector<ExecStreamEmbryo> sourceStreamEmbryos;
    sourceStreamEmbryos.push_back(sourceStreamEmbryo1);
    sourceStreamEmbryos.push_back(sourceStreamEmbryo2);
    return prepareConfluenceGraph(sourceStreamEmbryos, confluenceStreamEmbryo);
}

SharedExecStream ExecStreamUnitTestBase::prepareConfluenceTransformGraph(
    ExecStreamEmbryo &sourceStreamEmbryo1,
    ExecStreamEmbryo &sourceStreamEmbryo2,
    ExecStreamEmbryo &confluenceStreamEmbryo,
    ExecStreamEmbryo &transformStreamEmbryo)
{
    std::vector<ExecStreamEmbryo> sourceStreamEmbryos;
    sourceStreamEmbryos.push_back(sourceStreamEmbryo1);
    sourceStreamEmbryos.push_back(sourceStreamEmbryo2);

    std::vector<ExecStreamEmbryo>::iterator it;

    for (it = sourceStreamEmbryos.begin(); it != sourceStreamEmbryos.end();
        ++it)
    {
        pGraphEmbryo->saveStreamEmbryo(*it);
    }
    pGraphEmbryo->saveStreamEmbryo(confluenceStreamEmbryo);

    for (it = sourceStreamEmbryos.begin(); it != sourceStreamEmbryos.end();
        ++it)
    {
        pGraphEmbryo->addDataflow(
            (*it).getStream()->getName(),
            confluenceStreamEmbryo.getStream()->getName());
    }

    std::vector<ExecStreamEmbryo> transforms;
    transforms.push_back(transformStreamEmbryo);
    ExecStreamEmbryo& previousStream = confluenceStreamEmbryo;

    // save all transforms
    for (it = transforms.begin(); it != transforms.end(); ++it) {
        pGraphEmbryo->saveStreamEmbryo(*it);
    }

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
    std::vector<ExecStreamEmbryo> &sourceStreamEmbryos,
    ExecStreamEmbryo &confluenceStreamEmbryo)
{
    std::vector<std::vector<ExecStreamEmbryo> > sourceStreamEmbryosList;
    std::vector<ExecStreamEmbryo>::iterator it;
    std::vector<ExecStreamEmbryo> sourceStreamList;
    for (it = sourceStreamEmbryos.begin(); it != sourceStreamEmbryos.end();
        it++)
    {
        sourceStreamList.clear();
        sourceStreamList.push_back(*it);
        sourceStreamEmbryosList.push_back(sourceStreamList);
    }

    return
        prepareConfluenceGraph(sourceStreamEmbryosList, confluenceStreamEmbryo);
}

SharedExecStream ExecStreamUnitTestBase::prepareConfluenceGraph(
    std::vector<std::vector<ExecStreamEmbryo> > &sourceStreamEmbryosList,
    ExecStreamEmbryo &confluenceStreamEmbryo)
{
    pGraphEmbryo->saveStreamEmbryo(confluenceStreamEmbryo);

    for (int i = 0; i < sourceStreamEmbryosList.size(); i++) {
        for (int j = 0; j < sourceStreamEmbryosList[i].size(); j++) {
            pGraphEmbryo->saveStreamEmbryo(sourceStreamEmbryosList[i][j]);
        }

        // connect streams in each sourceStreamEmbryos list in a cascade
        for (int j = 1; j < sourceStreamEmbryosList[i].size(); j++) {
            pGraphEmbryo->addDataflow(
                sourceStreamEmbryosList[i][j - 1].getStream()->getName(),
                sourceStreamEmbryosList[i][j].getStream()->getName());
        }
        pGraphEmbryo->addDataflow(
            sourceStreamEmbryosList[i].back().getStream()->getName(),
            confluenceStreamEmbryo.getStream()->getName());
    }

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
    ExecStreamEmbryo &destStreamEmbryo,
    bool createSink,
    bool saveSrc)
{
    std::vector<std::vector<ExecStreamEmbryo> > listOfList;

    // Convert interStreamEmbryos to a vector of vectors.  E.g., if
    // interStreamEmbryos contains (1, 2, 3), it will get converted to:
    // ((1)) ((2)) ((3))
    for (uint i = 0; i < interStreamEmbryos.size(); i++) {
        std::vector<ExecStreamEmbryo> interStreamEmbryoList;

        interStreamEmbryoList.push_back(interStreamEmbryos[i]);
        listOfList.push_back(interStreamEmbryoList);
    }
    return prepareDAG(
        srcStreamEmbryo, splitterStreamEmbryo, listOfList, destStreamEmbryo,
        createSink, saveSrc);
}

SharedExecStream ExecStreamUnitTestBase::prepareDAG(
    ExecStreamEmbryo &srcStreamEmbryo,
    ExecStreamEmbryo &splitterStreamEmbryo,
    std::vector<std::vector<ExecStreamEmbryo> > &interStreamEmbryos,
    ExecStreamEmbryo &destStreamEmbryo,
    bool createSink,
    bool saveSrc)
{
    if (saveSrc) {
        pGraphEmbryo->saveStreamEmbryo(srcStreamEmbryo);
    }
    pGraphEmbryo->saveStreamEmbryo(splitterStreamEmbryo);

    // save all intermediate stream embryos
    for (int i = 0; i < interStreamEmbryos.size(); i++) {
        for (int j = 0; j < interStreamEmbryos[i].size(); j++) {
            pGraphEmbryo->saveStreamEmbryo(interStreamEmbryos[i][j]);
        }

        // connect streams in each interStreamEmbryos list in a cascade
        for (int j = 1; j < interStreamEmbryos[i].size(); j++) {
            pGraphEmbryo->addDataflow(
                interStreamEmbryos[i][j - 1].getStream()->getName(),
                interStreamEmbryos[i][j].getStream()->getName());
        }
    }

    pGraphEmbryo->saveStreamEmbryo(destStreamEmbryo);

    pGraphEmbryo->addDataflow(
        srcStreamEmbryo.getStream()->getName(),
        splitterStreamEmbryo.getStream()->getName());

    // connect all inter streams to src and dest
    for (int i = 0; i < interStreamEmbryos.size(); i++) {
        pGraphEmbryo->addDataflow(
            splitterStreamEmbryo.getStream()->getName(),
            interStreamEmbryos[i][0].getStream()->getName());
        pGraphEmbryo->addDataflow(
            interStreamEmbryos[i].back().getStream()->getName(),
            destStreamEmbryo.getStream()->getName());
    }

    SharedExecStream pAdaptedStream;

    if (createSink) {
        pAdaptedStream = pGraphEmbryo->addAdapterFor(
            destStreamEmbryo.getStream()->getName(), 0,
            BUFPROV_PRODUCER);
        pGraph->addOutputDataflow(pAdaptedStream->getStreamId());

        pGraphEmbryo->prepareGraph(shared_from_this(), "");
    }

    return pAdaptedStream;
}

void ExecStreamUnitTestBase::testCaseSetUp()
{
    ExecStreamTestBase::testCaseSetUp();
    openRandomSegment();
    pGraph = newStreamGraph();
    pGraphEmbryo = newStreamGraphEmbryo(pGraph);
    pGraph->setResourceGovernor(pResourceGovernor);

    // we don't bother with quotas for unit tests, but we do need
    // to be able to associate TxnId's in order for parallel
    // execution to work (since a cache page may be pinned
    // by one thread and then released by another)
    pCacheAccessor.reset(
        new TransactionalCacheAccessor(pCache));
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
    pGraph->setResourceGovernor(pResourceGovernor);
}

// refines ExecStreamTestBase::testCaseTearDown()
void ExecStreamUnitTestBase::tearDownExecStreamTest()
{
    pGraph.reset();
    pGraphEmbryo.reset();
}

void ExecStreamUnitTestBase::verifyOutput(
    ExecStream &stream,
    uint nRowsExpected,
    MockProducerExecStreamGenerator &generator,
    bool stopEarly)
{
    // TODO:  assertions about output tuple

    pResourceGovernor->requestResources(*pGraph);
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
            for (int col = 0; col < nCol; ++col) {
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
            if (stopEarly && nRows == nRowsExpected) {
                return;
            }
        }
    }
    BOOST_CHECK_EQUAL(nRowsExpected,nRows);
}

void ExecStreamUnitTestBase::verifyConstantOutput(
    ExecStream &stream,
    const TupleData &expectedTuple,
    uint nRowsExpected)
{
    // TODO:  assertions about output tuple

    pResourceGovernor->requestResources(*pGraph);
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

void ExecStreamUnitTestBase::verifyBufferedOutput(
    ExecStream &stream,
    TupleDescriptor outputTupleDesc,
    uint nRowsExpected,
    PBuffer expectedBuffer)
{
    // TODO:  assertions about output tuple

    TupleAccessor expectedOutputAccessor;
    expectedOutputAccessor.compute(outputTupleDesc);
    TupleData expectedTuple(outputTupleDesc);
    uint bufOffset = 0;
    pResourceGovernor->requestResources(*pGraph);
    pGraph->open();
    pScheduler->start();
    uint nRows = 0;
    for (;;) {
        ExecStreamBufAccessor &bufAccessor =
            pScheduler->readStream(stream);
        if (bufAccessor.getState() == EXECBUF_EOS) {
            break;
        }
        BOOST_REQUIRE(bufAccessor.getTupleDesc() == outputTupleDesc);
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
            expectedOutputAccessor.setCurrentTupleBuf(
                expectedBuffer + bufOffset);
            expectedOutputAccessor.unmarshal(expectedTuple);
            int c = outputTupleDesc.compareTuples(inputTuple, expectedTuple);
            if (c) {
                std::cout << "(Row) = (" << nRows << ")"
                    << " -- Tuples don't match"<< std::endl;
                BOOST_CHECK_EQUAL(0,c);
                return;
            }
            bufAccessor.consumeTuple();
            bufOffset += expectedOutputAccessor.getCurrentByteCount();
            ++nRows;
        }
    }
    BOOST_CHECK_EQUAL(nRowsExpected,nRows);
}

FENNEL_END_CPPFILE("$Id$");

// End ExecStreamUnitTestBase.cpp
