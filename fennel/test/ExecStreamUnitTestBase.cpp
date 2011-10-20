/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005 The Eigenbase Project
// Copyright (C) 2005 SQLstream, Inc.
// Copyright (C) 2005 Dynamo BI Corporation
// Portions Copyright (C) 2004 John V. Sichi
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

using std::vector;
using std::string;
using std::endl;
using std::cout;

ExecStream& ExecStreamUnitTestBase::completeGraph(ExecStream& final)
{
    // add a terminal sink, with an output edge for readStream():
    SharedExecStream pAdaptedStream =
        pGraphEmbryo->addAdapterFor(final.getName(), 0, BUFPROV_PRODUCER);
    pGraph->addOutputDataflow(pAdaptedStream->getStreamId());

    pGraphEmbryo->prepareGraph(shared_from_this(), "");
    return *pAdaptedStream;
}

SharedExecStream ExecStreamUnitTestBase::prepareSourceGraph(
    ExecStreamEmbryo &sourceStreamEmbryo)
{
    vector<ExecStreamEmbryo> transforms;
    return prepareTransformGraph(sourceStreamEmbryo, transforms);
}

SharedExecStream ExecStreamUnitTestBase::prepareTransformGraph(
    ExecStreamEmbryo &sourceStreamEmbryo,
    ExecStreamEmbryo &transformStreamEmbryo)
{
    vector<ExecStreamEmbryo> transforms;
    transforms.push_back(transformStreamEmbryo);
    return prepareTransformGraph(sourceStreamEmbryo, transforms);
}

SharedExecStream ExecStreamUnitTestBase::prepareTransformGraph(
    ExecStreamEmbryo &sourceStreamEmbryo,
    vector<ExecStreamEmbryo> &transforms)
{
    pGraphEmbryo->saveStreamEmbryo(sourceStreamEmbryo);
    vector<ExecStreamEmbryo>::iterator it;

    // save all transforms
    for (it = transforms.begin(); it != transforms.end(); ++it) {
        pGraphEmbryo->saveStreamEmbryo(*it);
    }

    // connect streams in a cascade
    ExecStreamEmbryo& previousStream = sourceStreamEmbryo;
    for (it = transforms.begin(); it != transforms.end(); ++it) {
        pGraphEmbryo->addDataflow(
            previousStream.getStream()->getName(),
            (*it).getStream()->getName());
        previousStream = *it;
    }
    return previousStream.getStream();
}

// converts a list like '(1 2 3) to a list of lists like '((1) (2) (3)):
template<class T> vector<vector<T> > explodeList(const vector<T>& in)
{
    vector<vector<T> > out;
    for (int i = 0; i < in.size(); ++i) {
        out.push_back(vector<T>(1, in[i]));
    }
    return out;
}

SharedExecStream ExecStreamUnitTestBase::prepareConfluenceGraph(
    ExecStreamEmbryo &sourceStreamEmbryo1,
    ExecStreamEmbryo &sourceStreamEmbryo2,
    ExecStreamEmbryo &confluenceStreamEmbryo)
{
    vector<ExecStreamEmbryo> sourceStreamEmbryos;
    sourceStreamEmbryos.push_back(sourceStreamEmbryo1);
    sourceStreamEmbryos.push_back(sourceStreamEmbryo2);
    return prepareConfluenceGraph(sourceStreamEmbryos, confluenceStreamEmbryo);
}

SharedExecStream ExecStreamUnitTestBase::prepareConfluenceTransformGraph(
    ExecStreamEmbryo &source1,
    ExecStreamEmbryo &source2,
    ExecStreamEmbryo &confluence,
    ExecStreamEmbryo &transform)
{
    vector<ExecStreamEmbryo> sources;
    sources.push_back(source1);
    sources.push_back(source2);
    vector<ExecStreamEmbryo> transforms;
    transforms.push_back(transform);
    vector<vector<ExecStreamEmbryo> > inputs = explodeList(sources);
    return prepareConfluenceTransformGraph(inputs, confluence, transforms);
}

SharedExecStream ExecStreamUnitTestBase::prepareConfluenceGraph(
    vector<ExecStreamEmbryo> &sources,
    ExecStreamEmbryo &confluence)
{
    vector<vector<ExecStreamEmbryo> > exploded = explodeList(sources);
    return prepareConfluenceGraph(exploded, confluence);
}


SharedExecStream ExecStreamUnitTestBase::prepareConfluenceGraph(
    vector<vector<ExecStreamEmbryo> > &sources,
    ExecStreamEmbryo &confluence)
{
    vector<ExecStreamEmbryo> transforms;
    return prepareConfluenceTransformGraph(sources, confluence, transforms);
}

SharedExecStream ExecStreamUnitTestBase::prepareConfluenceTransformGraph(
    vector<vector<ExecStreamEmbryo> > &inputs,
    ExecStreamEmbryo &confluence,
    vector<ExecStreamEmbryo> &transforms)
{
    pGraphEmbryo->saveStreamEmbryo(confluence);

    for (int i = 0; i < inputs.size(); i++) {
        for (int j = 0; j < inputs[i].size(); j++) {
            pGraphEmbryo->saveStreamEmbryo(inputs[i][j]);
        }
        // connect the nodes of each inputs list in a cascade:
        for (int j = 1; j < inputs[i].size(); j++) {
            pGraphEmbryo->addDataflow(
                inputs[i][j - 1].getStream()->getName(),
                inputs[i][j].getStream()->getName());
        }
        pGraphEmbryo->addDataflow(
            inputs[i].back().getStream()->getName(),
            confluence.getStream()->getName());
    }

    // add the transforms
    vector<ExecStreamEmbryo>::iterator it;
    for (it = transforms.begin(); it != transforms.end(); ++it) {
        pGraphEmbryo->saveStreamEmbryo(*it);
    }
    ExecStreamEmbryo& previousStream = confluence;
    for (it = transforms.begin(); it != transforms.end(); ++it) {
        pGraphEmbryo->addDataflow(
            previousStream.getStream()->getName(),
            (*it).getStream()->getName());
        previousStream = *it;
    }
    return previousStream.getStream();
}

SharedExecStream ExecStreamUnitTestBase::prepareDAG(
    ExecStreamEmbryo &src,
    ExecStreamEmbryo &splitter,
    vector<ExecStreamEmbryo> &inters,
    ExecStreamEmbryo &dest,
    bool saveSrc)
{
    vector<vector<ExecStreamEmbryo> > exploded = explodeList(inters);
    return prepareDAG(src, splitter, exploded, dest, saveSrc);
}

SharedExecStream ExecStreamUnitTestBase::prepareDAG(
    ExecStreamEmbryo &src,
    ExecStreamEmbryo &splitter,
    vector<vector<ExecStreamEmbryo> > &inters,
    ExecStreamEmbryo &dest,
    bool saveSrc)
{
    if (saveSrc) {
        pGraphEmbryo->saveStreamEmbryo(src);
    }
    pGraphEmbryo->saveStreamEmbryo(splitter);

    // save all intermediate stream embryos
    for (int i = 0; i < inters.size(); i++) {
        for (int j = 0; j < inters[i].size(); j++) {
            pGraphEmbryo->saveStreamEmbryo(inters[i][j]);
        }
        // connect the streams in each intermediate list in a cascade
        for (int j = 1; j < inters[i].size(); j++) {
            pGraphEmbryo->addDataflow(
                inters[i][j - 1].getStream()->getName(),
                inters[i][j].getStream()->getName());
        }
    }

    pGraphEmbryo->saveStreamEmbryo(dest);

    // connect src to splitter
    pGraphEmbryo->addDataflow(
        src.getStream()->getName(),
        splitter.getStream()->getName());
    // connect splitter and dest to all intermediate lists
    for (int i = 0; i < inters.size(); i++) {
        pGraphEmbryo->addDataflow(
            splitter.getStream()->getName(),
            inters[i][0].getStream()->getName());
        pGraphEmbryo->addDataflow(
            inters[i].back().getStream()->getName(),
            dest.getStream()->getName());
    }

    return dest.getStream();
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

// this functor expects a fixed TupleData.
class ConstantTupleChecker : public MockConsumerExecStreamTupleChecker {
    const TupleData expected;
public:
    ConstantTupleChecker(const TupleData& expected) : expected(expected) {}
    virtual bool check(int nrow, const TupleDescriptor&, const TupleData&);
};

// this functor has a generator
class TupleCheckerWithGenerator : public MockConsumerExecStreamTupleChecker {
    MockProducerExecStreamGenerator& gen;
public:
    TupleCheckerWithGenerator(MockProducerExecStreamGenerator& gen)
        : gen(gen) {}
    virtual bool check(int nrow, const TupleDescriptor&, const TupleData&);
};

// this functor has a buffer of expected data in marshalled format
class TupleCheckerWithBuffer : public MockConsumerExecStreamTupleChecker {
    const PBuffer bufStart;
    PBuffer bufNext;
    int nrow;
public:
    TupleCheckerWithBuffer(const PBuffer buf) : bufStart(buf) {
        bufNext = bufStart;
        nrow = 0;
    }
    virtual bool check(int nrow, const TupleDescriptor&, const TupleData&);
};

// this functor has a list of expected tuples, in string format, as from
// TuplePrinter
class TupleCheckerWithStrings : public MockConsumerExecStreamTupleChecker {
    vector<string> expectedStrings;
public:
    TupleCheckerWithStrings(const vector<string>& expected)
        : expectedStrings(expected) {}
    virtual bool check(int nrow, const TupleDescriptor&, const TupleData&);
};

bool ConstantTupleChecker::check(
    int nrow, const TupleDescriptor& desc, const TupleData& actual)
{
    bool val = (0 == desc.compareTuples(expected, actual));
    BOOST_CHECK_MESSAGE(val, "wrong output at row " << nrow);
    return val;
}

bool TupleCheckerWithStrings::check(
    int nrow, const TupleDescriptor& desc, const TupleData& actual)
{
    std::ostringstream oss;
    TuplePrinter tprint;
    tprint.print(oss, desc, actual);
    string actualString = oss.str();
    BOOST_REQUIRE(nrow < expectedStrings.size());
    bool val = (0 == actualString.compare(expectedStrings[nrow]));
    BOOST_CHECK_MESSAGE(
        val, "wrong output at row " << nrow
        << "; expected: " << expectedStrings[nrow]
        << " actual: " << actualString);
    return val;
}

bool TupleCheckerWithGenerator::check(
    int nrow, const TupleDescriptor& desc, const TupleData& actual)
{
    int nMismatches = 0;
    int nCol = desc.size();
    for (int col = 0; col < nCol; ++col) {
        int64_t actualValue =
            *reinterpret_cast<int64_t const *>(actual[col].pData);
        int64_t expectedValue = gen.generateValue(nrow, col);
        bool val = (expectedValue == actualValue);
        BOOST_CHECK_MESSAGE(
            val,
            "wrong output at row " << nrow << ", column " << col);
        if (!val) {
            nMismatches++;
        }
    }
    return (nMismatches == 0);
}

bool TupleCheckerWithBuffer::check(
    int nrow, const TupleDescriptor& desc, const TupleData& actualTuple)
{
    BOOST_REQUIRE(nrow == this->nrow);
    TupleData expectedTuple(desc);
    TupleAccessor expectedOutputAccessor;
    expectedOutputAccessor.compute(desc);
    expectedOutputAccessor.setCurrentTupleBuf(this->bufNext);
    expectedOutputAccessor.unmarshal(expectedTuple);
    this->nrow++;
    this->bufNext += expectedOutputAccessor.getCurrentByteCount();

    bool val = (0 == desc.compareTuples(expectedTuple, actualTuple));
    BOOST_CHECK_MESSAGE(val, "wrong output at row " << nrow);
    return val;
}

void ExecStreamUnitTestBase::verifyOutput(
    ExecStream &wasFinal,
    uint nRowsExpected,
    MockConsumerExecStreamTupleChecker &checker,
    bool stopEarly)
{
    ExecStream& stream = completeGraph(wasFinal);
    pResourceGovernor->requestResources(*pGraph);
    pGraph->open();
    pScheduler->start();

    ExecStreamId streamId = stream.getStreamId();
    BOOST_REQUIRE(stream.getGraph().getOutputCount(streamId) == 1);
    const TupleDescriptor& desc =
        stream.getGraph().getStreamOutputAccessor(streamId, 0)->getTupleDesc();
    TupleData actualTuple;
    actualTuple.compute(desc);

    uint nRows = 0;
    for (;;) {
        ExecStreamBufAccessor &bufAccessor = pScheduler->readStream(stream);
        if (bufAccessor.getState() == EXECBUF_EOS) {
            break;
        }
        BOOST_REQUIRE(bufAccessor.isConsumptionPossible());
        const uint nCol = bufAccessor.getConsumptionTupleAccessor().size();
        BOOST_REQUIRE(nCol == bufAccessor.getTupleDesc().size());
        BOOST_REQUIRE(nCol >= 1);

        for (;;) {
            if (!bufAccessor.demandData()) {
                break;
            }
            BOOST_REQUIRE(nRows < nRowsExpected);
            bufAccessor.unmarshalTuple(actualTuple);
            checker.check(nRows, desc, actualTuple);
            bufAccessor.consumeTuple();
            ++nRows;
            if (stopEarly && nRows == nRowsExpected) {
                return;
            }
        }
    }
    BOOST_CHECK_EQUAL(nRowsExpected, nRows);
}

void ExecStreamUnitTestBase::verifyOutput(
    ExecStream &stream,
    uint nRowsExpected,
    MockProducerExecStreamGenerator &generator,
    bool stopEarly)
{
    TupleCheckerWithGenerator checker(generator);
    verifyOutput(stream, nRowsExpected, checker, stopEarly);
}


void ExecStreamUnitTestBase::verifyConstantOutput(
    ExecStream &stream,
    const TupleData &expectedTuple,
    uint nRowsExpected)
{
    ConstantTupleChecker checker(expectedTuple);
    verifyOutput(stream, nRowsExpected, checker);
}

void ExecStreamUnitTestBase::verifyBufferedOutput(
    ExecStream &stream,
    TupleDescriptor outputTupleDesc,
    uint nRowsExpected,
    PBuffer expectedBuffer)
{
    TupleCheckerWithBuffer checker(expectedBuffer);
    verifyOutput(stream, nRowsExpected, checker);
}

void ExecStreamUnitTestBase::verifyStringOutput(
    ExecStream &stream,
    uint nRowsExpected,
    const vector<string>& expected)
{
    TupleCheckerWithStrings checker(expected);
    verifyOutput(stream, nRowsExpected, checker);
}

FENNEL_END_CPPFILE("$Id$");

// End ExecStreamUnitTestBase.cpp
