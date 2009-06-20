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
#include "fennel/test/ExecStreamUnitTestBase.h"
#include "fennel/test/SnapshotSegmentTestBase.h"
#include "fennel/lucidera/colstore/LcsClusterReplaceExecStream.h"
#include "fennel/lucidera/colstore/LcsRowScanExecStream.h"
#include "fennel/btree/BTreeBuilder.h"
#include "fennel/tuple/StandardTypeDescriptor.h"
#include "fennel/segment/SnapshotRandomAllocationSegment.h"
#include "fennel/segment/SegmentFactory.h"
#include "fennel/exec/MockProducerExecStream.h"
#include "fennel/exec/ValuesExecStream.h"
#include "fennel/exec/ExecStreamEmbryo.h"
#include "fennel/exec/DynamicParam.h"
#include "fennel/cache/Cache.h"
#include <stdarg.h>

#include <boost/test/test_tools.hpp>

using namespace fennel;

/**
 * Test class for exercising LcsClusterReplaceExecStream.  Note that the class
 * is derived from SnapshotSegmentTestBase, which allows the underlying segment
 * it uses for storage to be versioned.
 */
class LcsClusterReplaceExecStreamTest
    : public ExecStreamUnitTestBase, public SnapshotSegmentTestBase
{
    StandardTypeDescriptorFactory stdTypeFactory;
    TupleAttributeDescriptor attrDesc_int64;
    TupleAttributeDescriptor attrDesc_bitmap;

    PageId savedRootPageId;
    BTreeDescriptor btreeDescriptor;

    /**
     * Loads a cluster with the values specified by the mock producer stream
     * generator input parameter, using a LcsClusterAppendExecStream.
     *
     * @param nCols number of columns in the cluster
     * @param nRows number of rows to load
     * @param pInputGenerator the generator that produces the data to be loaded
     * into the cluster
     */
    void loadCluster(
        uint nCols,
        uint nRows,
        SharedMockProducerExecStreamGenerator pInputGenerator);

    /**
     * Verifies that a cluster contains the values specified by the mock
     * producer stream generator passed in.
     *
     * @param nCols number of columns in the cluster
     * @param nRows number of rows expected in the cluster
     * @param resultGenerator the generator that produces the expected data
     * in the cluster
     */
    void verifyCluster(
        uint nCols,
        uint nRows,
        MockProducerExecStreamGenerator &resultGenerator);

    /**
     * Replaces rows in an existing cluster column based on a mock producer
     * exec stream passed in.
     *
     * @param nCols the number of columns in the cluster
     * @param nRows the number of rows to be replaced
     * @param pInputGenerator the generator that generates the input into
     * the replace stream; each input row contains the rid value of the
     * row to be replaced and the new column values
     */
    void replaceCluster(
        uint nCols,
        uint nRows,
        SharedMockProducerExecStreamGenerator pInputGenerator);

    /**
     * Loads a cluster and then replaces selected rows from the cluster with
     * new input.
     *
     * @param nCols number of columns in the cluster
     * @param totalNumRows number of rows loaded into the original cluster
     * @param numReplRows number of rows to replace in the cluster
     * @param pOrigClusterInputGenerator generator that produces input to
     * be loaded into the original cluster
     * @param origClusterResultGenerator generator that produces the expected
     * result after loading the original cluster
     * @param pReplClusterInputGenerator generator that produces the rows to
     * be replaced in the cluster
     * @param replClusterResultGenerator generator that produces the expected
     * rows in the replaced cluster
     */
    void testClusterReplace(
        uint nCols,
        uint totalNumRows,
        uint numReplRows,
        SharedMockProducerExecStreamGenerator pOrigClusterInputGenerator,
        MockProducerExecStreamGenerator &origClusterResultGenerator,
        SharedMockProducerExecStreamGenerator pReplClusterInputGenerator,
        MockProducerExecStreamGenerator &replClusterResultGenerator);

    void testSingleColRepeatingSequence(
        uint nRows,
        uint inputSeqStart,
        uint replSeqStart,
        uint expectedSeqStart);

    void testMultiColClusterReplace(uint nCols, uint nRows);

    void initClusterAppendParams(
        LcsClusterAppendExecStreamParams &lcsAppendParams,
        uint nCols,
        bool replace);

public:
    explicit LcsClusterReplaceExecStreamTest()
    {
        FENNEL_UNIT_TEST_CASE(
            LcsClusterReplaceExecStreamTest, testSingleColOddNumRows);
        FENNEL_UNIT_TEST_CASE(
            LcsClusterReplaceExecStreamTest, testSingleColEvenNumRows);
        FENNEL_UNIT_TEST_CASE(
            LcsClusterReplaceExecStreamTest, testSingleColSeqStartAt1);
        FENNEL_UNIT_TEST_CASE(
            LcsClusterReplaceExecStreamTest, testReplaceAllRows);
        FENNEL_UNIT_TEST_CASE(
            LcsClusterReplaceExecStreamTest, testReplaceNoRows);
        FENNEL_UNIT_TEST_CASE(
            LcsClusterReplaceExecStreamTest, test3ColClusterReplace);
    }

    void testSingleColOddNumRows();
    void testSingleColEvenNumRows();
    void testSingleColSeqStartAt1();
    void testReplaceAllRows();
    void testReplaceNoRows();
    void test3ColClusterReplace();

    virtual void testCaseSetUp();
    virtual void testCaseTearDown();

    virtual void openRandomSegment();
};

void LcsClusterReplaceExecStreamTest::testSingleColOddNumRows()
{
    // Original cluster contains 5001 rows with values 0, 0, 2, 2, 4, 4, ...
    // Replace every other rid starting at rid 1 with the values 1, 3, 5, ...
    // Resulting cluster contains the values 0, 1, 2, 3, ...
    testSingleColRepeatingSequence(5001, 0, 1, 0);
}

void LcsClusterReplaceExecStreamTest::testSingleColEvenNumRows()
{
    // Original cluster contains 4000 rows with values 0, 0, 2, 2, 4, 4, ...
    // Replace every other rid starting at rid 1 with the values 1, 3, 5, ...
    // Resulting cluster contains the values 0, 1, 2, 3, ...
    testSingleColRepeatingSequence(4000, 0, 1, 0);
}

void LcsClusterReplaceExecStreamTest::testSingleColSeqStartAt1()
{
    // Original cluster contains 3000 rows with values 1, 1, 3, 3, 5, 5, ...
    // Replace every other rid starting at rid 0 with the values 0, 2, 4, ...
    // Resulting cluster contains the values 0, 1, 2, 3, ...
    testSingleColRepeatingSequence(3000, 1, 0, 0);
}

void LcsClusterReplaceExecStreamTest::testSingleColRepeatingSequence(
    uint nRows,
    uint inputSeqStart,
    uint replSeqStart,
    uint expectedSeqStart)
{
    // Original cluster contains the values inputSeqStart, inputSeqStart+2, ...
    // Replace every other rid starting at rid replSeqStart with values
    // replaceSeqStart, replaceSeqStart+2, ...,
    // The replace cluster should have the sequence expectedSeqStart,
    // expectedSeqStart+1, ...

    SharedMockProducerExecStreamGenerator pOrigClusterInputGenerator =
        SharedMockProducerExecStreamGenerator(
            new StairCaseExecStreamGenerator(2, 2, inputSeqStart));

    StairCaseExecStreamGenerator origClusterResultGenerator =
        StairCaseExecStreamGenerator(2, 2, inputSeqStart);

    vector<boost::shared_ptr<ColumnGenerator<int64_t> > > columnGenerators;
    SharedInt64ColumnGenerator colGenerator =
        SharedInt64ColumnGenerator(new SeqColumnGenerator(replSeqStart, 2));
    columnGenerators.push_back(colGenerator);
    colGenerator =
        SharedInt64ColumnGenerator(new SeqColumnGenerator(replSeqStart, 2));
    columnGenerators.push_back(colGenerator);
    SharedMockProducerExecStreamGenerator pReplClusterInputGenerator =
        SharedMockProducerExecStreamGenerator(
            new CompositeExecStreamGenerator(columnGenerators));

    RampExecStreamGenerator replClusterResultGenerator(expectedSeqStart);

    testClusterReplace(
        1,
        nRows,
        nRows / 2,
        pOrigClusterInputGenerator,
        origClusterResultGenerator,
        pReplClusterInputGenerator,
        replClusterResultGenerator);
}

void LcsClusterReplaceExecStreamTest::testReplaceAllRows()
{
    // Original cluster 2003 rows, all with the value 99.
    // Replace every row with the sequence 0, 1, 2, ...

    uint nRows = 2003;

    SharedMockProducerExecStreamGenerator pOrigClusterInputGenerator =
        SharedMockProducerExecStreamGenerator(
            new ConstExecStreamGenerator(99));

    ConstExecStreamGenerator origClusterResultGenerator =
        ConstExecStreamGenerator(99);

    vector<boost::shared_ptr<ColumnGenerator<int64_t> > > columnGenerators;
    SharedInt64ColumnGenerator colGenerator =
        SharedInt64ColumnGenerator(new SeqColumnGenerator(0, 1));
    columnGenerators.push_back(colGenerator);
    colGenerator =
        SharedInt64ColumnGenerator(new SeqColumnGenerator(0, 1));
    columnGenerators.push_back(colGenerator);
    SharedMockProducerExecStreamGenerator pReplClusterInputGenerator =
        SharedMockProducerExecStreamGenerator(
            new CompositeExecStreamGenerator(columnGenerators));

    RampExecStreamGenerator replClusterResultGenerator;

    testClusterReplace(
        1,
        nRows,
        nRows,
        pOrigClusterInputGenerator,
        origClusterResultGenerator,
        pReplClusterInputGenerator,
        replClusterResultGenerator);
}

void LcsClusterReplaceExecStreamTest::testReplaceNoRows()
{
    // Original cluster contains 1007 rows with the sequence 0, 1, 2, ...
    // Replace no rows.

    uint nRows = 1007;

    SharedMockProducerExecStreamGenerator pOrigClusterInputGenerator =
        SharedMockProducerExecStreamGenerator(
            new RampExecStreamGenerator());

    RampExecStreamGenerator origClusterResultGenerator =
        RampExecStreamGenerator();

    // It doesn't matter what we use for these ColumnGenerators, since no
    // rows will be generated.
    vector<boost::shared_ptr<ColumnGenerator<int64_t> > > columnGenerators;
    SharedInt64ColumnGenerator colGenerator =
        SharedInt64ColumnGenerator(new SeqColumnGenerator(0, 1));
    columnGenerators.push_back(colGenerator);
    colGenerator =
        SharedInt64ColumnGenerator(new SeqColumnGenerator(0, 1));
    columnGenerators.push_back(colGenerator);
    SharedMockProducerExecStreamGenerator pReplClusterInputGenerator =
        SharedMockProducerExecStreamGenerator(
            new CompositeExecStreamGenerator(columnGenerators));

    RampExecStreamGenerator replClusterResultGenerator;

    testClusterReplace(
        1,
        nRows,
        0,
        pOrigClusterInputGenerator,
        origClusterResultGenerator,
        pReplClusterInputGenerator,
        replClusterResultGenerator);
}

void LcsClusterReplaceExecStreamTest::test3ColClusterReplace()
{
    testMultiColClusterReplace(3, 800);
}

void LcsClusterReplaceExecStreamTest::testMultiColClusterReplace(
    uint nCols,
    uint nRows)
{
    vector<boost::shared_ptr<ColumnGenerator<int64_t> > > columnGenerators;
    SharedInt64ColumnGenerator colGenerator;

    // For the original cluster, each column is a repeating sequence of
    // values n,n,n+2,n+2,n+4,n+4, ..., where n corresponds to the column
    // number, starting at 0
    for (uint i = 0; i < nCols; i++) {
        colGenerator =
            SharedInt64ColumnGenerator(new StairCaseColumnGenerator(2, 2, i));
        columnGenerators.push_back(colGenerator);
    }
    SharedMockProducerExecStreamGenerator pOrigClusterInputGenerator =
        SharedMockProducerExecStreamGenerator(
            new CompositeExecStreamGenerator(columnGenerators));

    columnGenerators.clear();
    for (uint i = 0; i < nCols; i++) {
        colGenerator =
            SharedInt64ColumnGenerator(new StairCaseColumnGenerator(2, 2, i));
        columnGenerators.push_back(colGenerator);
    }
    CompositeExecStreamGenerator origClusterResultGenerator =
        CompositeExecStreamGenerator(columnGenerators);

    // Replace every other row, starting at rid 1, with the values n+1, n+3, ...
    // where n is the column number.  The first column generator below is
    // for the rid sequence.
    columnGenerators.clear();
    colGenerator =
        SharedInt64ColumnGenerator(new SeqColumnGenerator(1, 2));
    columnGenerators.push_back(colGenerator);
    for (uint i = 1; i < nCols + 1; i++) {
        colGenerator = SharedInt64ColumnGenerator(new SeqColumnGenerator(i, 2));
        columnGenerators.push_back(colGenerator);
    }
    SharedMockProducerExecStreamGenerator pReplClusterInputGenerator =
        SharedMockProducerExecStreamGenerator(
            new CompositeExecStreamGenerator(columnGenerators));

    // Expected resulting cluster should have values n,n+1, ... where n is
    // the column number
    columnGenerators.clear();
    for (uint i = 0; i < nCols; i++) {
        colGenerator = SharedInt64ColumnGenerator(new SeqColumnGenerator(i));
        columnGenerators.push_back(colGenerator);
    }
    CompositeExecStreamGenerator replClusterResultGenerator =
        CompositeExecStreamGenerator(columnGenerators);

    testClusterReplace(
        nCols,
        nRows,
        nRows / 2,
        pOrigClusterInputGenerator,
        origClusterResultGenerator,
        pReplClusterInputGenerator,
        replClusterResultGenerator);
}

void LcsClusterReplaceExecStreamTest::testClusterReplace(
    uint nCols,
    uint totalNumRows,
    uint numReplRows,
    SharedMockProducerExecStreamGenerator pOrigClusterInputGenerator,
    MockProducerExecStreamGenerator &origClusterResultGenerator,
    SharedMockProducerExecStreamGenerator pReplClusterInputGenerator,
    MockProducerExecStreamGenerator &replClusterResultGenerator)
{
    // Load the cluster and verify the load by scanning the resulting cluster
    loadCluster(nCols, totalNumRows, pOrigClusterInputGenerator);
    resetExecStreamTest();
    verifyCluster(nCols, totalNumRows, origClusterResultGenerator);
    resetExecStreamTest();

    // Commit the changes associated with the current txn and create a new
    // snapshot segment for the next txn
    SnapshotRandomAllocationSegment *pSnapshotSegment =
        SegmentFactory::dynamicCast<SnapshotRandomAllocationSegment *>(
            pSnapshotRandomSegment);
    pSnapshotSegment->commitChanges(currCsn);
    pSnapshotSegment->checkpoint(CHECKPOINT_FLUSH_ALL);
    currCsn = TxnId(1);
    pSnapshotRandomSegment2 =
        pSegmentFactory->newSnapshotRandomAllocationSegment(
            pVersionedRandomSegment,
            pVersionedRandomSegment,
            currCsn);
    setForceCacheUnmap(pSnapshotRandomSegment2);
    pRandomSegment = pSnapshotRandomSegment2;

    // Replace some sequence of rows from the cluster
    replaceCluster(nCols, numReplRows, pReplClusterInputGenerator);

    // Commit the changes and then verify that the resulting cluster contains
    // a combination of the original and replaced values.  It should be
    // 0, 1, 2, 3, ...
    pSnapshotSegment =
        SegmentFactory::dynamicCast<SnapshotRandomAllocationSegment *>(
            pSnapshotRandomSegment2);
    pSnapshotSegment->commitChanges(currCsn);
    pSnapshotSegment->checkpoint(CHECKPOINT_FLUSH_ALL);
    resetExecStreamTest();
    verifyCluster(nCols, totalNumRows, replClusterResultGenerator);
}

void LcsClusterReplaceExecStreamTest::loadCluster(
    uint nCols,
    uint nRows,
    SharedMockProducerExecStreamGenerator pInputGenerator)
{
    MockProducerExecStreamParams mockParams;
    for (uint i = 0; i < nCols; i++) {
        mockParams.outputTupleDesc.push_back(attrDesc_int64);
    }
    mockParams.nRows = nRows;
    mockParams.pGenerator = pInputGenerator;

    ExecStreamEmbryo mockStreamEmbryo;
    mockStreamEmbryo.init(new MockProducerExecStream(), mockParams);
    mockStreamEmbryo.getStream()->setName("MockProducerExecStream");

    LcsClusterAppendExecStreamParams lcsAppendParams;
    initClusterAppendParams(lcsAppendParams, nCols, false);

    // setup temporary btree descriptor to get an empty page to start the btree
    btreeDescriptor.segmentAccessor.pSegment = lcsAppendParams.pSegment;
    btreeDescriptor.segmentAccessor.pCacheAccessor = pCache;
    btreeDescriptor.tupleDescriptor = lcsAppendParams.tupleDesc;
    btreeDescriptor.keyProjection = lcsAppendParams.keyProj;
    btreeDescriptor.rootPageId = NULL_PAGE_ID;

    BTreeBuilder builder(btreeDescriptor, pRandomSegment);
    builder.createEmptyRoot();
    savedRootPageId = builder.getRootPageId();

    lcsAppendParams.rootPageId = btreeDescriptor.rootPageId = savedRootPageId;

    ExecStreamEmbryo lcsAppendStreamEmbryo;
    lcsAppendStreamEmbryo.init(
        new LcsClusterAppendExecStream(),
        lcsAppendParams);
    lcsAppendStreamEmbryo.getStream()->setName("LcsClusterAppendExecStream");

    SharedExecStream pOutputStream = prepareTransformGraph(
        mockStreamEmbryo, lcsAppendStreamEmbryo);

    // set up a generator which can produce the expected output
    vector<boost::shared_ptr<ColumnGenerator<int64_t> > > columnGenerators;
    SharedInt64ColumnGenerator colGen =
        SharedInt64ColumnGenerator(new SeqColumnGenerator(nRows));
    columnGenerators.push_back(colGen);
    colGen = SharedInt64ColumnGenerator(new SeqColumnGenerator(0));
    columnGenerators.push_back(colGen);

    CompositeExecStreamGenerator expectedResultGenerator(columnGenerators);

    verifyOutput(*pOutputStream, 1, expectedResultGenerator);
}

void LcsClusterReplaceExecStreamTest::initClusterAppendParams(
    LcsClusterAppendExecStreamParams &lcsAppendParams,
    uint nCols,
    bool replace)
{
    lcsAppendParams.scratchAccessor =
        pSegmentFactory->newScratchSegment(pCache, 10);
    lcsAppendParams.pCacheAccessor = pCache;
    lcsAppendParams.pSegment = pRandomSegment;
    lcsAppendParams.rootPageIdParamId = DynamicParamId(0);

    for (uint i = 0; i < nCols; i++) {
        lcsAppendParams.inputProj.push_back(i);
    }
    if (replace) {
        lcsAppendParams.inputProj.push_back(nCols);
    }

    // initialize the btree parameter portion of lcsAppendParams
    // BTree tuple desc only has one column
    (lcsAppendParams.tupleDesc).push_back(attrDesc_int64);
    (lcsAppendParams.tupleDesc).push_back(attrDesc_int64);

    // BTree key only has one column which is the first column.
    (lcsAppendParams.keyProj).push_back(0);

    // output only single row with 2 columns (# rows loaded, starting rid value)
    lcsAppendParams.outputTupleDesc.push_back(attrDesc_int64);
    lcsAppendParams.outputTupleDesc.push_back(attrDesc_int64);

    lcsAppendParams.pRootMap = 0;

    // Set up BTreeExecStreamParams using default values from BTreeDescriptor.
    lcsAppendParams.segmentId = btreeDescriptor.segmentId;
    lcsAppendParams.pageOwnerId = btreeDescriptor.pageOwnerId;
}

void LcsClusterReplaceExecStreamTest::verifyCluster(
    uint nCols,
    uint nRows,
    MockProducerExecStreamGenerator &resultGenerator)
{
    // setup parameters into scan
    //  single cluster with only one column, project that single column

    LcsRowScanExecStreamParams scanParams;
    scanParams.hasExtraFilter = false;
    scanParams.isFullScan = true;
    scanParams.samplingMode = SAMPLING_OFF;

    struct LcsClusterScanDef clusterScanDef;

    for (uint i = 0; i < nCols; i++) {
        clusterScanDef.clusterTupleDesc.push_back(attrDesc_int64);
    }
    clusterScanDef.pSegment = btreeDescriptor.segmentAccessor.pSegment;
    clusterScanDef.pCacheAccessor =
        btreeDescriptor.segmentAccessor.pCacheAccessor;
    clusterScanDef.tupleDesc = btreeDescriptor.tupleDescriptor;
    clusterScanDef.keyProj = btreeDescriptor.keyProjection;
    clusterScanDef.rootPageId = btreeDescriptor.rootPageId;
    clusterScanDef.segmentId = btreeDescriptor.segmentId;
    clusterScanDef.pageOwnerId = btreeDescriptor.pageOwnerId;

    scanParams.lcsClusterScanDefs.push_back(clusterScanDef);
    for (uint i = 0; i < nCols; i++) {
        scanParams.outputTupleDesc.push_back(attrDesc_int64);
        scanParams.outputProj.push_back(i);
    }

    ValuesExecStreamParams valuesParams;
    ExecStreamEmbryo valuesStreamEmbryo;
    boost::shared_array<FixedBuffer> pBuffer;

    valuesParams.outputTupleDesc.push_back(attrDesc_int64);
    valuesParams.outputTupleDesc.push_back(attrDesc_bitmap);
    valuesParams.outputTupleDesc.push_back(attrDesc_bitmap);

    uint bufferSize = 16;
    pBuffer.reset(new FixedBuffer[bufferSize]);
    valuesParams.pTupleBuffer = pBuffer;
    valuesParams.bufSize = 0;
    valuesStreamEmbryo.init(new ValuesExecStream(), valuesParams);
    valuesStreamEmbryo.getStream()->setName("ValuesExecStream");

    ExecStreamEmbryo scanStreamEmbryo;
    scanStreamEmbryo.init(new LcsRowScanExecStream(), scanParams);
    scanStreamEmbryo.getStream()->setName("RowScanExecStream");

    SharedExecStream pOutputStream =
        prepareTransformGraph(valuesStreamEmbryo, scanStreamEmbryo);

    verifyOutput(*pOutputStream, nRows, resultGenerator);
}

void LcsClusterReplaceExecStreamTest::replaceCluster(
    uint nCols,
    uint nRows,
    SharedMockProducerExecStreamGenerator pGenerator)
{
    MockProducerExecStreamParams mockParams;
    // +1 for the rid column
    for (uint i = 0; i < nCols + 1; i++) {
        mockParams.outputTupleDesc.push_back(attrDesc_int64);
    }
    mockParams.nRows = nRows;
    mockParams.pGenerator = pGenerator;

    ExecStreamEmbryo mockStreamEmbryo;
    mockStreamEmbryo.init(new MockProducerExecStream(), mockParams);
    mockStreamEmbryo.getStream()->setName("MockProducerExecStream");

    LcsClusterReplaceExecStreamParams lcsReplaceParams;
    initClusterAppendParams(lcsReplaceParams, nCols, true);

    btreeDescriptor.segmentAccessor.pSegment = lcsReplaceParams.pSegment;
    btreeDescriptor.segmentAccessor.pCacheAccessor = pCache;
    btreeDescriptor.tupleDescriptor = lcsReplaceParams.tupleDesc;
    btreeDescriptor.keyProjection = lcsReplaceParams.keyProj;

    lcsReplaceParams.rootPageId = btreeDescriptor.rootPageId = savedRootPageId;

    ExecStreamEmbryo lcsReplaceStreamEmbryo;
    lcsReplaceStreamEmbryo.init(
        new LcsClusterReplaceExecStream(),
        lcsReplaceParams);
    lcsReplaceStreamEmbryo.getStream()->setName("LcsClusterReplaceExecStream");

    SharedExecStream pOutputStream = prepareTransformGraph(
        mockStreamEmbryo, lcsReplaceStreamEmbryo);

    // set up a generator which can produce the expected output
    vector<boost::shared_ptr<ColumnGenerator<int64_t> > > columnGenerators;
    SharedInt64ColumnGenerator colGenerator =
        SharedInt64ColumnGenerator(new SeqColumnGenerator(nRows));
    columnGenerators.push_back(colGenerator);
    colGenerator = SharedInt64ColumnGenerator(new SeqColumnGenerator(0));
    columnGenerators.push_back(colGenerator);

    CompositeExecStreamGenerator expectedResultGenerator(columnGenerators);

    verifyOutput(*pOutputStream, 1, expectedResultGenerator);
}

void LcsClusterReplaceExecStreamTest::testCaseSetUp()
{
    ExecStreamUnitTestBase::testCaseSetUp();
    SnapshotSegmentTestBase::testCaseSetUp();

    attrDesc_int64 = TupleAttributeDescriptor(
        stdTypeFactory.newDataType(STANDARD_TYPE_INT_64));
    attrDesc_bitmap = TupleAttributeDescriptor(
        stdTypeFactory.newDataType(STANDARD_TYPE_CHAR),
        true, pRandomSegment->getUsablePageSize() / 8);

    savedRootPageId = NULL_PAGE_ID;
}

void LcsClusterReplaceExecStreamTest::openRandomSegment()
{
    // nothing to do since SnapshotSegmentTestBase::openSegmentStorage
    // has already set pRandomSegment to the snapshot segment.
}

void LcsClusterReplaceExecStreamTest::testCaseTearDown()
{
    btreeDescriptor.segmentAccessor.reset();
    ExecStreamUnitTestBase::testCaseTearDown();
}

FENNEL_UNIT_TEST_SUITE(LcsClusterReplaceExecStreamTest);

// End LcsClusterReplaceExecStreamTest.cpp
