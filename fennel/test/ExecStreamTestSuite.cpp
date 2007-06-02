/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005-2007 The Eigenbase Project
// Copyright (C) 2005-2007 Disruptive Tech
// Copyright (C) 2005-2007 LucidEra, Inc.
// Portions Copyright (C) 2004-2007 John V. Sichi
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
#include "fennel/common/FemEnums.h"
#include "fennel/test/ExecStreamTestSuite.h"
#include "fennel/exec/ExecStreamScheduler.h"
#include "fennel/exec/ExecStream.h"
#include "fennel/exec/ExecStreamGraph.h"
#include "fennel/exec/ExecStreamBufAccessor.h"
#include "fennel/exec/MockProducerExecStream.h"
#include "fennel/exec/ScratchBufferExecStream.h"
#include "fennel/exec/CopyExecStream.h"
#include "fennel/exec/MergeExecStream.h"
#include "fennel/exec/SegBufferExecStream.h"
#include "fennel/exec/CartesianJoinExecStream.h"
#include "fennel/exec/SortedAggExecStream.h"
#include "fennel/exec/ReshapeExecStream.h"
#include "fennel/exec/SplitterExecStream.h"
#include "fennel/exec/ExecStreamEmbryo.h"
#include "fennel/tuple/StandardTypeDescriptor.h"

using namespace fennel;

void ExecStreamTestSuite::testScratchBufferExecStream()
{
    StandardTypeDescriptorFactory stdTypeFactory;
    TupleAttributeDescriptor attrDesc(
        stdTypeFactory.newDataType(STANDARD_TYPE_INT_64));

    MockProducerExecStreamParams mockParams;
    mockParams.outputTupleDesc.push_back(attrDesc);
    mockParams.nRows = 5000;     // at least two buffers
    mockParams.pGenerator.reset(new RampExecStreamGenerator());
    
    ExecStreamEmbryo mockStreamEmbryo;
    mockStreamEmbryo.init(new MockProducerExecStream(),mockParams);
    mockStreamEmbryo.getStream()->setName("MockProducerExecStream");
    
    ScratchBufferExecStreamParams bufParams;
    bufParams.scratchAccessor =
        pSegmentFactory->newScratchSegment(pCache,1);

    ExecStreamEmbryo bufStreamEmbryo;
    bufStreamEmbryo.init(new ScratchBufferExecStream(),bufParams);
    bufStreamEmbryo.getStream()->setName("ScratchBufferExecStream");

    SharedExecStream pOutputStream = prepareTransformGraph(
        mockStreamEmbryo, bufStreamEmbryo);

    verifyOutput(
        *pOutputStream,
        mockParams.nRows,
        *(mockParams.pGenerator));
}

void ExecStreamTestSuite::testCopyExecStream()
{
    StandardTypeDescriptorFactory stdTypeFactory;
    TupleAttributeDescriptor attrDesc(
        stdTypeFactory.newDataType(STANDARD_TYPE_INT_32));
    
    MockProducerExecStreamParams mockParams;
    mockParams.outputTupleDesc.push_back(attrDesc);
    mockParams.nRows = 10000;   // at least two buffers

    ExecStreamEmbryo mockStreamEmbryo;
    mockStreamEmbryo.init(new MockProducerExecStream(),mockParams);
    mockStreamEmbryo.getStream()->setName("MockProducerExecStream");

    CopyExecStreamParams copyParams;
    copyParams.outputTupleDesc.push_back(attrDesc);
    
    ExecStreamEmbryo copyStreamEmbryo;
    copyStreamEmbryo.init(new CopyExecStream(),copyParams);
    copyStreamEmbryo.getStream()->setName("CopyExecStream");
    
    SharedExecStream pOutputStream = prepareTransformGraph(
        mockStreamEmbryo,copyStreamEmbryo);

    int32_t zero = 0;
    TupleDescriptor expectedDesc;
    expectedDesc.push_back(attrDesc);
    TupleData expectedTuple;
    expectedTuple.compute(expectedDesc);
    expectedTuple[0].pData = reinterpret_cast<PBuffer>(&zero);
    verifyConstantOutput(
        *pOutputStream,
        expectedTuple,
        mockParams.nRows);
}

void ExecStreamTestSuite::testMergeExecStream()
{
    // simulate SELECT * FROM t10k UNION ALL SELECT * FROM 10k;
    
    StandardTypeDescriptorFactory stdTypeFactory;
    TupleAttributeDescriptor attrDesc(
        stdTypeFactory.newDataType(STANDARD_TYPE_INT_32));
    
    MockProducerExecStreamParams paramsMock;
    paramsMock.outputTupleDesc.push_back(attrDesc);
    paramsMock.nRows = 10000; // at least two buffers

    ExecStreamEmbryo mockStreamEmbryo1;
    mockStreamEmbryo1.init(new MockProducerExecStream(),paramsMock);
    mockStreamEmbryo1.getStream()->setName("MockProducerExecStream1");

    ExecStreamEmbryo mockStreamEmbryo2;
    mockStreamEmbryo2.init(new MockProducerExecStream(),paramsMock);
    mockStreamEmbryo2.getStream()->setName("MockProducerExecStream2");

    MergeExecStreamParams paramsMerge;
    paramsMerge.outputTupleDesc.push_back(attrDesc);

    ExecStreamEmbryo mergeStreamEmbryo;
    mergeStreamEmbryo.init(new MergeExecStream(),paramsMerge);
    mergeStreamEmbryo.getStream()->setName("MergeExecStream");
    
    SharedExecStream pOutputStream = prepareConfluenceGraph(
        mockStreamEmbryo1,
        mockStreamEmbryo2,
        mergeStreamEmbryo);

    int32_t zero = 0;
    TupleDescriptor expectedDesc;
    expectedDesc.push_back(attrDesc);
    TupleData expectedTuple;
    expectedTuple.compute(expectedDesc);
    expectedTuple[0].pData = reinterpret_cast<PBuffer>(&zero);
    verifyConstantOutput(
        *pOutputStream,
        expectedTuple,
        2*paramsMock.nRows);
}

void ExecStreamTestSuite::testSegBufferExecStream()
{
    StandardTypeDescriptorFactory stdTypeFactory;
    TupleAttributeDescriptor attrDesc(
        stdTypeFactory.newDataType(STANDARD_TYPE_INT_32));
    
    MockProducerExecStreamParams mockParams;
    mockParams.outputTupleDesc.push_back(attrDesc);
    mockParams.nRows = 10000;     // at least two buffers
    
    ExecStreamEmbryo mockStreamEmbryo;
    mockStreamEmbryo.init(new MockProducerExecStream(),mockParams);
    mockStreamEmbryo.getStream()->setName("MockProducerExecStream");
    
    SegBufferExecStreamParams bufParams;
    bufParams.scratchAccessor.pSegment = pRandomSegment;
    bufParams.scratchAccessor.pCacheAccessor = pCache;
    bufParams.multipass = false;

    ExecStreamEmbryo bufStreamEmbryo;
    bufStreamEmbryo.init(new SegBufferExecStream(),bufParams);
    bufStreamEmbryo.getStream()->setName("SegBufferExecStream");

    SharedExecStream pOutputStream = prepareTransformGraph(
        mockStreamEmbryo, bufStreamEmbryo);

    int32_t zero = 0;
    TupleDescriptor expectedDesc;
    expectedDesc.push_back(attrDesc);
    TupleData expectedTuple;
    expectedTuple.compute(expectedDesc);
    expectedTuple[0].pData = reinterpret_cast<PBuffer>(&zero);
    verifyConstantOutput(
        *pOutputStream,
        expectedTuple,
        mockParams.nRows);
}

void ExecStreamTestSuite::testCartesianJoinExecStream(
    uint nRowsOuter,uint nRowsInner)
{
    // simulate SELECT * FROM t1, t2
    
    StandardTypeDescriptorFactory stdTypeFactory;
    TupleAttributeDescriptor attrDesc(
        stdTypeFactory.newDataType(STANDARD_TYPE_INT_32));
    
    MockProducerExecStreamParams paramsMockOuter;
    paramsMockOuter.outputTupleDesc.push_back(attrDesc);
    paramsMockOuter.nRows = nRowsOuter;
    
    ExecStreamEmbryo outerStreamEmbryo;
    outerStreamEmbryo.init(new MockProducerExecStream(),paramsMockOuter);
    outerStreamEmbryo.getStream()->setName("OuterProducerExecStream");

    MockProducerExecStreamParams paramsMockInner(paramsMockOuter);
    paramsMockInner.nRows = nRowsInner;
    
    ExecStreamEmbryo innerStreamEmbryo;
    innerStreamEmbryo.init(new MockProducerExecStream(),paramsMockInner);
    innerStreamEmbryo.getStream()->setName("InnerProducerExecStream");

    CartesianJoinExecStreamParams paramsJoin;
    
    ExecStreamEmbryo joinStreamEmbryo;
    joinStreamEmbryo.init(new CartesianJoinExecStream(),paramsJoin);
    joinStreamEmbryo.getStream()->setName("CartesianJoinExecStream");
    
    SharedExecStream pOutputStream = prepareConfluenceGraph(
        outerStreamEmbryo,
        innerStreamEmbryo,
        joinStreamEmbryo);

    int32_t zero = 0;
    TupleDescriptor expectedDesc;
    expectedDesc.push_back(attrDesc);
    expectedDesc.push_back(attrDesc);
    TupleData expectedTuple;
    expectedTuple.compute(expectedDesc);
    expectedTuple[0].pData = reinterpret_cast<PBuffer>(&zero);
    expectedTuple[1].pData = reinterpret_cast<PBuffer>(&zero);
    verifyConstantOutput(
        *pOutputStream,
        expectedTuple,
        nRowsOuter*nRowsInner);
}

void ExecStreamTestSuite::testCountAggExecStream()
{
    StandardTypeDescriptorFactory stdTypeFactory;
    TupleAttributeDescriptor attrDesc(
        stdTypeFactory.newDataType(STANDARD_TYPE_INT_64));
    
    MockProducerExecStreamParams mockParams;
    mockParams.outputTupleDesc.push_back(attrDesc);
    mockParams.nRows = 10000;   // at least two buffers

    ExecStreamEmbryo mockStreamEmbryo;
    mockStreamEmbryo.init(new MockProducerExecStream(),mockParams);
    mockStreamEmbryo.getStream()->setName("MockProducerExecStream");

    // simulate SELECT COUNT(*) FROM t10k
    SortedAggExecStreamParams aggParams;
    aggParams.groupByKeyCount = 0;
    aggParams.outputTupleDesc.push_back(attrDesc);
    AggInvocation countInvocation;
    countInvocation.aggFunction = AGG_FUNC_COUNT;
    countInvocation.iInputAttr = -1; // interpreted as COUNT(*)
    aggParams.aggInvocations.push_back(countInvocation);
    
    ExecStreamEmbryo aggStreamEmbryo;
    aggStreamEmbryo.init(new SortedAggExecStream(),aggParams);
    aggStreamEmbryo.getStream()->setName("SortedAggExecStream");
    
    SharedExecStream pOutputStream = prepareTransformGraph(
        mockStreamEmbryo,aggStreamEmbryo);

    // set up a generator which can produce the expected output
    // (a count of 10000)
    RampExecStreamGenerator expectedResultGenerator(mockParams.nRows);

    verifyOutput(*pOutputStream, 1, expectedResultGenerator);
}

void ExecStreamTestSuite::testSumAggExecStream()
{
    StandardTypeDescriptorFactory stdTypeFactory;
    TupleAttributeDescriptor attrDesc(
        stdTypeFactory.newDataType(STANDARD_TYPE_INT_64));
    
    MockProducerExecStreamParams mockParams;
    mockParams.outputTupleDesc.push_back(attrDesc);
    mockParams.nRows = 10000;   // at least two buffers
    mockParams.pGenerator.reset(new RampExecStreamGenerator());

    ExecStreamEmbryo mockStreamEmbryo;
    mockStreamEmbryo.init(new MockProducerExecStream(),mockParams);
    mockStreamEmbryo.getStream()->setName("MockProducerExecStream");

    // simulate SELECT SUM(x) FROM t10k with x iterating from 0 to 9999
    SortedAggExecStreamParams aggParams;
    aggParams.groupByKeyCount = 0;
    attrDesc.isNullable = true;
    aggParams.outputTupleDesc.push_back(attrDesc);
    AggInvocation sumInvocation;
    sumInvocation.aggFunction = AGG_FUNC_SUM;
    sumInvocation.iInputAttr = 0;
    aggParams.aggInvocations.push_back(sumInvocation);
    
    ExecStreamEmbryo aggStreamEmbryo;
    aggStreamEmbryo.init(new SortedAggExecStream(),aggParams);
    aggStreamEmbryo.getStream()->setName("SortedAggExecStream");
    
    SharedExecStream pOutputStream = prepareTransformGraph(
        mockStreamEmbryo,aggStreamEmbryo);

    // set up a generator which can produce the expected output
    // (a count of 5000*9999)
    RampExecStreamGenerator expectedResultGenerator(
        (mockParams.nRows-1)*mockParams.nRows/2);

    verifyOutput(*pOutputStream, 1, expectedResultGenerator);
}

void ExecStreamTestSuite::testGroupAggExecStreamNrows(uint nrows)
{
    StandardTypeDescriptorFactory stdTypeFactory;
    TupleAttributeDescriptor attrDesc(
        stdTypeFactory.newDataType(STANDARD_TYPE_INT_64));
    
    // Create two columns, both with two duplicates per column.
    MockProducerExecStreamParams mockParams;
    mockParams.outputTupleDesc.push_back(attrDesc);
    mockParams.outputTupleDesc.push_back(attrDesc);
    mockParams.nRows = nrows;   // at least two buffers
    mockParams.pGenerator.reset(new RampDuplicateExecStreamGenerator());

    ExecStreamEmbryo mockStreamEmbryo;
    mockStreamEmbryo.init(new MockProducerExecStream(),mockParams);
    mockStreamEmbryo.getStream()->setName("MockProducerExecStream");

    // simulate SELECT col, COUNT(*) FROM t10k GROUP BY col;
    SortedAggExecStreamParams aggParams;
    aggParams.groupByKeyCount = 1;
    aggParams.outputTupleDesc.push_back(attrDesc);
    aggParams.outputTupleDesc.push_back(attrDesc);
    AggInvocation countInvocation;
    countInvocation.aggFunction = AGG_FUNC_COUNT;
    countInvocation.iInputAttr = -1; // interpreted as COUNT(*)
    aggParams.aggInvocations.push_back(countInvocation);
    
    ExecStreamEmbryo aggStreamEmbryo;
    
    aggStreamEmbryo.init(new SortedAggExecStream(),aggParams);
    aggStreamEmbryo.getStream()->setName("SortedAggExecStream");    

    SharedExecStream pOutputStream = prepareTransformGraph(
        mockStreamEmbryo,aggStreamEmbryo);

    // Result should be a sequence of values in the first column
    // and 2 for the second column
    vector<boost::shared_ptr<ColumnGenerator<int64_t> > > columnGenerators;
    
    SharedInt64ColumnGenerator col =
        SharedInt64ColumnGenerator(new SeqColumnGenerator());
    columnGenerators.push_back(col);
    
    col = SharedInt64ColumnGenerator(new ConstColumnGenerator(2));
    columnGenerators.push_back(col);
    
    CompositeExecStreamGenerator expectedResultGenerator(columnGenerators);

    verifyOutput(*pOutputStream, mockParams.nRows/2, expectedResultGenerator);
}

void ExecStreamTestSuite::testReshapeExecStream(
    bool filter, bool cast, uint expectedNRows, int expectedStart)
{
    StandardTypeDescriptorFactory stdTypeFactory;
    TupleAttributeDescriptor nullAttrDesc(
        stdTypeFactory.newDataType(STANDARD_TYPE_INT_64),
        true, sizeof(int64_t));
    TupleAttributeDescriptor notnullAttrDesc(
        stdTypeFactory.newDataType(STANDARD_TYPE_INT_64));

    // Input consists of 6 not nullable columns
    // - the first 4 columns will consist of sequential values, the 0th
    //   column starting at 0, the first at 1, 2nd at 2, 3rd at 3
    // - the 4th column will consist of sequential values starting at 0, each
    //   value repeating 25 times
    // - the 5th column will also consist of sequential values starting at 0,
    //   each value repeating 10 times
    MockProducerExecStreamParams mockParams;
    for (int i = 0; i < 6; i++) {
        mockParams.outputTupleDesc.push_back(notnullAttrDesc);
    }
    vector<boost::shared_ptr<ColumnGenerator<int64_t> > > columnGenerators;
    SharedInt64ColumnGenerator colGen;
    for (int i = 0; i < 4; i++) {
        colGen = SharedInt64ColumnGenerator(new SeqColumnGenerator(i));
        columnGenerators.push_back(colGen);
    }
    colGen = SharedInt64ColumnGenerator(new DupColumnGenerator(25, 0));
    columnGenerators.push_back(colGen);
    colGen = SharedInt64ColumnGenerator(new DupColumnGenerator(10, 0));
    columnGenerators.push_back(colGen);
    mockParams.nRows = 1000;
    mockParams.pGenerator.reset(
        new CompositeExecStreamGenerator(columnGenerators));

    ExecStreamEmbryo mockStreamEmbryo;
    mockStreamEmbryo.init(new MockProducerExecStream(),mockParams);
    mockStreamEmbryo.getStream()->setName("MockProducerExecStream");

    // Setup stream parameters as follows:
    // 1. If filtering is specified, filter columns 4 and 5 against values 20
    //    and 50
    // 2. Project columns 3, 0, and 2; if casting is specified, project them
    //    into nullable columns; else not nullable
    ReshapeExecStreamParams rsParams;
    boost::shared_array<FixedBuffer> pBuffer;
    if (!filter) {
        rsParams.compOp = COMP_NOOP;
    } else {
        rsParams.compOp = COMP_EQ;
        pBuffer.reset(new FixedBuffer[16]);
        int64_t key1 = 20;
        int64_t key2 = 50;
        TupleDescriptor compareDesc;
        compareDesc.push_back(notnullAttrDesc);
        compareDesc.push_back(notnullAttrDesc);
        TupleData compareData;
        compareData.compute(compareDesc);
        compareData[0].pData = (PConstBuffer) &key1;
        compareData[1].pData = (PConstBuffer) &key2;
        TupleAccessor tupleAccessor;
        tupleAccessor.compute(compareDesc);
        tupleAccessor.marshal(compareData, pBuffer.get());
    }
    rsParams.pCompTupleBuffer = pBuffer;

    TupleProjection tupleProj;
    tupleProj.push_back(4);
    tupleProj.push_back(5);
    rsParams.inputCompareProj = tupleProj;

    tupleProj.clear();
    tupleProj.push_back(3);
    tupleProj.push_back(0);
    tupleProj.push_back(2);
    rsParams.outputProj = tupleProj;

    for (int i = 0; i < 3; i++) {
        if (cast) {
            rsParams.outputTupleDesc.push_back(nullAttrDesc);
        } else {
            rsParams.outputTupleDesc.push_back(notnullAttrDesc);
        }
    }

    ExecStreamEmbryo rsStreamEmbryo;
    rsStreamEmbryo.init(new ReshapeExecStream(),rsParams);
    rsStreamEmbryo.getStream()->setName("ReshapeExecStream");    
    SharedExecStream pOutputStream = prepareTransformGraph(
        mockStreamEmbryo, rsStreamEmbryo);

    columnGenerators.clear();
    colGen = SharedInt64ColumnGenerator(
        new SeqColumnGenerator(expectedStart + 3));
    columnGenerators.push_back(colGen);
    colGen = SharedInt64ColumnGenerator(
        new SeqColumnGenerator(expectedStart));
    columnGenerators.push_back(colGen);
    colGen = SharedInt64ColumnGenerator(
        new SeqColumnGenerator(expectedStart + 2));
    columnGenerators.push_back(colGen);

    CompositeExecStreamGenerator resultGenerator(columnGenerators);
    verifyOutput(*pOutputStream, expectedNRows, resultGenerator);
}

void ExecStreamTestSuite::testSingleValueAggExecStream()
{
    StandardTypeDescriptorFactory stdTypeFactory;
    TupleAttributeDescriptor attrDesc(
        stdTypeFactory.newDataType(STANDARD_TYPE_INT_64));
    TupleAttributeDescriptor attrDescNullable(
        stdTypeFactory.newDataType(STANDARD_TYPE_INT_64), true,
        sizeof(int64_t));
    
    // Result should be a sequence of values in the first column
    // and 2 for the second column
    vector<boost::shared_ptr<ColumnGenerator<int64_t> > > columnGeneratorsIn;

    SharedInt64ColumnGenerator col =
        SharedInt64ColumnGenerator(new DupColumnGenerator(1));
    columnGeneratorsIn.push_back(col);

    // Create two columns, both with two duplicates per column.
    MockProducerExecStreamParams mockParams;
    mockParams.outputTupleDesc.push_back(attrDesc);
    mockParams.nRows = 10;
    mockParams.pGenerator.reset(
        new CompositeExecStreamGenerator(columnGeneratorsIn));

    ExecStreamEmbryo mockStreamEmbryo;
    mockStreamEmbryo.init(new MockProducerExecStream(), mockParams);
    mockStreamEmbryo.getStream()->setName("MockProducerExecStream");

    // simulate SELECT col, SINGLE_VALUE(col) FROM t10k GROUP BY col;
    SortedAggExecStreamParams aggParams;
    aggParams.groupByKeyCount = 1;
    aggParams.outputTupleDesc.push_back(attrDesc);
    aggParams.outputTupleDesc.push_back(attrDescNullable);
    AggInvocation singleValueInvocation;
    singleValueInvocation.aggFunction = AGG_FUNC_SINGLE_VALUE;
    singleValueInvocation.iInputAttr = 0;
    aggParams.aggInvocations.push_back(singleValueInvocation);
    
    ExecStreamEmbryo aggStreamEmbryo;
    
    aggStreamEmbryo.init(new SortedAggExecStream(),aggParams);
    aggStreamEmbryo.getStream()->setName("SortedAggExecStream");    

    SharedExecStream pOutputStream = prepareTransformGraph(
        mockStreamEmbryo,aggStreamEmbryo);

    // Result should be a sequence of values in both columns
    vector<boost::shared_ptr<ColumnGenerator<int64_t> > > columnGeneratorsOut;
    
    col =
        SharedInt64ColumnGenerator(new DupColumnGenerator(1));
    columnGeneratorsOut.push_back(col);
    
    col =
        SharedInt64ColumnGenerator(new DupColumnGenerator(1));
    columnGeneratorsOut.push_back(col);
    
    CompositeExecStreamGenerator expectedResultGenerator(columnGeneratorsOut);

    verifyOutput(*pOutputStream, mockParams.nRows, expectedResultGenerator);
}

void ExecStreamTestSuite::testMergeImplicitPullInputs()
{
    // This testcase exercises the case where production of rows from inputs
    // into the MergeExecStream needs to occur independent of explicit requests
    // made by the MergeExecStream.  The initial input is a single stream
    // that is split by a SplitterExecStream and then brought back together by
    // the MergeExecSteam in a different order from the original input.

    StandardTypeDescriptorFactory stdTypeFactory;
    TupleAttributeDescriptor attrDesc(
        stdTypeFactory.newDataType(STANDARD_TYPE_INT_64));

    // Initial input stream is a repeating sequence of
    // 0, 1, 2, ... nInputs - 1, 0, 1, 2, ..., nInputs - 1, 0, 1, 2, ...
    // Note that we're using a column generator here because there's already a
    // column generator class that produces the kind of input stream we need.
    
    MockProducerExecStreamParams mockParams;
    mockParams.outputTupleDesc.push_back(attrDesc);
    // needs to fill up at least 2 buffers per input into the merge
    uint nInputs = 5;
    uint nRows = nInputs * 4000;
    mockParams.nRows = nRows;
    vector<boost::shared_ptr<ColumnGenerator<int64_t> > > columnGenerator;
    columnGenerator.push_back(
        SharedInt64ColumnGenerator(
            new DupRepeatingSeqColumnGenerator(nInputs, 1)));
    mockParams.pGenerator.reset(
        new CompositeExecStreamGenerator(columnGenerator));

    ExecStreamEmbryo mockStreamEmbryo;
    mockStreamEmbryo.init(new MockProducerExecStream(), mockParams);
    mockStreamEmbryo.getStream()->setName("MockProducerExecStream");

    // split the mock data stream with each value being redirected to
    // one of the merge inputs, based on its value, by using reshape streams
    // to filter the input values

    SplitterExecStreamParams splitterParams;
    ExecStreamEmbryo splitterStreamEmbryo;
    splitterStreamEmbryo.init(new SplitterExecStream(), splitterParams);
    splitterStreamEmbryo.getStream()->setName("SplitterExecStream");

    vector<vector<ExecStreamEmbryo> > reshapeEmbryoStreamList;
    for (int i = 0; i < nInputs; i++) {
        ReshapeExecStreamParams rsParams;
        boost::shared_array<FixedBuffer> pBuffer;
        rsParams.compOp = COMP_EQ;
        pBuffer.reset(new FixedBuffer[8]);
        int64_t key = i;
        TupleDescriptor compareDesc;
        compareDesc.push_back(attrDesc);
        TupleData compareData;
        compareData.compute(compareDesc);
        compareData[0].pData = (PConstBuffer) &key;
        TupleAccessor tupleAccessor;
        tupleAccessor.compute(compareDesc);
        tupleAccessor.marshal(compareData, pBuffer.get());
        rsParams.pCompTupleBuffer = pBuffer;
        TupleProjection tupleProj;
        tupleProj.push_back(0);
        rsParams.inputCompareProj = tupleProj;
        rsParams.outputProj = tupleProj;

        ExecStreamEmbryo rsStreamEmbryo;
        rsStreamEmbryo.init(new ReshapeExecStream(), rsParams);
        std::ostringstream oss;
        oss << "ReshapeExecStream" << "#" << i;
        rsStreamEmbryo.getStream()->setName(oss.str());

        vector<ExecStreamEmbryo> reshapeStreamEmbryo;
        reshapeStreamEmbryo.push_back(rsStreamEmbryo);

        // since the splitter needs to pass through all rows but the merge
        // needs to return each input in the order of the inputs, we need to
        // buffer all but the first input to prevent the splitter from blocking
        if (i != 0) {
            SegBufferExecStreamParams bufParams;
            bufParams.scratchAccessor.pSegment = pRandomSegment;
            bufParams.scratchAccessor.pCacheAccessor = pCache;
            bufParams.multipass = false;

            ExecStreamEmbryo bufStreamEmbryo;
            bufStreamEmbryo.init(new SegBufferExecStream(), bufParams);
            std::ostringstream oss;
            oss << "SegBufferExecStream" << "#" << i;
            bufStreamEmbryo.getStream()->setName(oss.str());

            reshapeStreamEmbryo.push_back(bufStreamEmbryo);
        }
        reshapeEmbryoStreamList.push_back(reshapeStreamEmbryo);
    }

    // merge the inputs with each input being read in sequence
    MergeExecStreamParams mergeParams;
    mergeParams.outputTupleDesc.push_back(attrDesc);

    ExecStreamEmbryo mergeStreamEmbryo;
    mergeStreamEmbryo.init(new MergeExecStream(), mergeParams);
    mergeStreamEmbryo.getStream()->setName("MergeExecStream");

    SharedExecStream pOutputStream =
        prepareDAG(
            mockStreamEmbryo,
            splitterStreamEmbryo,
            reshapeEmbryoStreamList,
            mergeStreamEmbryo);

    // setup the generator for the expected result -- 0's, followed by 1's,
    // followed by 2's, etc.
    StairCaseExecStreamGenerator expectedResultGenerator(1, nRows/nInputs);

    verifyOutput(*pOutputStream, nRows, expectedResultGenerator);
}

// End ExecStreamTest.cpp
