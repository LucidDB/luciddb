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
#include "fennel/btree/BTreeDescriptor.h"
#include "fennel/btree/BTreeBuilder.h"
#include "fennel/btree/BTreeReader.h"
#include "fennel/test/ExecStreamTestSuite.h"
#include "fennel/exec/ExecStreamScheduler.h"
#include "fennel/exec/ExecStream.h"
#include "fennel/exec/ExecStreamGraph.h"
#include "fennel/exec/ExecStreamBufAccessor.h"
#include "fennel/exec/MockProducerExecStream.h"
#include "fennel/exec/ScratchBufferExecStream.h"
#include "fennel/exec/DoubleBufferExecStream.h"
#include "fennel/exec/CopyExecStream.h"
#include "fennel/exec/MergeExecStream.h"
#include "fennel/exec/SegBufferExecStream.h"
#include "fennel/exec/CartesianJoinExecStream.h"
#include "fennel/exec/SortedAggExecStream.h"
#include "fennel/exec/ReshapeExecStream.h"
#include "fennel/exec/SplitterExecStream.h"
#include "fennel/exec/BarrierExecStream.h"
#include "fennel/exec/ValuesExecStream.h"
#include "fennel/exec/NestedLoopJoinExecStream.h"
#include "fennel/exec/ExecStreamEmbryo.h"
#include "fennel/tuple/StandardTypeDescriptor.h"
#include "fennel/ftrs/BTreeInsertExecStream.h"

using namespace fennel;

uint ExecStreamTestSuite::getDegreeOfParallelism()
{
    return 1;
}

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


void ExecStreamTestSuite::testDoubleBufferExecStream()
{
    StandardTypeDescriptorFactory stdTypeFactory;
    TupleAttributeDescriptor attrDesc(
        stdTypeFactory.newDataType(STANDARD_TYPE_INT_64));

    MockProducerExecStreamParams mockParams;
    mockParams.outputTupleDesc.push_back(attrDesc);
    mockParams.nRows = 25000;     // cycle through a few buffers
    mockParams.pGenerator.reset(new RampExecStreamGenerator());
    
    ExecStreamEmbryo mockStreamEmbryo;
    mockStreamEmbryo.init(new MockProducerExecStream(),mockParams);
    mockStreamEmbryo.getStream()->setName("MockProducerExecStream");
    
    DoubleBufferExecStreamParams bufParams;
    bufParams.scratchAccessor =
        pSegmentFactory->newScratchSegment(pCache,1);

    ExecStreamEmbryo bufStreamEmbryo;
    bufStreamEmbryo.init(new DoubleBufferExecStream(),bufParams);
    bufStreamEmbryo.getStream()->setName("DoubleBufferExecStream");

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
    if (getDegreeOfParallelism() != 1) {
        paramsMerge.isParallel = true;
    }

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
    bufParams.scratchAccessor.pCacheAccessor = pCacheAccessor;
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
    paramsJoin.leftOuter = false;
    
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
    bool filter, bool cast, uint expectedNRows, int expectedStart,
    bool compareParam,
    std::hash_set<int64_t> const &outputParams)
{
    assert(!compareParam || filter == compareParam);
    StandardTypeDescriptorFactory stdTypeFactory;
    TupleAttributeDescriptor nullAttrDesc(
        stdTypeFactory.newDataType(STANDARD_TYPE_INT_64),
        true, sizeof(int64_t));
    TupleAttributeDescriptor notNullAttrDesc(
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
        mockParams.outputTupleDesc.push_back(notNullAttrDesc);
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
    //    and 50.  If filtering on dynamic parameters, filter column 5 against
    //    the dynamic parameter value 50.
    // 2. Project columns 3, 0, and 2; if casting is specified, project them
    //    into nullable columns; else not nullable
    // 3. If outputting dynamic parameters, append them to the end of the
    //    output tuple
    ReshapeExecStreamParams rsParams;
    boost::shared_array<FixedBuffer> pBuffer;
    std::vector<int64_t> paramVals;
    paramVals.push_back(10);
    paramVals.push_back(20);
    paramVals.push_back(50);
    if (!filter) {
        rsParams.compOp = COMP_NOOP;
    } else {
        rsParams.compOp = COMP_EQ;
        TupleDescriptor compareDesc;
        // comparison type needs to be nullable to allow filtering of nulls
        compareDesc.push_back(nullAttrDesc);
        if (!compareParam) {
            compareDesc.push_back(nullAttrDesc);
        }
        TupleData compareData;
        compareData.compute(compareDesc);
        if (compareParam) {
            compareData[0].pData = (PConstBuffer) &paramVals[1];
        } else {
            compareData[0].pData = (PConstBuffer) &paramVals[1];
            compareData[1].pData = (PConstBuffer) &paramVals[2];
        }
        TupleAccessor tupleAccessor;
        tupleAccessor.compute(compareDesc);
        pBuffer.reset(new FixedBuffer[tupleAccessor.getMaxByteCount()]);
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
            rsParams.outputTupleDesc.push_back(notNullAttrDesc);
        }
    }

    // Setup the dynamic parameters, marking the ones that will be
    // included in the output stream.  If comparing against a dynamic
    // parameter, compare the third dynamic parameter against column 5.
    std::vector<ReshapeParameter> dynamicParams;
    if (compareParam || outputParams.size() > 0) {
        for (uint i = 1; i < paramVals.size() + 1; i++) {
            SharedDynamicParamManager pDynamicParamManager =
            pGraph->getDynamicParamManager();
            pDynamicParamManager->createParam(
                DynamicParamId(i), 
                notNullAttrDesc);
            TupleDatum paramValDatum;
            paramValDatum.pData = (PConstBuffer) &(paramVals[i - 1]);
            paramValDatum.cbData = sizeof(int64_t);
            pDynamicParamManager->writeParam(
                DynamicParamId(i),
                paramValDatum);
            dynamicParams.push_back(
                ReshapeParameter(
                    DynamicParamId(i),
                    ((i == 3) && compareParam) ? uint(5) : MAXU,
                    (outputParams.find(i - 1) != outputParams.end())));
        }
    }
    rsParams.dynamicParameters = dynamicParams;

    // Setup the expected result
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
    for (uint i = 0; i < dynamicParams.size(); i++) {
        if (dynamicParams[i].outputParam) {
            colGen =
                SharedInt64ColumnGenerator(
                    new ConstColumnGenerator(
                        paramVals[opaqueToInt(
                            dynamicParams[i].dynamicParamId) - 1]));
            columnGenerators.push_back(colGen);
            rsParams.outputTupleDesc.push_back(notNullAttrDesc);
        }
    }

    ExecStreamEmbryo rsStreamEmbryo;
    rsStreamEmbryo.init(new ReshapeExecStream(),rsParams);
    rsStreamEmbryo.getStream()->setName("ReshapeExecStream");    
    SharedExecStream pOutputStream = prepareTransformGraph(
        mockStreamEmbryo, rsStreamEmbryo);

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
    TupleAttributeDescriptor nullAttrDesc(
        stdTypeFactory.newDataType(STANDARD_TYPE_INT_64),
        true, sizeof(int64_t));

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
        int64_t key = i;
        TupleDescriptor compareDesc;
        // comparison type needs to be nullable to allow filtering of nulls
        compareDesc.push_back(nullAttrDesc);
        TupleData compareData;
        compareData.compute(compareDesc);
        compareData[0].pData = (PConstBuffer) &key;
        TupleAccessor tupleAccessor;
        tupleAccessor.compute(compareDesc);
        pBuffer.reset(new FixedBuffer[tupleAccessor.getMaxByteCount()]);
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
            bufParams.scratchAccessor.pCacheAccessor = pCacheAccessor;
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

void ExecStreamTestSuite::testBTreeInsertExecStream(
    bool useDynamicBTree,
    uint nRows)
{
    StandardTypeDescriptorFactory stdTypeFactory;
    TupleAttributeDescriptor attrDesc(
        stdTypeFactory.newDataType(STANDARD_TYPE_INT_64));

    // Use a MockProducer to generate the btree records with a single key
    // and single value for each record.  The key sequences from
    // 0 ... nRows-1 while the value sequences from nRows to nRows*2-1
    MockProducerExecStreamParams mockParams;
    mockParams.nRows = nRows;
    mockParams.outputTupleDesc.push_back(attrDesc);
    mockParams.outputTupleDesc.push_back(attrDesc);
    vector<boost::shared_ptr<ColumnGenerator<int64_t> > > columnGenerators;
    columnGenerators.push_back(
        SharedInt64ColumnGenerator(new SeqColumnGenerator(0)));
    columnGenerators.push_back(
        SharedInt64ColumnGenerator(new SeqColumnGenerator(nRows)));
    mockParams.pGenerator.reset(
        new CompositeExecStreamGenerator(columnGenerators));

    ExecStreamEmbryo mockStreamEmbryo;
    mockStreamEmbryo.init(new MockProducerExecStream(), mockParams);
    mockStreamEmbryo.getStream()->setName("MockProducerExecStream");

    // Setup the BTreeInsert stream
    BTreeDescriptor descriptor;
    BTreeInsertExecStreamParams bTreeInsertParams;
    descriptor.tupleDescriptor.push_back(attrDesc);
    descriptor.tupleDescriptor.push_back(attrDesc);
    descriptor.keyProjection.push_back(0);
    descriptor.segmentAccessor.pSegment = pRandomSegment;
    descriptor.segmentAccessor.pCacheAccessor = pCacheAccessor;
    BTreeBuilder builder(descriptor, pRandomSegment);
    if (!useDynamicBTree) {
        builder.createEmptyRoot();
        descriptor.rootPageId = builder.getRootPageId();
        bTreeInsertParams.rootPageIdParamId = DynamicParamId(0);
    } else {
        descriptor.rootPageId = NULL_PAGE_ID;
        bTreeInsertParams.rootPageIdParamId = DynamicParamId(1);
    }

    bTreeInsertParams.scratchAccessor =
        pSegmentFactory->newScratchSegment(pCache, 10);
    bTreeInsertParams.pCacheAccessor = pCacheAccessor;
    bTreeInsertParams.distinctness = DUP_FAIL;
    bTreeInsertParams.monotonic = true;
    bTreeInsertParams.pSegment = pRandomSegment;
    bTreeInsertParams.pCacheAccessor = pCacheAccessor;
    bTreeInsertParams.rootPageId = descriptor.rootPageId;
    bTreeInsertParams.segmentId = descriptor.segmentId;
    bTreeInsertParams.pageOwnerId = descriptor.pageOwnerId;
    bTreeInsertParams.tupleDesc = descriptor.tupleDescriptor;
    bTreeInsertParams.keyProj = descriptor.keyProjection;
    bTreeInsertParams.pRootMap = 0;
    bTreeInsertParams.outputTupleDesc.push_back(attrDesc);

    ExecStreamEmbryo bTreeInsertEmbryo;
    bTreeInsertEmbryo.init(new BTreeInsertExecStream(), bTreeInsertParams);
    bTreeInsertEmbryo.getStream()->setName("BTreeInsertExecStream");

    SharedExecStream pOutputStream =
        prepareTransformGraph(mockStreamEmbryo, bTreeInsertEmbryo);

    ConstExecStreamGenerator expectedResultGenerator(0);
    verifyOutput(*pOutputStream, 0, expectedResultGenerator);

    // Get the rootPageId created by the stream so we can read from the tree
    if (useDynamicBTree) {
        descriptor.rootPageId =
            *reinterpret_cast<PageId const *>(
                pGraph->getDynamicParamManager()->getParam(
                    DynamicParamId(1)).getDatum().pData);
    }

    // Now that we've loaded the btree, verify the contents by directly
    // reading from the btree using a BTreeReader
    BTreeReader reader(descriptor);
    bool found = reader.searchFirst();
    if (!found) {
        BOOST_FAIL("searchFirst found nothing");
    }
    TupleData tupleData;
    tupleData.compute(descriptor.tupleDescriptor);
    for (uint i = 0; i < nRows; i++) {
        if (!found) {
            BOOST_FAIL("Could not searchNext for key #" << i);
        }
        reader.getTupleAccessorForRead().unmarshal(tupleData);
        uint64_t key = *reinterpret_cast<uint64_t const *>(tupleData[0].pData);
        uint64_t val = *reinterpret_cast<uint64_t const *>(tupleData[1].pData);
        BOOST_CHECK_EQUAL(key, i);
        BOOST_CHECK_EQUAL(val, i + nRows);
        found = reader.searchNext();
    }
    if (!reader.isSingular()) {
        BOOST_FAIL("Should have reached end of tree");
    }
    reader.endSearch();
}

void ExecStreamTestSuite::testNestedLoopJoinExecStream(
    uint nRowsLeft,
    uint nRowsRight)
{
    // simulate SELECT t1.a, t2.a FROM t1, t2 WHERE t1.a = t2.a
    
    StandardTypeDescriptorFactory stdTypeFactory;
    TupleAttributeDescriptor attrDesc(
        stdTypeFactory.newDataType(STANDARD_TYPE_INT_64));
    
    // 1st input is a mock producer that generates values 0 ... nRowsLeft - 1
    MockProducerExecStreamParams paramsMockOuter;
    paramsMockOuter.outputTupleDesc.push_back(attrDesc);
    paramsMockOuter.nRows = nRowsLeft;
    paramsMockOuter.pGenerator.reset(new RampExecStreamGenerator(0, 1));
    
    ExecStreamEmbryo outerStreamEmbryo;
    outerStreamEmbryo.init(new MockProducerExecStream(), paramsMockOuter);
    outerStreamEmbryo.getStream()->setName("OuterProducerExecStream");

    // 2nd input consists of a mock producer that generates values
    // 0 ... nRowsRight - 1, but those values are then filtered by a
    // ReshapeExecStream that only outputs rows matching the current left hand
    // side row.  The current left hand side row is passed to the right via
    // a dynamic parameter
    MockProducerExecStreamParams paramsMockInner;
    paramsMockInner.outputTupleDesc.push_back(attrDesc);
    paramsMockInner.nRows = nRowsRight;
    paramsMockInner.pGenerator.reset(new RampExecStreamGenerator(0, 1));

    ExecStreamEmbryo innerStreamEmbryo;
    innerStreamEmbryo.init(new MockProducerExecStream(), paramsMockInner);
    innerStreamEmbryo.getStream()->setName("InnerProducerExecStream");

    ReshapeExecStreamParams paramsReshape;
    paramsReshape.compOp = COMP_EQ;
    paramsReshape.outputProj.push_back(0);
    paramsReshape.inputCompareProj.push_back(0);
    paramsReshape.dynamicParameters.push_back(
        ReshapeParameter(DynamicParamId(1), 0, false));
    paramsReshape.outputTupleDesc.push_back(attrDesc);

    ExecStreamEmbryo reshapeStreamEmbryo;
    reshapeStreamEmbryo.init(new ReshapeExecStream(), paramsReshape);
    reshapeStreamEmbryo.getStream()->setName("ReshapeExecStream");

    // For the 3rd input, just create a an empty values stream.  This doesn't
    // do anything, but is just there to make sure the stream handles reading
    // from it.
    ValuesExecStreamParams paramsValues;
    paramsValues.bufSize = 0;
    paramsValues.outputTupleDesc.push_back(attrDesc);
    ExecStreamEmbryo valuesStreamEmbryo;
    valuesStreamEmbryo.init(new ValuesExecStream(), paramsValues);
    valuesStreamEmbryo.getStream()->setName("ValuesExecStream");

    // String together the inputs
    std::vector<std::vector<ExecStreamEmbryo> > sourceStreamEmbryosList;
    std::vector<ExecStreamEmbryo> sourceStreamEmbryos;
    sourceStreamEmbryos.push_back(outerStreamEmbryo);
    sourceStreamEmbryosList.push_back(sourceStreamEmbryos);

    sourceStreamEmbryos.clear();
    sourceStreamEmbryos.push_back(innerStreamEmbryo);
    sourceStreamEmbryos.push_back(reshapeStreamEmbryo);
    sourceStreamEmbryosList.push_back(sourceStreamEmbryos);

    sourceStreamEmbryos.clear();
    sourceStreamEmbryos.push_back(valuesStreamEmbryo);
    sourceStreamEmbryosList.push_back(sourceStreamEmbryos);

    NestedLoopJoinExecStreamParams paramsJoin;
    paramsJoin.leftOuter = false;
    paramsJoin.leftJoinKeys.push_back(
        NestedLoopJoinKey(DynamicParamId(1), 0));
    
    ExecStreamEmbryo joinStreamEmbryo;
    joinStreamEmbryo.init(new NestedLoopJoinExecStream(), paramsJoin);
    joinStreamEmbryo.getStream()->setName("NestedLoopJoinExecStream");
    
    SharedExecStream pOutputStream =
        prepareConfluenceGraph(sourceStreamEmbryosList, joinStreamEmbryo);

    vector<boost::shared_ptr<ColumnGenerator<int64_t> > > columnGenerators;
    SharedInt64ColumnGenerator colGen =
        SharedInt64ColumnGenerator(new SeqColumnGenerator(0));
    columnGenerators.push_back(colGen);
    colGen = SharedInt64ColumnGenerator(new SeqColumnGenerator(0));
    columnGenerators.push_back(colGen);

    CompositeExecStreamGenerator resultGenerator(columnGenerators);
    verifyOutput(
        *pOutputStream, std::min(nRowsLeft, nRowsRight), resultGenerator);
}

void ExecStreamTestSuite::testSplitterPlusBarrier()
{
    StandardTypeDescriptorFactory stdTypeFactory;
    TupleAttributeDescriptor attrDesc(
        stdTypeFactory.newDataType(STANDARD_TYPE_INT_64));
    MockProducerExecStreamParams mockParams;
    mockParams.outputTupleDesc.push_back(attrDesc);
    uint nRows = 10000;
    mockParams.nRows = nRows;
    ExecStreamEmbryo mockStreamEmbryo;
    mockStreamEmbryo.init(new MockProducerExecStream(), mockParams);
    mockStreamEmbryo.getStream()->setName("MockProducerExecStream");
    
    SplitterExecStreamParams splitterParams;
    ExecStreamEmbryo splitterStreamEmbryo;
    splitterStreamEmbryo.init(new SplitterExecStream(), splitterParams);
    splitterStreamEmbryo.getStream()->setName("SplitterExecStream");

    vector<vector<ExecStreamEmbryo> > aggEmbryoStreamList;
    for (int i = 0; i < 10; i++) {
        SortedAggExecStreamParams aggParams;
        aggParams.groupByKeyCount = 0;
        aggParams.outputTupleDesc.push_back(attrDesc);
        AggInvocation countInvocation;
        countInvocation.aggFunction = AGG_FUNC_COUNT;
        countInvocation.iInputAttr = -1;
        aggParams.aggInvocations.push_back(countInvocation);
    
        ExecStreamEmbryo aggStreamEmbryo;
        aggStreamEmbryo.init(new SortedAggExecStream(),aggParams);
        std::ostringstream oss;
        oss << "AggExecStream" << "#" << i;
        aggStreamEmbryo.getStream()->setName(oss.str());
        vector<ExecStreamEmbryo> v;
        v.push_back(aggStreamEmbryo);
        aggEmbryoStreamList.push_back(v);
    }
    
    BarrierExecStreamParams barrierParams;
    barrierParams.outputTupleDesc.push_back(attrDesc);
    barrierParams.returnMode = BARRIER_RET_ANY_INPUT;
    ExecStreamEmbryo barrierStreamEmbryo;
    barrierStreamEmbryo.init(new BarrierExecStream(), barrierParams);
    barrierStreamEmbryo.getStream()->setName("BarrierExecStream");

    SharedExecStream pOutputStream =
        prepareDAG(
            mockStreamEmbryo,
            splitterStreamEmbryo,
            aggEmbryoStreamList,
            barrierStreamEmbryo);

    ConstExecStreamGenerator expectedResultGenerator(nRows);
    verifyOutput(*pOutputStream, 1, expectedResultGenerator);
}

// End ExecStreamTest.cpp
