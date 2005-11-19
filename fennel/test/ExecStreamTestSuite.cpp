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
#include "fennel/exec/ExecStreamEmbryo.h"
#include "fennel/tuple/StandardTypeDescriptor.h"

using namespace fennel;

void ExecStreamTestSuite::verifyZeroedOutput(
    ExecStream &stream,uint nBytesExpected)
{
    verifyConstantOutput(stream,nBytesExpected,0);
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

    verifyZeroedOutput(
        *pOutputStream,
        mockParams.nRows*sizeof(int32_t));
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

    verifyZeroedOutput(
        *pOutputStream,
        2*paramsMock.nRows*sizeof(int32_t));
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
    bufParams.scratchAccessor.pSegment = pLinearSegment;
    bufParams.scratchAccessor.pCacheAccessor = pCache;
    bufParams.multipass = false;

    ExecStreamEmbryo bufStreamEmbryo;
    bufStreamEmbryo.init(new SegBufferExecStream(),bufParams);
    bufStreamEmbryo.getStream()->setName("SegBufferExecStream");

    SharedExecStream pOutputStream = prepareTransformGraph(
        mockStreamEmbryo, bufStreamEmbryo);

    verifyZeroedOutput(
        *pOutputStream,
        mockParams.nRows*sizeof(int32_t));
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

    verifyZeroedOutput(
        *pOutputStream,
        nRowsOuter*nRowsInner*2*sizeof(int32_t));
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


// End ExecStreamTest.cpp
