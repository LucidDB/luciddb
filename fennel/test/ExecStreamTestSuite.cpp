/*
// $Id$
// Fennel is a relational database kernel.
// Copyright (C) 2004-2005 John V. Sichi.
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
#include "fennel/test/ExecStreamTestSuite.h"
#include "fennel/exec/ExecStreamScheduler.h"
#include "fennel/exec/ExecStream.h"
#include "fennel/exec/ExecStreamGraph.h"
#include "fennel/exec/ExecStreamBufAccessor.h"
#include "fennel/exec/MockProducerExecStream.h"
#include "fennel/exec/ScratchBufferExecStream.h"
#include "fennel/exec/CopyExecStream.h"
#include "fennel/exec/SegBufferExecStream.h"
#include "fennel/exec/CartesianJoinExecStream.h"
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

// End ExecStreamTest.cpp

