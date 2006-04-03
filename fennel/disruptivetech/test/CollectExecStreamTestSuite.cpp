/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2004-2005 Disruptive Tech
// Copyright (C) 2005-2005 The Eigenbase Project
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
#include "fennel/disruptivetech/test/CollectExecStreamTestSuite.h"
#include "fennel/disruptivetech/xo/CollectExecStream.h"
#include "fennel/disruptivetech/xo/UncollectExecStream.h"
#include "fennel/tuple/StandardTypeDescriptor.h"
#include "fennel/tuple/TupleOverflowExcn.h"
#include "fennel/exec/MockProducerExecStream.h"
#include "fennel/exec/ExecStreamEmbryo.h"

using namespace fennel;

CollectExecStreamTestSuite::CollectExecStreamTestSuite(bool addAllTests)
{
    if (addAllTests) {
        FENNEL_UNIT_TEST_CASE(CollectExecStreamTestSuite,testCollectInts);
        FENNEL_UNIT_TEST_CASE(CollectExecStreamTestSuite,testCollectUncollect);
        FENNEL_UNIT_TEST_CASE(CollectExecStreamTestSuite,testCollectCollectUncollectUncollect);
    }

    StandardTypeDescriptorFactory stdTypeFactory;

    descAttrInt64 = TupleAttributeDescriptor(stdTypeFactory.newDataType(STANDARD_TYPE_INT_64));
    descInt64.push_back(descAttrInt64);
    
    descAttrVarbinary32 = TupleAttributeDescriptor(stdTypeFactory.newDataType(STANDARD_TYPE_VARBINARY),true,32);
    descVarbinary32.push_back(descAttrVarbinary32);
}

void CollectExecStreamTestSuite::testCollectInts()
{

    uint rows = 2;
    MockProducerExecStreamParams mockParams;
    mockParams.outputTupleDesc.push_back(descAttrInt64);
    mockParams.nRows = rows;
    mockParams.pGenerator.reset(new RampExecStreamGenerator(1));

    CollectExecStreamParams collectParams;
    collectParams.outputTupleDesc = descVarbinary32;

    ExecStreamEmbryo mockStreamEmbryo;
    mockStreamEmbryo.init(new MockProducerExecStream(), mockParams);
    mockStreamEmbryo.getStream()->setName("MockProducerExecStream");

    ExecStreamEmbryo collectStreamEmbryo;
    collectStreamEmbryo.init(new CollectExecStream(), collectParams);
    collectStreamEmbryo.getStream()->setName("CollectExecStream"); 


    // setup the expected result
    uint8_t intArrayBuff[32];
    uint64_t one = 1;
    TupleData oneData(descInt64);
    oneData[0].pData = (PConstBuffer) &one;
    TupleAccessor oneAccessor;
    oneAccessor.compute(descInt64);
    assert(oneAccessor.getMaxByteCount() <= sizeof(intArrayBuff));
    oneAccessor.marshal(oneData, (PBuffer) intArrayBuff);

    uint64_t two = 2;
    TupleData twoData(descInt64);
    twoData[0].pData = (PConstBuffer) &two; 
    TupleAccessor twoAccessor;
    twoAccessor.compute(descInt64);
    assert((oneAccessor.getMaxByteCount() + twoAccessor.getMaxByteCount() ) <= 
           sizeof(intArrayBuff));
    twoAccessor.marshal(twoData, 
                        ((PBuffer)intArrayBuff)+oneAccessor.getMaxByteCount());

    uint8_t varbinaryBuff[1000];
    TupleData binData(descVarbinary32);
    binData[0].pData = (PConstBuffer) intArrayBuff;
    binData[0].cbData =
        oneAccessor.getMaxByteCount() + twoAccessor.getMaxByteCount();
    TupleAccessor binAccessor;
    binAccessor.compute(descVarbinary32);
    binAccessor.marshal(binData, (PBuffer) varbinaryBuff);


    SharedExecStream pOutputStream = prepareTransformGraph(
        mockStreamEmbryo, collectStreamEmbryo);

    verifyConstantOutput(*pOutputStream, binData, 1);
}

void CollectExecStreamTestSuite::testCollectUncollect()
{
    StandardTypeDescriptorFactory stdTypeFactory;
    uint rows = 127;

    TupleAttributeDescriptor tupleDescAttr(stdTypeFactory.newDataType(STANDARD_TYPE_VARBINARY),true,2*rows*sizeof(uint64_t));
    TupleDescriptor tupleDesc;
    tupleDesc.push_back(tupleDescAttr);

    MockProducerExecStreamParams mockParams;
    mockParams.outputTupleDesc.push_back(descAttrInt64);
    mockParams.nRows = rows;
    mockParams.pGenerator.reset(new RampExecStreamGenerator());

    CollectExecStreamParams collectParams;
    collectParams.outputTupleDesc = tupleDesc;

    UncollectExecStreamParams uncollectParams;
    uncollectParams.outputTupleDesc = descInt64;

    ExecStreamEmbryo mockStreamEmbryo;
    mockStreamEmbryo.init(new MockProducerExecStream(), mockParams);
    mockStreamEmbryo.getStream()->setName("MockProducerExecStream");

    ExecStreamEmbryo collectStreamEmbryo;
    collectStreamEmbryo.init(new CollectExecStream(), collectParams);
    collectStreamEmbryo.getStream()->setName("CollectExecStream"); 

    ExecStreamEmbryo uncollectStreamEmbryo;
    uncollectStreamEmbryo.init(new UncollectExecStream(), uncollectParams);
    uncollectStreamEmbryo.getStream()->setName("UncollectExecStream"); 


    std::vector<ExecStreamEmbryo> transforms;
    transforms.push_back(collectStreamEmbryo);
    transforms.push_back(uncollectStreamEmbryo);
    SharedExecStream pOutputStream = prepareTransformGraph(
         mockStreamEmbryo, transforms);

    RampExecStreamGenerator rampExpectedGenerator;

    verifyOutput(*pOutputStream, rows, rampExpectedGenerator);
}

void CollectExecStreamTestSuite::testCollectCollectUncollectUncollect() {
    StandardTypeDescriptorFactory stdTypeFactory;
    uint rows = 3;

    TupleAttributeDescriptor tupleDescAttr1(stdTypeFactory.newDataType(STANDARD_TYPE_VARBINARY),true,2*rows*sizeof(uint64_t));
    TupleDescriptor vbDesc1;
    vbDesc1.push_back(tupleDescAttr1);

    TupleAttributeDescriptor tupleDescAttr2(stdTypeFactory.newDataType(STANDARD_TYPE_VARBINARY),true,2*rows*rows*sizeof(uint64_t));
    TupleDescriptor vbDesc2;
    vbDesc2.push_back(tupleDescAttr2);

    MockProducerExecStreamParams mockParams;
    mockParams.outputTupleDesc.push_back(descAttrInt64);
    mockParams.nRows = rows;
    mockParams.pGenerator.reset(new RampExecStreamGenerator());

    CollectExecStreamParams collectParams1;
    collectParams1.outputTupleDesc = vbDesc1;

    CollectExecStreamParams collectParams2;
    collectParams2.outputTupleDesc = vbDesc2;

    UncollectExecStreamParams uncollectParams1;
    uncollectParams1.outputTupleDesc = vbDesc1;

    UncollectExecStreamParams uncollectParams2;
    uncollectParams2.outputTupleDesc = descInt64;

    ExecStreamEmbryo mockStreamEmbryo;
    mockStreamEmbryo.init(new MockProducerExecStream(), mockParams);
    mockStreamEmbryo.getStream()->setName("MockProducerExecStream");

    ExecStreamEmbryo collectStreamEmbryo1;
    collectStreamEmbryo1.init(new CollectExecStream(), collectParams1);
    collectStreamEmbryo1.getStream()->setName("CollectExecStream1"); 

    ExecStreamEmbryo collectStreamEmbryo2;
    collectStreamEmbryo2.init(new CollectExecStream(), collectParams2);
    collectStreamEmbryo2.getStream()->setName("CollectExecStream2"); 

    ExecStreamEmbryo uncollectStreamEmbryo1;
    uncollectStreamEmbryo1.init(new UncollectExecStream(), uncollectParams1);
    uncollectStreamEmbryo1.getStream()->setName("UncollectExecStream1"); 

    ExecStreamEmbryo uncollectStreamEmbryo2;
    uncollectStreamEmbryo2.init(new UncollectExecStream(), uncollectParams2);
    uncollectStreamEmbryo2.getStream()->setName("UncollectExecStream2"); 

    std::vector<ExecStreamEmbryo> transforms;
    transforms.push_back(collectStreamEmbryo1);
    transforms.push_back(collectStreamEmbryo2);
    transforms.push_back(uncollectStreamEmbryo1);
    transforms.push_back(uncollectStreamEmbryo2);
    SharedExecStream pOutputStream = prepareTransformGraph(
         mockStreamEmbryo, transforms);

    RampExecStreamGenerator rampExpectedGenerator;

    verifyOutput(*pOutputStream, rows, rampExpectedGenerator);
}
