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

CollectExecStreamTestSuite::CollectExecStreamTestSuite()
{
    FENNEL_UNIT_TEST_CASE(CollectExecStreamTestSuite,testCollectInts);
    FENNEL_UNIT_TEST_CASE(CollectExecStreamTestSuite,testCollectUncollect);

    StandardTypeDescriptorFactory stdTypeFactory;

    descAttrInt64 = TupleAttributeDescriptor(stdTypeFactory.newDataType(STANDARD_TYPE_INT_64));
    descInt64.push_back(descAttrInt64);
    
    descAttrVarbinary16 = TupleAttributeDescriptor(stdTypeFactory.newDataType(STANDARD_TYPE_VARBINARY),true,16);
    descVarbinary16.push_back(descAttrVarbinary16);
}

void CollectExecStreamTestSuite::testCollectInts()
{

    uint rows = 2;
    MockProducerExecStreamParams mockParams;
    mockParams.outputTupleDesc.push_back(descAttrInt64);
    mockParams.nRows = rows;
    mockParams.pGenerator.reset(new RampExecStreamGenerator(1));

    CollectExecStreamParams collectParams;
    collectParams.outputTupleDesc = descVarbinary16;

    ExecStreamEmbryo mockStreamEmbryo;
    mockStreamEmbryo.init(new MockProducerExecStream(), mockParams);
    mockStreamEmbryo.getStream()->setName("MockProducerExecStream");

    ExecStreamEmbryo collectStreamEmbryo;
    collectStreamEmbryo.init(new CollectExecStream(), collectParams);
    collectStreamEmbryo.getStream()->setName("CollectExecStream"); 


    // setup the expected result
    uint8_t intArrayBuff[16];
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
    TupleData binData(descVarbinary16);
    binData[0].pData = (PConstBuffer) intArrayBuff;
    TupleAccessor binAccessor;
    binAccessor.compute(descVarbinary16);
    binAccessor.marshal(binData, (PBuffer) varbinaryBuff);


    SharedExecStream pOutputStream = prepareTransformGraph(
        mockStreamEmbryo, collectStreamEmbryo);

    verifyConstantOutput(*pOutputStream, binData, 1);
}

void CollectExecStreamTestSuite::testCollectUncollect()
{
    StandardTypeDescriptorFactory stdTypeFactory;
    uint rows = 511;

    TupleAttributeDescriptor tupleDescAttr(stdTypeFactory.newDataType(STANDARD_TYPE_VARBINARY),true,rows*sizeof(uint64_t));
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
