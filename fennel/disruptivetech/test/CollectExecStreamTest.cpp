/*
// $Id$
// Fennel is a relational database kernel.
// Copyright (C) 2004-2004 Disruptive Tech
// Copyright (C) 2004-2004 John V. Sichi.
//
// This program is free software; you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation; either version 2 of the License, or
// (at your option) any later version.
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
#include "fennel/test/ExecStreamTestBase.h"
#include "fennel/disruptivetech/xo/CollectExecStream.h"
#include "fennel/tuple/StandardTypeDescriptor.h"
#include "fennel/tuple/TupleOverflowExcn.h"
#include "fennel/exec/MockProducerExecStream.h"
#include "fennel/exec/ExecStreamEmbryo.h"

#include <boost/test/test_tools.hpp>

using namespace fennel;

class CollectExecStreamTest : public ExecStreamTestBase
{
    // TupleAttributeDescriptor descUint8;
    //    TupleAttributeDescriptor descUint64;
    //    TupleAttributeDescriptor descVarbinary500;
    
    
public:
    
    explicit CollectExecStreamTest(); 

    
    /**
     * Tests an stream input ints gets collected into an continues array
     */
    void testCollectInts();
    
    /**
     * Tests with program that produces a tuple which overflows output buffer.
     */
    //    void testTupleOverflow();
};

CollectExecStreamTest::CollectExecStreamTest()
{
    FENNEL_UNIT_TEST_CASE(CollectExecStreamTest,testCollectInts);
}

void CollectExecStreamTest::testCollectInts()
{
        StandardTypeDescriptorFactory stdTypeFactory;
        //    TupleAttributeDescriptor descUint8(
        //         stdTypeFactory.newDataType(STANDARD_TYPE_UINT_8));
    TupleAttributeDescriptor descAttrUint64(
         stdTypeFactory.newDataType(STANDARD_TYPE_INT_64));
    TupleDescriptor descUint64;
    descUint64.push_back(descAttrUint64);
    
    TupleAttributeDescriptor descAttrVarbinary500(
         stdTypeFactory.newDataType(STANDARD_TYPE_VARBINARY),true,16);
    TupleDescriptor descVarbinary500;
    descVarbinary500.push_back(descAttrVarbinary500);

    uint rows = 2;
    MockProducerExecStreamParams mockParams;
    mockParams.outputTupleDesc.push_back(descAttrUint64);
    mockParams.nRows = rows;
    mockParams.pGenerator.reset(new RampExecStreamGenerator(1));

    CollectExecStreamParams collectParams;
    collectParams.outputTupleDesc = descVarbinary500;

    ExecStreamEmbryo mockStreamEmbryo;
    mockStreamEmbryo.init(new MockProducerExecStream(), mockParams);
    mockStreamEmbryo.getStream()->setName("MockProducerExecStream");

    ExecStreamEmbryo collectStreamEmbryo;
    collectStreamEmbryo.init(new CollectExecStream(), collectParams);
    collectStreamEmbryo.getStream()->setName("CollectExecStream"); 


    // setup the expected result
    uint8_t intArrayBuff[500];
    uint64_t one = 1;
    TupleData oneData(descUint64);
    oneData[0].pData = (PConstBuffer) &one;
    TupleAccessor oneAccessor;
    oneAccessor.compute(descUint64);
    assert(oneAccessor.getMaxByteCount() <= sizeof(intArrayBuff));
    oneAccessor.marshal(oneData, (PBuffer) intArrayBuff);

    uint64_t two = 2;
    TupleData twoData(descUint64);
    twoData[0].pData = (PConstBuffer) &two; 
    TupleAccessor twoAccessor;
    twoAccessor.compute(descUint64);
    assert((oneAccessor.getMaxByteCount() + twoAccessor.getMaxByteCount() ) <= 
           sizeof(intArrayBuff));
    twoAccessor.marshal(twoData, 
                        ((PBuffer)intArrayBuff)+oneAccessor.getMaxByteCount());

    uint8_t varbinaryBuff[1000];
    TupleData binData(descVarbinary500);
    binData[0].pData = (PConstBuffer) intArrayBuff;
    TupleAccessor binAccessor;
    binAccessor.compute(descVarbinary500);
    binAccessor.marshal(binData, (PBuffer) varbinaryBuff);


    SharedExecStream pOutputStream = prepareTransformGraph(
        mockStreamEmbryo, collectStreamEmbryo);

    verifyConstantOutput(*pOutputStream, binData, 1);
}

FENNEL_UNIT_TEST_SUITE(CollectExecStreamTest);

// End CollectExecStreamTest.cpp
