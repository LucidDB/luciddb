/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005-2009 The Eigenbase Project
// Copyright (C) 2004-2009 SQLstream, Inc.
// Copyright (C) 2009-2009 LucidEra, Inc.
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
#include "fennel/test/CorrelationJoinExecStreamTestSuite.h"
#include "fennel/exec/CorrelationJoinExecStream.h"
#include "fennel/tuple/StandardTypeDescriptor.h"
#include "fennel/tuple/TupleOverflowExcn.h"
#include "fennel/exec/MockProducerExecStream.h"
#include "fennel/exec/ExecStreamEmbryo.h"
#include "fennel/exec/ExecStreamGraph.h"

using namespace fennel;

CorrelationJoinExecStreamTestSuite::CorrelationJoinExecStreamTestSuite(
    bool addAllTests)
{
    if (addAllTests) {
        FENNEL_UNIT_TEST_CASE(
            CorrelationJoinExecStreamTestSuite, testCorrelationJoin);
    }

    StandardTypeDescriptorFactory stdTypeFactory;

    descAttrInt64 = TupleAttributeDescriptor(
                  stdTypeFactory.newDataType(STANDARD_TYPE_INT_64));
    descInt64.push_back(descAttrInt64);
}

void CorrelationJoinExecStreamTestSuite::testCorrelationJoin()
{
    MockProducerExecStreamParams paramsMockLeft;
    paramsMockLeft.outputTupleDesc.push_back(descAttrInt64);
    paramsMockLeft.pGenerator.reset(new RampExecStreamGenerator);
    paramsMockLeft.nRows = 5000;

    ExecStreamEmbryo leftStreamEmbryo;
    leftStreamEmbryo.init(new MockProducerExecStream(),paramsMockLeft);
    leftStreamEmbryo.getStream()->setName("LeftProducerExecStream");

    DynamicParamId dynamicParamId(1);
    MockProducerExecStreamParams paramsMockRight(paramsMockLeft);
    paramsMockRight.pGenerator.reset(new DynamicParamExecStreamGenerator(
                                            dynamicParamId,
                                            pGraph->getDynamicParamManager()));
    paramsMockRight.nRows = 10;

    ExecStreamEmbryo rightStreamEmbryo;
    rightStreamEmbryo.init(new MockProducerExecStream(),paramsMockRight);
    rightStreamEmbryo.getStream()->setName("RightProducerExecStream");

    CorrelationJoinExecStreamParams paramsJoin;

    Correlation correlation(dynamicParamId, 0);
    paramsJoin.correlations.push_back(correlation);

    ExecStreamEmbryo joinStreamEmbryo;
    joinStreamEmbryo.init(new CorrelationJoinExecStream(),paramsJoin);
    joinStreamEmbryo.getStream()->setName("CorrelationJoinExecStream");

    SharedExecStream pOutputStream = prepareConfluenceGraph(
        leftStreamEmbryo,
        rightStreamEmbryo,
        joinStreamEmbryo);

    StairCaseExecStreamGenerator rampExpectedGenerator(
        1, paramsMockRight.nRows);
    verifyOutput(
        *pOutputStream,
        paramsMockLeft.nRows * paramsMockRight.nRows,
        rampExpectedGenerator);
}

// End CorrelationJoinExecStreamTestSuite.cpp
