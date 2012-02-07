/*
// Licensed to DynamoBI Corporation (DynamoBI) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  DynamoBI licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at

//   http://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
*/

#ifndef Fennel_DT_Test_CorrelationJoinExecStreamTestSuite_Included
#define Fennel_DT_Test_CorrelationJoinExecStreamTestSuite_Included

#include "fennel/common/CommonPreamble.h"
#include "fennel/test/ExecStreamUnitTestBase.h"
#include <boost/test/test_tools.hpp>
using namespace fennel;

class CorrelationJoinExecStreamTestSuite : public ExecStreamUnitTestBase
{
    TupleAttributeDescriptor descAttrInt64;
    TupleDescriptor descInt64;
    TupleAttributeDescriptor descAttrVarbinary16;
    TupleDescriptor descVarbinary16;

public:
    explicit CorrelationJoinExecStreamTestSuite(bool addAllTests = true);
    void testCorrelationJoin();
};

#endif

// End CorrelationJoinExecStreamTestSuite.h
