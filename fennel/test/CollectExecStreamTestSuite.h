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

#ifndef Fennel_DT_Test_CollectExecStreamTestSuite_Included
#define Fennel_DT_Test_CollectExecStreamTestSuite_Included

#include "fennel/test/ExecStreamUnitTestBase.h"
#include <boost/test/test_tools.hpp>
using namespace fennel;

/**
 * Test Suite for the collect/uncollect xo's
 * @author Wael Chatila
 */
class CollectExecStreamTestSuite : public ExecStreamUnitTestBase
{
    TupleAttributeDescriptor descAttrInt64;
    TupleDescriptor descInt64;
    TupleAttributeDescriptor descAttrVarbinary32;
    TupleDescriptor descVarbinary32;

public:
    explicit CollectExecStreamTestSuite(bool addAllTests = true);

    /**
     * Tests an stream input ints gets collected into an continues array
     */
    void testCollectInts();

    /**
     * Tests an stream going through a cascade of the collect and
     * the uncollect xos, expecting the same result back
     */
    void testCollectUncollect();

    /**
     * Tests an stream going through a cascade of two collect and
     * two uncollect xos, expecting the same result back
     */
    void testCollectCollectUncollectUncollect();


};

#endif

// End CollectExecStreamTestSuite.h
