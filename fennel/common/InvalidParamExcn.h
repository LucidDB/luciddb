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

#ifndef Fennel_InvalidParamExcn_Included
#define Fennel_InvalidParamExcn_Included

#include "fennel/common/FennelExcn.h"

using namespace std;

FENNEL_BEGIN_NAMESPACE

/**
 * Exception class for invalid parameter settings
 */
class FENNEL_COMMON_EXPORT InvalidParamExcn : public FennelExcn
{
public:
    /**
     * Constructs a new InvalidParamExcn.
     *
     * @param min minimum valid value
     *
     * @param max maximum valid value
     */
    explicit InvalidParamExcn(string min, string max);
};

FENNEL_END_NAMESPACE

#endif

// End InvalidParamExcn.h
