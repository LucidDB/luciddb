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

#ifndef Fennel_ScratchMemExcn_Included
#define Fennel_ScratchMemExcn_Included

#include "fennel/common/FennelExcn.h"
#include "fennel/common/FennelResource.h"

using namespace std;

FENNEL_BEGIN_NAMESPACE

/**
 * Exception class for scratch memory allocation errors
 */
class FENNEL_SEGMENT_EXPORT ScratchMemExcn
    : public FennelExcn
{
public:
    /**
     * Constructs a new ScratchMemExcn.
     */
    explicit ScratchMemExcn()
        : FennelExcn(FennelResource::instance().scratchMemExhausted())
    {
    }
};

FENNEL_END_NAMESPACE

#endif

// End ScratchMemExcn.h
