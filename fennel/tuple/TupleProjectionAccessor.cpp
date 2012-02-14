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

#include "fennel/common/CommonPreamble.h"
#include "fennel/tuple/TupleAccessor.h"
#include "fennel/tuple/TupleProjectionAccessor.h"
#include "fennel/tuple/TupleDescriptor.h"
#include "fennel/tuple/TupleData.h"
#include "fennel/tuple/AttributeAccessor.h"

FENNEL_BEGIN_CPPFILE("$Id$");
TupleProjectionAccessor::TupleProjectionAccessor()
{
    pTupleAccessor = NULL;
}

TupleProjectionAccessor::~TupleProjectionAccessor()
{
}

void TupleProjectionAccessor::bind(
    TupleAccessor const &tupleAccessor,
    TupleProjection const &tupleProjection)
{
    pTupleAccessor = &tupleAccessor;
    ppAttributeAccessors.clear();
    for (uint i = 0; i < tupleProjection.size(); ++i) {
        ppAttributeAccessors.push_back(
            &(tupleAccessor.getAttributeAccessor(tupleProjection[i])));
    }
}

void TupleProjectionAccessor::unmarshal(TupleData::iterator tupleIter) const
{
    for (uint i = 0; i < ppAttributeAccessors.size(); ++i, ++tupleIter) {
        ppAttributeAccessors[i]->unmarshalValue(
            *pTupleAccessor,
            *tupleIter);
    }
}

FENNEL_END_CPPFILE("$Id$");

// End TupleProjectionAccessor.cpp
