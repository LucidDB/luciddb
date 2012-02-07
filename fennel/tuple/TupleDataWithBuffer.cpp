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
#include "fennel/tuple/TupleDataWithBuffer.h"

FENNEL_BEGIN_CPPFILE("$Id$");

TupleDataWithBuffer::TupleDataWithBuffer()
{
}

TupleDataWithBuffer::TupleDataWithBuffer(TupleDescriptor const& tupleDesc)
{
    computeAndAllocate(tupleDesc);
}

void TupleDataWithBuffer::computeAndAllocate(TupleDescriptor const& tupleDesc)
{
    tupleAccessor.compute(tupleDesc, TUPLE_FORMAT_ALL_FIXED);
    array.reset(new FixedBuffer[tupleAccessor.getMaxByteCount()]);
    tupleAccessor.setCurrentTupleBuf(array.get(), false);
    compute(tupleDesc);
    tupleAccessor.unmarshal(*this);
}

void TupleDataWithBuffer::resetBuffer()
{
    // reset data pointers to the associated buffer.
    tupleAccessor.unmarshal(*this);
}

TupleDataWithBuffer::~TupleDataWithBuffer()
{
}

FENNEL_END_CPPFILE("$Id$");

// End TupleDataWithBuffer.cpp
